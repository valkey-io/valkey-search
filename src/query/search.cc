/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/search.h"

#include <cstddef>
#include <deque>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "src/attribute_data_type.h"
#include "src/indexes/index_base.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/indexes/text.h"
#include "src/indexes/text/orproximity.h"
#include "src/indexes/text/proximity.h"
#include "src/indexes/text/text_fetcher.h"
#include "src/indexes/vector_base.h"
#include "src/indexes/vector_flat.h"
#include "src/indexes/vector_hnsw.h"
#include "src/metrics.h"
#include "src/query/planner.h"
#include "src/query/predicate.h"
#include "src/valkey_search.h"
#include "src/valkey_search_options.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/latency_sampler.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/time_sliced_mrmw_mutex.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query {

class InlineVectorFilter : public hnswlib::BaseFilterFunctor {
 public:
  InlineVectorFilter(
      query::Predicate *filter_predicate, indexes::VectorBase *vector_index,
      const InternedStringNodeHashMap<valkey_search::indexes::text::TextIndex>
          *per_key_indexes)
      : filter_predicate_(filter_predicate),
        vector_index_(vector_index),
        per_key_indexes_(per_key_indexes) {}
  ~InlineVectorFilter() override = default;

  bool operator()(hnswlib::labeltype id) override {
    auto key = vector_index_->GetKeyDuringSearch(id);
    if (!key.ok()) {
      return false;
    }
    const valkey_search::indexes::text::TextIndex *text_index = nullptr;
    if (per_key_indexes_) {
      text_index =
          valkey_search::indexes::text::TextIndexSchema::LookupTextIndex(
              *per_key_indexes_, *key);
    }
    indexes::PrefilterEvaluator evaluator(text_index);
    return evaluator.Evaluate(*filter_predicate_, *key);
  }

 private:
  query::Predicate *filter_predicate_;
  indexes::VectorBase *vector_index_;
  const InternedStringNodeHashMap<valkey_search::indexes::text::TextIndex>
      *per_key_indexes_;
};
absl::StatusOr<std::vector<indexes::Neighbor>> PerformVectorSearch(
    indexes::VectorBase *vector_index, const SearchParameters &parameters) {
  std::unique_ptr<InlineVectorFilter> inline_filter;
  if (parameters.filter_parse_results.root_predicate != nullptr) {
    const InternedStringNodeHashMap<valkey_search::indexes::text::TextIndex>
        *per_key_indexes = nullptr;
    if (parameters.index_schema->GetTextIndexSchema()) {
      per_key_indexes = &parameters.index_schema->GetTextIndexSchema()
                             ->GetPerKeyTextIndexes();
    }
    inline_filter = std::make_unique<InlineVectorFilter>(
        parameters.filter_parse_results.root_predicate.get(), vector_index,
        per_key_indexes);
    VMSDK_LOG(DEBUG, nullptr) << "Performing vector search with inline filter";
  }
  if (vector_index->GetIndexerType() == indexes::IndexerType::kHNSW) {
    auto vector_hnsw = dynamic_cast<indexes::VectorHNSW<float> *>(vector_index);

    auto latency_sample = SAMPLE_EVERY_N(100);
    auto res = vector_hnsw->Search(parameters.query, parameters.k,
                                   parameters.cancellation_token,
                                   std::move(inline_filter), parameters.ef,
                                   parameters.enable_partial_results);
    Metrics::GetStats().hnsw_vector_index_search_latency.SubmitSample(
        std::move(latency_sample));
    return res;
  }
  if (vector_index->GetIndexerType() == indexes::IndexerType::kFlat) {
    auto vector_flat = dynamic_cast<indexes::VectorFlat<float> *>(vector_index);
    auto latency_sample = SAMPLE_EVERY_N(100);
    auto res = vector_flat->Search(parameters.query, parameters.k,
                                   parameters.cancellation_token,
                                   std::move(inline_filter));
    Metrics::GetStats().flat_vector_index_search_latency.SubmitSample(
        std::move(latency_sample));
    return res;
  }
  CHECK(false) << "Unsupported indexer type: "
               << (int)vector_index->GetIndexerType();
}

void AppendQueue(
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> &dest,
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> &src) {
  while (!src.empty()) {
    dest.push(std::move(src.front()));
    src.pop();
  }
}

inline PredicateType EvaluateAsComposedPredicate(
    const Predicate *composed_predicate, bool negate) {
  auto predicate_type = composed_predicate->GetType();

  if (!negate) {
    return predicate_type;
  }
  if (predicate_type == PredicateType::kComposedAnd) {
    return PredicateType::kComposedOr;
  }
  return PredicateType::kComposedAnd;
}

// Helper fn to identify if query is not fully solved after the entries fetcher
// search, meaning it requires prefilter evaluation Prefiltering is needed when
// query contains an AND with numeric or tag predicates.
inline bool IsUnsolvedQuery(QueryOperations query_operations) {
  return query_operations & (QueryOperations::kContainsNumeric |
                             QueryOperations::kContainsTag) &&
         query_operations & QueryOperations::kContainsAnd;
}

// Helper fn to identify if deduplication is needed.
// (1) OR operations need deduplication.
// (2) Any TAG operations need deduplication.
inline bool NeedsDeduplication(QueryOperations query_operations) {
  bool has_or = query_operations & QueryOperations::kContainsOr;
  bool has_tag = query_operations & QueryOperations::kContainsTag;
  return has_or || has_tag;
}

// Builds TextIterator for text predicates. Returns pair of iterator and
// estimated size.
std::pair<std::unique_ptr<indexes::text::TextIterator>, size_t>
BuildTextIterator(const Predicate *predicate, bool negate,
                  bool require_positions) {
  if (predicate->GetType() == PredicateType::kComposedAnd ||
      predicate->GetType() == PredicateType::kComposedOr) {
    auto composed_predicate =
        dynamic_cast<const ComposedPredicate *>(predicate);
    auto predicate_type =
        EvaluateAsComposedPredicate(composed_predicate, negate);
    auto slop = composed_predicate->GetSlop();
    bool inorder = composed_predicate->GetInorder();
    bool child_require_positions = slop.has_value() || inorder;
    if (predicate_type == PredicateType::kComposedAnd) {
      absl::InlinedVector<std::unique_ptr<indexes::text::TextIterator>,
                          indexes::text::kProximityTermsInlineCapacity>
          iterators;
      size_t min_size = SIZE_MAX;
      for (const auto &child : composed_predicate->GetChildren()) {
        auto [iter, size] =
            BuildTextIterator(child.get(), negate, child_require_positions);
        if (iter) {
          iterators.push_back(std::move(iter));
          min_size = std::min(min_size, size);
        }
      }
      // The Composed AND only has non text predicates, return null
      // to have the caller handle it.
      if (iterators.empty()) return {nullptr, 0};
      bool skip_positional = !child_require_positions;
      size_t total_size = min_size == SIZE_MAX ? 0 : min_size;
      return {
          std::make_unique<indexes::text::ProximityIterator>(
              std::move(iterators), slop, inorder, nullptr, skip_positional),
          total_size};
    } else {
      absl::InlinedVector<std::unique_ptr<indexes::text::TextIterator>,
                          indexes::text::kProximityTermsInlineCapacity>
          iterators;
      size_t total_size = 0;
      bool has_non_text = false;
      for (const auto &child : composed_predicate->GetChildren()) {
        auto [iter, size] =
            BuildTextIterator(child.get(), negate, child_require_positions);
        if (iter) {
          iterators.push_back(std::move(iter));
          total_size += size;
        } else {
          has_non_text = true;
        }
      }
      // If the Composed OR has any non text predicate, we cannot
      // build a text iterator.
      if (iterators.empty() || has_non_text) return {nullptr, 0};
      return {std::make_unique<indexes::text::OrProximityIterator>(
                  std::move(iterators), nullptr),
              total_size};
    }
  }
  if (predicate->GetType() == PredicateType::kText) {
    auto text_predicate = dynamic_cast<const TextPredicate *>(predicate);
    auto fetcher_ptr = text_predicate->Search(negate);
    auto fetcher = static_cast<indexes::Text::EntriesFetcher *>(fetcher_ptr);
    fetcher->require_positions_ = require_positions;
    size_t size = fetcher->Size();
    return {text_predicate->BuildTextIterator(fetcher), size};
  }
  if (predicate->GetType() == PredicateType::kNegate) {
    auto negate_predicate = dynamic_cast<const NegatePredicate *>(predicate);
    return BuildTextIterator(negate_predicate->GetPredicate(), !negate,
                             require_positions);
  }
  // Numeric/Tag
  return {nullptr, 0};
}

size_t EvaluateFilterAsPrimary(
    const Predicate *predicate,
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> &entries_fetchers,
    bool negate, QueryOperations query_operations) {
  if (predicate->GetType() == PredicateType::kComposedAnd ||
      predicate->GetType() == PredicateType::kComposedOr) {
    auto composed_predicate =
        dynamic_cast<const ComposedPredicate *>(predicate);
    auto predicate_type =
        EvaluateAsComposedPredicate(composed_predicate, negate);
    if (predicate_type == PredicateType::kComposedAnd) {
      auto [text_iter, size] =
          BuildTextIterator(composed_predicate, negate, false);
      if (text_iter) {
        entries_fetchers.push(
            std::make_unique<indexes::text::TextIteratorFetcher>(
                std::move(text_iter), size));
        return size;
      }
      size_t min_size = SIZE_MAX;
      std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> best_fetchers;
      for (const auto &child : composed_predicate->GetChildren()) {
        std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> child_fetchers;
        size_t child_size = EvaluateFilterAsPrimary(child.get(), child_fetchers,
                                                    negate, query_operations);
        if (child_size < min_size) {
          min_size = child_size;
          best_fetchers = std::move(child_fetchers);
        }
      }
      AppendQueue(entries_fetchers, best_fetchers);
      return min_size;
    } else {
      size_t total_size = 0;
      for (const auto &child : composed_predicate->GetChildren()) {
        std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> child_fetchers;
        size_t child_size = EvaluateFilterAsPrimary(child.get(), child_fetchers,
                                                    negate, query_operations);
        AppendQueue(entries_fetchers, child_fetchers);
        total_size += child_size;
      }
      return total_size;
    }
  }
  if (predicate->GetType() == PredicateType::kTag) {
    auto tag_predicate = dynamic_cast<const TagPredicate *>(predicate);
    auto fetcher = tag_predicate->GetIndex()->Search(*tag_predicate, negate);
    size_t size = fetcher->Size();
    entries_fetchers.push(std::move(fetcher));
    return size;
  }
  if (predicate->GetType() == PredicateType::kNumeric) {
    auto numeric_predicate = dynamic_cast<const NumericPredicate *>(predicate);
    auto fetcher =
        numeric_predicate->GetIndex()->Search(*numeric_predicate, negate);
    size_t size = fetcher->Size();
    entries_fetchers.push(std::move(fetcher));
    return size;
  }
  if (predicate->GetType() == PredicateType::kText) {
    auto text_predicate = dynamic_cast<const TextPredicate *>(predicate);
    auto fetcher = std::unique_ptr<indexes::EntriesFetcherBase>(
        static_cast<indexes::EntriesFetcherBase *>(
            text_predicate->Search(negate)));
    size_t size = fetcher->Size();
    entries_fetchers.push(std::move(fetcher));
    return size;
  }
  if (predicate->GetType() == PredicateType::kNegate) {
    auto negate_predicate = dynamic_cast<const NegatePredicate *>(predicate);
    size_t result =
        EvaluateFilterAsPrimary(negate_predicate->GetPredicate(),
                                entries_fetchers, !negate, query_operations);
    return result;
  }
  CHECK(false);
}

struct PrefilteredKey {
  std::string key;
  float distance;
};

void EvaluatePrefilteredKeys(
    const SearchParameters &parameters,
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> &entries_fetchers,
    absl::AnyInvocable<bool(const InternedStringPtr &,
                            absl::flat_hash_set<const char *> &)>
        appender,
    size_t max_keys) {
  // If there was a union operation, we need to handle deduplication.
  // This implementation skips deduplication (flat_hash_set usage) if not needed
  // for performance.
  bool needs_dedup =
      NeedsDeduplication(parameters.filter_parse_results.query_operations);
  absl::flat_hash_set<const char *> result_keys;
  if (needs_dedup) {
    result_keys.reserve(max_keys);
  }
  // Get per-key text indexes directly since we have reader lock
  const InternedStringNodeHashMap<valkey_search::indexes::text::TextIndex>
      *per_key_indexes = nullptr;
  if (parameters.index_schema &&
      parameters.index_schema->GetTextIndexSchema()) {
    per_key_indexes =
        &parameters.index_schema->GetTextIndexSchema()->GetPerKeyTextIndexes();
  }
  while (!entries_fetchers.empty()) {
    auto fetcher = std::move(entries_fetchers.front());
    entries_fetchers.pop();
    auto iterator = fetcher->Begin();
    while (!iterator->Done()) {
      const auto &key = **iterator;
      // 1. Skip if already processed (only if dedup is needed)
      if (needs_dedup && result_keys.contains(key->Str().data())) {
        iterator->Next();
        continue;
      }
      const valkey_search::indexes::text::TextIndex *text_index = nullptr;
      if (per_key_indexes) {
        text_index =
            valkey_search::indexes::text::TextIndexSchema::LookupTextIndex(
                *per_key_indexes, key);
      }
      indexes::PrefilterEvaluator key_evaluator(text_index);
      // 3. Evaluate predicate
      if (key_evaluator.Evaluate(
              *parameters.filter_parse_results.root_predicate, key)) {
        if (needs_dedup) {
          result_keys.insert(key->Str().data());
        }
        appender(key, result_keys);
      }
      iterator->Next();
      if (parameters.cancellation_token->IsCancelled()) {
        return;
      }
    }
  }
}

std::priority_queue<std::pair<float, hnswlib::labeltype>>
CalcBestMatchingPrefilteredKeys(
    const SearchParameters &parameters,
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> &entries_fetchers,
    indexes::VectorBase *vector_index, size_t qualified_entries) {
  std::priority_queue<std::pair<float, hnswlib::labeltype>> results;
  auto results_appender =
      [&results, &parameters, vector_index](
          const InternedStringPtr &key,
          absl::flat_hash_set<const char *> &top_keys) -> bool {
    return vector_index->AddPrefilteredKey(parameters.query, parameters.k, key,
                                           results, top_keys);
  };
  EvaluatePrefilteredKeys(parameters, entries_fetchers,
                          std::move(results_appender), qualified_entries);
  return results;
}

std::string StringFormatVector(std::vector<char> vector) {
  if (vector.size() % sizeof(float) != 0) {
    return {vector.data(), vector.size()};
  }

  std::vector<std::string> float_strings;
  for (size_t i = 0; i < vector.size(); i += sizeof(float)) {
    float value;
    std::memcpy(&value, vector.data() + i, sizeof(float));
    float_strings.push_back(absl::StrCat(value));
  }

  return absl::StrCat("[", absl::StrJoin(float_strings, ","), "]");
}

absl::StatusOr<std::vector<indexes::Neighbor>> MaybeAddIndexedContent(
    absl::StatusOr<std::vector<indexes::Neighbor>> results,
    const SearchParameters &parameters) {
  if (!results.ok()) {
    return results;
  }
  if (parameters.no_content || parameters.return_attributes.empty()) {
    return results;
  }
  struct AttributeInfo {
    const ReturnAttribute *attribute;
    indexes::IndexBase *index;
  };
  std::vector<AttributeInfo> attributes;
  for (auto &attribute : parameters.return_attributes) {
    if (!attribute.attribute_alias.get()) {
      // Any attribute that is not indexed will result in all attributes being
      // fetched from the main thread for consistency.
      return results;
    }
    auto index = parameters.index_schema->GetIndex(
        vmsdk::ToStringView(attribute.attribute_alias.get()));
    if (!index.ok()) {
      return results;
    }
    attributes.push_back(AttributeInfo{&attribute, index.value().get()});
  }
  for (auto &neighbor : *results) {
    if (neighbor.attribute_contents.has_value()) {
      continue;
    }
    neighbor.attribute_contents = RecordsMap();
    bool any_value_missing = false;
    for (auto &attribute_info : attributes) {
      vmsdk::UniqueValkeyString attribute_value = nullptr;
      switch (attribute_info.index->GetIndexerType()) {
        case indexes::IndexerType::kTag: {
          auto tag_index = dynamic_cast<indexes::Tag *>(attribute_info.index);
          auto tag_value_ptr = tag_index->GetRawValue(neighbor.external_id);
          if (tag_value_ptr) {
            attribute_value = vmsdk::MakeUniqueValkeyString(*tag_value_ptr);
          }
          break;
        }
        case indexes::IndexerType::kNumeric: {
          auto numeric_index =
              dynamic_cast<indexes::Numeric *>(attribute_info.index);
          auto numeric = numeric_index->GetValue(neighbor.external_id);
          if (numeric != nullptr) {
            attribute_value =
                vmsdk::MakeUniqueValkeyString(absl::StrCat(*numeric));
          }
          break;
        }
        case indexes::IndexerType::kVector:
        case indexes::IndexerType::kHNSW:
        case indexes::IndexerType::kFlat: {
          auto vector_index =
              dynamic_cast<indexes::VectorBase *>(attribute_info.index);
          auto vector = vector_index->GetValue(neighbor.external_id);
          if (vector.ok()) {
            if (parameters.index_schema->GetAttributeDataType().ToProto() ==
                data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON) {
              attribute_value = vmsdk::MakeUniqueValkeyString(
                  StringFormatVector(vector.value()));
            } else {
              attribute_value =
                  vmsdk::UniqueValkeyString(ValkeyModule_CreateString(
                      nullptr, vector->data(), vector->size()));
            }
          } else {
            VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
                << "Failed to get vector value during fetching through index "
                   "contents: "
                << vector.status();
          }
          break;
        }
        case indexes::IndexerType::kText: {
          // Text indexes don't store retrievable raw values
          any_value_missing = true;
          break;
        }
        default:
          CHECK(false) << "Unsupported indexer type: "
                       << (int)attribute_info.index->GetIndexerType();
      }

      if (attribute_value != nullptr) {
        auto identifier = vmsdk::MakeUniqueValkeyString(
            vmsdk::ToStringView(attribute_info.attribute->identifier.get()));
        auto identifier_view = vmsdk::ToStringView(identifier.get());
        neighbor.attribute_contents->emplace(
            identifier_view,
            RecordsMapValue(std::move(identifier), std::move(attribute_value)));
      } else {
        // Mark this neighbor as needing content retrieval via the main thread
        // (e.g. the attribute value may exist but not be indexed due to type
        // mismatch).
        any_value_missing = true;
        break;
      }
    }
    if (any_value_missing) {
      neighbor.attribute_contents = std::nullopt;
    }
  }
  return results;
}

absl::StatusOr<std::vector<indexes::Neighbor>> SearchNonVectorQuery(
    const SearchParameters &parameters) {
  std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> entries_fetchers;
  size_t qualified_entries = EvaluateFilterAsPrimary(
      parameters.filter_parse_results.root_predicate.get(), entries_fetchers,
      false, parameters.filter_parse_results.query_operations);
  std::vector<indexes::Neighbor> neighbors;
  // TODO: For now, we just reserve a fixed size because text search operators
  // return a size of 0 currently.
  neighbors.reserve(5000);
  auto results_appender =
      [&neighbors, &parameters](
          const InternedStringPtr &key,
          absl::flat_hash_set<const char *> &top_keys) -> bool {
    neighbors.emplace_back(indexes::Neighbor{key, 0.0f});
    return true;
  };
  // Cannot skip evaluation if the query contains unsolved composed operations.
  bool requires_prefilter_evaluation =
      IsUnsolvedQuery(parameters.filter_parse_results.query_operations);
  if (!requires_prefilter_evaluation) {
    bool needs_dedup =
        NeedsDeduplication(parameters.filter_parse_results.query_operations);
    absl::flat_hash_set<const char *> seen_keys;
    if (needs_dedup) {
      // TODO: Use the qualified_entries size when text indexes return correct
      // size.
      seen_keys.reserve(5000);
    }
    while (!entries_fetchers.empty()) {
      auto fetcher = std::move(entries_fetchers.front());
      entries_fetchers.pop();
      auto iterator = fetcher->Begin();
      while (!iterator->Done()) {
        const auto &key = **iterator;
        if (needs_dedup) {
          if (seen_keys.contains(key->Str().data())) {
            iterator->Next();
            continue;
          }
          seen_keys.insert(key->Str().data());
        }
        neighbors.emplace_back(indexes::Neighbor{key, 0.0f});
        iterator->Next();
        if (parameters.cancellation_token->IsCancelled()) {
          return neighbors;
        }
      }
    }
    return neighbors;
  }
  EvaluatePrefilteredKeys(parameters, entries_fetchers,
                          std::move(results_appender), qualified_entries);
  return neighbors;
}

absl::StatusOr<std::vector<indexes::Neighbor>> DoSearch(
    const SearchParameters &parameters, SearchMode search_mode) {
  auto &time_sliced_mutex = parameters.index_schema->GetTimeSlicedMutex();
  vmsdk::ReaderMutexLock lock(&time_sliced_mutex);
  ++Metrics::GetStats().time_slice_queries;
  // Handle non vector queries first where attribute_alias is empty.
  if (parameters.IsNonVectorQuery()) {
    return SearchNonVectorQuery(parameters);
  }
  VMSDK_ASSIGN_OR_RETURN(auto index, parameters.index_schema->GetIndex(
                                         parameters.attribute_alias));
  auto vector_index = dynamic_cast<indexes::VectorBase *>(index.get());
  if (index->GetIndexerType() != indexes::IndexerType::kHNSW &&
      index->GetIndexerType() != indexes::IndexerType::kFlat) {
    return absl::InvalidArgumentError(
        absl::StrCat(parameters.attribute_alias, " is not a Vector index "));
  }

  if (!parameters.filter_parse_results.root_predicate) {
    return PerformVectorSearch(vector_index, parameters);
  }
  std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> entries_fetchers;
  size_t qualified_entries = EvaluateFilterAsPrimary(
      parameters.filter_parse_results.root_predicate.get(), entries_fetchers,
      false, parameters.filter_parse_results.query_operations);

  // Query planner makes the decision for pre-filtering vs inline-filtering.
  if (UsePreFiltering(qualified_entries, vector_index)) {
    VMSDK_LOG(DEBUG, nullptr)
        << "Using pre-filter query execution, qualified entries="
        << qualified_entries;
    // Do an exact nearest neighbour search on the reduced search space.
    ++Metrics::GetStats().query_prefiltering_requests_cnt;
    std::priority_queue<std::pair<float, hnswlib::labeltype>> results =
        CalcBestMatchingPrefilteredKeys(parameters, entries_fetchers,
                                        vector_index, qualified_entries);

    return vector_index->CreateReply(results);
  }
  ++Metrics::GetStats().query_inline_filtering_requests_cnt;
  lock.SetMayProlong();
  return PerformVectorSearch(vector_index, parameters);
}

// Check if no results should be returned based on query parameters.
// This handles two cases:
// 1. Any query with limit number == 0
// 2. Vector queries with limit first_index >= k
bool ShouldReturnNoResults(const SearchParameters &parameters) {
  return (parameters.IsVectorQuery() &&
          parameters.limit.first_index >=
              static_cast<uint64_t>(parameters.k)) ||
         parameters.limit.number == 0;
}

SearchResult::SearchResult(size_t total_count,
                           std::vector<indexes::Neighbor> neighbors,
                           const SearchParameters &parameters)
    : total_count(total_count),
      is_limited_with_buffer(false),
      is_offsetted(false) {
  // Clear neighbors if no results should be returned
  if (ShouldReturnNoResults(parameters)) {
    this->neighbors.clear();
    return;
  }
  this->neighbors = std::move(neighbors);
  // Check if the command needs all results (e.g. for sorting). Trim otherwise.
  if (!parameters.RequiresCompleteResults()) {
    TrimResults(this->neighbors, parameters);
  }
}

// Apply limiting in background thread if possible.
void SearchResult::TrimResults(std::vector<indexes::Neighbor> &neighbors,
                               const SearchParameters &parameters) {
  // Calculate max_needed for consistent vector/non-vector handling
  SerializationRange range = GetSerializationRange(parameters);
  size_t max_needed = static_cast<size_t>(
      range.end_index * options::GetSearchResultBufferMultiplier());
  // In standalone mode, we can optimize by trimming from front first.
  // Note: We cannot trim from the front in a Cluster Mode setting because
  // each shard produces X results and we need to trim the OFFSET on the
  // aggregated results. Thus, we can only trim from the end in searches for
  // individual nodes. In cluster mode, the offset based trimming is applied
  // after merging all results from shards at the coordinator level.
  if (!ValkeySearch::Instance().IsCluster()) {
    this->is_offsetted = true;
    // Trim from front (apply offset)
    if (range.start_index > 0 && range.start_index < neighbors.size()) {
      neighbors.erase(neighbors.begin(), neighbors.begin() + range.start_index);
      // After trimming from the front, we no longer have an offset.
      // We only need (end_index - start_index) items.
      size_t actual_count = range.end_index - range.start_index;
      max_needed = static_cast<size_t>(
          actual_count * options::GetSearchResultBufferMultiplier());
    } else if (range.start_index >= neighbors.size()) {
      neighbors.clear();
      return;
    }
  }
  // If we don't need to limit, return early.
  if (neighbors.size() <= max_needed) {
    return;
  }
  // Apply limiting with buffer
  this->is_limited_with_buffer = true;
  neighbors.erase(neighbors.begin() + max_needed, neighbors.end());
}

// Determine the range of neighbors to serialize in the response.
SerializationRange SearchResult::GetSerializationRange(
    const SearchParameters &parameters) const {
  CHECK(!ShouldReturnNoResults(parameters));
  // Determine start_index
  size_t start_index = 0;
  // If we have already offsetted, start_index is 0.
  if (!is_offsetted) {
    if (parameters.IsVectorQuery()) {
      CHECK_GT(parameters.k, parameters.limit.first_index);
    }
    start_index = std::min(neighbors.size(),
                           static_cast<size_t>(parameters.limit.first_index));
  }
  // Determine end_index logic
  size_t limit_count = static_cast<size_t>(parameters.limit.number);
  size_t count;
  if (parameters.IsNonVectorQuery()) {
    count = std::min(limit_count, neighbors.size());
  } else {
    count = std::min(
        {static_cast<size_t>(parameters.k), limit_count, neighbors.size()});
  }
  size_t end_index = std::min(start_index + count, neighbors.size());
  // Return the range
  return {start_index, end_index};
}

absl::StatusOr<SearchResult> Search(const SearchParameters &parameters,
                                    SearchMode search_mode) {
  auto result =
      MaybeAddIndexedContent(DoSearch(parameters, search_mode), parameters);
  if (!result.ok()) {
    return result.status();
  }
  size_t total_count = result.value().size();
  // return SearchResult(total_count, std::move(result.value()), parameters);
  auto search_result =
      SearchResult(total_count, std::move(result.value()), parameters);
  for (auto &n : search_result.neighbors) {
    n.sequence_number =
        parameters.index_schema->GetIndexMutationSequenceNumber(n.external_id);
  }
  return search_result;
}

absl::Status SearchAsync(std::unique_ptr<SearchParameters> parameters,
                         vmsdk::ThreadPool *thread_pool,
                         SearchResponseCallback callback,
                         SearchMode search_mode) {
  thread_pool->Schedule(
      [parameters = std::move(parameters), callback = std::move(callback),
       search_mode]() mutable {
        auto res = Search(*parameters, search_mode);
        callback(res, std::move(parameters));
      },
      vmsdk::ThreadPool::Priority::kHigh);
  return absl::OkStatus();
}

bool QueryHasTextPredicate(const SearchParameters &parameters) {
  return parameters.filter_parse_results.has_text_predicate;
}

}  // namespace valkey_search::query
