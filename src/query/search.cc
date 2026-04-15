/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/search.h"

#include <absl/strings/str_split.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "src/attribute_data_type.h"
#include "src/expr/value.h"
#include "src/indexes/index_base.h"
#include "src/indexes/numeric.h"
#include "src/indexes/scoring/scorer.h"
#include "src/indexes/tag.h"
#include "src/indexes/text.h"
#include "src/indexes/text/orproximity.h"
#include "src/indexes/text/posting.h"
#include "src/indexes/text/proximity.h"
#include "src/indexes/text/rax_wrapper.h"
#include "src/indexes/text/text_fetcher.h"
#include "src/indexes/text/text_index.h"
#include "src/indexes/universal_set_fetcher.h"
#include "src/indexes/vector_base.h"
#include "src/indexes/vector_flat.h"
#include "src/indexes/vector_hnsw.h"
#include "src/metrics.h"
#include "src/query/content_resolution.h"
#include "src/query/planner.h"
#include "src/query/predicate.h"
#include "src/valkey_search.h"
#include "src/valkey_search_options.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/info.h"
#include "vmsdk/src/latency_sampler.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/time_sliced_mrmw_mutex.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query {

namespace {
// Process-global count of live SearchParameters objects. See
// GetSearchParametersInFlight() in the header for the rationale.
std::atomic<int64_t> &SearchParametersInFlightCounter() {
  static std::atomic<int64_t> counter{0};
  return counter;
}
}  // namespace

int64_t GetSearchParametersInFlight() {
  return SearchParametersInFlightCounter().load(std::memory_order_relaxed);
}

namespace detail {
SearchParametersInFlightGuard::SearchParametersInFlightGuard() {
  SearchParametersInFlightCounter().fetch_add(1, std::memory_order_relaxed);
}
SearchParametersInFlightGuard::SearchParametersInFlightGuard(
    const SearchParametersInFlightGuard &) {
  SearchParametersInFlightCounter().fetch_add(1, std::memory_order_relaxed);
}
SearchParametersInFlightGuard::SearchParametersInFlightGuard(
    SearchParametersInFlightGuard &&) noexcept {
  SearchParametersInFlightCounter().fetch_add(1, std::memory_order_relaxed);
}
SearchParametersInFlightGuard::~SearchParametersInFlightGuard() {
  SearchParametersInFlightCounter().fetch_sub(1, std::memory_order_relaxed);
}
}  // namespace detail

// Query operation counters
DEV_INTEGER_COUNTER(query_stats, query_text_term_count);
DEV_INTEGER_COUNTER(query_stats, query_text_prefix_count);
DEV_INTEGER_COUNTER(query_stats, query_text_suffix_count);
DEV_INTEGER_COUNTER(query_stats, query_text_fuzzy_count);
DEV_INTEGER_COUNTER(query_stats, query_text_proximity_count);
DEV_INTEGER_COUNTER(query_stats, query_numeric_count);
DEV_INTEGER_COUNTER(query_stats, query_tag_count);
DEV_INTEGER_COUNTER(query_stats, nonvector_results_fetched_limited_count);

class InlineVectorFilter : public hnswlib::BaseFilterFunctor {
 public:
  InlineVectorFilter(
      query::Predicate *filter_predicate, indexes::VectorBase *vector_index,
      const std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
      QueryOperations query_operations)
      : filter_predicate_(filter_predicate),
        vector_index_(vector_index),
        text_index_schema_(text_index_schema),
        query_operations_(query_operations) {}
  ~InlineVectorFilter() override = default;

  bool operator()(hnswlib::labeltype id) override {
    BACKGROUND_PAUSEPOINT("search_inline_filter");
    auto key = vector_index_->GetKeyDuringSearch(id);
    if (!key.ok()) {
      return false;
    }
    const valkey_search::indexes::text::TextIndex *text_index = nullptr;
    if (text_index_schema_) {
      text_index = text_index_schema_->GetPerKeyTextIndex(*key, false);
    }
    indexes::PrefilterEvaluator evaluator(text_index, query_operations_);
    return evaluator.Evaluate(*filter_predicate_, *key);
  }

 private:
  query::Predicate *filter_predicate_;
  indexes::VectorBase *vector_index_;
  const std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema_;
  QueryOperations query_operations_;
};
absl::StatusOr<std::vector<indexes::Neighbor>> PerformVectorSearch(
    indexes::VectorBase *vector_index, const SearchParameters &parameters) {
  std::unique_ptr<InlineVectorFilter> inline_filter;
  if (parameters.filter_parse_results.root_predicate != nullptr) {
    const std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema =
        parameters.index_schema->GetTextIndexSchema();
    inline_filter = std::make_unique<InlineVectorFilter>(
        parameters.filter_parse_results.root_predicate.get(), vector_index,
        text_index_schema, parameters.filter_parse_results.query_operations);
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
    auto res = vector_flat->Search(
        parameters.query, parameters.k, parameters.cancellation_token,
        std::move(inline_filter), parameters.enable_partial_results);
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
// It is also needed when negate is involved.
inline bool IsUnsolvedQuery(QueryOperations query_operations,
                            bool is_match_all) {
  if (is_match_all) {
    return false;
  }
  return query_operations & (QueryOperations::kContainsNumeric |
                             QueryOperations::kContainsTag) &&
             query_operations & QueryOperations::kContainsAnd ||
         (query_operations & QueryOperations::kContainsNegate);
}

// Helper fn to identify if deduplication is needed.
// (1) OR operations need deduplication.
// (2) Any TAG operations need deduplication.
// (3) Non-text negation needs deduplication (uses NegateEntriesFetcher)
inline bool NeedsDeduplication(QueryOperations query_operations) {
  bool has_or = query_operations & QueryOperations::kContainsOr;
  bool has_tag = query_operations & QueryOperations::kContainsTag;
  bool has_negate = query_operations & QueryOperations::kContainsNegate;
  bool has_text = query_operations & QueryOperations::kContainsText;
  // Text + negate doesn't need dedup (handled by prefilter evaluation)
  if (has_text && has_negate) {
    return false;
  }
  return has_or || has_tag || has_negate;
}

// Builds TextIterator for text predicates. Returns pair of iterator and
// estimated size.
std::pair<std::unique_ptr<indexes::text::TextIterator>, size_t>
BuildTextIterator(const Predicate *predicate, bool negate,
                  bool require_positions, bool is_vec_query,
                  const indexes::scoring::Scorer *scorer) {
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
        auto [iter, size] = BuildTextIterator(
            child.get(), negate, child_require_positions, is_vec_query, scorer);
        if (iter) {
          iterators.push_back(std::move(iter));
          min_size = std::min(min_size, size);
        }
      }
      // The Composed AND only has non text predicates, return null
      // to have the caller handle it.
      if (iterators.empty()) {
        return {nullptr, 0};
      }
      bool skip_positional = !child_require_positions;
      size_t total_size = min_size == SIZE_MAX ? 0 : min_size;
      return {std::make_unique<indexes::text::ProximityIterator>(
                  std::move(iterators), slop, inorder, skip_positional,
                  composed_predicate->GetWeight()),
              total_size};
    } else {
      absl::InlinedVector<std::unique_ptr<indexes::text::TextIterator>,
                          indexes::text::kProximityTermsInlineCapacity>
          iterators;
      size_t total_size = 0;
      bool has_non_text = false;
      for (const auto &child : composed_predicate->GetChildren()) {
        auto [iter, size] = BuildTextIterator(
            child.get(), negate, child_require_positions, is_vec_query, scorer);
        if (iter) {
          iterators.push_back(std::move(iter));
          total_size += size;
        } else {
          has_non_text = true;
        }
      }
      // If the Composed OR has any non text predicate, we cannot
      // build a text iterator.
      if (iterators.empty() || has_non_text) {
        return {nullptr, 0};
      }
      return {std::make_unique<indexes::text::OrProximityIterator>(
                  std::move(iterators), composed_predicate->GetWeight()),
              total_size};
    }
  }
  if (predicate->GetType() == PredicateType::kText) {
    auto text_predicate = dynamic_cast<const TextPredicate *>(predicate);
    auto text_index = text_predicate->GetTextIndexSchema()->GetTextIndex();
    auto field_mask = text_predicate->GetFieldMask();
    size_t size = text_predicate->EstimateSize(is_vec_query);
    // Stamp the query-selected scorer so TermPredicate::BuildTextIterator can
    // build a scored TermIterator without a hardcoded scorer.
    text_predicate->SetScorer(scorer);
    auto result = text_predicate->BuildTextIterator(text_index, field_mask,
                                                    require_positions);
    return {std::move(result), size};
  }
  if (predicate->GetType() == PredicateType::kNegate) {
    // Cannot build text iterator for negation - return null
    return {nullptr, 0};
  }
  // Numeric/Tag
  return {nullptr, 0};
}

size_t EvaluateFilterAsPrimary(
    const SearchParameters &parameters, const Predicate *predicate,
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> &entries_fetchers,
    bool negate) {
  const QueryOperations query_operations =
      parameters.filter_parse_results.query_operations;
  const IndexSchema *index_schema = parameters.index_schema.get();
  const bool is_vec_query = parameters.IsVectorQuery();

  // Always use universal set when query has text + negate
  if ((query_operations & QueryOperations::kContainsText) &&
      (query_operations & QueryOperations::kContainsNegate)) {
    CHECK(index_schema != nullptr) << "IndexSchema required for text+negate";
    auto universal_fetcher =
        std::make_unique<indexes::UniversalSetFetcher>(index_schema);
    size_t size = universal_fetcher->Size();
    entries_fetchers.push(std::move(universal_fetcher));
    return size;
  }

  if (predicate->GetType() == PredicateType::kComposedAnd ||
      predicate->GetType() == PredicateType::kComposedOr) {
    auto composed_predicate =
        dynamic_cast<const ComposedPredicate *>(predicate);
    auto predicate_type =
        EvaluateAsComposedPredicate(composed_predicate, negate);
    if (predicate_type == PredicateType::kComposedAnd) {
      auto [text_iter, size] =
          BuildTextIterator(composed_predicate, negate, false, is_vec_query,
                            indexes::scoring::GetScorer(parameters.scorer));
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
        size_t child_size = EvaluateFilterAsPrimary(parameters, child.get(),
                                                    child_fetchers, negate);
        if (child_size < min_size) {
          min_size = child_size;
          best_fetchers = std::move(child_fetchers);
        }
      }
      AppendQueue(entries_fetchers, best_fetchers);
      return min_size;
    } else {
      // TODO: optimize with eliminate BuildTextIterator for OR in followup PR
      // All-text OR: build a single OrProximityIterator so a doc matching
      // multiple branches is scored on the sum of those branches (and any group
      // weight applies). Falls back to per-child fetchers when the OR mixes in
      // non-text predicates.
      auto [text_iter, size] =
          BuildTextIterator(composed_predicate, negate, false, is_vec_query,
                            indexes::scoring::GetScorer(parameters.scorer));
      if (text_iter) {
        entries_fetchers.push(
            std::make_unique<indexes::text::TextIteratorFetcher>(
                std::move(text_iter), size));
        return size;
      }
      size_t total_size = 0;
      for (const auto &child : composed_predicate->GetChildren()) {
        std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> child_fetchers;
        size_t child_size = EvaluateFilterAsPrimary(parameters, child.get(),
                                                    child_fetchers, negate);
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
    size_t size = text_predicate->EstimateSize(is_vec_query);
    auto fetcher = std::make_unique<indexes::Text::EntriesFetcher>(
        size, text_predicate->GetTextIndexSchema()->GetTextIndex(),
        text_predicate->GetFieldMask(), false);
    fetcher->predicate_ = text_predicate;
    // Stamp the query-selected scorer so the TermIterator built lazily in
    // EntriesFetcher::Begin() is scored (not the unscored stub).
    text_predicate->SetScorer(indexes::scoring::GetScorer(parameters.scorer));
    entries_fetchers.push(std::move(fetcher));
    return size;
  }
  if (predicate->GetType() == PredicateType::kNegate) {
    auto negate_predicate = dynamic_cast<const NegatePredicate *>(predicate);
    size_t result =
        EvaluateFilterAsPrimary(parameters, negate_predicate->GetPredicate(),
                                entries_fetchers, !negate);
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
    size_t max_keys, bool stop_on_fetch_limit) {
  // If there was a union operation, we need to handle deduplication.
  // This implementation skips deduplication (flat_hash_set usage) if not needed
  // for performance.
  bool needs_dedup =
      NeedsDeduplication(parameters.filter_parse_results.query_operations);
  absl::flat_hash_set<const char *> result_keys;
  if (needs_dedup) {
    result_keys.reserve(max_keys);
  }
  const std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema =
      parameters.index_schema ? parameters.index_schema->GetTextIndexSchema()
                              : nullptr;
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
      // 2. Skip keys not in the INKEYS set (when specified).
      if (!parameters.inkeys.empty() &&
          !parameters.inkeys.contains(key->Str())) {
        iterator->Next();
        continue;
      }
      const valkey_search::indexes::text::TextIndex *text_index =
          text_index_schema ? text_index_schema->GetPerKeyTextIndex(key, false)
                            : nullptr;
      indexes::PrefilterEvaluator key_evaluator(
          text_index, parameters.filter_parse_results.query_operations);
      BACKGROUND_PAUSEPOINT("search_prefilter_eval");
      // 3. Evaluate predicate
      if (key_evaluator.Evaluate(
              *parameters.filter_parse_results.root_predicate, key)) {
        bool result = appender(key, result_keys);
        if (needs_dedup && result) {
          result_keys.insert(key->Str().data());
        }
        // For non-vector queries that exceed the fetch limit, return early
        if (stop_on_fetch_limit && !result) {
          return;
        }
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
  float query_magnitude = indexes::kDefaultMagnitude;
  if (vector_index->GetNormalize()) {
    query_magnitude = indexes::CalcReciprocalMagnitude(
        reinterpret_cast<const float *>(parameters.query.data()),
        parameters.query.size() / sizeof(float));
  }
  auto results_appender =
      [&results, &parameters, vector_index, query_magnitude](
          const InternedStringPtr &key,
          absl::flat_hash_set<const char *> &top_keys) -> bool {
    return vector_index->AddPrefilteredKey(parameters.query, query_magnitude,
                                           key, parameters.k, results,
                                           top_keys);
  };
  EvaluatePrefilteredKeys(parameters, entries_fetchers,
                          std::move(results_appender), qualified_entries,
                          /*stop_on_fetch_limit=*/false);
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
          auto *numeric_index =
              dynamic_cast<indexes::Numeric *>(attribute_info.index);
          const auto *numeric = numeric_index->GetValue(neighbor.external_id);
          if (numeric != nullptr) {
            attribute_value =
                vmsdk::MakeUniqueValkeyString(expr::FormatDouble(*numeric));
          }
          break;
        }
        case indexes::IndexerType::kVector:
        case indexes::IndexerType::kHNSW:
        case indexes::IndexerType::kFlat: {
          const auto *vector_index =
              dynamic_cast<const indexes::VectorBase *>(attribute_info.index);
          auto vector =
              vector_index->GetVectorDuringSearch(neighbor.external_id);
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

// Final guard before a score reaches Neighbor.score.
// IsNaN (not std::isnan) because the build uses -ffast-math.
float SanitizeScore(float score) {
  return indexes::scoring::IsNaN(score) ? 0.0f : score;
}

// A term leaf's posting lists resolved once per query. A term matches via its
// original word plus any stem variants that stem to the same root (mirroring
// TermPredicate::Evaluate), so a leaf can resolve to several posting lists. The
// lists and document frequency (dt) are identical for every candidate, so they
// are resolved up front by ResolveLeaves rather than re-walked per document.
struct ResolvedLeaf {
  // --- Text leaf (TermPredicate) ---
  // Original term posting list first, followed by any stem-variant lists. Empty
  // when the term (and all its variants) are absent from the index.
  absl::InlinedVector<indexes::text::InvasivePtr<indexes::text::Postings>,
                      indexes::text::kStemVariantsInlineCapacity + 1>
      postings;
  uint32_t num_doc_contain_term = 0;
  // Query-invariant per-term weight (BM25 IDF), computed once here instead of
  // per candidate document.
  float term_weight = 0.0f;

  // --- Tag leaf (TagPredicate) ---
  // Null for text leaves. When set, `tag_values` holds one (query tag value,
  // precomputed IDF) entry per value that actually exists in the index; the
  // per-document walk looks the document's tags up via `tag_index` and sums the
  // BM25 term (with F ≡ 1) for each value the document carries. The one
  // dynamic_cast to TagPredicate happens once here (in ResolveLeaves), so the
  // per-candidate ScoreNode walk needs only a cheap map lookup.
  const indexes::Tag *tag_index = nullptr;
  absl::InlinedVector<std::pair<std::string, float>, 4> tag_values;
};

// Keyed on the base Predicate* (not TermPredicate*) so the per-document scoring
// walk can look leaves up without a dynamic_cast: a hit is a scored term leaf,
// a miss is a non-scored text predicate (prefix/suffix/fuzzy).
using ResolvedLeaves = absl::flat_hash_map<const Predicate *, ResolvedLeaf>;

// Runs once per query to hoist all document-independent scoring work out of the
// per-candidate loop. Walks the predicate tree and, for each TermPredicate
// leaf, precomputes the parts that are identical for every matching document:
//   - the posting lists (the expensive radix-tree lookup + stem expansion),
//   - the document frequency (dt), and
//   - the per-term BM25 weight (IDF).
// It also performs the one dynamic_cast needed to tell scored TermPredicates
// apart from non-scored text predicates (prefix/suffix/fuzzy) here, so the
// per-document walk can distinguish them with a cheap map lookup instead.
// Results go into `resolved`, keyed on the base Predicate*; the per-document
// walk then only does the cheap per-key term-frequency lookup. A leaf whose
// term (and all its variants) is absent from the index resolves to empty
// postings.
void ResolveLeaves(const Predicate *predicate, uint32_t total_docs,
                   const indexes::scoring::Scorer *scorer,
                   ResolvedLeaves &resolved) {
  CHECK(predicate != nullptr);
  switch (predicate->GetType()) {
    case PredicateType::kComposedAnd:
    case PredicateType::kComposedOr: {
      auto composed = static_cast<const ComposedPredicate *>(predicate);
      for (const auto &child : composed->GetChildren()) {
        ResolveLeaves(child.get(), total_docs, scorer, resolved);
      }
      break;
    }
    case PredicateType::kText: {
      // kText is shared by Term/Prefix/Suffix/Infix predicates; only
      // TermPredicate is scored, so this cast must stay dynamic.
      auto term_pred = dynamic_cast<const TermPredicate *>(predicate);
      if (!term_pred || resolved.contains(term_pred)) break;
      auto text_index_schema = term_pred->GetTextIndexSchema();
      CHECK(text_index_schema != nullptr);
      auto text_index = text_index_schema->GetTextIndex();
      CHECK(text_index != nullptr);
      const auto &prefix = text_index->GetPrefix();

      ResolvedLeaf leaf;
      // Collect the words the term matches on: the original word plus, for a
      // non-exact term on a stemmed field, every variant sharing its stem root
      // (matching TermPredicate::Evaluate). Ingestion stores original words in
      // the posting tree, so each word is resolved via FindPostingsTarget.
      auto add_word = [&](absl::string_view word) {
        auto postings = prefix.FindPostingsTarget(word);
        // TODO: scoring for stemming. Redis treat stem variant as a leaf
        // num_doc_contain_term is counted twice and need fix in future
        if (postings) {
          leaf.num_doc_contain_term += postings->GetKeyCount();
          leaf.postings.push_back(std::move(postings));
        }
      };
      add_word(term_pred->GetTextString());

      const uint64_t stem_field_mask =
          term_pred->GetFieldMask() & text_index_schema->GetStemTextFieldMask();
      if (!term_pred->IsExact() && stem_field_mask != 0) {
        absl::InlinedVector<absl::string_view,
                            indexes::text::kStemVariantsInlineCapacity>
            stem_variants;
        std::string stemmed = text_index_schema->GetAllStemVariants(
            term_pred->GetTextString(), stem_variants, stem_field_mask,
            /*lock_needed=*/true);
        if (stemmed != term_pred->GetTextString()) {
          add_word(stemmed);
        }
        for (const auto &variant : stem_variants) {
          add_word(variant);
        }
      }

      // dt feeds IDF, whose scorer checks dt <= total_docs. Summing key counts
      // across variants can double-count a doc indexed under several variants,
      // so clamp to keep the invariant.
      leaf.num_doc_contain_term =
          std::min(leaf.num_doc_contain_term, total_docs);
      leaf.term_weight =
          scorer->PrecomputeIDF({total_docs, leaf.num_doc_contain_term});
      resolved.emplace(term_pred, std::move(leaf));
      break;
    }
    case PredicateType::kTag: {
      // Only a real TagPredicate carries the index + values needed to score;
      // dynamic_cast guards against a non-TagPredicate kTag leaf (e.g. a test
      // mock), which resolves to an empty leaf and contributes 0.
      auto tag_pred = dynamic_cast<const TagPredicate *>(predicate);
      if (!tag_pred || resolved.contains(tag_pred)) break;
      const indexes::Tag *tag_index = tag_pred->GetIndex();
      if (tag_index == nullptr) break;

      // A tag value is scored as a BM25 term with F ≡ 1: IDF over the number of
      // documents carrying that value (dt). Resolve dt + IDF once per value
      // here; the per-document walk sums the values a document actually
      // carries. A union (`{red|blue}`) resolves several values, each
      // contributing its own term.
      ResolvedLeaf leaf;
      leaf.tag_index = tag_index;
      // Dedupe query values that collapse to the same tag under the index's
      // case rules (e.g. `{red|Red}` on a case-insensitive index)
      const bool case_sensitive = tag_index->IsCaseSensitive();
      absl::flat_hash_set<std::string> seen;
      for (const auto &value : tag_pred->GetTags()) {
        std::string norm =
            case_sensitive ? value : absl::AsciiStrToLower(value);
        if (!seen.insert(norm).second) continue;
        uint32_t dt = static_cast<uint32_t>(std::min<size_t>(
            tag_index->GetTagValueDocCount(value), total_docs));
        // A value absent from the index (dt == 0) has no matching document and
        // never contributes a term; skip it so the per-document walk stays a
        // simple sum over present values.
        if (dt == 0) continue;
        leaf.tag_values.emplace_back(value,
                                     scorer->PrecomputeIDF({total_docs, dt}));
      }
      resolved.emplace(tag_pred, std::move(leaf));
      break;
    }
    default:
      break;
  }
}

// Query-invariant scoring inputs, captured once per query so the per-document
// walk only does per-key lookups.
struct ScoreContext {
  const IndexSchema &index_schema;
  const indexes::scoring::Scorer *scorer;
  const ResolvedLeaves &resolved;
  uint32_t total_docs = 0;
  uint64_t total_doc_len = 0;
  float avg_doc_len = 0.0f;
  bool needs_doc_len = false;
  // When the index has no SCORE field, every document carries the same constant
  // document score, so the per-candidate GetDocumentScore lookup is skipped.
  bool has_score_field = false;
  float default_document_score = 1.0f;
};

std::optional<float> ScoreNode(const Predicate *predicate,
                               BorrowedInternedStringPtr key,
                               const ScoreContext &score_ctx) {
  CHECK(predicate != nullptr);

  switch (predicate->GetType()) {
    case PredicateType::kComposedAnd: {
      auto composed = static_cast<const ComposedPredicate *>(predicate);
      float sum = 0.0f;
      for (const auto &child : composed->GetChildren()) {
        auto child_score = ScoreNode(child.get(), key, score_ctx);
        // AND: every child must match, otherwise the document does not match
        // this group and contributes nothing from it.
        if (!child_score) return std::nullopt;
        sum += *child_score;
      }
      return predicate->GetWeight() * sum;
    }
    case PredicateType::kComposedOr: {
      auto composed = static_cast<const ComposedPredicate *>(predicate);
      float sum = 0.0f;
      bool matched = false;
      for (const auto &child : composed->GetChildren()) {
        auto child_score = ScoreNode(child.get(), key, score_ctx);
        // OR: any matching child contributes; non-matching children are
        // skipped.
        if (child_score) {
          matched = true;
          sum += *child_score;
        }
      }
      if (!matched) return std::nullopt;
      return predicate->GetWeight() * sum;
    }
    case PredicateType::kText: {
      // kText is shared by Term/Prefix/Suffix/Infix predicates; only
      // TermPredicate is present in the resolved map (see ResolveLeaves). A
      // miss here is a non-scored text predicate (prefix/suffix/fuzzy): not
      // scored, but the document still matched, so treat as a zero contribution
      // rather than a non-match. This avoids a per-candidate dynamic_cast.
      auto it = score_ctx.resolved.find(predicate);
      if (it == score_ctx.resolved.end()) return 0.0f;
      const ResolvedLeaf &leaf = it->second;
      if (leaf.postings.empty()) return std::nullopt;

      // Sum the term frequency across the original word and its stem variants:
      // a doc matches the leaf if any resolved posting list contains its key.
      // doc_len is co-located in the posting entry, so the same lookup yields
      // it (identical across postings for one key) — no separate per-key
      // scoring-map probe. It is 0 only when no posting matches, in which case
      // tf is 0 and we return early; avg_doc_len is 0 for a length-agnostic
      // scorer, which ScoreLeaf treats as a degenerate corpus and scores 0.
      uint32_t tf = 0;
      uint32_t doc_len = 0;
      for (const auto &postings : leaf.postings) {
        if (auto entry = postings->LookupKey(key)) {
          tf += entry->tf;
          doc_len = entry->doc_len;
        }
      }

      if (tf == 0) return std::nullopt;

      return score_ctx.scorer->ScoreLeaf({leaf.term_weight, tf, doc_len,
                                          score_ctx.avg_doc_len,
                                          predicate->GetWeight()});
    }
    // A numeric range match is a filter, never a ranker: it carries no IDF, no
    // term frequency, and no doc-length component, so under BM25STD it
    // contributes nothing (Redis reports "Irrelevant token -> score is 0").
    // Returning 0 leaves ordering unchanged whether or not a numeric clause is
    // present — a numeric candidate only reaches here because the pre-filter
    // admitted it, so there is no nullopt (non-match) path.
    case PredicateType::kNumeric:
      return 0.0f;
    // A tag value is scored as a BM25 term with F ≡ 1 (term frequency is not
    // counted): IDF over the per-tag-value document count, normalized by the
    // document's TEXT length, honoring $weight. A union (`{red|blue}`) sums the
    // terms for every matched value the document carries. Doc-length inputs
    // come from the index's TEXT field; on a text-less index avg_doc_len is 0
    // and ScoreLeaf returns 0 (a well-defined score, not Redis's nan).
    case PredicateType::kTag: {
      auto it = score_ctx.resolved.find(predicate);
      // A tag leaf is always resolved (unlike non-scored text predicates), but
      // guard defensively: an unresolved or index-less leaf contributes 0
      // without rejecting the already-admitted candidate.
      if (it == score_ctx.resolved.end()) return 0.0f;
      const ResolvedLeaf &leaf = it->second;
      if (leaf.tag_index == nullptr || leaf.tag_values.empty()) return 0.0f;

      uint32_t doc_len = 0;
      if (score_ctx.needs_doc_len && score_ctx.total_docs > 0) {
        doc_len = score_ctx.index_schema.GetDocumentLength(key);
      }

      // Sum the BM25 term (F ≡ 1) for each resolved value the document carries.
      // ContainsKey normalizes per the index's case rules and tests membership
      // via the value's posting bag, avoiding a per-candidate parse of the
      // document's full tag set. An untracked key matches no value and scores
      // 0.
      float sum = 0.0f;
      for (const auto &[value, idf] : leaf.tag_values) {
        if (!leaf.tag_index->ContainsKey(value, key)) continue;
        sum += score_ctx.scorer->ScoreLeaf({idf, /*term_frequency=*/1, doc_len,
                                            score_ctx.avg_doc_len,
                                            predicate->GetWeight()});
      }
      return sum;
    }
    // kNegate is a filter already applied by the pre-filter, and kNone has no
    // term occurrence to score; neither contributes to relevance.
    case PredicateType::kNegate:
    case PredicateType::kNone:
      return 0.0f;
  }
  return 0.0f;
}

void ScoreTextQuery(const IndexSchema &index_schema,
                    const Predicate *root_predicate,
                    const indexes::scoring::Scorer *scorer,
                    std::vector<indexes::BorrowedNeighbor> &candidates) {
  CHECK(scorer != nullptr);
  if (candidates.empty()) return;

  const uint32_t total_docs = index_schema.GetIndexKeyInfoSize();
  // Candidates came from this index, so total_docs should be > 0; degrade to
  // "no scores" rather than aborting if the invariant ever breaks (mirrors
  // SingleDocumentScorer). Candidates keep their initial 0.0 score.
  if (total_docs == 0) return;

  // Resolve each term leaf's posting list and per-term weight once; the
  // per-document walk below then only does the cheap per-key lookup. A
  // match-all
  // (`*`) query has no predicate: there are no leaves to resolve and the loop
  // below scores every document with the constant wildcard leaf instead.
  ResolvedLeaves resolved;
  if (root_predicate != nullptr) {
    ResolveLeaves(root_predicate, total_docs, scorer, resolved);
  }

  const bool needs_doc_len = scorer->NeedsDocumentLength();
  const uint64_t total_doc_len =
      needs_doc_len ? index_schema.GetTotalDocumentLength() : 0;
  const float avg_doc_len =
      (needs_doc_len && total_docs > 0)
          ? static_cast<float>(total_doc_len) / static_cast<float>(total_docs)
          : 0.0f;
  ScoreContext score_ctx{index_schema,
                         scorer,
                         resolved,
                         total_docs,
                         total_doc_len,
                         avg_doc_len,
                         needs_doc_len,
                         index_schema.HasScoreField(),
                         index_schema.GetScore()};

  std::vector<indexes::BorrowedNeighbor> scored;
  scored.reserve(candidates.size());
  for (const auto &candidate : candidates) {
    // Non-owning view: scoring runs under the shared index lock, so the
    // InternedString outlives the loop and no ref-count churn is needed.
    const BorrowedInternedStringPtr &key = candidate.key;
    std::optional<float> score;
    if (root_predicate != nullptr) {
      score = ScoreNode(root_predicate, key, score_ctx);
    } else {
      // Match-all (`*`): Redis scores the wildcard as a single BM25 leaf with a
      // constant IDF (1.0) and term frequency (1), normalized by the document's
      // text length. On a text-less index avg_doc_len is 0 and ScoreLeaf
      // returns a well-defined 0.
      const uint32_t doc_len =
          score_ctx.needs_doc_len ? index_schema.GetDocumentLength(key) : 0;
      score = scorer->ScoreLeaf({/*idf=*/1.0f, /*term_frequency=*/1, doc_len,
                                 score_ctx.avg_doc_len, /*leaf_weight=*/1.0f});
    }
    // no term contribute to score; return 0 rather than drop the doc
    const float resolved_score = score.value_or(0.0f);
    const float document_score = score_ctx.has_score_field
                                     ? index_schema.GetDocumentScore(key)
                                     : score_ctx.default_document_score;
    const float final_score = SanitizeScore(
        scorer->ComposeDocumentScore(resolved_score, document_score));
    scored.push_back({candidate.key, 0.0f, final_score});
  }

  candidates = std::move(scored);
}

// Applies text relevance scoring to KNN neighbors when the vector query also
// carries a text predicate (a hybrid `text=>[KNN]` query). Reuses
// ScoreTextQuery via a thin BorrowedNeighbor adapter: KNN preserves neighbor
// order, so the scores map back by index. Neighbor.distance is left untouched
// (still reported via the score_as field); only Neighbor.score is set to the
// text relevance, mirroring Redis WITHSCORES. Pure vector queries and vector
// queries filtered only by numeric/tag predicates keep the KNN distance as
// their score.
void ApplyHybridTextScore(const SearchParameters &parameters,
                          std::vector<indexes::Neighbor> &neighbors) {
  if (!QueryHasTextPredicate(parameters) || neighbors.empty()) return;
  std::vector<indexes::BorrowedNeighbor> borrowed;
  borrowed.reserve(neighbors.size());
  for (const auto &neighbor : neighbors) {
    borrowed.push_back(
        {BorrowedInternedStringPtr(neighbor.external_id), 0.0f, 0.0f});
  }
  ScoreTextQuery(*parameters.index_schema,
                 parameters.filter_parse_results.root_predicate.get(),
                 indexes::scoring::GetScorer(parameters.scorer), borrowed);
  for (size_t i = 0; i < neighbors.size(); ++i) {
    neighbors[i].score = borrowed[i].score;
  }
}

// State captured once at construction: everything ScoreTextQuery derives
// before its per-candidate loop. ResolvedLeaf holds ref-counted Postings
// pointers, so the resolved snapshot stays valid across lock releases.
struct SingleDocumentScorer::State {
  const IndexSchema &index_schema;
  const Predicate *root_predicate;
  const indexes::scoring::Scorer *scorer;
  ResolvedLeaves resolved;
  uint32_t total_docs = 0;
  uint64_t total_doc_len = 0;
  float avg_doc_len = 0.0f;
  bool needs_doc_len = false;
  bool has_score_field = false;
  float default_document_score = 1.0f;
};

SingleDocumentScorer::SingleDocumentScorer(
    const IndexSchema &index_schema, const Predicate *root_predicate,
    const indexes::scoring::Scorer *scorer)
    : state_(new State{index_schema, root_predicate, scorer}) {
  CHECK(root_predicate != nullptr);
  CHECK(scorer != nullptr);

  // Runs on the main thread during content fetch, outside the background
  // search's reader lock, so acquire our own to read index_key_info_ /
  // text-index metadata safely against background mutations.
  vmsdk::ReaderMutexLock lock(
      &const_cast<IndexSchema &>(index_schema).GetTimeSlicedMutex());

  // Source EVERY scoring input exactly as ScoreTextQuery does so a recomputed
  // score is on the same scale as the shard-side score:
  //   - total_docs          : GetIndexKeyInfoSize()
  //   - dt + per-term IDF   : ResolveLeaves() over the GLOBAL posting lists
  //                           (FindPostingsTarget/GetKeyCount) - NOT the
  //                           per-key text index used for membership
  //                           revalidation.
  //   - avg_doc_len         : GetTotalDocumentLength()
  //   - document score      : HasScoreField()/GetScore().
  // All of it is document-independent, so it is resolved ONCE here; Score()
  // only does the cheap per-key work (tf lookup, doc_len, document score).
  state_->total_docs = index_schema.GetIndexKeyInfoSize();
  // ScoreTextQuery CHECK()s total_docs > 0 (it only runs when candidates
  // exist). This path can be reached for a pure numeric/tag query on an empty
  // corpus, so degrade to "Score() returns nullopt" instead of aborting.
  if (state_->total_docs == 0) return;
  ResolveLeaves(root_predicate, state_->total_docs, scorer, state_->resolved);
  state_->needs_doc_len = scorer->NeedsDocumentLength();
  state_->total_doc_len =
      state_->needs_doc_len ? index_schema.GetTotalDocumentLength() : 0;
  state_->avg_doc_len = (state_->needs_doc_len && state_->total_docs > 0)
                            ? static_cast<float>(state_->total_doc_len) /
                                  static_cast<float>(state_->total_docs)
                            : 0.0f;
  state_->has_score_field = index_schema.HasScoreField();
  state_->default_document_score = index_schema.GetScore();
}

SingleDocumentScorer::~SingleDocumentScorer() = default;

std::optional<float> SingleDocumentScorer::Score(
    const InternedStringPtr &key) const {
  if (state_->total_docs == 0) return std::nullopt;
  // Contract: the caller must NOT already hold the time-sliced mutex (see
  // class comment in search.h); the reader lock is acquired below.
  // Per-key reads (LookupTermFrequency, GetDocumentLength, GetDocumentScore)
  // touch index structures, so take the reader lock for the walk. The
  // document-independent inputs were captured at construction.
  vmsdk::ReaderMutexLock lock(
      &const_cast<IndexSchema &>(state_->index_schema).GetTimeSlicedMutex());

  ScoreContext score_ctx{state_->index_schema,
                         state_->scorer,
                         state_->resolved,
                         state_->total_docs,
                         state_->total_doc_len,
                         state_->avg_doc_len,
                         state_->needs_doc_len,
                         state_->has_score_field,
                         state_->default_document_score};
  const BorrowedInternedStringPtr borrowed_key(key);
  // Single source of scoring math: the same ScoreNode walk ScoreTextQuery runs
  // per candidate. nullopt means ScoreNode re-derived a non-match (e.g. a term
  // absent from the global postings for this key); the caller scores 0 rather
  // than dropping the already-admitted document.
  auto sum = ScoreNode(state_->root_predicate, borrowed_key, score_ctx);
  if (!sum) return std::nullopt;
  const float document_score =
      score_ctx.has_score_field
          ? state_->index_schema.GetDocumentScore(borrowed_key)
          : score_ctx.default_document_score;
  return SanitizeScore(
      state_->scorer->ComposeDocumentScore(*sum, document_score));
}

absl::StatusOr<std::vector<indexes::BorrowedNeighbor>> DoSearchNonVector(
    const SearchParameters &parameters) {
  const IndexSchema *index_schema = parameters.index_schema.get();
  const auto text_index_schema =
      index_schema ? index_schema->GetTextIndexSchema() : nullptr;

  const auto *scorer = indexes::scoring::GetScorer(parameters.scorer);

  // In-iterator scoring captures only the text iterator's score/weight, so it
  // is valid solely for genuinely pure-text queries. Any query that also
  // contains a numeric, tag, or negation predicate -- including mixed OR
  // compositions that IsUnsolvedQuery leaves on the entries-fetcher path --
  // must be scored via ScoreTextQuery below so both enclosing and leaf
  // predicate weights survive.
  const bool has_non_text_predicate =
      parameters.filter_parse_results.query_operations &
      (QueryOperations::kContainsNumeric | QueryOperations::kContainsTag |
       QueryOperations::kContainsNegate);

  // In-iterator scoring runs only for pure text queries (when enabled by the
  // switch), and only when the text index has at least one indexed document.
  const bool iterator_scoring_enabled =
      !has_non_text_predicate && text_index_schema &&
      text_index_schema->GetTrackedKeyCount() > 0;

  std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> entries_fetchers;
  size_t qualified_entries = 0;
  if (parameters.filter_parse_results.is_match_all) {
    auto universal_fetcher = std::make_unique<indexes::UniversalSetFetcher>(
        parameters.index_schema.get());
    qualified_entries = universal_fetcher->Size();
    entries_fetchers.push(std::move(universal_fetcher));
  } else {
    qualified_entries = EvaluateFilterAsPrimary(
        parameters, parameters.filter_parse_results.root_predicate.get(),
        entries_fetchers, false);
  }

  // Get the config for maximum number of keys to accumulate before content
  // fetching
  const size_t max_keys = static_cast<size_t>(
      options::GetMaxNonVectorSearchResultsFetched().GetValue());
  std::vector<indexes::BorrowedNeighbor> borrowed;
  borrowed.reserve(std::min(qualified_entries, static_cast<size_t>(5000)));
  bool fetch_limited = false;
  auto results_appender =
      [&borrowed, max_keys, &fetch_limited](
          const InternedStringPtr &key,
          absl::flat_hash_set<const char *> &top_keys) -> bool {
    if (borrowed.size() >= max_keys) {
      fetch_limited = true;
      return false;
    }
    borrowed.push_back({.key = BorrowedInternedStringPtr(key),
                        .distance = 0.0f,
                        .score = 0.0f});
    return true;
  };
  // Cannot skip evaluation if the query contains unsolved composed operations.
  const bool requires_prefilter_evaluation =
      IsUnsolvedQuery(parameters.filter_parse_results.query_operations,
                      parameters.filter_parse_results.is_match_all);
  if (!requires_prefilter_evaluation) {
    bool needs_dedup =
        NeedsDeduplication(parameters.filter_parse_results.query_operations);
    absl::flat_hash_set<const char *> seen_keys;
    if (needs_dedup) {
      seen_keys.reserve(std::min(qualified_entries, static_cast<size_t>(5000)));
    }
    while (!entries_fetchers.empty()) {
      auto fetcher = std::move(entries_fetchers.front());
      entries_fetchers.pop();
      auto iterator = fetcher->Begin();
      while (!iterator->Done()) {
        const auto &key = **iterator;
        BACKGROUND_PAUSEPOINT("search_entries_fetcher");
        // Skip keys not in the INKEYS set (when specified).
        if (!parameters.inkeys.empty() &&
            !parameters.inkeys.contains(key->Str())) {
          iterator->Next();
          continue;
        }
        if (needs_dedup) {
          if (seen_keys.contains(key->Str().data())) {
            iterator->Next();
            continue;
          }
          seen_keys.insert(key->Str().data());
        }
        // Check if we've reached the limit
        if (borrowed.size() >= max_keys) {
          nonvector_results_fetched_limited_count.Increment();
          break;
        }
        borrowed.push_back({.key = BorrowedInternedStringPtr(key),
                            .distance = 0.0f,
                            .score = 0.0f});
        // Set the per-document relevance score when scoring is enabled.
        // Only used by pure text queries
        if (iterator_scoring_enabled) {
          if (auto *text_iter = iterator->GetTextIterator()) {
            float raw = text_iter->GetScore() * text_iter->GetWeight();
            borrowed.back().score = scorer->ComposeDocumentScore(
                raw,
                index_schema->GetDocumentScore(BorrowedInternedStringPtr(key)));
          }
        }
        iterator->Next();
        if (parameters.cancellation_token->IsCancelled()) {
          break;
        }
      }
      if (borrowed.size() >= max_keys ||
          parameters.cancellation_token->IsCancelled()) {
        break;
      }
    }
  } else {
    // Combined (text + numeric/tag/negate) queries take the prefilter path and
    // are scored in the extra step below, since in-iterator scoring only works
    // for pure text queries.
    EvaluatePrefilteredKeys(parameters, entries_fetchers,
                            std::move(results_appender), qualified_entries,
                            /*stop_on_fetch_limit=*/true);
  }
  if (fetch_limited) {
    nonvector_results_fetched_limited_count.Increment();
  }
  // extra step scoring logic: score all the candidates after prefilter. Used by
  // combined (text + numeric/tag/negate) queries and by match-all (`*`): its
  // universal-set scan carries no TextIterator, so in-iterator scoring is inert
  // for it (iterator_scoring_enabled may still be true) and it must be scored
  // here via the null-predicate wildcard branch in ScoreTextQuery.
  if (!borrowed.empty() && (parameters.filter_parse_results.is_match_all ||
                            !iterator_scoring_enabled)) {
    ScoreTextQuery(*parameters.index_schema,
                   parameters.filter_parse_results.root_predicate.get(), scorer,
                   borrowed);
  }
  return borrowed;
}

absl::StatusOr<std::vector<indexes::Neighbor>> DoSearchVector(
    const SearchParameters &parameters, SearchMode search_mode,
    vmsdk::ReaderMutexLock &lock) {
  VMSDK_ASSIGN_OR_RETURN(auto index, parameters.index_schema->GetIndex(
                                         parameters.attribute_alias));
  auto *vector_index = dynamic_cast<indexes::VectorBase *>(index.get());
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
      parameters, parameters.filter_parse_results.root_predicate.get(),
      entries_fetchers, false);

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

    VMSDK_ASSIGN_OR_RETURN(auto neighbors, vector_index->CreateReply(results));
    ApplyHybridTextScore(parameters, neighbors);
    return neighbors;
  }
  ++Metrics::GetStats().query_inline_filtering_requests_cnt;
  lock.SetMayProlong();
  VMSDK_ASSIGN_OR_RETURN(auto neighbors,
                         PerformVectorSearch(vector_index, parameters));
  ApplyHybridTextScore(parameters, neighbors);
  return neighbors;
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

SearchResult::SearchResult()
    : total_count(0), is_limited_with_buffer(false), is_offsetted(false) {}

SearchResult::SearchResult(size_t total_count,
                           std::vector<indexes::Neighbor> neighbors,
                           const SearchParameters &parameters,
                           bool trim_offset_in_background)
    : total_count(total_count),
      is_limited_with_buffer(false),
      is_offsetted(false) {
  this->neighbors = std::move(neighbors);
  if (ShouldReturnNoResults(parameters)) {
    this->neighbors.clear();
    return;
  }
  if (!parameters.RequiresCompleteResults()) {
    TrimResults(this->neighbors, parameters, trim_offset_in_background);
  }
}

SearchResult::SearchResult(size_t total_count,
                           std::vector<indexes::BorrowedNeighbor> borrowed,
                           const SearchParameters &parameters,
                           bool trim_offset_in_background)
    : total_count(total_count),
      is_limited_with_buffer(false),
      is_offsetted(false) {
  if (ShouldReturnNoResults(parameters)) {
    return;
  }
  if (!parameters.RequiresCompleteResults()) {
    TrimResults(borrowed, parameters, trim_offset_in_background);
  }
  // Materialize only the survivors into owning Neighbor vector.
  neighbors.reserve(borrowed.size());
  for (auto &b : borrowed) {
    neighbors.emplace_back(b.key.Materialize(), b.distance, b.score);
  }
}

template <typename T>
void SearchResult::TrimResults(std::vector<T> &vec,
                               const SearchParameters &parameters,
                               bool trim_offset_in_background) {
  SerializationRange range = GetSerializationRange(parameters, vec.size());
  size_t max_needed = static_cast<size_t>(
      range.end_index * options::GetSearchResultBufferMultiplier());
  // Sort by score descending. For non-vector results (BorrowedNeighbor), use
  // partial_sort to only order the elements we'll actually keep.
  if constexpr (std::is_same_v<T, indexes::BorrowedNeighbor>) {
    auto cmp = [](const indexes::BorrowedNeighbor &a,
                  const indexes::BorrowedNeighbor &b) {
      if (a.score != b.score) {
        return a.score > b.score;
      }
      // Tie-break on key ascending for a deterministic result order.
      return a.key->Str() < b.key->Str();
    };
    size_t sort_limit = std::min(max_needed, vec.size());
    if (sort_limit < vec.size()) {
      std::partial_sort(vec.begin(), vec.begin() + sort_limit, vec.end(), cmp);
    } else {
      std::sort(vec.begin(), vec.end(), cmp);
    }
  } else if (parameters.IsNonVectorQuery() ||
             QueryHasTextPredicate(parameters)) {
    // Two cases sort by score descending here:
    //   - Cluster-merge non-vector path: the merged Neighbor vector is drained
    //     from the fanout heap ascending and never sorted.
    //   - Hybrid `text=>[KNN]`: KNN produces neighbors ordered by distance, but
    //     the query score is the text relevance (set by ApplyHybridTextScore),
    //     so re-rank by it to match Redis. The vector distance is preserved on
    //     Neighbor.distance and still reported via the score_as field.
    // Content resolution later drops some neighbors, but drops preserve
    // relative order, so this ordering survives. Pure vector queries (no text
    // predicate) fall through and keep their distance-ascending order.
    std::stable_sort(
        vec.begin(), vec.end(),
        [](const indexes::Neighbor &a, const indexes::Neighbor &b) {
          if (a.score != b.score) {
            return a.score > b.score;
          }
          // Tie-break on key ascending for a deterministic order.
          return a.external_id->Str() < b.external_id->Str();
        });
  }
  // In standalone mode, we can optimize by trimming from front first.
  // In cluster mode on remote searches on individual shards, we cannot trim
  // from the front yet because each shard produces X results. However, the
  // coordinator (after merging) WILL trim from the front and back in the
  // background thread to avoid memory bloat with large offsets / limit counts
  // before returning to the main thread.
  if (!ValkeySearch::Instance().IsCluster() || trim_offset_in_background) {
    this->is_offsetted = true;
    // Trim from front (apply offset)
    if (range.start_index > 0 && range.start_index < vec.size()) {
      vec.erase(vec.begin(), vec.begin() + range.start_index);
      // After trimming from the front, we no longer have an offset.
      // We only need (end_index - start_index) items.
      size_t actual_count = range.end_index - range.start_index;
      max_needed = static_cast<size_t>(
          actual_count * options::GetSearchResultBufferMultiplier());
    } else if (range.start_index >= vec.size()) {
      vec.clear();
      return;
    }
  }
  // If we don't need to limit, return early.
  if (vec.size() <= max_needed) {
    return;
  }
  // Apply limiting with buffer
  this->is_limited_with_buffer = true;
  vec.erase(vec.begin() + max_needed, vec.end());
}

// Determine the range of neighbors to serialize in the response.
SerializationRange SearchResult::GetSerializationRange(
    const SearchParameters &parameters,
    std::optional<size_t> override_size) const {
  CHECK(!ShouldReturnNoResults(parameters));
  size_t sz = override_size.value_or(neighbors.size());
  // Determine start_index
  size_t start_index = 0;
  // If we have already offsetted, start_index is 0.
  if (!is_offsetted) {
    if (parameters.IsVectorQuery()) {
      CHECK_GT(parameters.k, parameters.limit.first_index);
    }
    start_index =
        std::min(sz, static_cast<size_t>(parameters.limit.first_index));
  }
  // Determine end_index logic
  size_t limit_count = static_cast<size_t>(parameters.limit.number);
  size_t count;
  if (parameters.IsNonVectorQuery()) {
    count = std::min(limit_count, sz);
  } else {
    count = std::min({static_cast<size_t>(parameters.k), limit_count, sz});
  }
  size_t end_index = std::min(start_index + count, sz);
  return {start_index, end_index};
}

absl::Status Search(SearchParameters &parameters, SearchMode search_mode) {
  // Reject already-cancelled queries before acquiring the time-slice mutex.
  // Without this, expired queries that sat in the queue still acquire a reader
  // slot and waste mutex time before discovering they're cancelled deep in the
  // iteration loop. Return OkStatus with empty results (same as what the
  // iteration loop produces when it discovers cancellation mid-search) so the
  // coordinator tracker counts this as a "successful" node with 0 results —
  // enabling partial results from other shards that did complete.
  if (parameters.cancellation_token->IsCancelled()) {
    return absl::OkStatus();
  }
  // INKEYS 0 (has_inkeys=true, empty set) means no keys can match.
  // Short-circuit before acquiring the lock to avoid unnecessary contention.
  if (parameters.has_inkeys && parameters.inkeys.empty()) {
    parameters.search_result = SearchResult(0, {}, parameters);
    return absl::OkStatus();
  }
  auto &time_sliced_mutex = parameters.index_schema->GetTimeSlicedMutex();
  vmsdk::ReaderMutexLock lock(&time_sliced_mutex);
  ++Metrics::GetStats().time_slice_queries;
  // Handle OOM for search requests, defends against request
  // coming from the coordinator
  if (search_mode == SearchMode::kRemote) {
    auto ctx = vmsdk::MakeUniqueValkeyThreadSafeContext(nullptr);
    auto ctx_flags = ValkeyModule_GetContextFlags(ctx.get());
    if (ctx_flags & VALKEYMODULE_CTX_FLAGS_OOM) {
      return absl::ResourceExhaustedError(kOOMMsg);
    }
  }
  if (parameters.IsNonVectorQuery()) {
    VMSDK_ASSIGN_OR_RETURN(auto borrowed, DoSearchNonVector(parameters));
    size_t total_count = borrowed.size();
    parameters.search_result =
        SearchResult(total_count, std::move(borrowed), parameters);
  } else {
    VMSDK_ASSIGN_OR_RETURN(auto neighbors,
                           DoSearchVector(parameters, search_mode, lock));
    VMSDK_ASSIGN_OR_RETURN(
        auto result, MaybeAddIndexedContent(std::move(neighbors), parameters));
    size_t total_count = result.size();
    parameters.search_result =
        SearchResult(total_count, std::move(result), parameters);
  }
  parameters.index_schema->PopulateIndexMutationSequenceNumbers(
      parameters.search_result.neighbors);
  return absl::OkStatus();
}

absl::Status SearchAsync(std::unique_ptr<SearchParameters> parameters,
                         vmsdk::ThreadPool *thread_pool,
                         SearchMode search_mode) {
  thread_pool->Schedule(
      [parameters = std::move(parameters), search_mode]() mutable {
        auto res = Search(*parameters, search_mode);
        BACKGROUND_PAUSEPOINT("background_search_completing");
        parameters->search_result.status = res;
        switch (parameters->GetContentProcessing()) {
          case ContentProcessing::kNoContent:
            parameters->QueryCompleteBackground(std::move(parameters));
            break;
          case ContentProcessing::kContentRequired:
          case ContentProcessing::kContentionCheckRequired:
            vmsdk::RunByMain([parameters = std::move(parameters)]() mutable {
              ResolveContent(std::move(parameters));
            });
            break;
          default:
            CHECK(false) << "Unknown content processing mode";
        }
      },
      vmsdk::ThreadPool::Priority::kHigh);
  return absl::OkStatus();
}

bool QueryHasTextPredicate(const SearchParameters &parameters) {
  return parameters.filter_parse_results.query_operations &
         QueryOperations::kContainsText;
}

// Increment query operation metrics based on query operations flags.
// File-internal helper function.
void IncrementQueryOperationMetrics(QueryOperations query_operations) {
  // High-level query type metrics
  if (query_operations & QueryOperations::kContainsText) {
    ++Metrics::GetStats().query_text_requests_cnt;
  }
  if (query_operations & QueryOperations::kContainsNumeric) {
    query_numeric_count.Increment();
  }
  if (query_operations & QueryOperations::kContainsTag) {
    query_tag_count.Increment();
  }
  // Text operation type metrics
  if (query_operations & QueryOperations::kContainsTextTerm) {
    query_text_term_count.Increment();
  }
  if (query_operations & QueryOperations::kContainsTextPrefix) {
    query_text_prefix_count.Increment();
  }
  if (query_operations & QueryOperations::kContainsTextSuffix) {
    query_text_suffix_count.Increment();
  }
  if (query_operations & QueryOperations::kContainsTextFuzzy) {
    query_text_fuzzy_count.Increment();
  }
  if (query_operations & QueryOperations::kContainsProximity) {
    query_text_proximity_count.Increment();
  }
}

absl::StatusOr<absl::string_view> SubstituteParam(
    query::SearchParameters &parameters, absl::string_view source) {
  if (source.empty() || source[0] != '$') {
    return source;
  } else {
    source.remove_prefix(1);
    auto itr = parameters.parse_vars.params.find(source);
    if (itr == parameters.parse_vars.params.end()) {
      return absl::NotFoundError(
          absl::StrCat("Parameter ", source, " not found."));
    } else {
      itr->second.first++;
      return itr->second.second;
    }
  }
}

absl::Status ParseKnnInner(query::SearchParameters &parameters,
                           std::string_view filter) {
  absl::InlinedVector<absl::string_view, 8> params =
      absl::StrSplit(filter, ' ', absl::SkipEmpty());
  if (params.empty()) {
    return absl::InvalidArgumentError("Missing parameters");
  }
  // TODO - need some investment to consolidate this with the common parsing
  // functionality
  if (!absl::EqualsIgnoreCase(params[0], "KNN")) {
    return absl::InvalidArgumentError(
        absl::StrCat("`", params[0], "`. Expecting `KNN`"));
  }
  if (params.size() == 1) {
    return absl::InvalidArgumentError("KNN argument is missing");
  }
  parameters.parse_vars.k_string = params[1];
  if (params.size() == 2) {
    return absl::InvalidArgumentError("Vector field argument is missing");
  }
  if (params[2].data()[0] != '@' || params[2].size() == 1) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected argument `", params[2],
                     "`. Expecting a vector field name, starting with '@'"));
  }
  parameters.attribute_alias =
      absl::string_view(params[2].data() + 1, params[2].size() - 1);
  if (params.size() == 3) {
    return absl::InvalidArgumentError("Blob attribute argument is missing");
  }
  parameters.parse_vars.query_vector_string = params[3];

  size_t i = 4;
  while (i < params.size()) {
    if (absl::EqualsIgnoreCase(params[i], "EF_RUNTIME")) {
      i++;
      if (i == params.size()) {
        return absl::InvalidArgumentError("EF_RUNTIME argument is missing");
      }
      parameters.parse_vars.ef_string = params[i++];
    } else if (absl::EqualsIgnoreCase(params[i], kAsParam)) {
      i++;
      if (i == params.size()) {
        return absl::InvalidArgumentError("AS argument is missing");
      }
      parameters.parse_vars.score_as_string = params[i++];
    } else {
      return absl::InvalidArgumentError(
          absl::StrCat("Unexpected argument `", params[i], "`"));
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<size_t> FindOpenSquareBracket(absl::string_view input) {
  for (size_t position = 0; position < input.size(); ++position) {
    if (input[position] == '[') {
      return position;
    }
    if (!std::isspace(input[position])) {
      return absl::InvalidArgumentError(
          absl::StrCat("Expecting '[' got '", input.substr(position, 1), "'"));
    }
  }
  return absl::InvalidArgumentError("Missing opening bracket");
}

absl::StatusOr<size_t> FindCloseSquareBracket(absl::string_view input) {
  for (auto position = input.size(); position > 0; --position) {
    if (input[position - 1] == ']') {
      return position - 1;
    }
    if (!std::isspace(input[position - 1])) {
      return absl::InvalidArgumentError(absl::StrCat(
          "Expecting ']' got '", input.substr(position - 1, 1), "'"));
    }
  }
  if (input[0] == ']') {
    return 0;
  }
  return absl::InvalidArgumentError("Missing closing bracket");
}

absl::StatusOr<FilterParseResults> ParsePreFilter(
    const IndexSchema &index_schema, absl::string_view pre_filter,
    const query::SearchParameters &search_params) {
  TextParsingOptions options{.verbatim = search_params.verbatim,
                             .inorder = search_params.inorder,
                             .slop = search_params.slop};
  FilterParser parser(index_schema, pre_filter, options);
  return parser.Parse();
}

absl::Status ParseKNN(query::SearchParameters &parameters,
                      absl::string_view filter_str) {
  if (filter_str.empty()) {
    return absl::InvalidArgumentError("Vector query clause is missing");
  }
  VMSDK_ASSIGN_OR_RETURN(auto close_position,
                         FindCloseSquareBracket(filter_str));
  size_t position = 0;
  VMSDK_ASSIGN_OR_RETURN(
      auto open_position,
      FindOpenSquareBracket(absl::string_view(filter_str.data() + position,
                                              close_position - position)));
  position += open_position;
  return ParseKnnInner(parameters,
                       absl::string_view(filter_str.data() + position + 1,
                                         close_position - position - 1));
}

//
// Scans for the vector filter delimiter `=>` that is followed by `[` (after
// optional whitespace). This distinguishes the vector KNN delimiter from QMA
// blocks which use `=> {`.
//
size_t FindVectorDelimiter(absl::string_view expr) {
  size_t pos = 0;
  while ((pos = expr.find(kVectorFilterDelimiter, pos)) !=
         absl::string_view::npos) {
    // Skip whitespace after =>
    size_t after = pos + kVectorFilterDelimiter.size();
    while (after < expr.size() && std::isspace(expr[after])) {
      after++;
    }
    if (after < expr.size() && expr[after] == '[') {
      return pos;  // This is the vector delimiter
    }
    pos += kVectorFilterDelimiter.size();  // Continue searching
  }
  return absl::string_view::npos;  // No vector delimiter found
}

//
// We don't have values for the $ substitution yet. so we break the parsing into
// two pieces
//
absl::Status query::SearchParameters::PreParseQueryString() {
  // Validate the query string's length.
  if (parse_vars.query_string.length() > options::GetQueryStringBytes()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Query string is too long, max length is ",
                     options::GetQueryStringBytes(), " bytes."));
  }
  auto filter_expression = absl::string_view(parse_vars.query_string);
  VMSDK_LOG(DEBUG, nullptr)
      << "Query: '" << vmsdk::config::RedactIfNeeded(parse_vars.query_string)
      << "'";
  auto pos = FindVectorDelimiter(filter_expression);
  absl::string_view pre_filter;
  absl::string_view vector_filter;
  // If the delimiter is not found (ie - non vector query), treat the whole
  // string as pre-filter.
  if (pos == absl::string_view::npos) {
    pre_filter = absl::StripAsciiWhitespace(filter_expression);
  } else {
    pre_filter = absl::StripAsciiWhitespace(filter_expression.substr(0, pos));
    vector_filter = absl::StripAsciiWhitespace(
        filter_expression.substr(pos + kVectorFilterDelimiter.size()));
  }
  // If INORDER OR SLOP, but the index schema does not support offsets, we
  // reject the query.
  if ((inorder || slop.has_value()) && !index_schema->HasTextOffsets()) {
    return absl::InvalidArgumentError("Index does not support offsets");
  }
  VMSDK_ASSIGN_OR_RETURN(
      filter_parse_results, ParsePreFilter(*index_schema, pre_filter, *this),
      _.SetPrepend() << "Invalid filter expression: `" << pre_filter << "`. ");
  if (!filter_parse_results.root_predicate && vector_filter.empty() &&
      !filter_parse_results.is_match_all) {
    // Return an error if no valid pre-filter and no vector filter is provided.
    return absl::InvalidArgumentError("Invalid query string syntax");
  }
  // Optionally parse the vector filter if it exists.
  if (!vector_filter.empty()) {
    if (filter_parse_results.root_predicate) {
      ++Metrics::GetStats().query_hybrid_requests_cnt;
    } else {
      // Pure vector query
      ++Metrics::GetStats().query_vector_requests_cnt;
    }
    VMSDK_RETURN_IF_ERROR(ParseKNN(*this, vector_filter)).SetPrepend()
        << "Error parsing vector similarity parameters: `" << vector_filter
        << "`. ";
    // Validate the index exists and is a vector index.
    VMSDK_ASSIGN_OR_RETURN(auto index, index_schema->GetIndex(attribute_alias));
    if (index->GetIndexerType() != indexes::IndexerType::kHNSW &&
        index->GetIndexerType() != indexes::IndexerType::kFlat) {
      return absl::InvalidArgumentError(absl::StrCat(
          "Index field `", attribute_alias, "` is not a Vector index "));
    }
    if (parse_vars.score_as_string.empty()) {
      VMSDK_ASSIGN_OR_RETURN(
          score_as, index_schema->DefaultReplyScoreAs(attribute_alias));
    } else {
      score_as = vmsdk::MakeUniqueValkeyString(parse_vars.score_as_string);
    }
  }

  // Pure non-vector query (no vector filter)
  if (vector_filter.empty() && filter_parse_results.root_predicate) {
    ++Metrics::GetStats().query_nonvector_requests_cnt;
  }
  // Increment operation-type metrics
  IncrementQueryOperationMetrics(filter_parse_results.query_operations);
  return absl::OkStatus();
}

absl::Status PostParseVectorParameters(query::SearchParameters &parameters) {
  VMSDK_ASSIGN_OR_RETURN(
      auto k_string,
      SubstituteParam(parameters, parameters.parse_vars.k_string));
  VMSDK_ASSIGN_OR_RETURN(parameters.k, vmsdk::To<unsigned>(k_string));

  VMSDK_ASSIGN_OR_RETURN(
      parameters.query,
      SubstituteParam(parameters, parameters.parse_vars.query_vector_string));

  VMSDK_ASSIGN_OR_RETURN(auto index, parameters.index_schema->GetIndex(
                                         parameters.attribute_alias));
  auto *vector_index = dynamic_cast<indexes::VectorBase *>(index.get());
  CHECK(vector_index != nullptr);
  if (parameters.query.size() !=
      static_cast<size_t>(vector_index->GetVectorDataSize())) {
    return absl::InvalidArgumentError(
        absl::StrCat("query vector blob size (", parameters.query.size(),
                     ") does not match index's expected size (",
                     vector_index->GetVectorDataSize(), ")."));
  }

  if (!parameters.parse_vars.ef_string.empty()) {
    VMSDK_ASSIGN_OR_RETURN(
        auto ef_string,
        SubstituteParam(parameters, parameters.parse_vars.ef_string));
    VMSDK_ASSIGN_OR_RETURN(parameters.ef, vmsdk::To<unsigned>(ef_string));
  }

  if (!parameters.parse_vars.score_as_string.empty()) {
    VMSDK_ASSIGN_OR_RETURN(
        parameters.parse_vars.score_as_string,
        SubstituteParam(parameters, parameters.parse_vars.score_as_string));
  }
  return absl::OkStatus();
}

absl::Status query::SearchParameters::PostParseQueryString() {
  if (IsVectorQuery()) {
    VMSDK_RETURN_IF_ERROR(PostParseVectorParameters(*this)).SetPrepend()
        << "Error parsing vector similarity parameters: ";
  }

  return absl::OkStatus();
}

ContentProcessing SearchParameters::GetContentProcessing() const {
  if (no_content) {
    return kNoContent;
  }
  // Currently, ContentAvailable isn't detected. Future use case.
  if (query::QueryHasTextPredicate(*this)) {
    return kContentionCheckRequired;
  }
  return kContentRequired;
}

}  // namespace valkey_search::query
