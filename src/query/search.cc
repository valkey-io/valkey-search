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
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "src/attribute_data_type.h"
#include "src/indexes/index_base.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/indexes/vector_base.h"
#include "src/indexes/vector_flat.h"
#include "src/indexes/vector_hnsw.h"
#include "src/metrics.h"
#include "src/query/planner.h"
#include "src/query/predicate.h"
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
  InlineVectorFilter(query::Predicate *filter_predicate,
                     indexes::VectorBase *vector_index)
      : filter_predicate_(filter_predicate), vector_index_(vector_index) {}
  ~InlineVectorFilter() override = default;

  bool operator()(hnswlib::labeltype id) override {
    auto key = vector_index_->GetKeyDuringSearch(id);
    if (!key.ok()) {
      return false;
    }
    indexes::PrefilterEvaluator evaluator;
    return evaluator.Evaluate(*filter_predicate_, *key);
  }

 private:
  query::Predicate *filter_predicate_;
  indexes::VectorBase *vector_index_;
};
absl::StatusOr<std::deque<indexes::Neighbor>> PerformVectorSearch(
    indexes::VectorBase *vector_index, const SearchParameters &parameters) {
  std::unique_ptr<InlineVectorFilter> inline_filter;
  if (parameters.filter_parse_results.root_predicate != nullptr) {
    inline_filter = std::make_unique<InlineVectorFilter>(
        parameters.filter_parse_results.root_predicate.get(), vector_index);
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

size_t EvaluateFilterAsPrimary(
    const Predicate *predicate,
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> &entries_fetchers,
    bool negate) {
  if (predicate->GetType() == PredicateType::kComposedAnd ||
      predicate->GetType() == PredicateType::kComposedOr) {
    auto composed_predicate =
        dynamic_cast<const ComposedPredicate *>(predicate);
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>>
        lhs_entries_fetchers;
    auto lhs_predicate = composed_predicate->GetLhsPredicate();
    auto lhs =
        EvaluateFilterAsPrimary(lhs_predicate, lhs_entries_fetchers, negate);
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>>
        rhs_entries_fetchers;
    auto rhs_predicate = composed_predicate->GetRhsPredicate();
    auto rhs =
        EvaluateFilterAsPrimary(rhs_predicate, rhs_entries_fetchers, negate);
    auto predicate_type =
        EvaluateAsComposedPredicate(composed_predicate, negate);
    if (predicate_type == PredicateType::kComposedAnd) {
      if (lhs < rhs) {
        AppendQueue(entries_fetchers, lhs_entries_fetchers);
        return lhs;
      }
      AppendQueue(entries_fetchers, rhs_entries_fetchers);
      return rhs;
    }
    AppendQueue(entries_fetchers, lhs_entries_fetchers);
    AppendQueue(entries_fetchers, rhs_entries_fetchers);
    return lhs + rhs;
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
  if (predicate->GetType() == PredicateType::kNegate) {
    auto negate_predicate = dynamic_cast<const NegatePredicate *>(predicate);
    return EvaluateFilterAsPrimary(negate_predicate->GetPredicate(),
                                   entries_fetchers, !negate);
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
        appender) {
  absl::flat_hash_set<const char *> result_keys;
  auto predicate = parameters.filter_parse_results.root_predicate.get();
  indexes::PrefilterEvaluator evaluator;
  while (!entries_fetchers.empty()) {
    auto fetcher = std::move(entries_fetchers.front());
    entries_fetchers.pop();
    auto iterator = fetcher->Begin();
    while (!iterator->Done()) {
      const auto &key = **iterator;
      // TODO: add a bloom filter to ensure distinct keys are evaluated
      // only once.
      if (!result_keys.contains(key->Str().data()) &&
          evaluator.Evaluate(*predicate, key)) {
        if (appender(key, result_keys)) {
          result_keys.insert(key->Str().data());
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
    indexes::VectorBase *vector_index) {
  std::priority_queue<std::pair<float, hnswlib::labeltype>> results;
  auto results_appender =
      [&results, &parameters, vector_index](
          const InternedStringPtr &key,
          absl::flat_hash_set<const char *> &top_keys) -> bool {
    return vector_index->AddPrefilteredKey(parameters.query, parameters.k, key,
                                           results, top_keys);
  };
  EvaluatePrefilteredKeys(parameters, entries_fetchers,
                          std::move(results_appender));
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

absl::StatusOr<std::deque<indexes::Neighbor>> MaybeAddIndexedContent(
    absl::StatusOr<std::deque<indexes::Neighbor>> results,
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
          if (tag_value_ptr != nullptr) {
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

absl::StatusOr<std::deque<indexes::Neighbor>> SearchNonVectorQuery(
    const SearchParameters &parameters) {
  std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> entries_fetchers;
  size_t qualified_entries = EvaluateFilterAsPrimary(
      parameters.filter_parse_results.root_predicate.get(), entries_fetchers,
      false);
  std::deque<indexes::Neighbor> neighbors;
  auto results_appender =
      [&neighbors, &parameters](
          const InternedStringPtr &key,
          absl::flat_hash_set<const char *> &top_keys) -> bool {
    neighbors.push_back(indexes::Neighbor{key, 0.0f});
    return true;
  };

  EvaluatePrefilteredKeys(parameters, entries_fetchers,
                          std::move(results_appender));

  return neighbors;
}

absl::StatusOr<std::deque<indexes::Neighbor>> DoSearch(
    const SearchParameters &parameters, SearchMode search_mode) {
  // Handle OOM for search requests, defends against request
  // coming from the coordinator
  if (search_mode == SearchMode::kRemote) {
    auto ctx = vmsdk::MakeUniqueValkeyThreadSafeContext(nullptr);
    auto ctx_flags = ValkeyModule_GetContextFlags(ctx.get());
    if (ctx_flags & VALKEYMODULE_CTX_FLAGS_OOM) {
      return absl::ResourceExhaustedError(kOOMMsg);
    }
  }

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
      false);

  // Query planner makes the decision for pre-filtering vs inline-filtering.
  if (UsePreFiltering(qualified_entries, vector_index)) {
    VMSDK_LOG(DEBUG, nullptr)
        << "Using pre-filter query execution, qualified entries="
        << qualified_entries;
    // Do an exact nearest neighbour search on the reduced search space.
    ++Metrics::GetStats().query_prefiltering_requests_cnt;
    std::priority_queue<std::pair<float, hnswlib::labeltype>> results =
        CalcBestMatchingPrefilteredKeys(parameters, entries_fetchers,
                                        vector_index);

    return vector_index->CreateReply(results);
  }
  ++Metrics::GetStats().query_inline_filtering_requests_cnt;
  lock.SetMayProlong();
  return PerformVectorSearch(vector_index, parameters);
}

// Helper functions to calculate start/end index with vector/non-vector
// awareness
size_t CalcStartIndex(const std::deque<indexes::Neighbor> &neighbors,
                      const SearchParameters &parameters) {
  if (!parameters.IsNonVectorQuery()) {
    CHECK_GT(parameters.k, parameters.limit.first_index);
  }
  if (neighbors.size() <= parameters.limit.first_index) {
    return neighbors.size();
  }
  return parameters.limit.first_index;
}

size_t CalcEndIndex(const std::deque<indexes::Neighbor> &neighbors,
                    const SearchParameters &parameters) {
  if (parameters.IsNonVectorQuery()) {
    return std::min(static_cast<size_t>(parameters.limit.number),
                    neighbors.size());
  }
  // Vector query
  return std::min(
      static_cast<size_t>(parameters.k),
      std::min(static_cast<size_t>(parameters.limit.number), neighbors.size()));
}

SearchResult::SearchResult(size_t total_count,
                           std::deque<indexes::Neighbor> neighbors,
                           const SearchParameters &parameters)
    : total_count(total_count) {
  // Check if sorting is needed first. Trim otherwise.
  if (NeedsSorting(parameters)) {
    this->neighbors = std::move(neighbors);
    this->is_limited = false;
  } else {
    this->neighbors = std::move(neighbors);
    this->is_limited = TrimResults(this->neighbors, parameters);
  }
}

// Determine if we need full results or can optimize with limiting.
// When SORTBY is present, we need full results to sort correctly.
bool SearchResult::NeedsSorting(const SearchParameters &parameters) {
  return parameters.HasSortBy();
}

// Apply limiting in background thread if possible.
bool SearchResult::TrimResults(std::deque<indexes::Neighbor> &neighbors,
                               const SearchParameters &parameters) {
  // Use CalcEndIndex for consistent vector/non-vector handling
  size_t end_needed = CalcEndIndex(neighbors, parameters);
  size_t max_needed =
      static_cast<size_t>((parameters.limit.first_index + end_needed) * 1.5);
  // If we don't need to limit, return false
  if (neighbors.size() <= max_needed) {
    return false;
  }
  // Apply limiting
  neighbors.erase(neighbors.begin() + max_needed, neighbors.end());
  return true;
}

absl::StatusOr<SearchResult> Search(const SearchParameters &parameters,
                                    SearchMode search_mode) {
  auto result =
      MaybeAddIndexedContent(DoSearch(parameters, search_mode), parameters);
  if (!result.ok()) {
    return result.status();
  }
  size_t total_count = result.value().size();
  return SearchResult(total_count, std::move(result.value()), parameters);
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

}  // namespace valkey_search::query
