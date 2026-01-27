/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
    indexes::InlineVectorEvaluator evaluator;
    return evaluator.Evaluate(*filter_predicate_, *key);
  }

 private:
  query::Predicate *filter_predicate_;
  indexes::VectorBase *vector_index_;
};
absl::StatusOr<std::deque<indexes::Neighbor>> PerformVectorSearch(
    indexes::VectorBase *vector_index,
    const VectorSearchParameters &parameters) {
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
                                   std::move(inline_filter), parameters.ef);
    Metrics::GetStats().hnsw_vector_index_search_latency.SubmitSample(
        std::move(latency_sample));
    return res;
  }
  if (vector_index->GetIndexerType() == indexes::IndexerType::kFlat) {
    auto vector_flat = dynamic_cast<indexes::VectorFlat<float> *>(vector_index);
    auto latency_sample = SAMPLE_EVERY_N(100);
    auto res = vector_flat->Search(parameters.query, parameters.k,
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

std::priority_queue<std::pair<float, hnswlib::labeltype>>
CalcBestMatchingPrefiltereddKeys(
    const VectorSearchParameters &parameters,
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> &entries_fetchers,
    indexes::VectorBase *vector_index) {
  std::priority_queue<std::pair<float, hnswlib::labeltype>> results;
  absl::flat_hash_set<hnswlib::labeltype> top_keys;
  auto predicate = parameters.filter_parse_results.root_predicate.get();
  indexes::InlineVectorEvaluator evaluator;
  while (!entries_fetchers.empty()) {
    auto fetcher = std::move(entries_fetchers.front());
    entries_fetchers.pop();
    auto iterator = fetcher->Begin();
    while (!iterator->Done()) {
      const auto &key = *iterator;
      // TODO: yairg - add bloom filter to ensure distinct keys are processed
      // just once.
      if (evaluator.Evaluate(*predicate, *key)) {
        vector_index->AddPrefilteredKey(parameters.query, parameters.k, *key,
                                        results, top_keys);
      }
      iterator->Next();
    }
  }
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
    const VectorSearchParameters &parameters) {
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
      vmsdk::UniqueRedisString attribute_value = nullptr;
      switch (attribute_info.index->GetIndexerType()) {
        case indexes::IndexerType::kTag: {
          auto tag_index = dynamic_cast<indexes::Tag *>(attribute_info.index);
          auto tag_value_ptr = tag_index->GetRawValue(neighbor.external_id);
          if (tag_value_ptr != nullptr) {
            attribute_value = vmsdk::MakeUniqueRedisString(*tag_value_ptr);
          }
          break;
        }
        case indexes::IndexerType::kNumeric: {
          auto numeric_index =
              dynamic_cast<indexes::Numeric *>(attribute_info.index);
          auto numeric = numeric_index->GetValue(neighbor.external_id);
          if (numeric != nullptr) {
            attribute_value =
                vmsdk::MakeUniqueRedisString(absl::StrCat(*numeric));
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
              attribute_value = vmsdk::MakeUniqueRedisString(
                  StringFormatVector(vector.value()));
            } else {
              attribute_value =
                  vmsdk::UniqueRedisString(RedisModule_CreateString(
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
        auto identifier = vmsdk::MakeUniqueRedisString(
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

absl::StatusOr<std::deque<indexes::Neighbor>> Search(
    const VectorSearchParameters &parameters, bool is_local_search) {
  VMSDK_ASSIGN_OR_RETURN(auto index, parameters.index_schema->GetIndex(
                                         parameters.attribute_alias));
  if (index->GetIndexerType() != indexes::IndexerType::kHNSW &&
      index->GetIndexerType() != indexes::IndexerType::kFlat) {
    return absl::InvalidArgumentError(
        absl::StrCat(parameters.attribute_alias, " is not a Vector index "));
  }
  auto vector_index = dynamic_cast<indexes::VectorBase *>(index.get());
  auto &time_sliced_mutex = parameters.index_schema->GetTimeSlicedMutex();
  vmsdk::ReaderMutexLock lock(&time_sliced_mutex);
  if (!parameters.filter_parse_results.root_predicate) {
    return MaybeAddIndexedContent(PerformVectorSearch(vector_index, parameters),
                                  parameters);
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
    auto results = CalcBestMatchingPrefiltereddKeys(
        parameters, entries_fetchers, vector_index);

    return vector_index->CreateReply(results);
  }
  if (is_local_search) {
    ++Metrics::GetStats().query_inline_filtering_requests_cnt;
  }
  lock.SetMayProlong();
  return MaybeAddIndexedContent(PerformVectorSearch(vector_index, parameters),
                                parameters);
}

<<<<<<< HEAD
absl::Status SearchAsync(std::unique_ptr<VectorSearchParameters> parameters,
=======
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
>>>>>>> 016e983 (Avoid deadlock while processing  MULTI/EXEC mutations (#629))
                         vmsdk::ThreadPool *thread_pool,
                         SearchResponseCallback callback,
                         bool is_local_search) {
  thread_pool->Schedule(
      [parameters = std::move(parameters), callback = std::move(callback),
       is_local_search]() mutable {
        auto res = Search(*parameters, is_local_search);
        callback(res, std::move(parameters));
      },
      vmsdk::ThreadPool::Priority::kHigh);
  return absl::OkStatus();
}

}  // namespace valkey_search::query
