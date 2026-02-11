/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_SEARCH_H_
#define VALKEYSEARCH_SRC_QUERY_SEARCH_H_

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "src/commands/filter_parser.h"
#include "src/index_schema.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"
#include "src/query/predicate.h"
#include "src/utils/cancel.h"
#include "src/valkey_search_options.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/thread_pool.h"

namespace valkey_search::query {

enum class SearchMode {
  kLocal,  // Search executed on local node/instance
  kRemote  // Search executed as part of distributed operation (from
           // coordinator)
};

constexpr int64_t kTimeoutMS{50000};
constexpr size_t kMaxTimeoutMs{60000};
constexpr absl::string_view kOOMMsg{
    "OOM command not allowed when used memory > 'maxmemory'"};
constexpr absl::string_view kFailedPreconditionMsg{
    "Index or slot consistency check failed"};
constexpr uint32_t kDialect{2};

struct LimitParameter {
  uint64_t first_index{0};
  uint64_t number{10};
};

struct ReturnAttribute {
  vmsdk::UniqueValkeyString identifier;
  vmsdk::UniqueValkeyString attribute_alias;
  vmsdk::UniqueValkeyString alias;
};

inline std::ostream& operator<<(std::ostream& os, const ReturnAttribute& r) {
  os << vmsdk::ToStringView(r.identifier.get());
  if (r.alias) {
    os << "[alias: " << vmsdk::ToStringView(r.alias.get()) << ']';
  }
  return os;
}

struct SearchParameters {
  mutable cancel::Token cancellation_token;
  virtual ~SearchParameters() = default;
  uint32_t db_num{0};
  std::shared_ptr<IndexSchema> index_schema;
  std::string index_schema_name;
  std::string attribute_alias;
  vmsdk::UniqueValkeyString score_as;
  std::string query;
  uint32_t dialect{kDialect};
  uint32_t db_num_;
  bool local_only{false};
  bool enable_partial_results{options::GetPreferPartialResults().GetValue()};
  bool enable_consistency{options::GetPreferConsistentResults().GetValue()};
  int k{0};
  std::optional<unsigned> ef;
  LimitParameter limit;
  uint64_t timeout_ms;
  bool no_content{false};
  FilterParseResults filter_parse_results;
  std::vector<ReturnAttribute> return_attributes;
  bool inorder{false};
  std::optional<uint32_t> slop;
  bool verbatim{false};
  coordinator::IndexFingerprintVersion index_fingerprint_version;
  uint64_t slot_fingerprint;
  struct ParseTimeVariables {
    // Members of this struct are only valid during the parsing of
    // VectorSearchParameters on the mainthread. They get cleared
    // at the end of the parse to ensure no dangling pointers.
    absl::string_view query_string;
    absl::string_view score_as_string;
    absl::string_view query_vector_string;
    absl::string_view k_string;
    absl::string_view ef_string;
    //
    // A Map of param names to values. The target of the map is a pair
    // that is the string of the value AND a reference count so that we can
    // detect unused parameters.
    // Marked mutable so that const parsing functions can bump the ref-count
    mutable absl::flat_hash_map<absl::string_view,
                                std::pair<int, absl::string_view>>
        params;
    void ClearAtEndOfParse() {
      query_string = absl::string_view();
      score_as_string = absl::string_view();
      query_vector_string = absl::string_view();
      k_string = absl::string_view();
      ef_string = absl::string_view();
      params.clear();
    }
  } parse_vars;
  bool IsNonVectorQuery() const { return attribute_alias.empty(); }
  bool IsVectorQuery() const { return !IsNonVectorQuery(); }
  // Indicates whether the search requires complete results (neighbors/keys) to
  // be able to return correct results. An example of this is when sorting on a
  // particular is needed on the results. This should be overridden in derived
  // classes if needed. The default implementation returns false.
  virtual bool RequiresCompleteResults() const { return false; }

  // In-flight retry completion callbacks. Override in derived classes to
  // handle completion of searches that were blocked waiting for in-flight
  // mutations. Default implementations do nothing (for basic SearchParameters
  // used in retry contexts).
  virtual void OnComplete(std::vector<indexes::Neighbor>& neighbors) {}
  virtual void OnCancelled() {}
  virtual std::vector<indexes::Neighbor>& GetNeighbors() {
    static std::vector<indexes::Neighbor> empty;
    return empty;
  }
  // Description for debugging in-flight retry code paths.
  virtual const char* GetDesc() const { return "base"; }
  virtual SearchParameters& GetParameters() { return *this; }

  SearchParameters(uint64_t timeout, grpc::CallbackServerContext* context,
                   uint32_t db_num)
      : timeout_ms(timeout),
        cancellation_token(cancel::Make(timeout, context)),
        db_num_(db_num) {}

  SearchParameters(SearchParameters&&) = default;
};

// Indicates the range of neighbors to serialize in a search response.
struct SerializationRange {
  size_t start_index;
  size_t end_index;
  size_t count() const { return end_index - start_index; }
};

// Wrapper for search results that trims the neighbor deque based on query type
struct SearchResult {
  size_t total_count;
  std::vector<indexes::Neighbor> neighbors;
  // True if neighbors were limited using LIMIT count with a buffer multiplier.
  bool is_limited_with_buffer;
  // True if neighbors were offset using LIMIT first_index.
  bool is_offsetted;

  // Constructor with automatic trimming based on query requirements
  SearchResult(size_t total_count, std::vector<indexes::Neighbor> neighbors,
               const SearchParameters& parameters);
  // Get the range of neighbors to serialize in response.
  SerializationRange GetSerializationRange(
      const SearchParameters& parameters) const;

 private:
  bool RetainAllNeighbors(const SearchParameters& parameters);
  void TrimResults(std::vector<indexes::Neighbor>& neighbors,
                   const SearchParameters& parameters);
};

// Callback to be called when the search is done.
using SearchResponseCallback = absl::AnyInvocable<void(
    absl::StatusOr<SearchResult>&, std::unique_ptr<SearchParameters>)>;

absl::StatusOr<SearchResult> Search(const SearchParameters& parameters,
                                    SearchMode search_mode);

absl::Status SearchAsync(std::unique_ptr<SearchParameters> parameters,
                         vmsdk::ThreadPool* thread_pool,
                         SearchResponseCallback callback,
                         SearchMode search_mode);

absl::StatusOr<std::vector<indexes::Neighbor>> MaybeAddIndexedContent(
    absl::StatusOr<std::vector<indexes::Neighbor>> results,
    const SearchParameters& parameters);

class Predicate;
// Defined in the header to support testing
size_t EvaluateFilterAsPrimary(
    const Predicate* predicate,
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>>& entries_fetchers,
    bool negate, QueryOperations query_operations);

// Defined in the header to support testing
absl::StatusOr<std::vector<indexes::Neighbor>> PerformVectorSearch(
    indexes::VectorBase* vector_index, const SearchParameters& parameters);

std::priority_queue<std::pair<float, hnswlib::labeltype>>
CalcBestMatchingPrefilteredKeys(
    const SearchParameters& parameters,
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>>& entries_fetchers,
    indexes::VectorBase* vector_index, size_t qualified_entries);

bool QueryHasTextPredicate(const SearchParameters& parameters);

// Check if no results should be returned based on limit parameters
bool ShouldReturnNoResults(const SearchParameters& parameters);

}  // namespace valkey_search::query
#endif  // VALKEYSEARCH_SRC_QUERY_SEARCH_H_
