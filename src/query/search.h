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
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"
#include "src/query/predicate.h"
#include "src/utils/cancel.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/thread_pool.h"

namespace valkey_search::query {

constexpr int64_t kTimeoutMS{50000};
const size_t kMaxTimeoutMs{60000};

struct LimitParameter {
  uint64_t first_index{0};
  uint64_t number{10};
};

struct ReturnAttribute {
  vmsdk::UniqueValkeyString identifier;
  vmsdk::UniqueValkeyString attribute_alias;
  vmsdk::UniqueValkeyString alias;
};

struct VectorSearchParameters {
  mutable cancel::Token cancellation_token;
  std::shared_ptr<IndexSchema> index_schema;
  std::string index_schema_name;
  std::string attribute_alias;
  vmsdk::UniqueValkeyString score_as;
  std::string query;
  uint32_t dialect{2};
  bool local_only{false};
  int k{0};
  std::optional<unsigned> ef;
  LimitParameter limit;
  uint64_t timeout_ms;
  bool no_content{false};
  FilterParseResults filter_parse_results;
  std::vector<ReturnAttribute> return_attributes;
  struct ParseTimeVariables {
    // Members of this struct are only valid during the parsing of
    // VectorSearchParameters on the mainthread. They get cleared
    // at the end of the parse to ensure no dangling pointers.
    absl::string_view query_string;
    absl::string_view score_as_string;
    absl::flat_hash_map<absl::string_view, std::pair<int, absl::string_view>>
        params;
    void ClearAtEndOfParse() {
      query_string = absl::string_view();
      score_as_string = absl::string_view();
      assert(params.empty());
    }
  } parse_vars;
  bool IsNonVectorQuery() const {
    return attribute_alias.empty();
  }
  VectorSearchParameters(uint64_t timeout, grpc::CallbackServerContext *context)
      : timeout_ms(timeout), cancellation_token(cancel::Make(timeout, context)) {}
};

// Callback to be called when the search is done.
using SearchResponseCallback =
    absl::AnyInvocable<void(absl::StatusOr<std::deque<indexes::Neighbor>>&,
                            std::unique_ptr<VectorSearchParameters>)>;

absl::StatusOr<std::deque<indexes::Neighbor>> Search(
    const VectorSearchParameters& parameters, bool is_local_search);

absl::Status SearchAsync(std::unique_ptr<VectorSearchParameters> parameters,
                         vmsdk::ThreadPool* thread_pool,
                         SearchResponseCallback callback, bool is_local_search);

absl::StatusOr<std::deque<indexes::Neighbor>> MaybeAddIndexedContent(
    absl::StatusOr<std::deque<indexes::Neighbor>> results,
    const VectorSearchParameters& parameters);

class Predicate;
// Defined in the header to support testing
size_t EvaluateFilterAsPrimary(
    const Predicate* predicate,
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>>& entries_fetchers,
    bool negate);

// Defined in the header to support testing
absl::StatusOr<std::deque<indexes::Neighbor>> PerformVectorSearch(
    indexes::VectorBase* vector_index,
    const VectorSearchParameters& parameters);

std::priority_queue<std::pair<float, hnswlib::labeltype>>
CalcBestMatchingPrefilteredKeys(
    const VectorSearchParameters& parameters,
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>>& entries_fetchers,
    indexes::VectorBase* vector_index);

}  // namespace valkey_search::query
#endif  // VALKEYSEARCH_SRC_QUERY_SEARCH_H_
