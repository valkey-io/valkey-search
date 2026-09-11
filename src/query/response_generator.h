/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_RESPONSE_GENERATOR_H_
#define VALKEYSEARCH_SRC_QUERY_RESPONSE_GENERATOR_H_

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "src/attribute_data_type.h"
#include "src/indexes/vector_base.h"
#include "src/query/search.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::options {

/// Return the configuration entry that allows the caller to control the
/// maximum content size for a record in the search response
vmsdk::config::Number &GetMaxSearchResultRecordSize();

/// Return the configuration entry that allows the caller to control the
/// maximum number of fields in the content of the search response
vmsdk::config::Number &GetMaxSearchResultFieldsCount();

}  // namespace valkey_search::options
namespace valkey_search::query {

// Check if this node owns the slot for the given key in cluster mode
bool CheckSlotOwnership(ValkeyModuleCtx *ctx, absl::string_view key);

// Result of a main-thread content-fetch revalidation of a neighbor.
struct FilterVerification {
  bool matches{false};
  // Present only when the neighbor was reached via the mutation-walk
  // (db_seq != sequence_number) and the caller asked for a score: the
  // document's score recomputed through the same Scorer seam ScoreTextQuery
  // uses, so it is on the same scale as the shard-side score. nullopt on the
  // fast (no-mutation) path and wherever Neighbor.score is a KNN distance,
  // which must never be overwritten with a relevance score.
  std::optional<float> recomputed_score;
};

// Re-checks `n` against the predicate in `parameters` using `records`, the
// content just fetched for it, and recomputes its score when the document
// changed since the search scored it. A document that has not been mutated
// since then returns a match with no recomputed score, which is the common
// case and costs one integer compare.
//
// `recompute_score_override` decides whether a changed document is rescored.
// Left unset it follows the query's own shape, rescoring only a non-vector
// query. FT.HYBRID sets it per arm, because an arm's score is a raw distance
// only when that arm is a pure vector search.
FilterVerification VerifyFilter(
    const query::SearchParameters &parameters, const RecordsMap &records,
    const indexes::Neighbor &n,
    std::unique_ptr<query::SingleDocumentScorer> &document_scorer,
    std::optional<bool> recompute_score_override = std::nullopt);

// Adds all local content for neighbors to the list of neighbors.
// Skipping neighbors if one of the following:
// Neighbor already contained in the attribute content map.
// Neighbor without any attribute content.
// Neighbor not comply to the pre-filter expression.
void ProcessNeighborsForReply(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    std::vector<indexes::Neighbor> &neighbors,
    const query::SearchParameters &parameters,
    const std::optional<std::string> &vector_identifier);

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_RESPONSE_GENERATOR_H_
