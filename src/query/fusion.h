/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_FUSION_H_
#define VALKEYSEARCH_SRC_QUERY_FUSION_H_

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "src/indexes/vector_base.h"

namespace valkey_search::query::fusion {

// Per-arm input to the fusion stage. The fusion functions consume the
// neighbors by reference but do not modify them; the caller retains ownership.
//
// `score_alias`, when set, names a per-arm score field. For each arm, every
// surviving fused neighbor that originated in (or matched) that arm has its
// raw arm score added to `Neighbor::attribute_contents` under this key. This
// makes per-arm scores reachable from the aggregate pipeline (APPLY, FILTER,
// SORTBY).
struct ArmInput {
  const std::vector<indexes::Neighbor>* neighbors;  // owned by caller
  std::optional<std::string> score_alias;
  // For LINEAR: per-arm weight (alpha for arm 0, beta for arm 1, ...).
  double weight = 1.0;
  // For RRF: arm-level RRF constant. The spec uses one global constant; the
  // per-arm field allows future variants without reshaping the API.
  uint32_t rrf_constant = 60;
  // For both RRF and LINEAR: window cap on this arm's contribution. Only the
  // top `window` neighbors (by the arm's pre-fusion order — vector arms are
  // sorted ascending by distance) participate in fusion. 0 = unlimited.
  uint32_t window = 0;
};

// Reciprocal Rank Fusion: per arm, walk top `window` neighbors and add
// `1 / (rrf_constant + rank+1)` to a hash-map accumulator keyed by external_id.
// Returns the merged neighbors sorted by descending fused score (stored in
// Neighbor::distance — overloaded to mean "score, higher=better" for fused
// results, the OPPOSITE of the vector-distance convention).
//
// For each arm whose `score_alias` is set, the surviving fused neighbor's
// `attribute_contents` map gains a key/value pair: alias -> raw arm distance
// (formatted as "%.12g"). Docs absent from a given arm have no entry for that
// arm's alias.
std::vector<indexes::Neighbor> FuseRRF(std::vector<ArmInput> arms);

// Linear combination: for each arm, min-max normalize the arm's distances to
// [0,1] (lower distance -> higher normalized score), then sum
// `weight_i * normalized_i` per doc. Missing-from-arm contributes 0 to that
// arm's term.
//
// Score storage and `score_alias` propagation behave identically to FuseRRF.
std::vector<indexes::Neighbor> FuseLinear(std::vector<ArmInput> arms);

// User-defined combination (COMBINE FUNCTION). Builds the union of documents
// across arms, gathers each document's per-arm raw scores (the arm distance,
// or nullopt where the document did not appear in that arm), and calls
// `score_fn` to compute the combined score. `score_fn` receives a vector
// indexed by arm position. Score storage (Neighbor::distance, higher = better)
// and `score_alias` propagation behave identically to FuseRRF.
//
// `score_fn` is kept as a std::function so the fusion library stays free of any
// dependency on the expression compiler — the caller (ft_hybrid.cc) supplies a
// closure that evaluates the compiled COMBINE FUNCTION expression.
std::vector<indexes::Neighbor> FuseFunction(
    std::vector<ArmInput> arms,
    const std::function<double(const std::vector<std::optional<double>>&)>&
        score_fn);

}  // namespace valkey_search::query::fusion

#endif  // VALKEYSEARCH_SRC_QUERY_FUSION_H_
