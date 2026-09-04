/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "src/indexes/vector_base.h"

namespace valkey_search::query::rank_fusion {

// Per-arm input to the fusion stage. The fusion functions consume the
// neighbors by reference but do not modify them; the caller retains ownership.
//
// Every arm's `Neighbor::score` must already be higher-is-better by the time it
// reaches fusion. A text arm's BM25 value is; a pure vector arm's raw distance
// is not, and the caller converts it to a similarity first (see
// ConvertVectorArmScoresToSimilarity in ft_hybrid.cc).
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
// Returns the merged neighbors sorted by descending fused score, stored in
// Neighbor::score (higher = better). Neighbor::distance is set to the same
// value.
//
// For each arm whose `score_alias` is set, the surviving fused neighbor's
// `attribute_contents` map gains a key/value pair: alias -> that arm's raw
// Neighbor::score (formatted as "%.12g"). Docs absent from a given arm have no
// entry for that arm's alias.
std::vector<indexes::Neighbor> RRF(std::vector<ArmInput> arms);

// Linear combination: sum `weight_i * score_i` per doc over the arms, using
// each arm's raw Neighbor::score as it stands. Missing-from-arm contributes 0
// to that arm's term. There is deliberately no per-arm normalization — see
// Linear in rank_fusion.cc.
//
// Score storage and `score_alias` propagation behave identically to RRF.
std::vector<indexes::Neighbor> Linear(std::vector<ArmInput> arms);

// User-defined combination (COMBINE FUNCTION). Builds the union of documents
// across arms, gathers each document's per-arm raw scores (the arm's
// Neighbor::score, or nullopt where the document did not appear in that arm),
// and calls `score_fn` to compute the combined score. `score_fn` receives a
// vector indexed by arm position. Score storage (Neighbor::score, higher =
// better) and `score_alias` propagation behave identically to RRF.
//
// `score_fn` is kept as a std::function so the fusion library stays free of any
// dependency on the expression compiler — the caller (ft_hybrid.cc) supplies a
// closure that evaluates the compiled COMBINE FUNCTION expression.
std::vector<indexes::Neighbor> Function(
    std::vector<ArmInput> arms,
    const std::function<double(const std::vector<std::optional<double>>&)>&
        score_fn);

}  // namespace valkey_search::query::rank_fusion
