/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_SCORING_SLOP_CALCULATOR_H_
#define VALKEYSEARCH_SRC_INDEXES_SCORING_SLOP_CALCULATOR_H_

#include <cstdint>
#include <vector>

#include "absl/types/span.h"

namespace valkey_search::indexes::scoring {

// Term position within a document, matching indexes::text::Position.
using SlopPosition = uint32_t;

// Computes the TFIDF slop for a single (query, doc) pair: the floored
// Euclidean distance across the position gaps between consecutive
// outermost query nodes.
//
//   slop = max(1, floor(sqrt(sum of MinGap(node[i], node[i+1])^2)))
//
// The min-1 guard applies only to the final result; inner gaps are taken
// as-is (so two terms sharing a position contribute a gap of 0).
//
// The caller drives the calculator as a passenger on the per-doc query
// tree walk. The root group is unwrapped by the caller: its direct
// children are the outermost level, so the caller iterates them without
// an enclosing EnterGroup/ExitGroup. Only nested groups fire the group
// hooks; a nested group collapses to the union of its leaf positions and
// contributes a single anchor to its parent level.
//
// Single use: drive one document, call Finalize, then discard.
class SlopCalculator {
 public:
  SlopCalculator() = default;

  // Records a query term occurring at the given sorted positions in the
  // document. An empty span (term absent in this doc) contributes nothing.
  void OnTerm(absl::Span<const SlopPosition> positions);

  // Opens a nested group. Its leaf positions are unioned together and
  // surface to the enclosing level as one anchor on ExitGroup.
  void EnterGroup();

  // Closes the nested group opened by the matching EnterGroup.
  void ExitGroup();

  // Returns the slop for the document. Must be called once, after the walk.
  uint32_t Finalize();

 private:
  // An anchor is one outermost-level node reduced to the positions it
  // occupies. A bare term yields its own positions; a nested group yields
  // the union of its leaves. Gaps are measured between consecutive anchors.
  using Anchor = std::vector<SlopPosition>;

  // Folds a finished union into the enclosing level: a nested group becomes
  // one anchor on its parent, the outermost level appends to `anchors_`.
  void EmitAnchor(Anchor anchor);

  std::vector<Anchor> anchors_;
  std::vector<Anchor> group_stack_;
};

}  // namespace valkey_search::indexes::scoring

#endif  // VALKEYSEARCH_SRC_INDEXES_SCORING_SLOP_CALCULATOR_H_
