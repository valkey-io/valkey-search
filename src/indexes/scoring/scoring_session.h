/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_SCORING_SCORING_SESSION_H_
#define VALKEYSEARCH_SRC_INDEXES_SCORING_SCORING_SESSION_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "src/indexes/scoring/scorer.h"
#include "src/indexes/scoring/scoring_stats.h"

namespace valkey_search::indexes::scoring {

// Opaque per-query document identifier. The integration boundary
// (e.g. TermIterator wiring) is responsible for mapping the index's
// native key type (InternedStringPtr) to and from DocId.
using DocId = uint64_t;

struct RankedDoc {
  DocId doc_id;
  float score;
};

// Per-query collector and ranker. One ScoringSession is created per
// FT.SEARCH invocation, driven by a depth-first walk of the predicate
// tree, and discarded once Rank() has produced the sorted output.
//
// Lifecycle:
//   ScoringSession session(scorer);
//   for each predicate-tree node (depth-first):
//     if leaf:           session.RecordLeaf(stats, leaf_weight);
//     if group enter:    session.EnterGroup();
//     if group exit:     session.ExitGroup(group_weight);
//   auto results = session.Rank();
//
// Group-weight semantics:
//   final = ... w_outer * (... + w_inner * (sum_of_leaves) ...)
//
// The session does not apply LIMIT offset/count slicing. Rank() returns
// every candidate document sorted by score descending; the result-
// collection path applies LIMIT on the returned vector.
class ScoringSession {
 public:
  // The Scorer is borrowed and must outlive this session. Typically a
  // single Scorer instance is shared across all queries in a process.
  explicit ScoringSession(const Scorer* scorer);

  // Record one (query leaf, candidate document) match.
  //
  // The session takes its own copy of `stats` (via ScoringStats::Clone)
  // for the eventual ComposeDocumentScore call, so the caller's
  // instance need not outlive this call. The doc-level fields in
  // `stats` (e.g. document_score) must be invariant across all leaves
  // recorded for the same `stats.doc_id`.
  void RecordLeaf(const ScoringStats& stats, float leaf_weight);

  // Open a new group scope. RecordLeaf calls between EnterGroup() and
  // its matching ExitGroup(w) are accumulated independently; on
  // ExitGroup the per-doc sums are multiplied by w and merged into
  // the enclosing scope. Calls must be balanced.
  void EnterGroup();
  void ExitGroup(float group_weight);

  // Compute final scores via Scorer::ComposeDocumentScore and return
  // every candidate document sorted by score descending. Tied scores
  // are broken by doc_id ascending for deterministic ordering.
  //
  // Must be called exactly once, after all groups have been exited.
  std::vector<RankedDoc> Rank();

 private:
  const Scorer* scorer_;

  // Stack of per-group accumulators (DocId -> running sum). Initialized
  // with a single root scope; EnterGroup pushes, ExitGroup pops after
  // applying the group weight and merging into the parent scope. After
  // a balanced sequence of calls, only the root scope remains and
  // holds the final per-doc sums passed to ComposeDocumentScore.
  std::vector<absl::flat_hash_map<DocId, float>> group_stack_;

  // Per-doc ScoringStats remembered for the ComposeDocumentScore call
  // at Rank() time. The session owns these copies so its lifetime is
  // independent of the caller-supplied stats passed to RecordLeaf.
  absl::flat_hash_map<DocId, std::unique_ptr<ScoringStats>> doc_stats_;
};

}  // namespace valkey_search::indexes::scoring

#endif
