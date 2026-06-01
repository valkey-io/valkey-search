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

using DocId = uint64_t;

struct RankedDoc {
  DocId doc_id;
  float score;
};

// Per-query collector and ranker. Driven by a depth-first walk of the
// predicate tree:
//   ScoringSession session(scorer);
//   for each node (depth-first):
//     leaf:        session.RecordLeaf(stats, leaf_weight);
//     group enter: session.EnterGroup();
//     group exit:  session.ExitGroup(group_weight);
//   auto results = session.Rank();
//
// Doc-level fields in `stats` (e.g. document_score) must be invariant
// across all leaves recorded for the same doc_id. Rank() returns every
// candidate sorted by score desc, doc_id asc; LIMIT is applied by the
// caller.
class ScoringSession {
 public:
  // The Scorer is borrowed and must outlive this session.
  explicit ScoringSession(const Scorer* scorer);

  void RecordLeaf(const ScoringStats& stats, float leaf_weight);

  void EnterGroup();
  void ExitGroup(float group_weight);

  // Must be called exactly once, after all groups have been exited.
  std::vector<RankedDoc> Rank();

 private:
  const Scorer* scorer_;

  std::vector<absl::flat_hash_map<DocId, float>> group_stack_;

  absl::flat_hash_map<DocId, std::unique_ptr<ScoringStats>> doc_stats_;
};

}  // namespace valkey_search::indexes::scoring

#endif
