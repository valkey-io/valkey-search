/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_SCORING_SCORING_SESSION_H_
#define VALKEYSEARCH_SRC_INDEXES_SCORING_SCORING_SESSION_H_

#include <vector>

#include "absl/container/flat_hash_map.h"
#include "src/indexes/scoring/scorer.h"
#include "src/indexes/scoring/scoring_stats.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes::scoring {

struct RankedDoc {
  InternedStringPtr key;
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
// across all leaves recorded for the same key. Rank() returns every
// candidate sorted by score desc, key asc; LIMIT is applied by the
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

  std::vector<absl::flat_hash_map<InternedStringPtr, float>> group_stack_;

  absl::flat_hash_map<InternedStringPtr, float> doc_score_;
};

}  // namespace valkey_search::indexes::scoring

#endif
