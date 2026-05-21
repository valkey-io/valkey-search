/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/scoring/scoring_session.h"

#include <algorithm>

#include "absl/log/check.h"

namespace valkey_search::indexes::scoring {

// Initialize the session with a single root scope on the group stack.
// Leaves recorded outside any EnterGroup/ExitGroup pair land here, and
// after a balanced sequence of group calls only this scope remains —
// it holds the per-doc sums fed into ComposeDocumentScore at Rank().
ScoringSession::ScoringSession(const Scorer* scorer) : scorer_(scorer) {
  CHECK(scorer_ != nullptr);
  group_stack_.emplace_back();
}

// Record one (query leaf, candidate document) match into the
// innermost group scope. Computes the leaf's contribution via the
// Scorer and adds it to that doc's running sum within the scope.
void ScoringSession::RecordLeaf(const ScoringStats& stats,
                                double leaf_weight) {
  CHECK(!group_stack_.empty());

  const double leaf_score = scorer_->ScoreLeaf(stats, leaf_weight);
  group_stack_.back()[stats.doc_id] += leaf_score;

  // Remember the stats for ComposeDocumentScore. First write wins;
  // subsequent records for the same doc must agree on doc-level fields
  // (document_score, etc.), which is the caller's contract.
  doc_stats_.try_emplace(stats.doc_id, &stats);
}

// Push a fresh scope onto the group stack. Subsequent RecordLeaf
// calls accumulate into the new scope until the matching ExitGroup
// folds it back into the parent.
void ScoringSession::EnterGroup() {
  CHECK(!group_stack_.empty());
  group_stack_.emplace_back();
}

// Close the innermost group scope and merge it into its parent,
// scaling each per-doc partial by group_weight first. This is what
// makes layered weights compose multiplicatively across nested
// groups.
void ScoringSession::ExitGroup(double group_weight) {
  CHECK_GE(group_stack_.size(), 2u);

  auto inner = std::move(group_stack_.back());
  group_stack_.pop_back();

  auto& outer = group_stack_.back();
  for (const auto& [doc_id, partial] : inner) {
    outer[doc_id] += group_weight * partial;
  }
}

// Finalize the session: take every doc's accumulated sum from the
// root scope, apply ComposeDocumentScore, and return the docs sorted
// by score descending (doc_id ascending breaks ties). The session is
// consumed by this call — subsequent RecordLeaf/EnterGroup/ExitGroup
// will fail.
std::vector<RankedDoc> ScoringSession::Rank() {
  CHECK_EQ(group_stack_.size(), 1u);

  auto root = std::move(group_stack_.back());
  group_stack_.clear();

  std::vector<RankedDoc> results;
  results.reserve(root.size());
  for (const auto& [doc_id, sum] : root) {
    auto stats_it = doc_stats_.find(doc_id);
    CHECK(stats_it != doc_stats_.end());
    const double final_score =
        scorer_->ComposeDocumentScore(sum, *stats_it->second);
    results.push_back({doc_id, final_score});
  }
  doc_stats_.clear();

  // Sort by score descending; tie-break by doc_id ascending for
  // deterministic ordering.
  std::sort(results.begin(), results.end(),
            [](const RankedDoc& a, const RankedDoc& b) {
              if (a.score != b.score) return a.score > b.score;
              return a.doc_id < b.doc_id;
            });
  return results;
}

}  // namespace valkey_search::indexes::scoring
