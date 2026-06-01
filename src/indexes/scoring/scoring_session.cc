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

ScoringSession::ScoringSession(const Scorer* scorer) : scorer_(scorer) {
  CHECK(scorer_ != nullptr);
  group_stack_.emplace_back();
}

void ScoringSession::RecordLeaf(const ScoringStats& stats, float leaf_weight) {
  CHECK(!group_stack_.empty());

  const float leaf_score = scorer_->ScoreLeaf(stats, leaf_weight);
  group_stack_.back()[stats.doc_id] += leaf_score;

  if (!doc_stats_.contains(stats.doc_id)) {
    doc_stats_.emplace(stats.doc_id, stats.Clone());
  }
}

void ScoringSession::EnterGroup() {
  CHECK(!group_stack_.empty());
  group_stack_.emplace_back();
}

void ScoringSession::ExitGroup(float group_weight) {
  CHECK_GE(group_stack_.size(), 2u);

  auto inner = std::move(group_stack_.back());
  group_stack_.pop_back();

  auto& outer = group_stack_.back();
  for (const auto& [doc_id, partial] : inner) {
    outer[doc_id] += group_weight * partial;
  }
}

std::vector<RankedDoc> ScoringSession::Rank() {
  CHECK_EQ(group_stack_.size(), 1u);

  auto root = std::move(group_stack_.back());
  group_stack_.clear();

  std::vector<RankedDoc> results;
  results.reserve(root.size());
  for (const auto& [doc_id, sum] : root) {
    auto stats_it = doc_stats_.find(doc_id);
    CHECK(stats_it != doc_stats_.end());
    const float final_score =
        scorer_->ComposeDocumentScore(sum, *stats_it->second);
    results.push_back({doc_id, final_score});
  }
  doc_stats_.clear();

  std::sort(results.begin(), results.end(),
            [](const RankedDoc& a, const RankedDoc& b) {
              if (a.score != b.score) return a.score > b.score;
              return a.doc_id < b.doc_id;
            });
  return results;
}

}  // namespace valkey_search::indexes::scoring
