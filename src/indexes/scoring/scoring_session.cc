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
  group_stack_.back()[stats.key] += leaf_score;

  doc_score_.try_emplace(stats.key, stats.document_score);
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
  for (const auto& [key, partial] : inner) {
    outer[key] += group_weight * partial;
  }
}

std::vector<RankedDoc> ScoringSession::Rank() {
  CHECK_EQ(group_stack_.size(), 1u);

  auto root = std::move(group_stack_.back());
  group_stack_.clear();

  std::vector<RankedDoc> results;
  results.reserve(root.size());
  for (const auto& [key, sum] : root) {
    auto it = doc_score_.find(key);
    CHECK(it != doc_score_.end());
    const float final_score = scorer_->ComposeDocumentScore(sum, it->second);
    results.push_back({key, final_score});
  }
  doc_score_.clear();

  // if score ties, sort by document key
  std::sort(results.begin(), results.end(),
            [](const RankedDoc& a, const RankedDoc& b) {
              if (a.score != b.score) return a.score > b.score;
              return a.key->Str() < b.key->Str();
            });
  return results;
}

}  // namespace valkey_search::indexes::scoring
