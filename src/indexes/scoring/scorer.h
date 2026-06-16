/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_SCORING_SCORER_H_
#define VALKEYSEARCH_SRC_INDEXES_SCORING_SCORER_H_

#include <string_view>

#include "absl/types/span.h"
#include "src/indexes/scoring/scoring_stats.h"

namespace valkey_search::indexes::scoring {

// std::isinf is unreliable under -ffast-math (which this code is built with);
// detect ±inf by IEEE 754 bit pattern instead.
bool IsInf(float f);

enum class ScorerType {
  kBm25Std,
  kTfidf,
};

// Stateless, thread-safe scoring algorithm. Per-query state lives in
// ScoringSession. Concrete scorers expect a specific ScoringStats
// subtype and CHECK the contract.
class Scorer {
 public:
  virtual ~Scorer() = default;

  virtual std::string_view Name() const = 0;
  virtual ScorerType Type() const = 0;

  virtual float ScoreLeaf(const ScoringStats& stats,
                          float leaf_weight) const = 0;

  virtual float ComposeDocumentScore(float sum_of_terms,
                                     float document_score) const = 0;

  float CombineGroup(absl::Span<const float> child_scores,
                     float group_weight) const;
};

const Scorer* GetScorer(ScorerType type);

}  // namespace valkey_search::indexes::scoring

#endif
