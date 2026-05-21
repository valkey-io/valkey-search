/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_SCORING_BM25STD_SCORER_H_
#define VALKEYSEARCH_SRC_INDEXES_SCORING_BM25STD_SCORER_H_

#include <string_view>

#include "src/indexes/scoring/scorer.h"
#include "src/indexes/scoring/scoring_stats.h"

namespace valkey_search::indexes::scoring {

// Standard Okapi BM25 scorer ("BM25STD"). Per-leaf and per-document
// math:
//
//   IDF       = ln(1 + (N - dt + 0.5) / (dt + 0.5))
//   bm25_leaf = leaf_weight * IDF *
//               F * (k1 + 1) /
//               (F + k1 * (1 - b + b * doc_len / avg_doc_len))
//   final     = sum_of_leaves * document_score
//
// k1 = 1.2, b = 0.75 — aligned with Redis 8.6 / OpenSearch / Wikipedia
// standard.
//
// Expects Bm25StdStats as the ScoringStats subtype. Mismatched subtypes
// trigger a DCHECK in debug builds.
class Bm25StdScorer : public Scorer {
 public:
  static constexpr std::string_view kName = "BM25STD";
  static constexpr double kK1 = 1.2;
  static constexpr double kB = 0.75;

  std::string_view Name() const override { return kName; }
  ScorerType Type() const override { return ScorerType::kBm25Std; }

  double ScoreLeaf(const ScoringStats& stats,
                   double leaf_weight) const override;

  double ComposeDocumentScore(double sum_of_terms,
                              const ScoringStats& stats) const override;
};

}  // namespace valkey_search::indexes::scoring

#endif
