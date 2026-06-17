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

namespace valkey_search::indexes::scoring {

// Standard Okapi BM25 ("BM25STD"). k1=1.2, b=0.75.
//
//   IDF       = ln(1 + (N - dt + 0.5) / (dt + 0.5))
//   bm25_leaf = leaf_weight * IDF *
//               F * (k1 + 1) /
//               (F + k1 * (1 - b + b * doc_len / avg_doc_len))
//   final     = sum_of_leaves * document_score
class Bm25StdScorer : public Scorer {
 public:
  static constexpr std::string_view kName = "BM25STD";
  static constexpr float kK1 = 1.2f;
  static constexpr float kB = 0.75f;

  std::string_view Name() const override { return kName; }
  ScorerType Type() const override { return ScorerType::kBm25Std; }

  float ScoreLeaf(const LeafInput& input, float leaf_weight) const override;

  float ComposeDocumentScore(float sum_of_terms,
                             float document_score) const override;
};

}  // namespace valkey_search::indexes::scoring

#endif
