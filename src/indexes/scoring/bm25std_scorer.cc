/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/scoring/bm25std_scorer.h"

#include <cmath>
#include <cstdint>
#include <cstring>

#include "absl/log/check.h"
#include "src/indexes/scoring/scoring_stats.h"

namespace valkey_search::indexes::scoring {

float Bm25StdScorer::PrecomputeIDF(uint32_t total_docs,
                                   uint32_t num_doc_contain_term) const {
  CHECK_LE(num_doc_contain_term, total_docs);
  const float n = static_cast<float>(total_docs);
  const float dt = static_cast<float>(num_doc_contain_term);
  return std::log1pf((n - dt + 0.5f) / (dt + 0.5f));
}

float Bm25StdScorer::ScoreLeaf(float idf, const ScoringStats& stats,
                               float leaf_weight) const {
  const auto* bm25_stats = dynamic_cast<const Bm25StdStats*>(&stats);
  CHECK(bm25_stats != nullptr);

  if (bm25_stats->avg_doc_len <= 0.0f) return 0.0f;

  const float f = static_cast<float>(bm25_stats->term_frequency);
  const float dl = static_cast<float>(bm25_stats->doc_len);

  const float numerator = f * (kK1 + 1.0f);
  const float denominator =
      f + kK1 * (1.0f - kB + kB * dl / bm25_stats->avg_doc_len);
  return leaf_weight * idf * (numerator / denominator);
}

float Bm25StdScorer::ComposeDocumentScore(float sum_of_terms,
                                          float document_score) const {
  // Avoid 0 * inf -> NaN; propagate ±inf as the final score.
  if (IsInf(document_score)) {
    return document_score;
  }
  return sum_of_terms * document_score;
}

}  // namespace valkey_search::indexes::scoring
