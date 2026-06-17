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
#include "src/indexes/scoring/scorer.h"

namespace valkey_search::indexes::scoring {

namespace {

float Idf(uint32_t total_docs, uint32_t num_doc_contain_term) {
  CHECK_LE(num_doc_contain_term, total_docs);
  const float n = static_cast<float>(total_docs);
  const float dt = static_cast<float>(num_doc_contain_term);
  return std::log1pf((n - dt + 0.5f) / (dt + 0.5f));
}

}  // namespace

float Bm25StdScorer::ScoreLeaf(const LeafInput& input,
                               float leaf_weight) const {
  if (input.total_docs == 0) return 0.0f;
  const float avgdl = static_cast<float>(input.total_doc_len) /
                      static_cast<float>(input.total_docs);
  if (avgdl <= 0.0f) return 0.0f;

  const float idf = Idf(input.total_docs, input.num_doc_contain_term);

  const float f = static_cast<float>(input.term_frequency);
  const float dl = static_cast<float>(input.doc_len);

  const float numerator = f * (kK1 + 1.0f);
  const float denominator = f + kK1 * (1.0f - kB + kB * dl / avgdl);
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
