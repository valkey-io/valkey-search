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

namespace {

// std::isinf is unreliable under -ffast-math (the build enables it for
// performance — see cmake/Modules/valkey_search.cmake). Detect by IEEE
// 754 bit pattern: exponent bits all 1, mantissa all 0.
bool IsInf(double d) {
  static constexpr uint64_t kExponentMask = 0x7FF0000000000000ULL;
  static constexpr uint64_t kMantissaMask = 0x000FFFFFFFFFFFFFULL;
  uint64_t bits;
  std::memcpy(&bits, &d, sizeof(bits));
  return (bits & kExponentMask) == kExponentMask &&
         (bits & kMantissaMask) == 0;
}

// IDF = ln(1 + (N - dt + 0.5) / (dt + 0.5)).
// Invariant enforced by caller: dt <= N (counter consistency).
double Idf(uint64_t total_docs, uint64_t num_doc_contain_term) {
  CHECK_LE(num_doc_contain_term, total_docs);
  const double n = static_cast<double>(total_docs);
  const double dt = static_cast<double>(num_doc_contain_term);
  return std::log1p((n - dt + 0.5) / (dt + 0.5));
}

}  // namespace

double Bm25StdScorer::ScoreLeaf(const ScoringStats& stats,
                                double leaf_weight) const {
  const auto* bm25_stats = dynamic_cast<const Bm25StdStats*>(&stats);
  CHECK(bm25_stats != nullptr);

  // Empty / pathological index — nothing meaningful to score.
  if (bm25_stats->avg_doc_len <= 0.0) return 0.0;

  const double idf = Idf(bm25_stats->total_docs,
                         bm25_stats->num_doc_contain_term);
  if (idf == 0.0) return 0.0;

  const double f = static_cast<double>(bm25_stats->term_frequency);
  const double dl = static_cast<double>(bm25_stats->doc_len);
  const double avgdl = bm25_stats->avg_doc_len;

  // bm25_leaf = leaf_weight * IDF * F*(k1+1) /
  //             (F + k1*(1 - b + b * dl/avgdl))
  const double numerator = f * (kK1 + 1.0);
  const double denominator =
      f + kK1 * (1.0 - kB + kB * dl / avgdl);
  return leaf_weight * idf * (numerator / denominator);
}

double Bm25StdScorer::ComposeDocumentScore(
    double sum_of_terms, const ScoringStats& stats) const {
  // Short-circuit on infinite document_score: the final score is
  // predetermined regardless of the per-leaf math, and
  // sum_of_terms * ±inf would otherwise yield NaN when sum_of_terms is
  // zero. Both +inf and -inf are propagated as the final score; this
  // implementation does not filter -inf documents out of results.
  if (IsInf(stats.document_score)) {
    return stats.document_score;
  }

  // BM25STD does not clamp negative document_score.
  return sum_of_terms * stats.document_score;
}

}  // namespace valkey_search::indexes::scoring
