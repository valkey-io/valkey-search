/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_SCORING_BM25STD_SCORER_H_
#define VALKEYSEARCH_SRC_INDEXES_SCORING_BM25STD_SCORER_H_

#include <cstdint>
#include <string_view>

#include "src/indexes/scoring/scorer.h"
#include "src/indexes/scoring/scoring_stats.h"

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

  // IDF = ln(1 + (N - dt + 0.5) / (dt + 0.5)).
  float PrecomputeIDF(uint32_t total_docs,
                      uint32_t num_doc_contain_term) const override;

  // Scores one leaf given a precomputed IDF. Returns 0 for a degenerate corpus
  // (avg_doc_len <= 0). Check stats is a Bm25StdStats.
  float ScoreLeaf(float idf, const ScoringStats& stats,
                  float leaf_weight) const override;

  // Inline, non-virtual scalar fast path for callers that already hold a typed
  // Bm25StdScorer* (e.g. leaf iterators on the per-document hot path). Avoids
  // the dynamic_cast and polymorphic-Bm25StdStats construction the virtual
  // overload above incurs. Returns 0 for a degenerate corpus.
  float ScoreLeaf(float idf, uint32_t term_frequency, uint32_t doc_len,
                  float avg_doc_len, float leaf_weight) const {
    if (avg_doc_len <= 0.0f) return 0.0f;
    const float f = static_cast<float>(term_frequency);
    const float dl = static_cast<float>(doc_len);
    const float numerator = f * (kK1 + 1.0f);
    const float denominator = f + kK1 * (1.0f - kB + kB * dl / avg_doc_len);
    return leaf_weight * idf * (numerator / denominator);
  }

  // Query-invariant BM25 coefficients for one leaf (term + tree weight),
  // precomputed once so the per-document score reduces to a single divide.
  //
  // The leaf score is:
  //     score = leaf_weight * idf * F*(k1+1) / (F + k1*(1 - b + b*dl/avgdl))
  // where only F (term frequency) and dl (doc_len) vary per document; every
  // other input is fixed for the term. Distribute k1 through the denominator:
  //     denom = F + k1*(1-b) + (k1*b/avgdl)*dl
  //             \_____ w1 ____/   \___ w2 ___/
  // and fold the numerator's constant factors into a single weight:
  //     w0 = leaf_weight * idf * (k1+1)
  // Then the per-document score is just:  w0 * F / (F + w1 + w2*dl)
  // which is one multiply-add (w2*dl + w1) and one divide -- the second divide
  // (b*dl/avgdl) is gone because avgdl is baked into w2 here, once per term.
  struct LeafCoeffs {
    float w0 = 0.0f;  // leaf_weight * idf * (k1 + 1)      -- numerator scale
    float w1 =
        0.0f;  // k1 * (1 - b)                      -- constant denom term
    float w2 = 0.0f;  // k1 * b / avg_doc_len              -- per-dl denom slope
    bool valid = false;  // false for a degenerate corpus (avg_doc_len <= 0)
  };

  // Precompute the LeafCoeffs above. Call once per leaf iterator (idf and
  // leaf_weight are term/tree constants; avg_doc_len is corpus-wide).
  LeafCoeffs PrecomputeLeafCoeffs(float idf, float avg_doc_len,
                                  float leaf_weight) const {
    if (avg_doc_len <= 0.0f) return {};
    return LeafCoeffs{
        /*w0=*/leaf_weight * idf * (kK1 + 1.0f),
        /*w1=*/kK1 * (1.0f - kB),
        /*w2=*/kK1 * kB / avg_doc_len,
        /*valid=*/true,
    };
  }

  // Per-document hot path: one multiply-add for the denominator, one divide.
  // Mathematically equivalent (up to float reassociation, already permitted by
  // -ffast-math) to ScoreLeaf() above; used only for relative ranking so a
  // sub-ULP difference cannot reorder results.
  static float ScoreLeafFast(const LeafCoeffs& c, uint32_t term_frequency,
                             uint32_t doc_len) {
    if (!c.valid) return 0.0f;
    const float f = static_cast<float>(term_frequency);
    const float dl = static_cast<float>(doc_len);
    return c.w0 * f / (f + c.w1 + c.w2 * dl);
  }

  float ComposeDocumentScore(float sum_of_terms,
                             float document_score) const override;
};

}  // namespace valkey_search::indexes::scoring

#endif
