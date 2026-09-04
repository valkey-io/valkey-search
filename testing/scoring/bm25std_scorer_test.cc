/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/scoring/bm25std_scorer.h"

#include <cstdint>
#include <cstring>
#include <limits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/indexes/scoring/scorer.h"
#include "testing/scoring/scoring_test_data.h"

namespace valkey_search::indexes::scoring {
namespace {

constexpr float kFloatTolerance = 1e-4f;

using test_data::LeafData;

LeafData MakeLeaf(uint32_t total_docs, uint64_t total_doc_len,
                  uint32_t num_doc_contain_term, uint32_t term_frequency,
                  uint32_t doc_len) {
  LeafData in;
  in.total_docs = total_docs;
  in.total_doc_len = total_doc_len;
  in.num_doc_contain_term = num_doc_contain_term;
  in.term_frequency = term_frequency;
  in.doc_len = doc_len;
  return in;
}

// Precomputes the IDF from the leaf, then scores it via the scalar ScoreLeaf.
// The production path precomputes the IDF once per term; tests recompute per
// call.
float ScoreLeaf(const Bm25StdScorer& scorer, const LeafData& leaf,
                float leaf_weight) {
  const float idf =
      scorer.PrecomputeIDF({leaf.total_docs, leaf.num_doc_contain_term});
  const float avg_doc_len = leaf.total_docs > 0
                                ? static_cast<float>(leaf.total_doc_len) /
                                      static_cast<float>(leaf.total_docs)
                                : 0.0f;
  return scorer.ScoreLeaf(
      {idf, leaf.term_frequency, leaf.doc_len, avg_doc_len, leaf_weight});
}

// --- Direct scorer math ---

TEST(Bm25StdScorerTest, IdentityNameAndType) {
  Bm25StdScorer scorer;
  EXPECT_EQ(scorer.Name(), "BM25STD");
  EXPECT_EQ(scorer.Type(), ScorerType::kBm25Std);
}

TEST(Bm25StdScorerTest, ScoreLeafCorpusReference) {
  Bm25StdScorer scorer;
  LeafData leaf = test_data::LeafForHello(test_data::kDocs[4]);
  EXPECT_NEAR(ScoreLeaf(scorer, leaf, 1.0f), 0.574385f, kFloatTolerance);
}

TEST(Bm25StdScorerTest, ScoreLeafLeafWeightScalesLinearly) {
  Bm25StdScorer scorer;
  LeafData leaf = test_data::LeafForHello(test_data::kDocs[0]);
  const float base = ScoreLeaf(scorer, leaf, 1.0f);
  EXPECT_NEAR(ScoreLeaf(scorer, leaf, 5.0f), 5.0f * base, kFloatTolerance);
  EXPECT_EQ(ScoreLeaf(scorer, leaf, 0.0f), 0.0f);
}

TEST(Bm25StdScorerTest, ScoreLeafZeroFrequencyReturnsZero) {
  Bm25StdScorer scorer;
  LeafData leaf = test_data::LeafForWorld(test_data::kDocs[4]);
  ASSERT_EQ(leaf.term_frequency, 0u);
  EXPECT_EQ(ScoreLeaf(scorer, leaf, 1.0f), 0.0f);
}

TEST(Bm25StdScorerTest, ScoreLeafEmptyIndexReturnsZero) {
  Bm25StdScorer scorer;
  LeafData leaf = MakeLeaf(/*N=*/0, /*total_doc_len=*/0,
                           /*dt=*/0, /*F=*/0, /*doc_len=*/0);
  EXPECT_EQ(ScoreLeaf(scorer, leaf, 1.0f), 0.0f);
}

TEST(Bm25StdScorerTest, IdfDifferentiatesByDt) {
  Bm25StdScorer scorer;
  const float hello =
      ScoreLeaf(scorer, test_data::LeafForHello(test_data::kDocs[0]), 1.0f);
  const float rare =
      ScoreLeaf(scorer, test_data::LeafForRare(test_data::kDocs[5]), 1.0f);
  const float unique =
      ScoreLeaf(scorer, test_data::LeafForUnique(test_data::kDocs[5]), 1.0f);
  EXPECT_GT(rare, hello);
  EXPECT_GT(unique, rare);
}

TEST(Bm25StdScorerTest, LengthNormalizationFavorsShorterDoc) {
  Bm25StdScorer scorer;
  const float doc4 =
      ScoreLeaf(scorer, test_data::LeafForHello(test_data::kDocs[3]), 1.0f);
  const float doc5 =
      ScoreLeaf(scorer, test_data::LeafForHello(test_data::kDocs[4]), 1.0f);
  EXPECT_GT(doc5, doc4);
}

// dt > total_docs violates the IDF precondition, but callers clamp to uphold
// it (dt and total_docs are read from separate, independently-locked counters
// and can be transiently out of sync). The scorer must not abort production on
// this transient skew: the guard is debug-only (DCHECK), so it dies in debug
// builds but is tolerated in release.
TEST(Bm25StdScorerDeathTest, DtGreaterThanNIsDebugOnly) {
  Bm25StdScorer scorer;
  LeafData leaf = MakeLeaf(/*N=*/2, /*total_doc_len=*/20,
                           /*dt=*/3, /*F=*/1, /*doc_len=*/10);
  EXPECT_DEBUG_DEATH(ScoreLeaf(scorer, leaf, 1.0f), "");
}

TEST(Bm25StdScorerTest, ComposeMultipliesByDocumentScore) {
  Bm25StdScorer scorer;
  EXPECT_NEAR(scorer.ComposeDocumentScore(0.5f, /*document_score=*/0.7f),
              0.5f * 0.7f, kFloatTolerance);
}

TEST(Bm25StdScorerTest, ComposeInfinityShortCircuits) {
  Bm25StdScorer scorer;
  const float kInf = std::numeric_limits<float>::infinity();
  EXPECT_EQ(scorer.ComposeDocumentScore(0.5f, kInf), kInf);
  EXPECT_EQ(scorer.ComposeDocumentScore(0.5f, -kInf), -kInf);
}

// The mirror of the case above. An infinite sum_of_terms times a zero document
// score is the other way to reach 0 * inf -> NaN, and a NaN score is worse than
// wrong: SearchResult::TrimResults sorts on it, and NaN compares false against
// everything, which violates strict weak ordering and makes std::sort
// undefined.
TEST(Bm25StdScorerTest, ComposeInfiniteSumWithZeroDocumentScoreIsNotNaN) {
  Bm25StdScorer scorer;
  const float kInf = std::numeric_limits<float>::infinity();
  EXPECT_FALSE(IsNaN(scorer.ComposeDocumentScore(kInf, 0.0f)));
  EXPECT_EQ(scorer.ComposeDocumentScore(kInf, 0.0f), kInf);
  EXPECT_FALSE(IsNaN(scorer.ComposeDocumentScore(-kInf, 0.0f)));
  EXPECT_EQ(scorer.ComposeDocumentScore(-kInf, 0.0f), -kInf);
}

// IsInf/IsNaN exist because the build uses -ffast-math (-ffinite-math-only),
// under which the compiler assumes no inf/NaN and may fold std::isinf,
// std::isnan and the `f != f` idiom to a constant. Construct the NaN from its
// bit pattern so the test does not depend on the optimizer preserving one
// either.
TEST(ScorerFloatClassificationTest, DetectsInfAndNaNBitPatterns) {
  static constexpr uint32_t kQuietNaNBits = 0x7FC00000U;
  float nan_value;
  std::memcpy(&nan_value, &kQuietNaNBits, sizeof(nan_value));
  EXPECT_TRUE(IsNaN(nan_value));
  EXPECT_FALSE(IsInf(nan_value));

  const float kInf = std::numeric_limits<float>::infinity();
  EXPECT_TRUE(IsInf(kInf));
  EXPECT_TRUE(IsInf(-kInf));
  EXPECT_FALSE(IsNaN(kInf));

  for (float finite : {0.0f, -0.0f, 1.5f, std::numeric_limits<float>::max()}) {
    EXPECT_FALSE(IsInf(finite)) << finite;
    EXPECT_FALSE(IsNaN(finite)) << finite;
  }
}

// Per-document ranking (score-desc / key-asc ordering, document_score
// multiplier, AND/OR accumulation) is exercised end-to-end through the scoring
// integration suite (integration/test_scoring.py), since it depends on the
// predicate-tree walk rather than the scorer in isolation.

}  // namespace
}  // namespace valkey_search::indexes::scoring
