/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/scoring/bm25std_scorer.h"

#include <limits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/indexes/scoring/scorer.h"
#include "testing/scoring/scoring_test_data.h"

namespace valkey_search::indexes::scoring {
namespace {

constexpr float kFloatTolerance = 1e-4f;

LeafInput MakeLeaf(uint32_t total_docs, uint64_t total_doc_len,
                   uint32_t num_doc_contain_term, uint32_t term_frequency,
                   uint32_t doc_len) {
  LeafInput in;
  in.total_docs = total_docs;
  in.total_doc_len = total_doc_len;
  in.num_doc_contain_term = num_doc_contain_term;
  in.term_frequency = term_frequency;
  in.doc_len = doc_len;
  return in;
}

// --- Direct scorer math ---

TEST(Bm25StdScorerTest, IdentityNameAndType) {
  Bm25StdScorer scorer;
  EXPECT_EQ(scorer.Name(), "BM25STD");
  EXPECT_EQ(scorer.Type(), ScorerType::kBm25Std);
}

TEST(Bm25StdScorerTest, ScoreLeafCorpusReference) {
  Bm25StdScorer scorer;
  LeafInput leaf = test_data::LeafForHello(test_data::kDocs[4]);
  EXPECT_NEAR(scorer.ScoreLeaf(leaf, 1.0f), 0.574385f, kFloatTolerance);
}

TEST(Bm25StdScorerTest, ScoreLeafLeafWeightScalesLinearly) {
  Bm25StdScorer scorer;
  LeafInput leaf = test_data::LeafForHello(test_data::kDocs[0]);
  const float base = scorer.ScoreLeaf(leaf, 1.0f);
  EXPECT_NEAR(scorer.ScoreLeaf(leaf, 5.0f), 5.0f * base, kFloatTolerance);
  EXPECT_EQ(scorer.ScoreLeaf(leaf, 0.0f), 0.0f);
}

TEST(Bm25StdScorerTest, ScoreLeafZeroFrequencyReturnsZero) {
  Bm25StdScorer scorer;
  LeafInput leaf = test_data::LeafForWorld(test_data::kDocs[4]);
  ASSERT_EQ(leaf.term_frequency, 0u);
  EXPECT_EQ(scorer.ScoreLeaf(leaf, 1.0f), 0.0f);
}

TEST(Bm25StdScorerTest, ScoreLeafEmptyIndexReturnsZero) {
  Bm25StdScorer scorer;
  LeafInput leaf = MakeLeaf(/*N=*/0, /*total_doc_len=*/0,
                            /*dt=*/0, /*F=*/0, /*doc_len=*/0);
  EXPECT_EQ(scorer.ScoreLeaf(leaf, 1.0f), 0.0f);
}

TEST(Bm25StdScorerTest, IdfDifferentiatesByDt) {
  Bm25StdScorer scorer;
  const float hello =
      scorer.ScoreLeaf(test_data::LeafForHello(test_data::kDocs[0]), 1.0f);
  const float rare =
      scorer.ScoreLeaf(test_data::LeafForRare(test_data::kDocs[5]), 1.0f);
  const float unique =
      scorer.ScoreLeaf(test_data::LeafForUnique(test_data::kDocs[5]), 1.0f);
  EXPECT_GT(rare, hello);
  EXPECT_GT(unique, rare);
}

TEST(Bm25StdScorerTest, LengthNormalizationFavorsShorterDoc) {
  Bm25StdScorer scorer;
  const float doc4 =
      scorer.ScoreLeaf(test_data::LeafForHello(test_data::kDocs[3]), 1.0f);
  const float doc5 =
      scorer.ScoreLeaf(test_data::LeafForHello(test_data::kDocs[4]), 1.0f);
  EXPECT_GT(doc5, doc4);
}

TEST(Bm25StdScorerDeathTest, DtGreaterThanNCrashes) {
  Bm25StdScorer scorer;
  LeafInput leaf = MakeLeaf(/*N=*/2, /*total_doc_len=*/20,
                            /*dt=*/3, /*F=*/1, /*doc_len=*/10);
  EXPECT_DEATH(scorer.ScoreLeaf(leaf, 1.0f), "");
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

// Per-document ranking (score-desc / key-asc ordering, document_score
// multiplier, AND/OR accumulation) is exercised end-to-end through
// ScoreTextQuery in testing/query/text_scoring_test.cc and the scoring
// integration suite, since it depends on the predicate-tree walk rather than
// the scorer in isolation.

}  // namespace
}  // namespace valkey_search::indexes::scoring
