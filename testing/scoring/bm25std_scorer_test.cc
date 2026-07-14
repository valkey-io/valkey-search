/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/scoring/bm25std_scorer.h"

#include <limits>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/indexes/scoring/scorer.h"
#include "src/indexes/scoring/scoring_stats.h"
#include "testing/scoring/scoring_test_data.h"

namespace valkey_search::indexes::scoring {
namespace {

constexpr float kFloatTolerance = 1e-4f;

Bm25StdStats MakeStats(uint32_t total_docs, float avg_doc_len,
                       uint32_t num_doc_contain_term, uint32_t term_frequency,
                       uint32_t doc_len, float document_score = 1.0f) {
  Bm25StdStats s;
  s.total_docs = total_docs;
  s.avg_doc_len = avg_doc_len;
  s.num_doc_contain_term = num_doc_contain_term;
  s.term_frequency = term_frequency;
  s.doc_len = doc_len;
  s.document_score = document_score;
  return s;
}

// Precomputes the IDF from the stats, then scores the leaf. The direct-math
// tests use it to score a single stats object in one call.
float ScoreLeaf(const Scorer& scorer, const ScoringStats& stats,
                float leaf_weight) {
  const float idf =
      scorer.PrecomputeIDF(stats.total_docs, stats.num_doc_contain_term);
  return scorer.ScoreLeaf(idf, stats, leaf_weight);
}

// --- Direct scorer math ---

TEST(Bm25StdScorerTest, IdentityNameAndType) {
  Bm25StdScorer scorer;
  EXPECT_EQ(scorer.Name(), "BM25STD");
  EXPECT_EQ(scorer.Type(), ScorerType::kBm25Std);
}

TEST(Bm25StdScorerTest, ScoreLeafCorpusReference) {
  Bm25StdScorer scorer;
  Bm25StdStats stats = test_data::StatsForHello(test_data::kDocs[4]);
  EXPECT_NEAR(ScoreLeaf(scorer, stats, 1.0f), 0.574385f, kFloatTolerance);
}

TEST(Bm25StdScorerTest, ScoreLeafLeafWeightScalesLinearly) {
  Bm25StdScorer scorer;
  Bm25StdStats stats = test_data::StatsForHello(test_data::kDocs[0]);
  const float base = ScoreLeaf(scorer, stats, 1.0f);
  EXPECT_NEAR(ScoreLeaf(scorer, stats, 5.0f), 5.0f * base, kFloatTolerance);
  EXPECT_EQ(ScoreLeaf(scorer, stats, 0.0f), 0.0f);
}

TEST(Bm25StdScorerTest, ScoreLeafZeroFrequencyReturnsZero) {
  Bm25StdScorer scorer;
  Bm25StdStats stats = test_data::StatsForWorld(test_data::kDocs[4]);
  ASSERT_EQ(stats.term_frequency, 0u);
  EXPECT_EQ(ScoreLeaf(scorer, stats, 1.0f), 0.0f);
}

TEST(Bm25StdScorerTest, ScoreLeafEmptyIndexReturnsZero) {
  Bm25StdScorer scorer;
  Bm25StdStats stats = MakeStats(/*N=*/0, /*avg_doc_len=*/0.0f,
                                 /*dt=*/0, /*F=*/0, /*doc_len=*/0);
  EXPECT_EQ(ScoreLeaf(scorer, stats, 1.0f), 0.0f);
}

TEST(Bm25StdScorerTest, IdfDifferentiatesByDt) {
  Bm25StdScorer scorer;
  const float hello =
      ScoreLeaf(scorer, test_data::StatsForHello(test_data::kDocs[0]), 1.0f);
  const float rare =
      ScoreLeaf(scorer, test_data::StatsForRare(test_data::kDocs[5]), 1.0f);
  const float unique =
      ScoreLeaf(scorer, test_data::StatsForUnique(test_data::kDocs[5]), 1.0f);
  EXPECT_GT(rare, hello);
  EXPECT_GT(unique, rare);
}

TEST(Bm25StdScorerTest, LengthNormalizationFavorsShorterDoc) {
  Bm25StdScorer scorer;
  const float doc4 =
      ScoreLeaf(scorer, test_data::StatsForHello(test_data::kDocs[3]), 1.0f);
  const float doc5 =
      ScoreLeaf(scorer, test_data::StatsForHello(test_data::kDocs[4]), 1.0f);
  EXPECT_GT(doc5, doc4);
}

TEST(Bm25StdScorerDeathTest, DtGreaterThanNCrashes) {
  Bm25StdScorer scorer;
  Bm25StdStats stats = MakeStats(/*N=*/2, /*avg_doc_len=*/10.0f,
                                 /*dt=*/3, /*F=*/1, /*doc_len=*/10);
  EXPECT_DEATH(ScoreLeaf(scorer, stats, 1.0f), "");
}

TEST(Bm25StdScorerDeathTest, WrongStatsSubtypeCrashes) {
  Bm25StdScorer scorer;
  ScoringStats base_stats;
  base_stats.total_docs = 10;
  base_stats.num_doc_contain_term = 1;
  base_stats.term_frequency = 1;
  EXPECT_DEATH(ScoreLeaf(scorer, base_stats, 1.0f), "");
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

TEST(Bm25StdScorerTest, CombineGroupAppliesWeight) {
  Bm25StdScorer scorer;
  std::vector<float> children = {1.0f, 2.0f, 3.0f};
  EXPECT_NEAR(scorer.CombineGroup(children, /*group_weight=*/2.0f), 12.0f,
              kFloatTolerance);
}

TEST(Bm25StdScorerTest, CombineGroupEmptyChildrenIsZero) {
  Bm25StdScorer scorer;
  EXPECT_EQ(scorer.CombineGroup({}, 5.0f), 0.0f);
}

}  // namespace
}  // namespace valkey_search::indexes::scoring
