/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/scoring/bm25std_scorer.h"

#include <limits>
#include <vector>

#include "gtest/gtest.h"
#include "src/indexes/scoring/scorer.h"
#include "src/indexes/scoring/scoring_stats.h"
#include "testing/scoring/scoring_test_data.h"

namespace valkey_search::indexes::scoring {
namespace {

constexpr double kFloatTolerance = 1e-4;

// Ad-hoc Bm25StdStats builder for edge-case inputs that the shared
// corpus in scoring_test_data.h cannot express (N=0, dt>N).
Bm25StdStats MakeStats(uint64_t total_docs, double avg_doc_len,
                       uint64_t num_doc_contain_term, uint32_t term_frequency,
                       uint32_t doc_len, double document_score = 1.0) {
  Bm25StdStats s;
  s.total_docs = total_docs;
  s.avg_doc_len = avg_doc_len;
  s.num_doc_contain_term = num_doc_contain_term;
  s.term_frequency = term_frequency;
  s.doc_len = doc_len;
  s.document_score = document_score;
  return s;
}

TEST(Bm25StdScorerTest, IdentityNameAndType) {
  Bm25StdScorer scorer;
  EXPECT_EQ(scorer.Name(), "BM25STD");
  EXPECT_EQ(scorer.Type(), ScorerType::kBm25Std);
}

// One absolute-score reference against the corpus (doc:5 hello, F=4,
// dl=4).
TEST(Bm25StdScorerTest, ScoreLeafCorpusReference) {
  Bm25StdScorer scorer;
  Bm25StdStats stats = test_data::StatsForHello(test_data::kDocs[4]);
  EXPECT_NEAR(scorer.ScoreLeaf(stats, 1.0), 0.574385, kFloatTolerance);
}

TEST(Bm25StdScorerTest, ScoreLeafLeafWeightScalesLinearly) {
  Bm25StdScorer scorer;
  Bm25StdStats stats = test_data::StatsForHello(test_data::kDocs[0]);
  const double base = scorer.ScoreLeaf(stats, 1.0);
  EXPECT_NEAR(scorer.ScoreLeaf(stats, 5.0), 5.0 * base, kFloatTolerance);
  EXPECT_EQ(scorer.ScoreLeaf(stats, 0.0), 0.0);
}

// Term absent from this doc (F=0): contributes 0.
TEST(Bm25StdScorerTest, ScoreLeafZeroFrequencyReturnsZero) {
  Bm25StdScorer scorer;
  Bm25StdStats stats = test_data::StatsForWorld(test_data::kDocs[4]);
  ASSERT_EQ(stats.term_frequency, 0u);
  EXPECT_EQ(scorer.ScoreLeaf(stats, 1.0), 0.0);
}

// Empty index (N=0, avg_doc_len=0): ScoreLeaf returns 0 instead of
// dividing by zero.
TEST(Bm25StdScorerTest, ScoreLeafEmptyIndexReturnsZero) {
  Bm25StdScorer scorer;
  Bm25StdStats stats = MakeStats(/*N=*/0, /*avg_doc_len=*/0.0,
                                 /*dt=*/0, /*F=*/0, /*doc_len=*/0);
  EXPECT_EQ(scorer.ScoreLeaf(stats, 1.0), 0.0);
}

// IDF axis: rarer terms (lower dt) score higher than common ones at
// matched-once-per-doc inputs.
TEST(Bm25StdScorerTest, IdfDifferentiatesByDt) {
  Bm25StdScorer scorer;
  const double hello = scorer.ScoreLeaf(
      test_data::StatsForHello(test_data::kDocs[0]), 1.0);  // dt=6
  const double rare = scorer.ScoreLeaf(
      test_data::StatsForRare(test_data::kDocs[5]), 1.0);  // dt=2
  const double unique = scorer.ScoreLeaf(
      test_data::StatsForUnique(test_data::kDocs[5]), 1.0);  // dt=1
  EXPECT_GT(rare, hello);
  EXPECT_GT(unique, rare);
}

// Length-norm axis: doc:5 (F=4, dl=4) outranks doc:4 (F=5, dl=9) on
// hello despite doc:4 having a higher F.
TEST(Bm25StdScorerTest, LengthNormalizationFavorsShorterDoc) {
  Bm25StdScorer scorer;
  const double doc4 =
      scorer.ScoreLeaf(test_data::StatsForHello(test_data::kDocs[3]), 1.0);
  const double doc5 =
      scorer.ScoreLeaf(test_data::StatsForHello(test_data::kDocs[4]), 1.0);
  EXPECT_GT(doc5, doc4);
}

TEST(Bm25StdScorerDeathTest, DtGreaterThanNCrashes) {
  Bm25StdScorer scorer;
  Bm25StdStats stats = MakeStats(/*N=*/2, /*avg_doc_len=*/10.0,
                                 /*dt=*/3, /*F=*/1, /*doc_len=*/10);
  EXPECT_DEATH(scorer.ScoreLeaf(stats, 1.0), "");
}

TEST(Bm25StdScorerDeathTest, WrongStatsSubtypeCrashes) {
  Bm25StdScorer scorer;
  ScoringStats base_stats;
  base_stats.total_docs = 10;
  base_stats.num_doc_contain_term = 1;
  base_stats.term_frequency = 1;
  EXPECT_DEATH(scorer.ScoreLeaf(base_stats, 1.0), "");
}

TEST(Bm25StdScorerTest, ComposeMultipliesByDocumentScore) {
  Bm25StdScorer scorer;
  Bm25StdStats stats = test_data::StatsForHello(test_data::kDocs[0]);
  stats.document_score = 0.7;
  EXPECT_NEAR(scorer.ComposeDocumentScore(0.5, stats), 0.5 * 0.7,
              kFloatTolerance);
}

// Infinite document_score short-circuits to itself. std::isinf is
// unreliable under -ffast-math, so the scorer uses a bit-pattern
// check; this test guards both signs of the IsInf path.
TEST(Bm25StdScorerTest, ComposeInfinityShortCircuits) {
  Bm25StdScorer scorer;
  const double kInf = std::numeric_limits<double>::infinity();
  Bm25StdStats stats = test_data::StatsForHello(test_data::kDocs[0]);
  stats.document_score = kInf;
  EXPECT_EQ(scorer.ComposeDocumentScore(0.5, stats), kInf);
  stats.document_score = -kInf;
  EXPECT_EQ(scorer.ComposeDocumentScore(0.5, stats), -kInf);
}

TEST(Bm25StdScorerTest, CombineGroupAppliesWeight) {
  Bm25StdScorer scorer;
  std::vector<double> children = {1.0, 2.0, 3.0};
  EXPECT_NEAR(scorer.CombineGroup(children, /*group_weight=*/2.0), 12.0,
              kFloatTolerance);
}

TEST(Bm25StdScorerTest, CombineGroupEmptyChildrenIsZero) {
  Bm25StdScorer scorer;
  EXPECT_EQ(scorer.CombineGroup({}, 5.0), 0.0);
}

}  // namespace
}  // namespace valkey_search::indexes::scoring
