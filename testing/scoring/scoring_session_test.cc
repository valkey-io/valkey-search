/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/scoring/scoring_session.h"

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/indexes/scoring/bm25std_scorer.h"
#include "src/indexes/scoring/scoring_stats.h"
#include "testing/scoring/scoring_test_data.h"

namespace valkey_search::indexes::scoring {
namespace {

using ::testing::ElementsAre;

constexpr float kFloatTolerance = 1e-4f;

std::vector<DocId> RankOrder(const std::vector<RankedDoc>& ranked) {
  std::vector<DocId> ids;
  ids.reserve(ranked.size());
  for (const auto& r : ranked) ids.push_back(r.doc_id);
  return ids;
}

float ScoreFor(const std::vector<RankedDoc>& ranked, DocId id) {
  for (const auto& r : ranked) {
    if (r.doc_id == id) return r.score;
  }
  ADD_FAILURE() << "doc_id " << id << " not in ranked results";
  return 0.0f;
}

template <typename Builder>
std::vector<Bm25StdStats> BuildStatsForTerm(Builder builder) {
  std::vector<Bm25StdStats> out;
  out.reserve(std::size(test_data::kDocs));
  for (const auto& doc : test_data::kDocs) {
    Bm25StdStats s = builder(doc);
    if (s.term_frequency > 0) out.push_back(s);
  }
  return out;
}

TEST(ScoringSessionTest, SingleLeafHelloRankOrder) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  auto stats = BuildStatsForTerm(test_data::StatsForHello);
  for (const auto& s : stats) session.RecordLeaf(s, 1.0f);

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(5, 4, 3, 2, 7, 1));
  EXPECT_NEAR(ScoreFor(ranked, 5), 0.574385f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 4), 0.523122f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 0.477286f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 0.430172f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 0.430172f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 1), 0.331888f, kFloatTolerance);
}

// Implicit AND `hello world`: only docs containing both terms are admitted.
TEST(ScoringSessionTest, MultiLeafHelloWorld) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);

  std::vector<Bm25StdStats> hello_stats, world_stats;
  hello_stats.reserve(std::size(test_data::kDocs));
  world_stats.reserve(std::size(test_data::kDocs));
  for (const auto& doc : test_data::kDocs) {
    if (doc.f_hello > 0 && doc.f_world > 0) {
      hello_stats.push_back(test_data::StatsForHello(doc));
      world_stats.push_back(test_data::StatsForWorld(doc));
    }
  }
  for (size_t i = 0; i < hello_stats.size(); ++i) {
    session.RecordLeaf(hello_stats[i], 1.0f);
    session.RecordLeaf(world_stats[i], 1.0f);
  }

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(4, 3, 2, 7, 1));
  EXPECT_NEAR(ScoreFor(ranked, 4), 0.774956f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 0.763658f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 0.737626f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 0.737626f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 1), 0.663776f, kFloatTolerance);
}

// `(hello)=>{$weight:5}` must produce exactly 5x the single-leaf score.
TEST(ScoringSessionTest, LeafWeightScalesScore) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  auto stats = BuildStatsForTerm(test_data::StatsForHello);
  for (const auto& s : stats) session.RecordLeaf(s, 5.0f);

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(5, 4, 3, 2, 7, 1));
  EXPECT_NEAR(ScoreFor(ranked, 5), 2.871923f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 4), 2.615608f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 2.386431f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 2.150861f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 2.150861f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 1), 1.659439f, kFloatTolerance);
}

// `((hello)=>{$weight:4} (world)=>{$weight:3})=>{$weight:2}`:
//   final = 2 * (4*hello + 3*world)
TEST(ScoringSessionTest, NestedGroupsLayeredWeights) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);

  std::vector<Bm25StdStats> hello_stats, world_stats;
  hello_stats.reserve(std::size(test_data::kDocs));
  world_stats.reserve(std::size(test_data::kDocs));
  for (const auto& doc : test_data::kDocs) {
    if (doc.f_hello > 0 && doc.f_world > 0) {
      hello_stats.push_back(test_data::StatsForHello(doc));
      world_stats.push_back(test_data::StatsForWorld(doc));
    }
  }

  session.EnterGroup();
  for (size_t i = 0; i < hello_stats.size(); ++i) {
    session.RecordLeaf(hello_stats[i], 4.0f);
    session.RecordLeaf(world_stats[i], 3.0f);
  }
  session.ExitGroup(2.0f);

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(4, 3, 2, 7, 1));
  EXPECT_NEAR(ScoreFor(ranked, 4), 5.695980f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 5.536520f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 5.286103f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 5.286103f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 1), 4.646429f, kFloatTolerance);
}

// `(hello world) | rare`: doc:6 has `world` but enters via `rare`, so its
// `world` token contributes nothing.
TEST(ScoringSessionTest, OrAtTopMixedAdmissionPaths) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);

  std::vector<Bm25StdStats> all_stats;
  all_stats.reserve(2 * std::size(test_data::kDocs));

  for (const auto& doc : test_data::kDocs) {
    const bool rare_branch = doc.f_rare > 0;
    const bool and_branch = doc.f_hello > 0 && doc.f_world > 0;
    if (rare_branch) {
      all_stats.push_back(test_data::StatsForRare(doc));
      session.RecordLeaf(all_stats.back(), 1.0f);
    } else if (and_branch) {
      all_stats.push_back(test_data::StatsForHello(doc));
      session.RecordLeaf(all_stats.back(), 1.0f);
      all_stats.push_back(test_data::StatsForWorld(doc));
      session.RecordLeaf(all_stats.back(), 1.0f);
    }
  }

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(8, 6, 4, 3, 2, 7, 1));
  EXPECT_NEAR(ScoreFor(ranked, 8), 1.915183f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 6), 1.419164f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 4), 0.774956f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 0.763658f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 0.737626f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 0.737626f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 1), 0.663776f, kFloatTolerance);
}

// `hello (world | rare)`: admitted docs (1-4, 7) have no `rare`, so the
// OR group reduces to `world` and final scores match plain `hello world`.
TEST(ScoringSessionTest, AndOfLeafAndOrGroup) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);

  std::vector<Bm25StdStats> all_stats;
  all_stats.reserve(2 * std::size(test_data::kDocs));

  for (const auto& doc : test_data::kDocs) {
    const bool or_match = doc.f_world > 0 || doc.f_rare > 0;
    if (doc.f_hello > 0 && or_match) {
      all_stats.push_back(test_data::StatsForHello(doc));
      session.RecordLeaf(all_stats.back(), 1.0f);
      session.EnterGroup();
      if (doc.f_world > 0) {
        all_stats.push_back(test_data::StatsForWorld(doc));
        session.RecordLeaf(all_stats.back(), 1.0f);
      }
      if (doc.f_rare > 0) {
        all_stats.push_back(test_data::StatsForRare(doc));
        session.RecordLeaf(all_stats.back(), 1.0f);
      }
      session.ExitGroup(1.0f);
    }
  }

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(4, 3, 2, 7, 1));
  EXPECT_NEAR(ScoreFor(ranked, 4), 0.774956f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 0.763658f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 0.737626f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 0.737626f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 1), 0.663776f, kFloatTolerance);
}

// `((hello)=>{$weight:4} | (rare)=>{$weight:2})=>{$weight:3}`:
//   final = 3 * (4*hello if matched + 2*rare if matched)
TEST(ScoringSessionTest, PerLeafWeightInsideOrWithGroupWeight) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);

  std::vector<Bm25StdStats> all_stats;
  all_stats.reserve(2 * std::size(test_data::kDocs));

  for (const auto& doc : test_data::kDocs) {
    if (doc.f_hello == 0 && doc.f_rare == 0) continue;
    session.EnterGroup();
    if (doc.f_hello > 0) {
      all_stats.push_back(test_data::StatsForHello(doc));
      session.RecordLeaf(all_stats.back(), 4.0f);
    }
    if (doc.f_rare > 0) {
      all_stats.push_back(test_data::StatsForRare(doc));
      session.RecordLeaf(all_stats.back(), 2.0f);
    }
    session.ExitGroup(3.0f);
  }

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(8, 6, 5, 4, 3, 2, 7, 1));
  EXPECT_NEAR(ScoreFor(ranked, 8), 11.491096f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 6), 8.514985f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 5), 6.892615f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 4), 6.277460f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 5.727435f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 5.162066f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 5.162066f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 1), 3.982653f, kFloatTolerance);
}

TEST(ScoringSessionTest, NonExistentTermEmpty) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  auto ranked = session.Rank();
  EXPECT_TRUE(ranked.empty());
}

// doc:2 and doc:7 have byte-identical inputs on `hello`; tie-break by doc_id.
TEST(ScoringSessionTest, TiesBrokenByDocIdAscending) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  auto stats = BuildStatsForTerm(test_data::StatsForHello);
  for (const auto& s : stats) session.RecordLeaf(s, 1.0f);

  auto ranked = session.Rank();
  int pos2 = -1, pos7 = -1;
  for (size_t i = 0; i < ranked.size(); ++i) {
    if (ranked[i].doc_id == 2) pos2 = static_cast<int>(i);
    if (ranked[i].doc_id == 7) pos7 = static_cast<int>(i);
  }
  ASSERT_NE(pos2, -1);
  ASSERT_NE(pos7, -1);
  EXPECT_LT(pos2, pos7);
  EXPECT_FLOAT_EQ(ranked[pos2].score, ranked[pos7].score);
}

TEST(ScoringSessionDeathTest, RecordLeafAfterRankCrashes) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  (void)session.Rank();
  Bm25StdStats stats = test_data::StatsForHello(test_data::kDocs[0]);
  EXPECT_DEATH(session.RecordLeaf(stats, 1.0f), "");
}

TEST(ScoringSessionDeathTest, ExitGroupWithoutEnterCrashes) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  EXPECT_DEATH(session.ExitGroup(1.0f), "");
}

TEST(ScoringSessionDeathTest, RankWithUnbalancedStackCrashes) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  session.EnterGroup();
  EXPECT_DEATH((void)session.Rank(), "");
}

TEST(ScoringSessionDeathTest, NullScorerCrashes) {
  EXPECT_DEATH({ ScoringSession session(nullptr); }, "");
}

}  // namespace
}  // namespace valkey_search::indexes::scoring
