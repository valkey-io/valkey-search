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

// Tests for ScoringSession driven against the shared 8-doc corpus in
// testing/scoring/scoring_test_data.h.
namespace valkey_search::indexes::scoring {
namespace {

using ::testing::ElementsAre;

constexpr double kFloatTolerance = 1e-4;

// Extract the doc_id sequence from a Rank() result for ranking
// assertions that don't care about absolute scores.
std::vector<DocId> RankOrder(const std::vector<RankedDoc>& ranked) {
  std::vector<DocId> ids;
  ids.reserve(ranked.size());
  for (const auto& r : ranked) ids.push_back(r.doc_id);
  return ids;
}

// Find the score for a specific doc_id in a Rank() result. Asserts the
// doc is present.
double ScoreFor(const std::vector<RankedDoc>& ranked, DocId id) {
  for (const auto& r : ranked) {
    if (r.doc_id == id) return r.score;
  }
  ADD_FAILURE() << "doc_id " << id << " not in ranked results";
  return 0.0;
}

// Build a vector of Bm25StdStats for every doc in kDocs that has a
// non-zero frequency for the named term. The vector is reserved up
// front so pointers handed to RecordLeaf remain valid.
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

// ---- Normal-path tests ----

// Single-leaf "hello" query: rank by length-normalized TF across
// docs that contain "hello" at least once. Verifies rank order AND
// absolute scores (Redis-verified).
TEST(ScoringSessionTest, SingleLeafHelloRankOrder) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  auto stats = BuildStatsForTerm(test_data::StatsForHello);
  for (const auto& s : stats) session.RecordLeaf(s, 1.0);

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(5, 4, 3, 2, 7, 1));
  EXPECT_NEAR(ScoreFor(ranked, 5), 0.574385, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 4), 0.523122, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 0.477286, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 0.430172, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 0.430172, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 1), 0.331888, kFloatTolerance);
}

// Implicit AND `hello world`: only docs that contain both terms are
// admitted. Each admitted doc is scored on both leaves; the session
// accumulates per-doc.
TEST(ScoringSessionTest, MultiLeafHelloWorld) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);

  // Admission filter: docs that have BOTH hello and world present.
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
    session.RecordLeaf(hello_stats[i], 1.0);
    session.RecordLeaf(world_stats[i], 1.0);
  }

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(4, 3, 2, 7, 1));
  EXPECT_NEAR(ScoreFor(ranked, 4), 0.774956, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 0.763658, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 0.737626, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 0.737626, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 1), 0.663776, kFloatTolerance);
}

// Leaf weight scales every contribution linearly. `(hello)=>{$weight:5}`
// must produce exactly 5x the single-leaf hello score, same ranking.
TEST(ScoringSessionTest, LeafWeightScalesScore) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  auto stats = BuildStatsForTerm(test_data::StatsForHello);
  for (const auto& s : stats) session.RecordLeaf(s, 5.0);

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(5, 4, 3, 2, 7, 1));
  EXPECT_NEAR(ScoreFor(ranked, 5), 2.871923, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 4), 2.615608, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 2.386431, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 2.150861, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 2.150861, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 1), 1.659439, kFloatTolerance);
}

// `((hello)=>{$weight:4} (world)=>{$weight:3})=>{$weight:2}` — outer
// group weight composes multiplicatively over per-leaf weights:
//   final = 2 * (4*hello + 3*world)
// Drives EnterGroup / ExitGroup once around two weighted leaves.
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
    session.RecordLeaf(hello_stats[i], 4.0);
    session.RecordLeaf(world_stats[i], 3.0);
  }
  session.ExitGroup(2.0);

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(4, 3, 2, 7, 1));
  EXPECT_NEAR(ScoreFor(ranked, 4), 5.695980, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 5.536520, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 5.286103, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 5.286103, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 1), 4.646429, kFloatTolerance);
}

// `(hello world) | rare` — OR at the top, two distinct admission paths.
// Each doc is scored only on the leaves matched on the path that
// admitted it. doc:6 has `world` but enters via `rare`, so its `world`
// token contributes nothing.
TEST(ScoringSessionTest, OrAtTopMixedAdmissionPaths) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);

  // Persistent storage for the stats. Reserve up front so RecordLeaf's
  // pointer stays valid across pushes.
  std::vector<Bm25StdStats> all_stats;
  all_stats.reserve(2 * std::size(test_data::kDocs));

  for (const auto& doc : test_data::kDocs) {
    const bool rare_branch = doc.f_rare > 0;
    const bool and_branch = doc.f_hello > 0 && doc.f_world > 0;
    if (rare_branch) {
      all_stats.push_back(test_data::StatsForRare(doc));
      session.RecordLeaf(all_stats.back(), 1.0);
    } else if (and_branch) {
      all_stats.push_back(test_data::StatsForHello(doc));
      session.RecordLeaf(all_stats.back(), 1.0);
      all_stats.push_back(test_data::StatsForWorld(doc));
      session.RecordLeaf(all_stats.back(), 1.0);
    }
  }

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(8, 6, 4, 3, 2, 7, 1));
  EXPECT_NEAR(ScoreFor(ranked, 8), 1.915183, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 6), 1.419164, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 4), 0.774956, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 0.763658, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 0.737626, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 0.737626, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 1), 0.663776, kFloatTolerance);
}

// `hello (world | rare)` — AND of leaf + OR group. None of the
// admitted docs (1-4, 7) contain `rare`, so the OR group contributes
// only `world`. Final scores match plain `hello world`.
TEST(ScoringSessionTest, AndOfLeafAndOrGroup) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);

  std::vector<Bm25StdStats> all_stats;
  all_stats.reserve(2 * std::size(test_data::kDocs));

  for (const auto& doc : test_data::kDocs) {
    const bool or_match = doc.f_world > 0 || doc.f_rare > 0;
    if (doc.f_hello > 0 && or_match) {
      all_stats.push_back(test_data::StatsForHello(doc));
      session.RecordLeaf(all_stats.back(), 1.0);
      session.EnterGroup();
      if (doc.f_world > 0) {
        all_stats.push_back(test_data::StatsForWorld(doc));
        session.RecordLeaf(all_stats.back(), 1.0);
      }
      if (doc.f_rare > 0) {
        all_stats.push_back(test_data::StatsForRare(doc));
        session.RecordLeaf(all_stats.back(), 1.0);
      }
      session.ExitGroup(1.0);
    }
  }

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(4, 3, 2, 7, 1));
  EXPECT_NEAR(ScoreFor(ranked, 4), 0.774956, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 0.763658, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 0.737626, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 0.737626, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 1), 0.663776, kFloatTolerance);
}

// `((hello)=>{$weight:4} | (rare)=>{$weight:2})=>{$weight:3}` — the
// only test that exercises both per-leaf weights and an outer group
// weight in composition. Each admitted doc is scored on whichever
// leaves matched, weighted accordingly:
//   final = 3 * (4*hello if matched + 2*rare if matched)
// doc:6 has `world` but `world` is not in the query, so it isn't
// scored. doc:8 (rare-only) ranks highest because rare's IDF dominates.
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
      session.RecordLeaf(all_stats.back(), 4.0);
    }
    if (doc.f_rare > 0) {
      all_stats.push_back(test_data::StatsForRare(doc));
      session.RecordLeaf(all_stats.back(), 2.0);
    }
    session.ExitGroup(3.0);
  }

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked), ElementsAre(8, 6, 5, 4, 3, 2, 7, 1));
  EXPECT_NEAR(ScoreFor(ranked, 8), 11.491096, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 6), 8.514985, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 5), 6.892615, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 4), 6.277460, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 3), 5.727435, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 2), 5.162066, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 7), 5.162066, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, 1), 3.982653, kFloatTolerance);
}

// Empty result: no leaves recorded, Rank() returns an empty vector.
TEST(ScoringSessionTest, NonExistentTermEmpty) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  auto ranked = session.Rank();
  EXPECT_TRUE(ranked.empty());
}

// Docs with byte-identical scoring inputs (doc:2 vs doc:7 on `hello`)
// must be tie-broken by doc_id ascending.
TEST(ScoringSessionTest, TiesBrokenByDocIdAscending) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  auto stats = BuildStatsForTerm(test_data::StatsForHello);
  for (const auto& s : stats) session.RecordLeaf(s, 1.0);

  auto ranked = session.Rank();
  // Find the positions of doc:2 and doc:7 in the ranking.
  int pos2 = -1, pos7 = -1;
  for (size_t i = 0; i < ranked.size(); ++i) {
    if (ranked[i].doc_id == 2) pos2 = static_cast<int>(i);
    if (ranked[i].doc_id == 7) pos7 = static_cast<int>(i);
  }
  ASSERT_NE(pos2, -1);
  ASSERT_NE(pos7, -1);
  EXPECT_LT(pos2, pos7);
  EXPECT_DOUBLE_EQ(ranked[pos2].score, ranked[pos7].score);
}

// ---- Death tests: contract violations ----

TEST(ScoringSessionDeathTest, RecordLeafAfterRankCrashes) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  (void)session.Rank();
  Bm25StdStats stats = test_data::StatsForHello(test_data::kDocs[0]);
  EXPECT_DEATH(session.RecordLeaf(stats, 1.0), "");
}

TEST(ScoringSessionDeathTest, ExitGroupWithoutEnterCrashes) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  EXPECT_DEATH(session.ExitGroup(1.0), "");
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
