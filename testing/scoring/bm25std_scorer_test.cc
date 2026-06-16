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
#include "src/indexes/scoring/scoring_session.h"
#include "src/indexes/scoring/scoring_stats.h"
#include "src/utils/string_interning.h"
#include "testing/scoring/scoring_test_data.h"

namespace valkey_search::indexes::scoring {
namespace {

using ::testing::ElementsAre;

constexpr float kFloatTolerance = 1e-4f;

InternedStringPtr Key(const char* name) {
  return StringInternStore::Intern(name);
}

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

std::vector<InternedStringPtr> RankOrder(const std::vector<RankedDoc>& ranked) {
  std::vector<InternedStringPtr> keys;
  keys.reserve(ranked.size());
  for (const auto& r : ranked) keys.push_back(r.key);
  return keys;
}

float ScoreFor(const std::vector<RankedDoc>& ranked, InternedStringPtr id) {
  for (const auto& r : ranked) {
    if (r.key == id) return r.score;
  }
  ADD_FAILURE() << "key " << id->Str() << " not in ranked results";
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

// --- Direct scorer math ---

TEST(Bm25StdScorerTest, IdentityNameAndType) {
  Bm25StdScorer scorer;
  EXPECT_EQ(scorer.Name(), "BM25STD");
  EXPECT_EQ(scorer.Type(), ScorerType::kBm25Std);
}

TEST(Bm25StdScorerTest, ScoreLeafCorpusReference) {
  Bm25StdScorer scorer;
  Bm25StdStats stats = test_data::StatsForHello(test_data::kDocs[4]);
  EXPECT_NEAR(scorer.ScoreLeaf(stats, 1.0f), 0.574385f, kFloatTolerance);
}

TEST(Bm25StdScorerTest, ScoreLeafLeafWeightScalesLinearly) {
  Bm25StdScorer scorer;
  Bm25StdStats stats = test_data::StatsForHello(test_data::kDocs[0]);
  const float base = scorer.ScoreLeaf(stats, 1.0f);
  EXPECT_NEAR(scorer.ScoreLeaf(stats, 5.0f), 5.0f * base, kFloatTolerance);
  EXPECT_EQ(scorer.ScoreLeaf(stats, 0.0f), 0.0f);
}

TEST(Bm25StdScorerTest, ScoreLeafZeroFrequencyReturnsZero) {
  Bm25StdScorer scorer;
  Bm25StdStats stats = test_data::StatsForWorld(test_data::kDocs[4]);
  ASSERT_EQ(stats.term_frequency, 0u);
  EXPECT_EQ(scorer.ScoreLeaf(stats, 1.0f), 0.0f);
}

TEST(Bm25StdScorerTest, ScoreLeafEmptyIndexReturnsZero) {
  Bm25StdScorer scorer;
  Bm25StdStats stats = MakeStats(/*N=*/0, /*avg_doc_len=*/0.0f,
                                 /*dt=*/0, /*F=*/0, /*doc_len=*/0);
  EXPECT_EQ(scorer.ScoreLeaf(stats, 1.0f), 0.0f);
}

TEST(Bm25StdScorerTest, IdfDifferentiatesByDt) {
  Bm25StdScorer scorer;
  const float hello =
      scorer.ScoreLeaf(test_data::StatsForHello(test_data::kDocs[0]), 1.0f);
  const float rare =
      scorer.ScoreLeaf(test_data::StatsForRare(test_data::kDocs[5]), 1.0f);
  const float unique =
      scorer.ScoreLeaf(test_data::StatsForUnique(test_data::kDocs[5]), 1.0f);
  EXPECT_GT(rare, hello);
  EXPECT_GT(unique, rare);
}

TEST(Bm25StdScorerTest, LengthNormalizationFavorsShorterDoc) {
  Bm25StdScorer scorer;
  const float doc4 =
      scorer.ScoreLeaf(test_data::StatsForHello(test_data::kDocs[3]), 1.0f);
  const float doc5 =
      scorer.ScoreLeaf(test_data::StatsForHello(test_data::kDocs[4]), 1.0f);
  EXPECT_GT(doc5, doc4);
}

TEST(Bm25StdScorerDeathTest, DtGreaterThanNCrashes) {
  Bm25StdScorer scorer;
  Bm25StdStats stats = MakeStats(/*N=*/2, /*avg_doc_len=*/10.0f,
                                 /*dt=*/3, /*F=*/1, /*doc_len=*/10);
  EXPECT_DEATH(scorer.ScoreLeaf(stats, 1.0f), "");
}

TEST(Bm25StdScorerDeathTest, WrongStatsSubtypeCrashes) {
  Bm25StdScorer scorer;
  ScoringStats base_stats;
  base_stats.total_docs = 10;
  base_stats.num_doc_contain_term = 1;
  base_stats.term_frequency = 1;
  EXPECT_DEATH(scorer.ScoreLeaf(base_stats, 1.0f), "");
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

// --- Query-driven tests (scorer + session integration) ---

TEST(Bm25StdScorerQueryTest, SingleLeafHelloRankOrder) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  auto stats = BuildStatsForTerm(test_data::StatsForHello);
  for (const auto& s : stats) session.RecordLeaf(s, 1.0f);

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked),
              ElementsAre(Key("doc:5"), Key("doc:4"), Key("doc:3"),
                          Key("doc:2"), Key("doc:7"), Key("doc:1")));
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:5")), 0.574385f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:4")), 0.523122f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:3")), 0.477286f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:2")), 0.430172f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:7")), 0.430172f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:1")), 0.331888f, kFloatTolerance);
}

TEST(Bm25StdScorerQueryTest, MultiLeafHelloWorld) {
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
  EXPECT_THAT(RankOrder(ranked),
              ElementsAre(Key("doc:4"), Key("doc:3"), Key("doc:2"),
                          Key("doc:7"), Key("doc:1")));
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:4")), 0.774956f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:3")), 0.763658f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:2")), 0.737626f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:7")), 0.737626f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:1")), 0.663776f, kFloatTolerance);
}

TEST(Bm25StdScorerQueryTest, LeafWeightScalesScore) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  auto stats = BuildStatsForTerm(test_data::StatsForHello);
  for (const auto& s : stats) session.RecordLeaf(s, 5.0f);

  auto ranked = session.Rank();
  EXPECT_THAT(RankOrder(ranked),
              ElementsAre(Key("doc:5"), Key("doc:4"), Key("doc:3"),
                          Key("doc:2"), Key("doc:7"), Key("doc:1")));
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:5")), 2.871923f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:4")), 2.615608f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:3")), 2.386431f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:2")), 2.150861f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:7")), 2.150861f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:1")), 1.659439f, kFloatTolerance);
}

TEST(Bm25StdScorerQueryTest, NestedGroupsLayeredWeights) {
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
  EXPECT_THAT(RankOrder(ranked),
              ElementsAre(Key("doc:4"), Key("doc:3"), Key("doc:2"),
                          Key("doc:7"), Key("doc:1")));
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:4")), 5.695980f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:3")), 5.536520f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:2")), 5.286103f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:7")), 5.286103f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:1")), 4.646429f, kFloatTolerance);
}

TEST(Bm25StdScorerQueryTest, OrAtTopMixedAdmissionPaths) {
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
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:8")), 1.915183f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:6")), 1.419164f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:4")), 0.774956f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:3")), 0.763658f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:2")), 0.737626f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:7")), 0.737626f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:1")), 0.663776f, kFloatTolerance);
}

TEST(Bm25StdScorerQueryTest, AndOfLeafAndOrGroup) {
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
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:4")), 0.774956f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:3")), 0.763658f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:2")), 0.737626f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:7")), 0.737626f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:1")), 0.663776f, kFloatTolerance);
}

TEST(Bm25StdScorerQueryTest, PerLeafWeightInsideOrWithGroupWeight) {
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
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:8")), 11.491096f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:6")), 8.514985f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:5")), 6.892615f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:4")), 6.277460f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:3")), 5.727435f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:2")), 5.162066f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:7")), 5.162066f, kFloatTolerance);
  EXPECT_NEAR(ScoreFor(ranked, Key("doc:1")), 3.982653f, kFloatTolerance);
}

TEST(Bm25StdScorerQueryTest, NonExistentTermEmpty) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  auto ranked = session.Rank();
  EXPECT_TRUE(ranked.empty());
}

TEST(Bm25StdScorerQueryTest, TiesBrokenByKeyAscending) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  auto stats = BuildStatsForTerm(test_data::StatsForHello);
  for (const auto& s : stats) session.RecordLeaf(s, 1.0f);

  auto ranked = session.Rank();
  int pos2 = -1, pos7 = -1;
  auto key2 = Key("doc:2");
  auto key7 = Key("doc:7");
  for (size_t i = 0; i < ranked.size(); ++i) {
    if (ranked[i].key == key2) pos2 = static_cast<int>(i);
    if (ranked[i].key == key7) pos7 = static_cast<int>(i);
  }
  ASSERT_NE(pos2, -1);
  ASSERT_NE(pos7, -1);
  EXPECT_LT(pos2, pos7);
  EXPECT_FLOAT_EQ(ranked[pos2].score, ranked[pos7].score);
}

// --- Death tests (session misuse) ---

TEST(Bm25StdScorerDeathTest, RecordLeafAfterRankCrashes) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  (void)session.Rank();
  Bm25StdStats stats = test_data::StatsForHello(test_data::kDocs[0]);
  EXPECT_DEATH(session.RecordLeaf(stats, 1.0f), "");
}

TEST(Bm25StdScorerDeathTest, ExitGroupWithoutEnterCrashes) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  EXPECT_DEATH(session.ExitGroup(1.0f), "");
}

TEST(Bm25StdScorerDeathTest, RankWithUnbalancedStackCrashes) {
  Bm25StdScorer scorer;
  ScoringSession session(&scorer);
  session.EnterGroup();
  EXPECT_DEATH((void)session.Rank(), "");
}

TEST(Bm25StdScorerDeathTest, NullScorerCrashes) {
  EXPECT_DEATH({ ScoringSession session(nullptr); }, "");
}

}  // namespace
}  // namespace valkey_search::indexes::scoring
