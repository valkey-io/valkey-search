/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/fusion.h"

#include <cmath>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "src/indexes/vector_base.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search::query::fusion {
namespace {

// Build a Neighbor with a given key and distance; no attribute_contents.
indexes::Neighbor N(absl::string_view key, float distance) {
  return indexes::Neighbor{StringInternStore::Intern(std::string(key)),
                           distance};
}

// Construct a vector<Neighbor> from a parameter pack. Avoids the
// copy-required initializer_list path (Neighbor is move-only).
template <typename... Ns>
std::vector<indexes::Neighbor> Vec(Ns&&... ns) {
  std::vector<indexes::Neighbor> v;
  v.reserve(sizeof...(Ns));
  (v.push_back(std::forward<Ns>(ns)), ...);
  return v;
}

const indexes::Neighbor* Find(const std::vector<indexes::Neighbor>& neighbors,
                              absl::string_view key) {
  for (const auto& n : neighbors) {
    if (n.external_id->Str() == key) {
      return &n;
    }
  }
  return nullptr;
}

std::optional<double> AliasScore(const indexes::Neighbor& n,
                                 absl::string_view alias) {
  if (!n.attribute_contents.has_value()) {
    return std::nullopt;
  }
  auto it = n.attribute_contents->find(alias);
  if (it == n.attribute_contents->end()) {
    return std::nullopt;
  }
  auto sv = vmsdk::ToStringView(it->second.value.get());
  return std::stod(std::string(sv));
}

// vmsdk::ValkeyTest::SetUp installs the mock module-API table. Required
// because AttachArmScore allocates ValkeyModuleStrings via ValkeyModule_*.
using FuseRRFTest = vmsdk::ValkeyTest;
using FuseLinearTest = vmsdk::ValkeyTest;

// ---------------------------- RRF basic shapes ----------------------------

TEST_F(FuseRRFTest, DisjointArmsContributeIndependently) {
  auto arm0 = Vec(N("doc:1", 0.1f), N("doc:2", 0.2f));
  auto arm1 = Vec(N("doc:3", 0.3f), N("doc:4", 0.4f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .rrf_constant = 60, .window = 0});
  arms.push_back({.neighbors = &arm1, .rrf_constant = 60, .window = 0});
  auto fused = FuseRRF(std::move(arms));
  EXPECT_EQ(fused.size(), 4u);
  EXPECT_NEAR(Find(fused, "doc:1")->distance, 1.0 / 61.0, 1e-7);
  EXPECT_NEAR(Find(fused, "doc:3")->distance, 1.0 / 61.0, 1e-7);
  EXPECT_NEAR(Find(fused, "doc:2")->distance, 1.0 / 62.0, 1e-7);
  EXPECT_NEAR(Find(fused, "doc:4")->distance, 1.0 / 62.0, 1e-7);
}

TEST_F(FuseRRFTest, FullyOverlappingArmsAddRRFContributions) {
  auto arm0 = Vec(N("doc:1", 0.1f), N("doc:2", 0.2f));
  auto arm1 = Vec(N("doc:1", 0.5f), N("doc:2", 0.6f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .rrf_constant = 60, .window = 0});
  arms.push_back({.neighbors = &arm1, .rrf_constant = 60, .window = 0});
  auto fused = FuseRRF(std::move(arms));
  EXPECT_EQ(fused.size(), 2u);
  EXPECT_NEAR(Find(fused, "doc:1")->distance, 2.0 / 61.0, 1e-7);
  EXPECT_NEAR(Find(fused, "doc:2")->distance, 2.0 / 62.0, 1e-7);
}

// The critical case: partial overlap. Documents in both arms get summed
// scores; docs in only one arm get only that arm's contribution.
TEST_F(FuseRRFTest, PartialOverlapSumsAcrossArmsForSharedDocs) {
  auto arm0 = Vec(N("doc:1", 0.1f), N("doc:2", 0.2f), N("doc:3", 0.3f));
  auto arm1 = Vec(N("doc:2", 0.5f), N("doc:4", 0.6f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .rrf_constant = 60, .window = 0});
  arms.push_back({.neighbors = &arm1, .rrf_constant = 60, .window = 0});
  auto fused = FuseRRF(std::move(arms));
  EXPECT_EQ(fused.size(), 4u);
  EXPECT_NEAR(Find(fused, "doc:2")->distance, 1.0 / 62.0 + 1.0 / 61.0, 1e-7);
  EXPECT_NEAR(Find(fused, "doc:1")->distance, 1.0 / 61.0, 1e-7);
  EXPECT_NEAR(Find(fused, "doc:4")->distance, 1.0 / 62.0, 1e-7);
  EXPECT_NEAR(Find(fused, "doc:3")->distance, 1.0 / 63.0, 1e-7);
  EXPECT_EQ(fused[0].external_id->Str(), "doc:2");
  EXPECT_EQ(fused[3].external_id->Str(), "doc:3");
}

TEST_F(FuseRRFTest, SameDocAtDifferentRanksAcrossArms) {
  auto arm0 = Vec(N("doc:1", 0.0f));  // rank 0
  auto arm1 = Vec(N("doc:x", 0.1f), N("doc:y", 0.2f), N("doc:z", 0.3f),
                  N("doc:a", 0.4f), N("doc:b", 0.5f), N("doc:1", 0.9f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .rrf_constant = 60, .window = 0});
  arms.push_back({.neighbors = &arm1, .rrf_constant = 60, .window = 0});
  auto fused = FuseRRF(std::move(arms));
  EXPECT_NEAR(Find(fused, "doc:1")->distance, 1.0 / 61.0 + 1.0 / 66.0, 1e-7);
}

TEST_F(FuseRRFTest, WindowTruncatesArmContribution) {
  std::vector<indexes::Neighbor> arm0;
  arm0.reserve(100);
  for (int i = 0; i < 100; ++i) {
    arm0.push_back(N("doc:" + std::to_string(i), float(i)));
  }
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .rrf_constant = 60, .window = 10});
  auto fused = FuseRRF(std::move(arms));
  EXPECT_EQ(fused.size(), 10u);
  EXPECT_EQ(Find(fused, "doc:50"), nullptr);
}

TEST_F(FuseRRFTest, ConstantVariationPreservesOrdering) {
  auto arm0 = Vec(N("doc:1", 0.1f), N("doc:2", 0.2f), N("doc:3", 0.3f));
  for (uint32_t k : {1u, 60u, 1000u}) {
    std::vector<ArmInput> arms;
    arms.push_back({.neighbors = &arm0, .rrf_constant = k, .window = 0});
    auto fused = FuseRRF(std::move(arms));
    EXPECT_EQ(fused[0].external_id->Str(), "doc:1");
    EXPECT_EQ(fused[1].external_id->Str(), "doc:2");
    EXPECT_EQ(fused[2].external_id->Str(), "doc:3");
  }
}

TEST_F(FuseRRFTest, EmptyArmYieldsOtherArmsResults) {
  std::vector<indexes::Neighbor> arm0;
  auto arm1 = Vec(N("doc:a", 0.1f), N("doc:b", 0.2f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .rrf_constant = 60, .window = 0});
  arms.push_back({.neighbors = &arm1, .rrf_constant = 60, .window = 0});
  auto fused = FuseRRF(std::move(arms));
  EXPECT_EQ(fused.size(), 2u);
  EXPECT_NEAR(Find(fused, "doc:a")->distance, 1.0 / 61.0, 1e-7);
}

TEST_F(FuseRRFTest, AllArmsEmpty) {
  std::vector<indexes::Neighbor> arm0;
  std::vector<indexes::Neighbor> arm1;
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .rrf_constant = 60, .window = 0});
  arms.push_back({.neighbors = &arm1, .rrf_constant = 60, .window = 0});
  auto fused = FuseRRF(std::move(arms));
  EXPECT_EQ(fused.size(), 0u);
}

TEST_F(FuseRRFTest, SingleArmDegenerate) {
  auto arm0 = Vec(N("doc:a", 0.1f), N("doc:b", 0.2f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .rrf_constant = 60, .window = 0});
  auto fused = FuseRRF(std::move(arms));
  EXPECT_EQ(fused.size(), 2u);
  EXPECT_NEAR(Find(fused, "doc:a")->distance, 1.0 / 61.0, 1e-7);
  EXPECT_NEAR(Find(fused, "doc:b")->distance, 1.0 / 62.0, 1e-7);
}

TEST_F(FuseRRFTest, ScoreAliasPropagatesPerArmDistance) {
  auto arm0 = Vec(N("doc:1", 0.5f), N("doc:2", 0.7f));
  auto arm1 = Vec(N("doc:1", 0.2f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0,
                  .score_alias = std::string("search_score"),
                  .rrf_constant = 60,
                  .window = 0});
  arms.push_back({.neighbors = &arm1,
                  .score_alias = std::string("vec_score"),
                  .rrf_constant = 60,
                  .window = 0});
  auto fused = FuseRRF(std::move(arms));
  ASSERT_EQ(fused.size(), 2u);
  const auto* doc1 = Find(fused, "doc:1");
  ASSERT_NE(doc1, nullptr);
  EXPECT_TRUE(AliasScore(*doc1, "search_score").has_value());
  EXPECT_TRUE(AliasScore(*doc1, "vec_score").has_value());
  EXPECT_NEAR(*AliasScore(*doc1, "search_score"), 0.5, 1e-6);
  EXPECT_NEAR(*AliasScore(*doc1, "vec_score"), 0.2, 1e-6);
  const auto* doc2 = Find(fused, "doc:2");
  ASSERT_NE(doc2, nullptr);
  EXPECT_TRUE(AliasScore(*doc2, "search_score").has_value());
  EXPECT_FALSE(AliasScore(*doc2, "vec_score").has_value());
}

TEST_F(FuseRRFTest, DeterministicTieBreakByExternalId) {
  auto arm0 = Vec(N("doc:zzz", 0.1f));
  auto arm1 = Vec(N("doc:aaa", 0.1f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .rrf_constant = 60, .window = 0});
  arms.push_back({.neighbors = &arm1, .rrf_constant = 60, .window = 0});
  auto fused = FuseRRF(std::move(arms));
  ASSERT_EQ(fused.size(), 2u);
  EXPECT_EQ(fused[0].external_id->Str(), "doc:aaa");
  EXPECT_EQ(fused[1].external_id->Str(), "doc:zzz");
}

// ---------------------------- LINEAR ----------------------------

TEST_F(FuseLinearTest, DisjointArmsScaledByWeight) {
  auto arm0 = Vec(N("doc:1", 0.0f), N("doc:2", 1.0f));
  auto arm1 = Vec(N("doc:3", 0.0f), N("doc:4", 1.0f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .weight = 0.7, .window = 0});
  arms.push_back({.neighbors = &arm1, .weight = 0.3, .window = 0});
  auto fused = FuseLinear(std::move(arms));
  EXPECT_EQ(fused.size(), 4u);
  EXPECT_NEAR(Find(fused, "doc:1")->distance, 0.7, 1e-6);
  EXPECT_NEAR(Find(fused, "doc:2")->distance, 0.0, 1e-6);
  EXPECT_NEAR(Find(fused, "doc:3")->distance, 0.3, 1e-6);
  EXPECT_NEAR(Find(fused, "doc:4")->distance, 0.0, 1e-6);
}

TEST_F(FuseLinearTest, OverlappingDocSumsWeightedNormalized) {
  auto arm0 = Vec(N("doc:1", 0.0f), N("doc:2", 1.0f));
  auto arm1 = Vec(N("doc:1", 0.0f), N("doc:3", 1.0f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .weight = 0.5, .window = 0});
  arms.push_back({.neighbors = &arm1, .weight = 0.5, .window = 0});
  auto fused = FuseLinear(std::move(arms));
  EXPECT_NEAR(Find(fused, "doc:1")->distance, 1.0, 1e-6);
}

TEST_F(FuseLinearTest, AllEqualDistancesNormalizesToOne) {
  auto arm0 = Vec(N("doc:1", 0.5f), N("doc:2", 0.5f), N("doc:3", 0.5f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .weight = 1.0, .window = 0});
  auto fused = FuseLinear(std::move(arms));
  for (const auto& n : fused) {
    EXPECT_NEAR(n.distance, 1.0, 1e-6);
  }
}

TEST_F(FuseLinearTest, AlphaZeroSilencesArm) {
  auto arm0 = Vec(N("doc:1", 0.0f));
  auto arm1 = Vec(N("doc:1", 0.0f), N("doc:2", 1.0f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .weight = 0.0, .window = 0});
  arms.push_back({.neighbors = &arm1, .weight = 1.0, .window = 0});
  auto fused = FuseLinear(std::move(arms));
  EXPECT_NEAR(Find(fused, "doc:1")->distance, 1.0, 1e-6);
  EXPECT_NEAR(Find(fused, "doc:2")->distance, 0.0, 1e-6);
}

TEST_F(FuseLinearTest, EmptyArmContributesZero) {
  std::vector<indexes::Neighbor> arm0;
  auto arm1 = Vec(N("doc:1", 0.0f), N("doc:2", 1.0f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .weight = 0.5, .window = 0});
  arms.push_back({.neighbors = &arm1, .weight = 0.5, .window = 0});
  auto fused = FuseLinear(std::move(arms));
  EXPECT_NEAR(Find(fused, "doc:1")->distance, 0.5, 1e-6);
  EXPECT_NEAR(Find(fused, "doc:2")->distance, 0.0, 1e-6);
}

// ---------------------------- FUNCTION ----------------------------

using FuseFunctionTest = vmsdk::ValkeyTest;

TEST_F(FuseFunctionTest, ScoreFnReceivesAllArmScores) {
  auto arm0 = Vec(N("doc:1", 0.5f), N("doc:2", 0.7f));
  auto arm1 = Vec(N("doc:1", 0.2f), N("doc:3", 0.9f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .window = 0});
  arms.push_back({.neighbors = &arm1, .window = 0});
  // Verify via the resulting fused scores: combined = arm0*10 + arm1.
  auto fused =
      FuseFunction(std::move(arms),
                   [](const std::vector<std::optional<double>>& s) -> double {
                     double a = s[0].has_value() ? *s[0] : 0.0;
                     double b = s[1].has_value() ? *s[1] : 0.0;
                     return a * 10.0 + b;
                   });
  ASSERT_EQ(fused.size(), 3u);
  // doc:1 in both arms: 0.5*10 + 0.2 = 5.2
  EXPECT_NEAR(Find(fused, "doc:1")->distance, 5.2, 1e-5);
  // doc:2 in arm0 only: 0.7*10 + 0 = 7.0
  EXPECT_NEAR(Find(fused, "doc:2")->distance, 7.0, 1e-5);
  // doc:3 in arm1 only: 0*10 + 0.9 = 0.9
  EXPECT_NEAR(Find(fused, "doc:3")->distance, 0.9, 1e-5);
  // Sorted descending by combined score: doc:2 (7.0) > doc:1 (5.2) > doc:3.
  EXPECT_EQ(fused[0].external_id->Str(), "doc:2");
  EXPECT_EQ(fused[1].external_id->Str(), "doc:1");
  EXPECT_EQ(fused[2].external_id->Str(), "doc:3");
}

TEST_F(FuseFunctionTest, AbsentArmScoreIsNullopt) {
  auto arm0 = Vec(N("doc:1", 0.5f));
  auto arm1 = Vec(N("doc:2", 0.9f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .window = 0});
  arms.push_back({.neighbors = &arm1, .window = 0});
  auto fused =
      FuseFunction(std::move(arms),
                   [](const std::vector<std::optional<double>>& s) -> double {
                     // Return 1.0 if BOTH arms present, else 0.0 — lets us
                     // assert presence.
                     return (s[0].has_value() && s[1].has_value()) ? 1.0 : 0.0;
                   });
  ASSERT_EQ(fused.size(), 2u);
  // Neither doc appears in both arms, so both score 0.
  EXPECT_NEAR(Find(fused, "doc:1")->distance, 0.0, 1e-9);
  EXPECT_NEAR(Find(fused, "doc:2")->distance, 0.0, 1e-9);
}

TEST_F(FuseFunctionTest, ScoreAliasesStillPropagated) {
  auto arm0 = Vec(N("doc:1", 0.5f));
  auto arm1 = Vec(N("doc:1", 0.2f));
  std::vector<ArmInput> arms;
  arms.push_back(
      {.neighbors = &arm0, .score_alias = std::string("s"), .window = 0});
  arms.push_back(
      {.neighbors = &arm1, .score_alias = std::string("v"), .window = 0});
  auto fused =
      FuseFunction(std::move(arms),
                   [](const std::vector<std::optional<double>>& s) -> double {
                     return *s[0] + *s[1];
                   });
  ASSERT_EQ(fused.size(), 1u);
  const auto* doc1 = Find(fused, "doc:1");
  ASSERT_NE(doc1, nullptr);
  EXPECT_NEAR(doc1->distance, 0.7, 1e-5);
  EXPECT_NEAR(*AliasScore(*doc1, "s"), 0.5, 1e-6);
  EXPECT_NEAR(*AliasScore(*doc1, "v"), 0.2, 1e-6);
}

TEST_F(FuseLinearTest, ScoreAliasPropagates) {
  auto arm0 = Vec(N("doc:1", 0.5f));
  auto arm1 = Vec(N("doc:1", 0.2f), N("doc:2", 0.4f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0,
                  .score_alias = std::string("s"),
                  .weight = 0.5,
                  .window = 0});
  arms.push_back({.neighbors = &arm1,
                  .score_alias = std::string("v"),
                  .weight = 0.5,
                  .window = 0});
  auto fused = FuseLinear(std::move(arms));
  const auto* doc1 = Find(fused, "doc:1");
  ASSERT_NE(doc1, nullptr);
  EXPECT_NEAR(*AliasScore(*doc1, "s"), 0.5, 1e-6);
  EXPECT_NEAR(*AliasScore(*doc1, "v"), 0.2, 1e-6);
  const auto* doc2 = Find(fused, "doc:2");
  ASSERT_NE(doc2, nullptr);
  EXPECT_FALSE(AliasScore(*doc2, "s").has_value());
  EXPECT_NEAR(*AliasScore(*doc2, "v"), 0.4, 1e-6);
}

}  // namespace
}  // namespace valkey_search::query::fusion
