/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/rank_fusion.h"

#include <algorithm>
#include <cmath>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"
#include "src/attribute_data_type.h"
#include "src/indexes/scoring/scorer.h"
#include "src/indexes/vector_base.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search::query::rank_fusion {
namespace {

// Build a Neighbor with a given key and raw arm score; no attribute_contents.
// The two-arg Neighbor constructor mirrors `distance` into `score`, which is
// the field fusion reads.
indexes::Neighbor N(absl::string_view key, float score) {
  return indexes::Neighbor{StringInternStore::Intern(std::string(key)), score};
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

// Add an attribute to a neighbor the way the content producers do: the map key
// is a string_view into the identifier the mapped value itself owns.
void AddAttribute(indexes::Neighbor& n, absl::string_view identifier,
                  absl::string_view value) {
  if (!n.attribute_contents.has_value()) {
    n.attribute_contents.emplace();
  }
  auto id_str = vmsdk::MakeUniqueValkeyString(identifier);
  auto val_str = vmsdk::MakeUniqueValkeyString(value);
  auto id_view = vmsdk::ToStringView(id_str.get());
  n.attribute_contents->emplace(
      id_view, RecordsMapValue(std::move(id_str), std::move(val_str)));
}

// A RecordsMap key must be a string_view into the bytes its own mapped value
// owns. Checks the pointers, not just the text: a key left behind by an
// overwrite that replaced the value in place would still compare equal by
// content while pointing at freed memory.
void ExpectKeysViewOwnIdentifiers(const indexes::Neighbor& n) {
  ASSERT_TRUE(n.attribute_contents.has_value());
  for (const auto& [key, value] : *n.attribute_contents) {
    auto owned = vmsdk::ToStringView(value.GetIdentifier());
    EXPECT_EQ(key.data(), owned.data())
        << "key `" << key << "` does not view its own value's identifier";
    EXPECT_EQ(key, owned);
  }
}

std::optional<std::string> AttributeValue(const indexes::Neighbor& n,
                                          absl::string_view identifier) {
  if (!n.attribute_contents.has_value()) {
    return std::nullopt;
  }
  auto it = n.attribute_contents->find(identifier);
  if (it == n.attribute_contents->end()) {
    return std::nullopt;
  }
  return std::string(vmsdk::ToStringView(it->second.value.get()));
}

// vmsdk::ValkeyTest::SetUp installs the mock module-API table. Required
// because AttachArmScore allocates ValkeyModuleStrings via ValkeyModule_*.
using RRFTest = vmsdk::ValkeyTest;
using LinearTest = vmsdk::ValkeyTest;

// ---------------------------- RRF basic shapes ----------------------------

TEST_F(RRFTest, DisjointArmsContributeIndependently) {
  auto arm0 = Vec(N("doc:1", 0.1f), N("doc:2", 0.2f));
  auto arm1 = Vec(N("doc:3", 0.3f), N("doc:4", 0.4f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .rrf_constant = 60, .window = 0});
  arms.push_back({.neighbors = &arm1, .rrf_constant = 60, .window = 0});
  auto fused = RRF(std::move(arms));
  EXPECT_EQ(fused.size(), 4u);
  EXPECT_NEAR(Find(fused, "doc:1")->score, 1.0 / 61.0, 1e-7);
  EXPECT_NEAR(Find(fused, "doc:3")->score, 1.0 / 61.0, 1e-7);
  EXPECT_NEAR(Find(fused, "doc:2")->score, 1.0 / 62.0, 1e-7);
  EXPECT_NEAR(Find(fused, "doc:4")->score, 1.0 / 62.0, 1e-7);
}

TEST_F(RRFTest, FullyOverlappingArmsAddRRFContributions) {
  auto arm0 = Vec(N("doc:1", 0.1f), N("doc:2", 0.2f));
  auto arm1 = Vec(N("doc:1", 0.5f), N("doc:2", 0.6f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .rrf_constant = 60, .window = 0});
  arms.push_back({.neighbors = &arm1, .rrf_constant = 60, .window = 0});
  auto fused = RRF(std::move(arms));
  EXPECT_EQ(fused.size(), 2u);
  EXPECT_NEAR(Find(fused, "doc:1")->score, 2.0 / 61.0, 1e-7);
  EXPECT_NEAR(Find(fused, "doc:2")->score, 2.0 / 62.0, 1e-7);
}

// The critical case: partial overlap. Documents in both arms get summed
// scores; docs in only one arm get only that arm's contribution.
TEST_F(RRFTest, PartialOverlapSumsAcrossArmsForSharedDocs) {
  auto arm0 = Vec(N("doc:1", 0.1f), N("doc:2", 0.2f), N("doc:3", 0.3f));
  auto arm1 = Vec(N("doc:2", 0.5f), N("doc:4", 0.6f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .rrf_constant = 60, .window = 0});
  arms.push_back({.neighbors = &arm1, .rrf_constant = 60, .window = 0});
  auto fused = RRF(std::move(arms));
  EXPECT_EQ(fused.size(), 4u);
  EXPECT_NEAR(Find(fused, "doc:2")->score, 1.0 / 62.0 + 1.0 / 61.0, 1e-7);
  EXPECT_NEAR(Find(fused, "doc:1")->score, 1.0 / 61.0, 1e-7);
  EXPECT_NEAR(Find(fused, "doc:4")->score, 1.0 / 62.0, 1e-7);
  EXPECT_NEAR(Find(fused, "doc:3")->score, 1.0 / 63.0, 1e-7);
  EXPECT_EQ(fused[0].external_id->Str(), "doc:2");
  EXPECT_EQ(fused[3].external_id->Str(), "doc:3");
}

TEST_F(RRFTest, SameDocAtDifferentRanksAcrossArms) {
  auto arm0 = Vec(N("doc:1", 0.0f));  // rank 0
  auto arm1 = Vec(N("doc:x", 0.1f), N("doc:y", 0.2f), N("doc:z", 0.3f),
                  N("doc:a", 0.4f), N("doc:b", 0.5f), N("doc:1", 0.9f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .rrf_constant = 60, .window = 0});
  arms.push_back({.neighbors = &arm1, .rrf_constant = 60, .window = 0});
  auto fused = RRF(std::move(arms));
  EXPECT_NEAR(Find(fused, "doc:1")->score, 1.0 / 61.0 + 1.0 / 66.0, 1e-7);
}

TEST_F(RRFTest, WindowTruncatesArmContribution) {
  std::vector<indexes::Neighbor> arm0;
  arm0.reserve(100);
  for (int i = 0; i < 100; ++i) {
    arm0.push_back(N("doc:" + std::to_string(i), float(i)));
  }
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .rrf_constant = 60, .window = 10});
  auto fused = RRF(std::move(arms));
  EXPECT_EQ(fused.size(), 10u);
  EXPECT_EQ(Find(fused, "doc:50"), nullptr);
}

TEST_F(RRFTest, ConstantVariationPreservesOrdering) {
  auto arm0 = Vec(N("doc:1", 0.1f), N("doc:2", 0.2f), N("doc:3", 0.3f));
  for (double k : {1.0, 60.0, 1000.0}) {
    std::vector<ArmInput> arms;
    arms.push_back({.neighbors = &arm0, .rrf_constant = k, .window = 0});
    auto fused = RRF(std::move(arms));
    EXPECT_EQ(fused[0].external_id->Str(), "doc:1");
    EXPECT_EQ(fused[1].external_id->Str(), "doc:2");
    EXPECT_EQ(fused[2].external_id->Str(), "doc:3");
  }
}

TEST_F(RRFTest, EmptyArmYieldsOtherArmsResults) {
  std::vector<indexes::Neighbor> arm0;
  auto arm1 = Vec(N("doc:a", 0.1f), N("doc:b", 0.2f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .rrf_constant = 60, .window = 0});
  arms.push_back({.neighbors = &arm1, .rrf_constant = 60, .window = 0});
  auto fused = RRF(std::move(arms));
  EXPECT_EQ(fused.size(), 2u);
  EXPECT_NEAR(Find(fused, "doc:a")->score, 1.0 / 61.0, 1e-7);
}

TEST_F(RRFTest, AllArmsEmpty) {
  std::vector<indexes::Neighbor> arm0;
  std::vector<indexes::Neighbor> arm1;
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .rrf_constant = 60, .window = 0});
  arms.push_back({.neighbors = &arm1, .rrf_constant = 60, .window = 0});
  auto fused = RRF(std::move(arms));
  EXPECT_EQ(fused.size(), 0u);
}

TEST_F(RRFTest, SingleArmDegenerate) {
  auto arm0 = Vec(N("doc:a", 0.1f), N("doc:b", 0.2f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .rrf_constant = 60, .window = 0});
  auto fused = RRF(std::move(arms));
  EXPECT_EQ(fused.size(), 2u);
  EXPECT_NEAR(Find(fused, "doc:a")->score, 1.0 / 61.0, 1e-7);
  EXPECT_NEAR(Find(fused, "doc:b")->score, 1.0 / 62.0, 1e-7);
}

TEST_F(RRFTest, ScoreAliasPropagatesPerArmDistance) {
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
  auto fused = RRF(std::move(arms));
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

// A neighbor can arrive at fusion already carrying a database field whose
// identifier equals the arm's score_alias (the cluster path brings full
// document content into fusion). The requested score must win, and the map
// must stay well-formed: overwriting in place would leave the surviving key
// pointing at the freed identifier of the value it replaced.
TEST_F(RRFTest, ScoreAliasOverwritesCollidingAttribute) {
  auto arm0 = Vec(N("doc:1", 0.5f));
  AddAttribute(arm0[0], "search_score", "from_database");
  AddAttribute(arm0[0], "title", "hello");
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0,
                  .score_alias = std::string("search_score"),
                  .rrf_constant = 60,
                  .window = 0});
  auto fused = RRF(std::move(arms));
  ASSERT_EQ(fused.size(), 1u);
  const auto* doc1 = Find(fused, "doc:1");
  ASSERT_NE(doc1, nullptr);
  ASSERT_TRUE(doc1->attribute_contents.has_value());
  // The colliding field was replaced, not duplicated or dropped.
  EXPECT_EQ(doc1->attribute_contents->size(), 2u);
  EXPECT_EQ(AttributeValue(*doc1, "title"), "hello");
  ASSERT_TRUE(AliasScore(*doc1, "search_score").has_value());
  EXPECT_NEAR(*AliasScore(*doc1, "search_score"), 0.5, 1e-6);
  ExpectKeysViewOwnIdentifiers(*doc1);
}

TEST_F(RRFTest, DeterministicTieBreakByExternalId) {
  auto arm0 = Vec(N("doc:zzz", 0.1f));
  auto arm1 = Vec(N("doc:aaa", 0.1f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .rrf_constant = 60, .window = 0});
  arms.push_back({.neighbors = &arm1, .rrf_constant = 60, .window = 0});
  auto fused = RRF(std::move(arms));
  ASSERT_EQ(fused.size(), 2u);
  EXPECT_EQ(fused[0].external_id->Str(), "doc:aaa");
  EXPECT_EQ(fused[1].external_id->Str(), "doc:zzz");
}

// ---------------------------- LINEAR ----------------------------

TEST_F(LinearTest, DisjointArmsScaledByWeight) {
  auto arm0 = Vec(N("doc:1", 1.0f), N("doc:2", 0.5f));
  auto arm1 = Vec(N("doc:3", 1.0f), N("doc:4", 0.5f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .weight = 0.7, .window = 0});
  arms.push_back({.neighbors = &arm1, .weight = 0.3, .window = 0});
  auto fused = Linear(std::move(arms));
  EXPECT_EQ(fused.size(), 4u);
  EXPECT_NEAR(Find(fused, "doc:1")->score, 0.7, 1e-6);
  EXPECT_NEAR(Find(fused, "doc:2")->score, 0.35, 1e-6);
  EXPECT_NEAR(Find(fused, "doc:3")->score, 0.3, 1e-6);
  EXPECT_NEAR(Find(fused, "doc:4")->score, 0.15, 1e-6);
}

TEST_F(LinearTest, OverlappingDocSumsBothArms) {
  auto arm0 = Vec(N("doc:1", 0.8f), N("doc:2", 0.2f));
  auto arm1 = Vec(N("doc:1", 0.4f), N("doc:3", 0.6f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .weight = 0.5, .window = 0});
  arms.push_back({.neighbors = &arm1, .weight = 0.5, .window = 0});
  auto fused = Linear(std::move(arms));
  EXPECT_NEAR(Find(fused, "doc:1")->score, 0.6, 1e-6);
  EXPECT_NEAR(Find(fused, "doc:2")->score, 0.1, 1e-6);
  EXPECT_NEAR(Find(fused, "doc:3")->score, 0.3, 1e-6);
}

// The whole point of not normalizing: a document's fused score depends only on
// its own arm scores, never on what else came back in the same arm.
TEST_F(LinearTest, ScoreDoesNotDependOnTheRestOfTheArm) {
  auto narrow = Vec(N("doc:1", 0.4f), N("doc:2", 0.5f));
  std::vector<ArmInput> a;
  a.push_back({.neighbors = &narrow, .weight = 1.0, .window = 0});
  EXPECT_NEAR(Find(Linear(std::move(a)), "doc:1")->score, 0.4, 1e-6);

  auto wide = Vec(N("doc:1", 0.4f), N("doc:2", 9.0f));
  std::vector<ArmInput> b;
  b.push_back({.neighbors = &wide, .weight = 1.0, .window = 0});
  EXPECT_NEAR(Find(Linear(std::move(b)), "doc:1")->score, 0.4, 1e-6);
}

TEST_F(LinearTest, HigherRawScoreLeads) {
  auto arm0 = Vec(N("doc:1", 3.0f), N("doc:2", 1.0f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .weight = 1.0, .window = 0});
  auto fused = Linear(std::move(arms));
  ASSERT_EQ(fused.size(), 2u);
  EXPECT_EQ(fused[0].external_id->Str(), "doc:1");
  EXPECT_NEAR(fused[0].score, 3.0, 1e-6);
  // The fused score is mirrored into `distance` for consumers still keyed
  // on it.
  EXPECT_NEAR(fused[0].distance, fused[0].score, 1e-6);
}

TEST_F(LinearTest, AlphaZeroSilencesArm) {
  auto arm0 = Vec(N("doc:1", 1.0f));
  auto arm1 = Vec(N("doc:1", 0.25f), N("doc:2", 0.5f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .weight = 0.0, .window = 0});
  arms.push_back({.neighbors = &arm1, .weight = 1.0, .window = 0});
  auto fused = Linear(std::move(arms));
  EXPECT_NEAR(Find(fused, "doc:1")->score, 0.25, 1e-6);
  EXPECT_NEAR(Find(fused, "doc:2")->score, 0.5, 1e-6);
}

TEST_F(LinearTest, EmptyArmContributesZero) {
  std::vector<indexes::Neighbor> arm0;
  auto arm1 = Vec(N("doc:1", 1.0f), N("doc:2", 0.5f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .weight = 0.5, .window = 0});
  arms.push_back({.neighbors = &arm1, .weight = 0.5, .window = 0});
  auto fused = Linear(std::move(arms));
  EXPECT_NEAR(Find(fused, "doc:1")->score, 0.5, 1e-6);
  EXPECT_NEAR(Find(fused, "doc:2")->score, 0.25, 1e-6);
}

// ---------------------------- FUNCTION ----------------------------

using FunctionTest = vmsdk::ValkeyTest;

TEST_F(FunctionTest, ScoreFnReceivesAllArmScores) {
  auto arm0 = Vec(N("doc:1", 0.5f), N("doc:2", 0.7f));
  auto arm1 = Vec(N("doc:1", 0.2f), N("doc:3", 0.9f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .window = 0});
  arms.push_back({.neighbors = &arm1, .window = 0});
  // Verify via the resulting fused scores: combined = arm0*10 + arm1.
  auto fused =
      Function(std::move(arms),
               [](const std::vector<std::optional<double>>& s) -> double {
                 double a = s[0].has_value() ? *s[0] : 0.0;
                 double b = s[1].has_value() ? *s[1] : 0.0;
                 return a * 10.0 + b;
               });
  ASSERT_EQ(fused.size(), 3u);
  // doc:1 in both arms: 0.5*10 + 0.2 = 5.2
  EXPECT_NEAR(Find(fused, "doc:1")->score, 5.2, 1e-5);
  // doc:2 in arm0 only: 0.7*10 + 0 = 7.0
  EXPECT_NEAR(Find(fused, "doc:2")->score, 7.0, 1e-5);
  // doc:3 in arm1 only: 0*10 + 0.9 = 0.9
  EXPECT_NEAR(Find(fused, "doc:3")->score, 0.9, 1e-5);
  // Sorted descending by combined score: doc:2 (7.0) > doc:1 (5.2) > doc:3.
  EXPECT_EQ(fused[0].external_id->Str(), "doc:2");
  EXPECT_EQ(fused[1].external_id->Str(), "doc:1");
  EXPECT_EQ(fused[2].external_id->Str(), "doc:3");
}

TEST_F(FunctionTest, AbsentArmScoreIsNullopt) {
  auto arm0 = Vec(N("doc:1", 0.5f));
  auto arm1 = Vec(N("doc:2", 0.9f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .window = 0});
  arms.push_back({.neighbors = &arm1, .window = 0});
  auto fused =
      Function(std::move(arms),
               [](const std::vector<std::optional<double>>& s) -> double {
                 // Return 1.0 if BOTH arms present, else 0.0 — lets us
                 // assert presence.
                 return (s[0].has_value() && s[1].has_value()) ? 1.0 : 0.0;
               });
  ASSERT_EQ(fused.size(), 2u);
  // Neither doc appears in both arms, so both score 0.
  EXPECT_NEAR(Find(fused, "doc:1")->score, 0.0, 1e-9);
  EXPECT_NEAR(Find(fused, "doc:2")->score, 0.0, 1e-9);
}

TEST_F(FunctionTest, ScoreAliasesStillPropagated) {
  auto arm0 = Vec(N("doc:1", 0.5f));
  auto arm1 = Vec(N("doc:1", 0.2f));
  std::vector<ArmInput> arms;
  arms.push_back(
      {.neighbors = &arm0, .score_alias = std::string("s"), .window = 0});
  arms.push_back(
      {.neighbors = &arm1, .score_alias = std::string("v"), .window = 0});
  auto fused =
      Function(std::move(arms),
               [](const std::vector<std::optional<double>>& s) -> double {
                 return *s[0] + *s[1];
               });
  ASSERT_EQ(fused.size(), 1u);
  const auto* doc1 = Find(fused, "doc:1");
  ASSERT_NE(doc1, nullptr);
  EXPECT_NEAR(doc1->score, 0.7, 1e-5);
  EXPECT_NEAR(*AliasScore(*doc1, "s"), 0.5, 1e-6);
  EXPECT_NEAR(*AliasScore(*doc1, "v"), 0.2, 1e-6);
}

TEST_F(LinearTest, ScoreAliasPropagates) {
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
  auto fused = Linear(std::move(arms));
  const auto* doc1 = Find(fused, "doc:1");
  ASSERT_NE(doc1, nullptr);
  EXPECT_NEAR(*AliasScore(*doc1, "s"), 0.5, 1e-6);
  EXPECT_NEAR(*AliasScore(*doc1, "v"), 0.2, 1e-6);
  const auto* doc2 = Find(fused, "doc:2");
  ASSERT_NE(doc2, nullptr);
  EXPECT_FALSE(AliasScore(*doc2, "s").has_value());
  EXPECT_NEAR(*AliasScore(*doc2, "v"), 0.4, 1e-6);
}

// A COMBINE FUNCTION that divides by zero yields NaN, which has no order under
// `<`/`>`. Comparing it directly makes it equivalent to every other score while
// those stay ordered among themselves, which is not a strict weak ordering:
// std::sort's unguarded loops then run off the end of the range. NaN ranks last
// instead, so the order is total and the document still reaches the reply.
TEST_F(FunctionTest, NanScoreRanksLastAndLeavesTheRestOrdered) {
  auto arm0 = Vec(N("doc:1", 1.0f), N("doc:2", 0.0f), N("doc:3", 3.0f),
                  N("doc:4", 2.0f));
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .window = 0});
  // doc:2 scores NaN. FuncDiv produces it two ways for 0/0: the current path
  // divides at runtime, and the emulate-release path returns std::nan("")
  // outright (src/expr/value.cc:559-567). This mirrors the latter, because a
  // literal `a / a` here is folded to 1.0 under -ffast-math before it can
  // become a NaN at all.
  auto fused =
      Function(std::move(arms),
               [](const std::vector<std::optional<double>>& s) -> double {
                 double a = s[0].has_value() ? *s[0] : 0.0;
                 return a == 0.0 ? std::nan("") : a;
               });
  ASSERT_EQ(fused.size(), 4u);
  EXPECT_TRUE(indexes::scoring::IsNaN(Find(fused, "doc:2")->score));
  // The real scores keep their descending order (3.0, 2.0, 1.0) and doc:2
  // sorts last, despite being neither the largest nor the smallest by key.
  EXPECT_EQ(fused[0].external_id->Str(), "doc:3");
  EXPECT_EQ(fused[1].external_id->Str(), "doc:4");
  EXPECT_EQ(fused[2].external_id->Str(), "doc:1");
  EXPECT_EQ(fused[3].external_id->Str(), "doc:2");
}

// The comparator has to stay a strict weak ordering when NaN is the majority,
// and when NaNs tie with each other. Ties among them fall through to the key,
// so the order is deterministic rather than merely legal.
//
// CAVEAT: this pins that behaviour, it does not guard the fix. It passes
// against the previous comparator too, even at this size, which was chosen to
// push std::sort off its short-run insertion path. That comparator opened with
// `a.score != b.score`, which for two NaNs should be true and leave them
// arbitrarily ordered -- but -ffast-math implies -ffinite-math-only, under
// which the compiler may fold `x != x` to false, so it fell through to the
// same key tie-break and produced the same order. The regression guard is
// NanScoreRanksLastAndLeavesTheRestOrdered above, which does fail without the
// fix.
TEST_F(FunctionTest, SeveralNanScoresStayOrderedByKey) {
  // Large enough that std::sort takes its introsort path rather than the
  // insertion sort used for short ranges, where a broken comparator can still
  // land on the right answer by luck.
  std::vector<indexes::Neighbor> arm0;
  arm0.push_back(N("doc:1", 4.0f));
  for (int i = 2; i <= 40; ++i) {
    arm0.push_back(N(absl::StrCat("doc:", i), 0.0f));
  }
  std::vector<ArmInput> arms;
  arms.push_back({.neighbors = &arm0, .window = 0});
  auto fused =
      Function(std::move(arms),
               [](const std::vector<std::optional<double>>& s) -> double {
                 double a = s[0].has_value() ? *s[0] : 0.0;
                 return a == 0.0 ? std::nan("") : a;
               });
  ASSERT_EQ(fused.size(), 40u);
  // doc:1 keeps its 4.0 and leads; every other document scores NaN and the
  // ties fall through to the key, which orders lexically ("doc:10" < "doc:2").
  EXPECT_EQ(fused[0].external_id->Str(), "doc:1");
  EXPECT_FALSE(indexes::scoring::IsNaN(fused[0].score));
  std::vector<std::string> keys;
  for (size_t i = 1; i < fused.size(); ++i) {
    EXPECT_TRUE(indexes::scoring::IsNaN(fused[i].score))
        << fused[i].external_id->Str();
    keys.push_back(std::string(fused[i].external_id->Str()));
  }
  EXPECT_TRUE(std::is_sorted(keys.begin(), keys.end()))
      << "NaN-scored documents are not in key order";
}

}  // namespace
}  // namespace valkey_search::query::rank_fusion
