/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/fanout_content_limit.h"

#include <cmath>
#include <cstdint>
#include <memory>
#include <vector>

#include "absl/types/span.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/query/search.h"
#include "testing/common.h"
#include "vmsdk/src/cluster_map.h"

namespace valkey_search::query::fanout {
namespace {

using ::testing::Each;
using ::testing::ElementsAre;

std::vector<uint64_t> Estimate(double alpha, uint64_t k,
                               std::vector<double> weights) {
  std::vector<uint64_t> limits(weights.size());
  RecoveryRateEstimator(alpha).Estimate({k, weights}, absl::MakeSpan(limits));
  return limits;
}

std::vector<double> Equal(size_t n) {
  return std::vector<double>(n, 1.0 / static_cast<double>(n));
}

// ---------------------------------------------------------------------------
// RecoveryRateEstimator
// ---------------------------------------------------------------------------

// Values checked against an exact binomial computation.
TEST(RecoveryRateEstimatorTest, EqualSharesMatchExactBinomial) {
  EXPECT_THAT(Estimate(0.10, 10, Equal(2)), ElementsAre(8, 8));
  EXPECT_THAT(Estimate(0.10, 10, Equal(4)), Each(5));
  EXPECT_THAT(Estimate(0.01, 10, Equal(2)), Each(9));
  EXPECT_THAT(Estimate(0.01, 10, Equal(4)), Each(7));
  EXPECT_THAT(Estimate(0.10, 100, Equal(2)), Each(58));
  EXPECT_THAT(Estimate(0.10, 100, Equal(4)), Each(34));
}

TEST(RecoveryRateEstimatorTest, StaysBetweenMeanShareAndK) {
  for (double alpha : {0.001, 0.01, 0.1, 0.5, 1.0}) {
    for (uint64_t k : {1u, 2u, 7u, 10u, 100u, 1000u, 100000u}) {
      for (size_t n : {2u, 3u, 8u, 64u, 500u}) {
        const auto weights = Equal(n);
        const uint64_t mean_share =
            static_cast<uint64_t>(std::ceil(k * weights[0] - 1e-9));
        for (uint64_t limit : Estimate(alpha, k, weights)) {
          EXPECT_GE(limit, mean_share) << alpha << " " << k << " " << n;
          EXPECT_LE(limit, k) << alpha << " " << k << " " << n;
        }
      }
    }
  }
}

TEST(RecoveryRateEstimatorTest, ZeroRateNeverBounds) {
  EXPECT_THAT(Estimate(0.0, 100, Equal(8)), Each(100));
}

TEST(RecoveryRateEstimatorTest, FullRateIsMostAggressive) {
  // Each shard may overflow with probability 1/N, so the limit sits a little
  // above the mean share of 25.
  EXPECT_THAT(Estimate(1.0, 100, Equal(4)), Each(28));
  EXPECT_THAT(Estimate(1.0, 10, Equal(2)), Each(5));
}

TEST(RecoveryRateEstimatorTest, HigherRateNeverRaisesTheLimit) {
  uint64_t previous = 100;
  for (double alpha = 0.001; alpha <= 1.0; alpha *= 2) {
    const uint64_t limit = Estimate(alpha, 100, Equal(4))[0];
    EXPECT_LE(limit, previous) << alpha;
    previous = limit;
  }
}

TEST(RecoveryRateEstimatorTest, LargerShareGetsLargerLimit) {
  EXPECT_THAT(Estimate(0.10, 100, {0.6, 0.4}), ElementsAre(68, 48));
}

TEST(RecoveryRateEstimatorTest, ShardWithNoShareGetsNoContent) {
  EXPECT_THAT(Estimate(0.10, 10, {1.0, 0.0}), ElementsAre(10, 0));
}

// ---------------------------------------------------------------------------
// FanoutContentPlanner
// ---------------------------------------------------------------------------

// FT.SEARCH opts in; the base parameters do not.
class BoundableSearchParameters : public UnitTestSearchParameters {
 public:
  bool SupportsBoundedContent() const override { return supports; }
  bool supports{true};
};

BoundableSearchParameters TextSearch(uint64_t offset, uint64_t count) {
  BoundableSearchParameters parameters;
  parameters.limit = LimitParameter{offset, count};
  return parameters;
}

std::vector<vmsdk::cluster_map::NodeInfo> Targets(size_t n) {
  return std::vector<vmsdk::cluster_map::NodeInfo>(n);
}

// The planner tests build search parameters, which use the module mock.
class FanoutContentPlannerTest : public ValkeySearchTest {};

// Always estimates `value`, to test the planner apart from the estimator.
class FixedEstimator : public ContentLimitEstimator {
 public:
  explicit FixedEstimator(uint64_t value) : value_(value) {}
  void Estimate(const EstimateInput &,
                absl::Span<uint64_t> limits) const override {
    for (auto &limit : limits) {
      limit = value_;
    }
  }

 private:
  uint64_t value_;
};

TEST_F(FanoutContentPlannerTest, PlansKCandidatesAndEstimatedContent) {
  RecoveryRateEstimator estimator(0.10);
  auto parameters = TextSearch(0, 10);
  auto targets = Targets(4);
  auto plan = FanoutContentPlanner(estimator).Plan(parameters, targets);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ(plan->candidate_limit, 10u);
  EXPECT_THAT(plan->content_limits, ElementsAre(5, 5, 5, 5));
}

TEST_F(FanoutContentPlannerTest, CandidateLimitIsOffsetPlusCount) {
  FixedEstimator estimator(1);
  auto parameters = TextSearch(5, 10);
  auto targets = Targets(2);
  auto plan = FanoutContentPlanner(estimator).Plan(parameters, targets);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ(plan->candidate_limit, 15u);
}

TEST_F(FanoutContentPlannerTest, VectorQueryUsesK) {
  FixedEstimator estimator(1);
  BoundableSearchParameters parameters;
  parameters.attribute_alias = "vec";
  parameters.k = 20;
  parameters.limit = LimitParameter{0, 10};
  auto targets = Targets(2);
  auto plan = FanoutContentPlanner(estimator).Plan(parameters, targets);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ(plan->candidate_limit, 20u);
}

TEST_F(FanoutContentPlannerTest, NoPlanWhenNothingIsSaved) {
  FixedEstimator estimator(10);
  auto parameters = TextSearch(0, 10);
  auto targets = Targets(4);
  EXPECT_FALSE(FanoutContentPlanner(estimator).Plan(parameters, targets));
}

TEST_F(FanoutContentPlannerTest, NoPlanForASingleTarget) {
  FixedEstimator estimator(1);
  auto parameters = TextSearch(0, 10);
  auto targets = Targets(1);
  EXPECT_FALSE(FanoutContentPlanner(estimator).Plan(parameters, targets));
}

TEST_F(FanoutContentPlannerTest, IneligibleShapes) {
  auto no_content = TextSearch(0, 10);
  no_content.no_content = true;
  EXPECT_FALSE(FanoutContentPlanner::IsEligible(no_content));

  auto sorted = TextSearch(0, 10);
  sorted.sortby_parameter = SortByParameter{};
  EXPECT_FALSE(FanoutContentPlanner::IsEligible(sorted));

  auto inkeys = TextSearch(0, 10);
  inkeys.inkeys.emplace();
  EXPECT_FALSE(FanoutContentPlanner::IsEligible(inkeys));

  auto not_opted_in = TextSearch(0, 10);
  not_opted_in.supports = false;
  EXPECT_FALSE(FanoutContentPlanner::IsEligible(not_opted_in));

  EXPECT_FALSE(FanoutContentPlanner::IsEligible(TextSearch(0, 0)));

  BoundableSearchParameters offset_past_k;
  offset_past_k.attribute_alias = "vec";
  offset_past_k.k = 5;
  offset_past_k.limit = LimitParameter{5, 10};
  EXPECT_FALSE(FanoutContentPlanner::IsEligible(offset_past_k));

  EXPECT_TRUE(FanoutContentPlanner::IsEligible(TextSearch(0, 10)));
}

TEST_F(FanoutContentPlannerTest, SharesFollowSlotOwnership) {
  // 3/4 of the slots on one shard, 1/4 on the other.
  vmsdk::cluster_map::ShardInfo big;
  big.owned_slots = {{0, 12287}};
  vmsdk::cluster_map::ShardInfo small;
  small.owned_slots = {{12288, 16383}};
  auto targets = Targets(2);
  targets[0].shard = &big;
  targets[1].shard = &small;
  RecoveryRateEstimator estimator(0.10);
  auto parameters = TextSearch(0, 100);
  auto plan = FanoutContentPlanner(estimator).Plan(parameters, targets);
  ASSERT_TRUE(plan.has_value());
  std::vector<uint64_t> expected(2);
  estimator.Estimate({100, std::vector<double>{0.75, 0.25}},
                     absl::MakeSpan(expected));
  EXPECT_THAT(plan->content_limits, ElementsAre(expected[0], expected[1]));
  EXPECT_GT(plan->content_limits[0], plan->content_limits[1]);
}

TEST_F(FanoutContentPlannerTest, UnknownSlotOwnershipFallsBackToEqualShares) {
  vmsdk::cluster_map::ShardInfo known;
  known.owned_slots = {{0, 12287}};
  auto targets = Targets(2);
  targets[0].shard = &known;  // targets[1].shard stays null
  RecoveryRateEstimator estimator(0.10);
  auto parameters = TextSearch(0, 10);
  auto plan = FanoutContentPlanner(estimator).Plan(parameters, targets);
  ASSERT_TRUE(plan.has_value());
  EXPECT_THAT(plan->content_limits, ElementsAre(8, 8));
}

}  // namespace
}  // namespace valkey_search::query::fanout
