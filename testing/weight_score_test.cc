/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "src/query/predicate.h"
#include "src/query/search.h"

namespace valkey_search {

namespace {

using testing::TestParamInfo;
using testing::ValuesIn;

using query::ComposedPredicate;
using query::ComputeMatchedPredicateScore;
using query::LogicalOperator;
using query::NegatePredicate;
using query::Predicate;
using query::PredicateType;

// Concrete minimal predicate for testing score computation
class ScoreTestPredicate : public Predicate {
 public:
  explicit ScoreTestPredicate(PredicateType type) : Predicate(type) {}
  query::EvaluationResult Evaluate(
      query::Evaluator & /*evaluator*/) const override {
    return query::EvaluationResult(true);
  }
};

struct LeafScoreTestCase {
  std::string test_name;
  PredicateType type;
  float weight;
  float expected_score;
};

class LeafScoreTest : public testing::TestWithParam<LeafScoreTestCase> {};

TEST_P(LeafScoreTest, ScoreEqualsWeight) {
  const auto &test_case = GetParam();
  auto pred = std::make_unique<ScoreTestPredicate>(test_case.type);
  pred->SetWeight(test_case.weight);
  float score = ComputeMatchedPredicateScore(pred.get());
  EXPECT_FLOAT_EQ(score, test_case.expected_score);
}

INSTANTIATE_TEST_SUITE_P(LeafScoreTests, LeafScoreTest,
                         ValuesIn<LeafScoreTestCase>({
                             {
                                 .test_name = "tag_default_weight",
                                 .type = PredicateType::kTag,
                                 .weight = 1.0f,
                                 .expected_score = 1.0f,
                             },
                             {
                                 .test_name = "numeric_weight_2_5",
                                 .type = PredicateType::kNumeric,
                                 .weight = 2.5f,
                                 .expected_score = 2.5f,
                             },
                             {
                                 .test_name = "text_weight_0_1",
                                 .type = PredicateType::kText,
                                 .weight = 0.1f,
                                 .expected_score = 0.1f,
                             },
                             {
                                 .test_name = "text_weight_100",
                                 .type = PredicateType::kText,
                                 .weight = 100.0f,
                                 .expected_score = 100.0f,
                             },
                         }),
                         [](const TestParamInfo<LeafScoreTestCase> &info) {
                           return info.param.test_name;
                         });

TEST(WeightScoreTest, AndPredicateScoreSumsChildWeights) {
  std::vector<std::unique_ptr<Predicate>> children;
  auto child1 = std::make_unique<ScoreTestPredicate>(PredicateType::kTag);
  child1->SetWeight(2.0f);
  children.push_back(std::move(child1));
  auto child2 = std::make_unique<ScoreTestPredicate>(PredicateType::kNumeric);
  child2->SetWeight(3.0f);
  children.push_back(std::move(child2));

  auto composed = std::make_unique<ComposedPredicate>(LogicalOperator::kAnd,
                                                      std::move(children));
  float score = ComputeMatchedPredicateScore(composed.get());
  // 2.0 + 3.0 = 5.0, AND weight is default 1.0
  EXPECT_FLOAT_EQ(score, 5.0f);
}

TEST(WeightScoreTest, AndPredicateOwnWeightMultipliesSum) {
  std::vector<std::unique_ptr<Predicate>> children;
  auto child1 = std::make_unique<ScoreTestPredicate>(PredicateType::kTag);
  child1->SetWeight(2.0f);
  children.push_back(std::move(child1));
  auto child2 = std::make_unique<ScoreTestPredicate>(PredicateType::kNumeric);
  child2->SetWeight(3.0f);
  children.push_back(std::move(child2));

  auto composed = std::make_unique<ComposedPredicate>(LogicalOperator::kAnd,
                                                      std::move(children));
  composed->SetWeight(4.0f);
  float score = ComputeMatchedPredicateScore(composed.get());
  // (2.0 + 3.0) * 4.0 = 20.0
  EXPECT_FLOAT_EQ(score, 20.0f);
}

TEST(WeightScoreTest, AndDefaultWeightChildrenSumToCount) {
  std::vector<std::unique_ptr<Predicate>> children;
  children.push_back(std::make_unique<ScoreTestPredicate>(PredicateType::kTag));
  children.push_back(
      std::make_unique<ScoreTestPredicate>(PredicateType::kNumeric));
  children.push_back(
      std::make_unique<ScoreTestPredicate>(PredicateType::kText));

  auto composed = std::make_unique<ComposedPredicate>(LogicalOperator::kAnd,
                                                      std::move(children));
  float score = ComputeMatchedPredicateScore(composed.get());
  // 3 children * 1.0 each = 3.0
  EXPECT_FLOAT_EQ(score, 3.0f);
}

TEST(WeightScoreTest, OrPredicateDefaultWeightScoresOne) {
  std::vector<std::unique_ptr<Predicate>> children;
  auto child1 = std::make_unique<ScoreTestPredicate>(PredicateType::kTag);
  child1->SetWeight(50.0f);
  children.push_back(std::move(child1));
  auto child2 = std::make_unique<ScoreTestPredicate>(PredicateType::kNumeric);
  child2->SetWeight(25.0f);
  children.push_back(std::move(child2));

  auto composed = std::make_unique<ComposedPredicate>(LogicalOperator::kOr,
                                                      std::move(children));
  // Default weight 1.0
  float score = ComputeMatchedPredicateScore(composed.get());
  EXPECT_FLOAT_EQ(score, 1.0f);
}

TEST(WeightScoreTest, NestedAndWeightsComposeMultiplicatively) {
  // Inner AND: (child1=2.0 + child2=3.0) * inner_weight=4.0 = 20.0
  std::vector<std::unique_ptr<Predicate>> inner_children;
  auto leaf1 = std::make_unique<ScoreTestPredicate>(PredicateType::kTag);
  leaf1->SetWeight(2.0f);
  inner_children.push_back(std::move(leaf1));
  auto leaf2 = std::make_unique<ScoreTestPredicate>(PredicateType::kNumeric);
  leaf2->SetWeight(3.0f);
  inner_children.push_back(std::move(leaf2));

  auto inner_and = std::make_unique<ComposedPredicate>(
      LogicalOperator::kAnd, std::move(inner_children));
  inner_and->SetWeight(4.0f);

  // Outer AND: inner_and(20.0) + outer_leaf(5.0) = 25.0
  std::vector<std::unique_ptr<Predicate>> outer_children;
  outer_children.push_back(std::move(inner_and));
  auto outer_leaf = std::make_unique<ScoreTestPredicate>(PredicateType::kText);
  outer_leaf->SetWeight(5.0f);
  outer_children.push_back(std::move(outer_leaf));

  auto outer_and = std::make_unique<ComposedPredicate>(
      LogicalOperator::kAnd, std::move(outer_children));
  float score = ComputeMatchedPredicateScore(outer_and.get());
  // (2+3)*4 + 5 = 25.0
  EXPECT_FLOAT_EQ(score, 25.0f);
}

TEST(WeightScoreTest, NegateContributesZero) {
  // Negation is a filter, not a scoring clause: it must contribute 0 even if
  // a weight was attached, since there is no term occurrence to score.
  auto inner = std::make_unique<ScoreTestPredicate>(PredicateType::kText);
  inner->SetWeight(5.0f);
  auto negate = std::make_unique<NegatePredicate>(std::move(inner));
  negate->SetWeight(7.0f);
  float score = ComputeMatchedPredicateScore(negate.get());
  EXPECT_FLOAT_EQ(score, 0.0f);
}

TEST(WeightScoreTest, NegateDoesNotInflateEnclosingAnd) {
  // `hello -world`: the matched text leaf contributes its weight, the negation
  // contributes nothing, so the AND sum is the leaf weight alone.
  std::vector<std::unique_ptr<Predicate>> children;
  auto matched = std::make_unique<ScoreTestPredicate>(PredicateType::kText);
  matched->SetWeight(3.0f);
  children.push_back(std::move(matched));

  auto negated_inner =
      std::make_unique<ScoreTestPredicate>(PredicateType::kTag);
  negated_inner->SetWeight(9.0f);
  children.push_back(
      std::make_unique<NegatePredicate>(std::move(negated_inner)));

  auto composed = std::make_unique<ComposedPredicate>(LogicalOperator::kAnd,
                                                      std::move(children));
  float score = ComputeMatchedPredicateScore(composed.get());
  // 3.0 (text) + 0.0 (negate) = 3.0
  EXPECT_FLOAT_EQ(score, 3.0f);
}

}  // namespace
}  // namespace valkey_search
