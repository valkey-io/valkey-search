/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <optional>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/commands/filter_parser.h"
#include "src/query/predicate.h"
#include "testing/common.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search {
namespace {

using query::EvaluationResult;
using query::Evaluator;
using query::NumericPredicate;
using query::PredicateType;
using query::TagPredicate;
using query::TextPredicate;
using query::VectorRangePredicate;

// Mock evaluator that tracks calls to EvaluateVectorRange.
class MockEvaluator : public Evaluator {
 public:
  MockEvaluator() : Evaluator(QueryOperations::kContainsVectorRange) {}

  MOCK_METHOD(EvaluationResult, EvaluateText,
              (const TextPredicate &predicate, bool require_positions),
              (override));
  MOCK_METHOD(EvaluationResult, EvaluateTags, (const TagPredicate &predicate),
              (override));
  MOCK_METHOD(EvaluationResult, EvaluateNumeric,
              (const NumericPredicate &predicate), (override));
  MOCK_METHOD(EvaluationResult, EvaluateVectorRange,
              (const VectorRangePredicate &predicate), (override));
  MOCK_METHOD(const InternedStringPtr &, GetTargetKey, (), (const, override));
};

// --- Construction and accessor tests ---

class VectorRangePredicateTest : public vmsdk::ValkeyTest {};

TEST_F(VectorRangePredicateTest, ConstructionWithAllParameters) {
  VectorRangePredicate pred("my_vec", "vec_id", 0.5, "blob_param",
                            std::optional<std::string>("dist_field"),
                            std::optional<double>(0.01));

  EXPECT_EQ(pred.GetAlias(), "my_vec");
  EXPECT_EQ(pred.GetIdentifier(), "vec_id");
  EXPECT_DOUBLE_EQ(pred.GetRadius(), 0.5);
  EXPECT_EQ(pred.GetVectorParamName(), "blob_param");
  ASSERT_TRUE(pred.GetScoreAs().has_value());
  EXPECT_EQ(pred.GetScoreAs().value(), "dist_field");
  ASSERT_TRUE(pred.GetEpsilon().has_value());
  EXPECT_DOUBLE_EQ(pred.GetEpsilon().value(), 0.01);
  EXPECT_EQ(pred.GetType(), PredicateType::kVectorRange);
}

TEST_F(VectorRangePredicateTest, ConstructionWithoutOptionalParameters) {
  VectorRangePredicate pred("field", "field_id", 1.0, "vec_param", std::nullopt,
                            std::nullopt);

  EXPECT_EQ(pred.GetAlias(), "field");
  EXPECT_EQ(pred.GetIdentifier(), "field_id");
  EXPECT_DOUBLE_EQ(pred.GetRadius(), 1.0);
  EXPECT_EQ(pred.GetVectorParamName(), "vec_param");
  EXPECT_FALSE(pred.GetScoreAs().has_value());
  EXPECT_FALSE(pred.GetEpsilon().has_value());
}

TEST_F(VectorRangePredicateTest, ConstructionWithScoreAsOnly) {
  VectorRangePredicate pred("vec", "v_id", 2.5, "blob",
                            std::optional<std::string>("my_score"),
                            std::nullopt);

  ASSERT_TRUE(pred.GetScoreAs().has_value());
  EXPECT_EQ(pred.GetScoreAs().value(), "my_score");
  EXPECT_FALSE(pred.GetEpsilon().has_value());
}

TEST_F(VectorRangePredicateTest, ConstructionWithEpsilonOnly) {
  VectorRangePredicate pred("vec", "v_id", 2.5, "blob", std::nullopt,
                            std::optional<double>(0.1));

  EXPECT_FALSE(pred.GetScoreAs().has_value());
  ASSERT_TRUE(pred.GetEpsilon().has_value());
  EXPECT_DOUBLE_EQ(pred.GetEpsilon().value(), 0.1);
}

TEST_F(VectorRangePredicateTest, ZeroRadius) {
  VectorRangePredicate pred("vec", "v_id", 0.0, "blob", std::nullopt,
                            std::nullopt);

  EXPECT_DOUBLE_EQ(pred.GetRadius(), 0.0);
}

TEST_F(VectorRangePredicateTest, LargeRadius) {
  VectorRangePredicate pred("vec", "v_id", 1e12, "blob", std::nullopt,
                            std::nullopt);

  EXPECT_DOUBLE_EQ(pred.GetRadius(), 1e12);
}

// --- SetResolvedQuery / GetResolvedQuery round-trip ---

TEST_F(VectorRangePredicateTest, ResolvedQueryRoundTrip) {
  VectorRangePredicate pred("vec", "v_id", 1.0, "blob", std::nullopt,
                            std::nullopt);

  EXPECT_TRUE(pred.GetResolvedQuery().empty());

  std::string query_blob(16, '\0');
  for (int i = 0; i < 16; ++i) {
    query_blob[i] = static_cast<char>(i);
  }
  pred.SetResolvedQuery(query_blob);

  EXPECT_EQ(pred.GetResolvedQuery(), query_blob);
  EXPECT_EQ(pred.GetResolvedQuery().size(), 16);
}

TEST_F(VectorRangePredicateTest, ResolvedQueryOverwrite) {
  VectorRangePredicate pred("vec", "v_id", 1.0, "blob", std::nullopt,
                            std::nullopt);

  pred.SetResolvedQuery("first_query");
  EXPECT_EQ(pred.GetResolvedQuery(), "first_query");

  pred.SetResolvedQuery("second_query");
  EXPECT_EQ(pred.GetResolvedQuery(), "second_query");
}

TEST_F(VectorRangePredicateTest, ResolvedQueryBinaryData) {
  VectorRangePredicate pred("vec", "v_id", 1.0, "blob", std::nullopt,
                            std::nullopt);

  // Simulate a 4-dimensional float32 vector blob.
  std::string binary_blob(16, '\0');
  float values[] = {1.0f, 2.0f, 3.0f, 4.0f};
  memcpy(binary_blob.data(), values, sizeof(values));

  pred.SetResolvedQuery(binary_blob);
  EXPECT_EQ(pred.GetResolvedQuery().size(), 16);
  EXPECT_EQ(pred.GetResolvedQuery(), binary_blob);
}

// --- Evaluate delegates to mock evaluator ---

TEST_F(VectorRangePredicateTest, EvaluateDelegatesToEvaluateVectorRange) {
  VectorRangePredicate pred("vec", "v_id", 0.5, "blob", std::nullopt,
                            std::nullopt);
  MockEvaluator evaluator;

  EXPECT_CALL(evaluator, EvaluateVectorRange(testing::Ref(pred)))
      .WillOnce(testing::Return(EvaluationResult(true)));

  auto result = pred.Evaluate(evaluator);
  EXPECT_TRUE(result.matches);
}

TEST_F(VectorRangePredicateTest, EvaluateDelegatesAndReturnsFalse) {
  VectorRangePredicate pred("vec", "v_id", 0.5, "blob", std::nullopt,
                            std::nullopt);
  MockEvaluator evaluator;

  EXPECT_CALL(evaluator, EvaluateVectorRange(testing::Ref(pred)))
      .WillOnce(testing::Return(EvaluationResult(false)));

  auto result = pred.Evaluate(evaluator);
  EXPECT_FALSE(result.matches);
}

}  // namespace
}  // namespace valkey_search
