/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <memory>
#include <string>

#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/commands/filter_parser.h"
#include "src/index_schema.pb.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/indexes/text.h"
#include "src/query/predicate.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"

namespace valkey_search {
namespace {

using testing::TestParamInfo;
using testing::ValuesIn;

void InitQMATestIndexSchema(MockIndexSchema *index_schema) {
  data_model::TagIndex tag_index_proto;
  tag_index_proto.set_separator(",");
  tag_index_proto.set_case_sensitive(true);
  auto tag_index =
      std::make_shared<IndexTeser<indexes::Tag, data_model::TagIndex>>(
          tag_index_proto);
  VMSDK_EXPECT_OK(tag_index->AddRecord("key1", "dogs"));
  VMSDK_EXPECT_OK(index_schema->AddIndex("title", "title", tag_index));

  data_model::NumericIndex numeric_index_proto;
  auto numeric_index =
      std::make_shared<IndexTeser<indexes::Numeric, data_model::NumericIndex>>(
          numeric_index_proto);
  VMSDK_EXPECT_OK(numeric_index->AddRecord("key1", "50"));
  VMSDK_EXPECT_OK(index_schema->AddIndex("price", "price", numeric_index));

  index_schema->CreateTextIndexSchema();
  auto text_index_schema = index_schema->GetTextIndexSchema();
  data_model::TextIndex text_index_proto =
      CreateTextIndexProto(true, false, 1.0);
  auto text_index =
      std::make_shared<indexes::Text>(text_index_proto, text_index_schema);
  VMSDK_EXPECT_OK(index_schema->AddIndex("body", "body", text_index));

  auto key1 = StringInternStore::Intern("key1");
  std::string test_data = "dogs cats hello world";
  VMSDK_EXPECT_OK(text_index->AddRecord(key1, test_data));
  text_index_schema->CommitKeyData(key1);
}

struct QMAParserTestCase {
  std::string test_name;
  std::string filter;
  bool parse_success{false};
  std::string expected_error_substr;
  // Expected weight on root predicate (only checked if parse_success)
  float expected_weight{1.0f};
  // For multi-clause tests: expected weights on children
  bool check_children{false};
  size_t expected_child_count{0};
  float expected_child_weight_0{1.0f};
  float expected_child_weight_1{1.0f};
};

class QMAWeightParserTest
    : public ValkeySearchTestWithParam<QMAParserTestCase> {};

TEST_P(QMAWeightParserTest, ParseQMA) {
  const auto &test_case = GetParam();
  auto index_schema = CreateIndexSchema("qma_parser_test").value();
  InitQMATestIndexSchema(index_schema.get());
  EXPECT_CALL(*index_schema, GetIdentifier(::testing::_))
      .Times(::testing::AnyNumber());

  TextParsingOptions options{};
  FilterParser parser(*index_schema, test_case.filter, options);
  auto result = parser.Parse();

  EXPECT_EQ(test_case.parse_success, result.ok())
      << "Filter: " << test_case.filter << " Error: "
      << (result.ok() ? "" : std::string(result.status().message()));

  if (!test_case.parse_success) {
    if (!test_case.expected_error_substr.empty()) {
      EXPECT_THAT(std::string(result.status().message()),
                  ::testing::HasSubstr(test_case.expected_error_substr))
          << "Filter: " << test_case.filter;
    }
    return;
  }

  ASSERT_NE(result.value().root_predicate, nullptr);

  if (test_case.check_children) {
    auto *root = result.value().root_predicate.get();
    ASSERT_EQ(root->GetType(), query::PredicateType::kComposedAnd);
    auto *composed = dynamic_cast<query::ComposedPredicate *>(root);
    ASSERT_NE(composed, nullptr);
    ASSERT_EQ(composed->GetChildCount(), test_case.expected_child_count);
    EXPECT_NEAR(composed->GetChildren()[0]->GetWeight(),
                test_case.expected_child_weight_0, 0.01f);
    EXPECT_NEAR(composed->GetChildren()[1]->GetWeight(),
                test_case.expected_child_weight_1, 0.01f);
  } else {
    EXPECT_NEAR(result.value().root_predicate->GetWeight(),
                test_case.expected_weight, 0.01f)
        << "Filter: " << test_case.filter;
  }
}

INSTANTIATE_TEST_SUITE_P(
    QMAWeightParserTests, QMAWeightParserTest,
    ValuesIn<QMAParserTestCase>({
        // Property 1: QMA block parsing extracts correct weight
        {
            .test_name = "weight_integer",
            .filter = "(@title:{dogs}) => { $weight: 2; }",
            .parse_success = true,
            .expected_weight = 2.0f,
        },
        {
            .test_name = "weight_small_float",
            .filter = "(@title:{dogs}) => { $weight: 0.01; }",
            .parse_success = true,
            .expected_weight = 0.01f,
        },
        {
            .test_name = "weight_large_float",
            .filter = "(@title:{dogs}) => { $weight: 99.9; }",
            .parse_success = true,
            .expected_weight = 99.9f,
        },
        {
            .test_name = "weight_no_semicolon",
            .filter = "(@title:{dogs}) => { $weight: 5.0 }",
            .parse_success = true,
            .expected_weight = 5.0f,
        },
        // OR clause with QMA weight
        {
            .test_name = "or_clause_with_weight",
            .filter = "(@title:{dogs} | @body:cats) => { $weight: 6.0; }",
            .parse_success = true,
            .expected_weight = 6.0f,
        },
        // Numeric predicate with weight
        {
            .test_name = "numeric_with_weight",
            .filter = "(@price:[10 100]) => { $weight: 2.5; }",
            .parse_success = true,
            .expected_weight = 2.5f,
        },
        // Multiple clauses each with their own weight (nested composition)
        {
            .test_name = "two_clauses_each_with_weight",
            .filter = "(@title:{dogs}) => { $weight: 3.0; } "
                      "(@price:[10 100]) => { $weight: 8.0; }",
            .parse_success = true,
            .check_children = true,
            .expected_child_count = 2,
            .expected_child_weight_0 = 3.0f,
            .expected_child_weight_1 = 8.0f,
        },
        // Unrecognized QMA attributes are rejected
        {
            .test_name = "unrecognized_attr_word",
            .filter = "(@title:{dogs}) => { $none: 2.0; }",
            .parse_success = false,
            .expected_error_substr = "Unsupported QMA attribute",
        },
        {
            .test_name = "unrecognized_attr_number",
            .filter = "(@title:{dogs}) => { $1234: 1.0; }",
            .parse_success = false,
            .expected_error_substr = "Unsupported QMA attribute",
        },
        // Invalid weight values are rejected
        {
            .test_name = "weight_zero",
            .filter = "(@title:{dogs}) => { $weight: 0; }",
            .parse_success = false,
            .expected_error_substr = "positive",
        },
        {
            .test_name = "weight_negative",
            .filter = "(@title:{dogs}) => { $weight: -1.0; }",
            .parse_success = false,
        },
        {
            .test_name = "weight_non_numeric_abc",
            .filter = "(@title:{dogs}) => { $weight: abc; }",
            .parse_success = false,
        },
        {
            .test_name = "weight_non_numeric_true",
            .filter = "(@title:{dogs}) => { $weight: true; }",
            .parse_success = false,
        },
        {
            .test_name = "weight_non_numeric_nan",
            .filter = "(@title:{dogs}) => { $weight: NaN; }",
            .parse_success = false,
        },
        {
            .test_name = "weight_missing_value_semicolon",
            .filter = "(@title:{dogs}) => { $weight: ; }",
            .parse_success = false,
        },
        {
            .test_name = "weight_missing_value_brace",
            .filter = "(@title:{dogs}) => { $weight: }",
            .parse_success = false,
        },
        // Malformed QMA block
        {
            .test_name = "missing_closing_brace",
            .filter = "(@title:{dogs}) => { $weight: 2.0; ",
            .parse_success = false,
        },
    }),
    [](const TestParamInfo<QMAParserTestCase> &info) {
      return info.param.test_name;
    });

}  // namespace
}  // namespace valkey_search
