/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/commands/filter_parser.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/indexes/vector_base.h"
#include "src/query/predicate.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"
namespace valkey_search {

namespace {

using testing::TestParamInfo;
using testing::ValuesIn;

struct FilterTestCase {
  std::string test_name;
  std::string filter;
  bool create_success{false};
  std::string create_expected_error_message;
  bool evaluate_success{false};
  std::string key{"key1"};
};

class FilterTest : public ValkeySearchTestWithParam<FilterTestCase> {
 public:
  indexes::PrefilterEvaluator evaluator_;
};

void InitIndexSchema(MockIndexSchema *index_schema) {
  data_model::NumericIndex numeric_index_proto;

  auto numeric_index_1_5 =
      std::make_shared<IndexTeser<indexes::Numeric, data_model::NumericIndex>>(
          numeric_index_proto);

  auto numeric_index_2_0 =
      std::make_shared<IndexTeser<indexes::Numeric, data_model::NumericIndex>>(
          numeric_index_proto);
  VMSDK_EXPECT_OK(numeric_index_1_5->AddRecord("key1", "1.5"));
  VMSDK_EXPECT_OK(numeric_index_2_0->AddRecord("key1", "2.0"));
  VMSDK_EXPECT_OK(index_schema->AddIndex("num_field_1.5", "num_field_1.5",
                                         numeric_index_1_5));
  VMSDK_EXPECT_OK(index_schema->AddIndex("num_field_2.0", "num_field_2.0",
                                         numeric_index_2_0));

  data_model::TagIndex tag_index_proto;
  tag_index_proto.set_separator(",");
  tag_index_proto.set_case_sensitive(true);
  auto tag_index_1 =
      std::make_shared<IndexTeser<indexes::Tag, data_model::TagIndex>>(
          tag_index_proto);
  VMSDK_EXPECT_OK(tag_index_1->AddRecord("key1", "tag1"));
  // Add records with literal special characters for escape testing
  // key_pipe has tag "a|b" (literal pipe in the stored value)
  VMSDK_EXPECT_OK(tag_index_1->AddRecord("key_pipe", "a|b"));
  // key_backslash_pipe has tag "a\|b" (backslash + pipe)
  VMSDK_EXPECT_OK(tag_index_1->AddRecord("key_backslash_pipe", R"(a\|b)"));
  // key_backslash has tag "a\" (trailing backslash)
  VMSDK_EXPECT_OK(tag_index_1->AddRecord("key_backslash", R"(a\)"));
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("tag_field_1", "tag_field_1", tag_index_1));
  auto tag_index_1_2 =
      std::make_shared<IndexTeser<indexes::Tag, data_model::TagIndex>>(
          tag_index_proto);
  VMSDK_EXPECT_OK(tag_index_1_2->AddRecord("key1", "tag2,tag1"));
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("tag_field_1_2", "tag_field_1_2", tag_index_1_2));
  auto tag_index_with_space =
      std::make_shared<IndexTeser<indexes::Tag, data_model::TagIndex>>(
          tag_index_proto);
  VMSDK_EXPECT_OK(tag_index_with_space->AddRecord("key1", "tag 1 ,tag 2"));
  VMSDK_EXPECT_OK(index_schema->AddIndex(
      "tag_field_with_space", "tag_field_with_space", tag_index_with_space));

  data_model::TagIndex tag_case_insensitive_proto;
  tag_case_insensitive_proto.set_separator("@");
  tag_case_insensitive_proto.set_case_sensitive(false);
  auto tag_field_case_insensitive =
      std::make_shared<IndexTeser<indexes::Tag, data_model::TagIndex>>(
          tag_case_insensitive_proto);
  VMSDK_EXPECT_OK(tag_field_case_insensitive->AddRecord("key1", "tag1"));
  VMSDK_EXPECT_OK(index_schema->AddIndex("tag_field_case_insensitive",
                                         "tag_field_case_insensitive",
                                         tag_field_case_insensitive));
}

TEST_P(FilterTest, ParseParams) {
  const FilterTestCase &test_case = GetParam();
  auto index_schema = CreateIndexSchema("index_schema_name").value();
  InitIndexSchema(index_schema.get());
  EXPECT_CALL(*index_schema, GetIdentifier(::testing::_))
      .Times(::testing::AnyNumber());
  FilterParser parser(*index_schema, test_case.filter);
  auto parse_results = parser.Parse();
  EXPECT_EQ(test_case.create_success, parse_results.ok());
  if (!test_case.create_success) {
    EXPECT_EQ(parse_results.status().message(),
              test_case.create_expected_error_message);
    return;
  }
  auto interned_key = StringInternStore::Intern(test_case.key);
  EXPECT_EQ(
      test_case.evaluate_success,
      evaluator_.Evaluate(*parse_results.value().root_predicate, interned_key));
}

INSTANTIATE_TEST_SUITE_P(
    FilterTests, FilterTest,
    ValuesIn<FilterTestCase>({
        {
            .test_name = "numeric_happy_path_1",
            .filter = "@num_field_1.5:[1.0 2.0]",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "numeric_happy_path_comma_separated",
            .filter = "@num_field_1.5:[1.0,2.0]",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "numeric_missing_key_1",
            .filter = "@num_field_1.5:[1.0 2.0]",
            .create_success = true,
            .evaluate_success = false,
            .key = "missing_key2",
        },
        {
            .test_name = "numeric_happy_path_2",
            .filter = "@num_field_2.0:[1.5 2.5] @num_field_1.5:[1.0 2.0]",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "numeric_happy_path_inclusive_1",
            .filter = "@num_field_2.0:[2 2.5] @num_field_1.5:[1.0 1.5]",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "numeric_invalid_range1",
            .filter = "@num_field_2.0:[2.8 2.5] @num_field_1.5:[1.0 1.5]",
            .create_success = false,
            .create_expected_error_message =
                "Start and end values of a numeric field indicate an empty "
                "range. Position: 24",
        },
        {
            .test_name = "numeric_invalid_range2",
            .filter = "@num_field_2.0:[2.5 (2.5] @num_field_1.5:[1.0 1.5]",
            .create_success = false,
            .create_expected_error_message =
                "Start and end values of a numeric field indicate an empty "
                "range. Position: 25",
        },
        {
            .test_name = "numeric_invalid_range3",
            .filter = "@num_field_2.0:[(2.5 2.5] @num_field_1.5:[1.0 1.5]",
            .create_success = false,
            .create_expected_error_message =
                "Start and end values of a numeric field indicate an empty "
                "range. Position: 25",
        },
        {
            .test_name = "numeric_valid_range1",
            .filter = "@num_field_2.0:[2.5 2.5] @num_field_1.5:[1.0 1.5]",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "numeric_happy_path_inclusive_2",
            .filter = "@num_field_2.0:[1 2] @num_field_1.5:[1.0 1.5]",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "numeric_happy_path_exclusive_1",
            .filter = "@num_field_2.0:[(2 2.5] @num_field_1.5:[1.0 1.5]",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "numeric_happy_path_exclusive_2",
            .filter = "@num_field_2.0:[1 (2.0] @num_field_1.5:[1.0 1.5]",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "numeric_happy_path_inf_1",
            .filter = "@num_field_2.0:[-inf 2.5] @num_field_1.5:[1.0 1.5]",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "numeric_happy_path_inf_2",
            .filter = " @num_field_1.5:[1.0 1.5]  @num_field_2.0:[1 +inf] ",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "numeric_happy_path_inf_3",
            .filter = " @num_field_1.5:[1.0 1.5]  @num_field_2.0:[1 inf] ",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "numeric_negate_1",
            .filter = " -@num_field_1.5:[1.0 1.4]  @num_field_2.0:[1 +inf] ",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "numeric_negate_twice_with_and",
            .filter = " -@num_field_1.5:[1.0 1.4]  -@num_field_2.0:[3 +inf] ",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "numeric_negate_twice_with_and_1",
            .filter = " -@num_field_1.5:[1.0 1.5]  -@num_field_2.0:[3 +inf] ",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "numeric_negate_twice_with_and_2",
            .filter = " -@num_field_1.5:[1.0 1.4]  -@num_field_2.0:[2 +inf] ",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "numeric_negate_twice_with_and_3",
            .filter = " -@num_field_1.5:[1.0 1.5]  -@num_field_2.0:[2 +inf] ",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "numeric_negate_twice_with_or_1",
            .filter = " -@num_field_1.5:[1.0 1.4] | -@num_field_2.0:[2 +inf] ",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "numeric_negate_twice_with_or_2",
            .filter = " -@num_field_1.5:[1.0 1.6] | -@num_field_2.0:[3 +inf] ",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "numeric_negate_twice_with_or_3",
            .filter = " -@num_field_1.5:[1.0 1.5] | -@num_field_2.0:[2 +inf] ",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "numeric_negate_2",
            .filter = " @num_field_1.5:[1.0 1.5]  -@num_field_2.0:[5 +inf] ",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "numeric_negate_3",
            .filter = " @num_field_1.5:[1.0 1.4]  @num_field_2.0:[3 +inf] ",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "numeric_negate_4",
            .filter = " -(@num_field_1.5:[1.0 1.4]  @num_field_2.0:[3 +inf]) ",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "numeric_negate_5",
            .filter =
                " - ( - (@num_field_1.5:[1.0 1.4]  @num_field_2.0:[3 +inf]) )",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "numeric_negate_6",
            .filter = " -(@num_field_1.5:[1.0 1.4] | @num_field_2.0:[3 +inf]) ",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "numeric_negate_7",
            .filter = " -(@num_field_1.5:[1.0,2] | @num_field_2.0:[3 +inf]) ",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "numeric_happy_path_or_1",
            .filter = " (@num_field_1.5:[1.0 1.5])",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "numeric_happy_path_or_2",
            .filter = " ( (@num_field_1.5:[1.0 1.5])  )",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "numeric_happy_path_or_3",
            .filter = "(@num_field_1.5:[5.0 6.5]) | (@num_field_1.5:[1.0 1.5])",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "numeric_happy_path_or_4",
            .filter = "( (   (@num_field_1.5:[5.0 6.5]) | (@num_field_1.5:[1.0 "
                      "1.5]) ) ) ",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "tag_happy_path_1",
            .filter = "@tag_field_1:{tag1}",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "tag_case_sensitive_1",
            .filter = "@tag_field_1:{Tag1}",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "tag_case_sensitive_2",
            .filter = "@tag_field_case_insensitive:{Tag1}",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "tag_case_sensitive_3",
            .filter = "@tag_field_case_insensitive:{Tag0|Tag1}",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "tag_case_sensitive_4",
            .filter = "@tag_field_case_insensitive:{Tag0@Tag5}",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "tag_missing_key_1",
            .filter = "@tag_field_1:{tag1}",
            .create_success = true,
            .evaluate_success = false,
            .key = "missing_key2",
        },
        {
            .test_name = "tag_happy_path_2",
            .filter = "@tag_field_1:{tag1|tag2}",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "tag_happy_path_4",
            .filter = "@tag_field_with_space:{tag 1|tag4}",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "tag_not_found_1",
            .filter = "@tag_field_1:{tag3 , tag4}",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "tag_not_found_2",
            .filter = "-@tag_field_with_space:{tag1|tag 2}",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "missing_closing_bracket",
            .filter = "@tag_field_with_space:{tag1 , tag 2",
            .create_success = false,
            .create_expected_error_message = "Missing closing TAG bracket, '}'",
        },
        {
            .test_name = "left_associative_1",
            .filter = "@num_field_2.0:[23 25] @num_field_2.0:[23 25] | "
                      "@num_field_2.0:[-inf 2.5]",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "left_associative_2",
            .filter = "@num_field_2.0:[23 25] @num_field_2.0:[23 25] | "
                      "@num_field_2.0:[23 25]",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "left_associative_3",
            .filter = "@num_field_2.0:[0 2.5] @num_field_2.0:[23 25] | "
                      "@num_field_2.0:[-inf 2.5]",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "left_associative_4",
            .filter = "@num_field_2.0 : [23 25] @num_field_2.0:[0 2.5] | "
                      "@num_field_2.0:[23 25]",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "or_precedence_1",
            .filter = "@num_field_2.0 : [23 25] @num_field_2.0:[23 25] | "
                      "@num_field_2.0:[0 2.5]",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "or_precedence_2",
            .filter = "@num_field_2.0 : [23 25] @num_field_2.0:[23 25] | "
                      "@num_field_2.0:[0 2.5] @num_field_2.0:[0 2.5]",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "or_precedence_3",
            .filter = "@num_field_2.0 : [23 25] @num_field_2.0:[23 25] | "
                      "@num_field_2.0:[23 25] @num_field_2.0:[0 2.5]",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "or_precedence_4",
            .filter = "@num_field_2.0 : [23 25] @num_field_2.0:[23 25] | "
                      "@num_field_2.0:[0 2.5] @num_field_2.0:[23 25]",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "or_precedence_5",
            .filter = "@num_field_2.0 : [0 2.5] @num_field_2.0:[23 25] | "
                      "@num_field_2.0:[0 2.5] @num_field_2.0:[23 25]",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "or_precedence_6",
            .filter = "@num_field_2.0 : [23 25] @num_field_2.0:[0 2.5] | "
                      "@num_field_2.0:[0 2.5] @num_field_2.0:[23 25]",
            .create_success = true,
            .evaluate_success = false,
        },
        {
            .test_name = "or_precedence_7",
            .filter = "@num_field_2.0 : [0 2.5] @num_field_2.0:[0 2.5] | "
                      "@num_field_2.0:[0 2.5] @num_field_2.0:[23 25]",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "bad_filter_1",
            .filter = "@num_field_2.0 : [23 25] -| @num_field_2.0:[0 2.5] ",
            .create_success = false,
            .create_expected_error_message =
                "Unexpected character at position 27: `|`",
        },
        {
            .test_name = "bad_filter_2",
            .filter = "@num_field_2.0 : [23 25] - | @num_field_2.0:[0 2.5] ",
            .create_success = false,
            .create_expected_error_message =
                "Unexpected character at position 28: `|`",
        },
        {
            .test_name = "bad_filter_3",
            .filter = "@num_field_2.0 : [23 25] | num_field_2.0:[0 2.5] ",
            .create_success = false,
            .create_expected_error_message =
                "Unexpected character at position 28: `n`, expecting `@`",
        },
        {
            .test_name = "bad_filter_4",
            .filter = "@num_field_2.0 : [23 25] | @num_field_2.0[0 2.5] ",
            .create_success = false,
            .create_expected_error_message =
                "Unexpected character at position 45: `2`, expecting `:`",
        },
        {
            .test_name = "bad_filter_5",
            .filter = "@num_field_2.0 : [23 25] $  @num_field_2.0:[0 2.5] ",
            .create_success = false,
            .create_expected_error_message =
                "Unexpected character at position 26: `$`, expecting `@`",
        },
        {
            .test_name = "bad_filter_6",
            .filter = "@num_field_2.0 : [23 25]   @aa:[0 2.5] ",
            .create_success = false,
            .create_expected_error_message =
                "`aa` is not indexed as a numeric field",
        },
        {
            .test_name = "bad_filter_7",
            .filter = "@num_field_2.0 : [23 25]   @ :[0 2.5] ",
            .create_success = false,
            .create_expected_error_message =
                "`` is not indexed as a numeric field",
        },
        {
            .test_name = "bad_filter_8",
            .filter = "@num_field_2.0 : [23 25]   @num_field_2.0:{0 2.5] ",
            .create_success = false,
            .create_expected_error_message =
                "`num_field_2.0` is not indexed as a tag field",
        },
        {
            .test_name = "bad_filter_9",
            .filter = "@num_field_2.0 : [23 25]   @num_field_2.0:[0 2.5} ",
            .create_success = false,
            .create_expected_error_message =
                "Expected ']' got '}'. Position: 48",
        },
        {
            .test_name = "bad_filter_10",
            .filter = "@num_field_2.0 : [23 25]   @aa:{tag1} ",
            .create_success = false,
            .create_expected_error_message =
                "`aa` is not indexed as a tag field",
        },
        {
            .test_name = "bad_filter_11",
            .filter = "@num_field_2.0 : [23 25]   @tag_field_1:[tag1} ",
            .create_success = false,
            .create_expected_error_message =
                "`tag_field_1` is not indexed as a numeric field",
        },
        {
            .test_name = "bad_filter_12",
            .filter = "@num_field_2.0 : [23 25]   @tag_field_1:{tag1] ",
            .create_success = false,
            .create_expected_error_message = "Missing closing TAG bracket, '}'",
        },
        // =================================================================
        // Tests for escaped pipe separator in tag values
        // =================================================================
        //
        // Test data setup:
        //   key1 has tag "tag1"
        //   key_pipe has tag "a|b" (literal pipe)
        //   key_backslash_pipe has tag "a\|b" (backslash + pipe)
        //   key_backslash has tag "a\" (trailing backslash)
        //
        {
            .test_name = "tag_escaped_pipe_matches_literal_pipe",
            .filter = R"(@tag_field_1:{a\|b})",
            .create_success = true,
            .evaluate_success = true,
            .key = "key_pipe",  // has tag "a|b"
        },
        {
            .test_name = "tag_escaped_backslash_matches_literal_backslash",
            .filter = R"(@tag_field_1:{a\\})",
            .create_success = true,
            .evaluate_success = true,
            .key = "key_backslash",  // has tag "a\"
        },
        {
            .test_name = "tag_escaped_backslash_pipe_matches_literal",
            .filter = R"(@tag_field_1:{a\\\|b})",
            .create_success = true,
            .evaluate_success = true,
            .key = "key_backslash_pipe",  // has tag "a\|b"
        },
        {
            .test_name = "tag_escaped_pipe_or_unescaped_first_matches",
            .filter = R"(@tag_field_1:{a\|b|tag1})",
            .create_success = true,
            .evaluate_success = true,
            .key = "key_pipe",  // has tag "a|b"
        },
        {
            .test_name = "tag_escaped_pipe_or_unescaped_second_matches",
            .filter = R"(@tag_field_1:{a\|b|tag1})",
            .create_success = true,
            .evaluate_success = true,  // "tag1" matches via naive split too
            .key = "key1",
        },
        {
            .test_name = "tag_escaped_backslash_or_literal",
            .filter = R"(@tag_field_1:{a\\|b})",
            .create_success = true,
            .evaluate_success = true,
            .key = "key_backslash",  // has tag "a\"
        },
        {
            .test_name = "tag_escaped_pipe_no_match",
            .filter = R"(@tag_field_1:{x\|y})",
            .create_success = true,
            .evaluate_success = false,
            .key = "key1",
        },
        // =================================================================
        // Hierarchical tests: ensure escaping works with AND/OR operators
        // =================================================================
        {
            .test_name = "tag_escaped_with_and_numeric",
            .filter = R"(@tag_field_1:{a\|b|tag1} @num_field_1.5:[1.0 2.0])",
            .create_success = true,
            .evaluate_success = true,
            .key = "key1",
        },
        {
            .test_name = "tag_only_escaped_matches_with_or_numeric",
            .filter = R"(@tag_field_1:{a\|b} | @num_field_1.5:[100 200])",
            .create_success = true,
            .evaluate_success = true,  // "a|b" should match, numeric doesn't
            .key = "key_pipe",
        },
    }),
    [](const TestParamInfo<FilterTestCase> &info) {
      return info.param.test_name;
    });

}  // namespace
}  // namespace valkey_search
