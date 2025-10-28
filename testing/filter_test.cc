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
#include "src/commands/ft_create_parser.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/indexes/text.h"
#include "src/indexes/text/text_index.h"
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
  indexes::InlineVectorEvaluator evaluator_;
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

  index_schema->CreateTextIndexSchema();
  auto text_index_schema = index_schema->GetTextIndexSchema();
  data_model::TextIndex text_index_proto = CreateTextIndexProto(true, false, 4);
  auto text_index_1 =
      std::make_shared<indexes::Text>(text_index_proto, text_index_schema);
  auto text_index_2 =
      std::make_shared<indexes::Text>(text_index_proto, text_index_schema);
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("text_field1", "text_field1", text_index_1));
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("text_field2", "text_field2", text_index_2));
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
            .filter = "@tag_field_case_insensitive:{Tag0@Tag1}",
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
            .filter = "@tag_field_1:{tag1 , tag2}",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "tag_happy_path_4",
            .filter = "@tag_field_with_space:{tag 1 , tag4}",
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
            .filter = "-@tag_field_with_space:{tag1 , tag 2}",
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
            .test_name = "exact_term",
            .filter = "@text_field1:word",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "exact_prefix",
            .filter = "@text_field1:word*",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "exact_suffix",
            .filter = "@text_field1:*word",
            .create_success = false,
            .create_expected_error_message = "Index created without Suffix Trie",
        },
        {
            .test_name = "exact_inffix",
            .filter = "@text_field1:*word*",
            .create_success = false,
            .create_expected_error_message = "Index created without Suffix Trie",
        },
        {
            .test_name = "exact_fuzzy1",
            .filter = "@text_field1:%word%",
            .create_success = false,
            .create_expected_error_message = "Unsupported query operation",
        },
        {
            .test_name = "exact_fuzzy2",
            .filter = "@text_field1:%%word%%",
            .create_success = false,
            .create_expected_error_message = "Unsupported query operation",
        },
        {
            .test_name = "exact_fuzzy3",
            .filter = "@text_field1:%%%word%%%",
            .create_success = false,
            .create_expected_error_message = "Unsupported query operation",
        },
        {
            .test_name = "proximity1",
            .filter = "@text_field1:\"hello my name is\"",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "proximity2",
            .filter = "@text_field1:hello @text_field2:my @text_field1:name "
                      "@text_field2:is",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "default_field_text",
            .filter = "Hello, how are you doing?",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "default_field_exact_phrase",
            .filter = "\"Hello, how are you doing?\"",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "default_field_exact_phrase_with_punct",
            .filter = "\"Hello, h(ow a)re yo#u doi_n$g?\"",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "default_field_with_escape1",
            .filter = "\"\\\\\\\\\\Hello, \\how \\\\are \\\\\\you \\\\\\\\doing?\"",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "default_field_with_escape2",
            .filter = "\\\\\\\\\\Hello, \\how \\\\are \\\\\\you \\\\\\\\doing?",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "default_field_with_escape3",
            .filter = "Hel\\(lo, ho\\$w a\\*re yo\\{u do\\|ing?",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "default_field_with_escape4",
            .filter = "\\\\\\\\\\(Hello, \\$how \\\\\\*are \\\\\\-you \\\\\\\\\\%doing?",
            .create_success = true,
            .evaluate_success = true,
        },        
        {
            .test_name = "default_field_with_escape5",
            .filter = "Hello, how are you\\% doing",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "default_field_with_escape6",
            .filter = "Hello, how are you\\\\\\\\\\% doing",
            .create_success = true,
            .evaluate_success = true,
        },
        {
            .test_name = "default_field_with_all_operations",
            .filter = "%Hllo%, how are *ou do* *oda*",
            .create_success = false,
            .create_expected_error_message = "Index created without Suffix Trie",
        },
        {
            .test_name = "proximity3",
            .filter =
                "@text_field1:\"Advanced Neural Networking in plants\" | "
                "@text_field1:Advanced @text_field2:neu* @text_field1:network"
                "@num_field_2.0:[10 100] @text_field1:hello | "
                "@tag_field_1:{books} @text_field2:Neural | "
                "@text_field1:%%%word%%% @text_field2:network",
            .create_success = false,
            .create_expected_error_message = "Invalid range: Value above maximum; Query string is too complex: max number of terms can't exceed 16",
        },
        {
            .test_name = "invalid_fuzzy1",
            .filter = "Hello, how are you% doing",
            .create_success = false,
            .create_expected_error_message = "Invalid fuzzy '%' markers",
        },
        {
            .test_name = "invalid_fuzzy2",
            .filter = "Hello, how are %you%% doing",
            .create_success = false,
            .create_expected_error_message = "Invalid fuzzy '%' markers",
        },
        {
            .test_name = "invalid_fuzzy3",
            .filter = "Hello, how are %%you% doing",
            .create_success = false,
            .create_expected_error_message = "Invalid fuzzy '%' markers",
        },
        {
            .test_name = "invalid_fuzzy4",
            .filter = "Hello, how are %%%you%%%doing%%%",
            .create_success = false,
            .create_expected_error_message = "Invalid fuzzy '%' markers",
        },
        {
            .test_name = "invalid_escape1",
            .filter = "\\\\\\\\\\(Hello, \\$how \\\\*are \\\\\\-you \\\\\\\\%doing?",
            .create_success = false,
            .create_expected_error_message = "Invalid fuzzy '%' markers",
        },
        {
            .test_name = "invalid_wildcard1",
            .filter = "Hello, how are **you* doing",
            .create_success = false,
            .create_expected_error_message = "Invalid wildcard '*' markers",
        },
        {
            .test_name = "invalid_wildcard2",
            .filter = "Hello, how are *you** doing",
            .create_success = false,
            .create_expected_error_message = "Index created without Suffix Trie",
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
            .create_success = true,
            .evaluate_success = true,
            // .create_success = false,
            // .create_expected_error_message =
            //     "Unexpected character at position 28: `n`, expecting `@`",
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
                "Invalid Query Syntax",
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
    }),
    [](const TestParamInfo<FilterTestCase> &info) {
      return info.param.test_name;
    });

}  // namespace
}  // namespace valkey_search
