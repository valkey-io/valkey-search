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
  std::optional<bool> evaluate_success;
  std::string key{"key1"};
  std::string expected_tree_structure;
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
  data_model::TextIndex text_index_proto1 =
      CreateTextIndexProto(true, false, 4);
  data_model::TextIndex text_index_proto2 =
      CreateTextIndexProto(false, true, 0);
  auto text_index_1 =
      std::make_shared<indexes::Text>(text_index_proto1, text_index_schema);
  auto text_index_2 =
      std::make_shared<indexes::Text>(text_index_proto2, text_index_schema);

  VMSDK_EXPECT_OK(
      index_schema->AddIndex("text_field1", "text_field1", text_index_1));
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("text_field2", "text_field2", text_index_2));

  // Add TEXT data for basic tests (exact_term, exact_prefix, proximity, etc.)
  auto key1 = StringInternStore::Intern("key1");
  std::string test_data = "word hello my name is hello how are you doing?";
  VMSDK_EXPECT_OK(text_index_1->AddRecord(key1, test_data));
  VMSDK_EXPECT_OK(text_index_2->AddRecord(key1, test_data));

  text_index_schema->CommitKeyData(key1);
}

TEST_P(FilterTest, ParseParams) {
  const FilterTestCase &test_case = GetParam();
  auto index_schema = CreateIndexSchema("index_schema_name").value();
  InitIndexSchema(index_schema.get());
  EXPECT_CALL(*index_schema, GetIdentifier(::testing::_))
      .Times(::testing::AnyNumber());
  TextParsingOptions options{};
  FilterParser parser(*index_schema, test_case.filter, options);
  auto parse_results = parser.Parse();
  EXPECT_EQ(test_case.create_success, parse_results.ok());
  if (!test_case.create_success) {
    EXPECT_EQ(parse_results.status().message(),
              test_case.create_expected_error_message);
    return;
  }

  // Generate the actual predicate tree structure
  std::string actual_tree =
      PrintPredicateTree(parse_results.value().root_predicate.get());
  // Compare expected vs actual tree structure
  if (!test_case.expected_tree_structure.empty()) {
    EXPECT_EQ(actual_tree, test_case.expected_tree_structure)
        << "Tree structure mismatch for filter: " << test_case.filter;
  }

  // Now evaluate all predicates, including text predicates
  if (test_case.evaluate_success.has_value()) {
    auto interned_key = StringInternStore::Intern(test_case.key);
    EXPECT_EQ(test_case.evaluate_success.value(),
              evaluator_.Evaluate(*parse_results.value().root_predicate,
                                  interned_key));
  }
}

INSTANTIATE_TEST_SUITE_P(
    FilterTests, FilterTest,
    ValuesIn<FilterTestCase>({
        {
            .test_name = "numeric_happy_path_1",
            .filter = "@num_field_1.5:[1.0 2.0]",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "NUMERIC(num_field_1.5)\n",
        },
        {
            .test_name = "numeric_happy_path_comma_separated",
            .filter = "@num_field_1.5:[1.0,2.0]",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "NUMERIC(num_field_1.5)\n",
        },
        {
            .test_name = "numeric_missing_key_1",
            .filter = "@num_field_1.5:[1.0 2.0]",
            .create_success = true,
            .evaluate_success = false,
            .key = "missing_key2",
            .expected_tree_structure = "NUMERIC(num_field_1.5)\n",
        },
        {
            .test_name = "numeric_happy_path_2",
            .filter = "@num_field_2.0:[1.5 2.5] @num_field_1.5:[1.0 2.0]",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  NUMERIC(num_field_2.0)\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_happy_path_inclusive_1",
            .filter = "@num_field_2.0:[2 2.5] @num_field_1.5:[1.0 1.5]",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  NUMERIC(num_field_2.0)\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "}\n",
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
            .expected_tree_structure = "AND{\n"
                                       "  NUMERIC(num_field_2.0)\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "}\n",
        },
        {
            .test_name = "complex_nested_expression",
            .filter = "@num_field_1.5:[1.0 2.0] @num_field_2.0:[1.5 2.5] | "
                      "(@tag_field_1:{tag1} @tag_field_1_2:{tag2} | "
                      "(@num_field_1.5:[1.0 2.0] @num_field_2.0:[1.5 2.5] | "
                      "@tag_field_1:{tag1} @tag_field_1_2:{tag2} "
                      "(@num_field_1.5:[1.0 2.0] @num_field_2.0:[1.5 2.5]) ) ) "
                      "@tag_field_1:{tag1} @tag_field_1_2:{tag2} | "
                      "@num_field_1.5:[1.0 2.0] @num_field_2.0:[1.5 2.5] | "
                      "@tag_field_1:{tag1} @tag_field_1_2:{tag2}",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "OR{\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "  AND{\n"
                                       "    OR{\n"
                                       "      AND{\n"
                                       "        TAG(tag_field_1)\n"
                                       "        TAG(tag_field_1_2)\n"
                                       "      }\n"
                                       "      OR{\n"
                                       "        AND{\n"
                                       "          NUMERIC(num_field_1.5)\n"
                                       "          NUMERIC(num_field_2.0)\n"
                                       "        }\n"
                                       "        AND{\n"
                                       "          TAG(tag_field_1)\n"
                                       "          TAG(tag_field_1_2)\n"
                                       "          AND{\n"
                                       "            NUMERIC(num_field_1.5)\n"
                                       "            NUMERIC(num_field_2.0)\n"
                                       "          }\n"
                                       "        }\n"
                                       "      }\n"
                                       "    }\n"
                                       "    TAG(tag_field_1)\n"
                                       "    TAG(tag_field_1_2)\n"
                                       "  }\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "  AND{\n"
                                       "    TAG(tag_field_1)\n"
                                       "    TAG(tag_field_1_2)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_happy_path_inclusive_2",
            .filter = "@num_field_2.0:[1 2] @num_field_1.5:[1.0 1.5]",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  NUMERIC(num_field_2.0)\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_happy_path_exclusive_1",
            .filter = "@num_field_2.0:[(2 2.5] @num_field_1.5:[1.0 1.5]",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "AND{\n"
                                       "  NUMERIC(num_field_2.0)\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_happy_path_exclusive_2",
            .filter = "@num_field_2.0:[1 (2.0] @num_field_1.5:[1.0 1.5]",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "AND{\n"
                                       "  NUMERIC(num_field_2.0)\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_happy_path_inf_1",
            .filter = "@num_field_2.0:[-inf 2.5] @num_field_1.5:[1.0 1.5]",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  NUMERIC(num_field_2.0)\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_happy_path_inf_2",
            .filter = " @num_field_1.5:[1.0 1.5]  @num_field_2.0:[1 +inf] ",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "  NUMERIC(num_field_2.0)\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_happy_path_inf_3",
            .filter = " @num_field_1.5:[1.0 1.5]  @num_field_2.0:[1 inf] ",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "  NUMERIC(num_field_2.0)\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_negate_1",
            .filter = " -@num_field_1.5:[1.0 1.4]  @num_field_2.0:[1 +inf] ",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  NOT{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "  }\n"
                                       "  NUMERIC(num_field_2.0)\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_negate_twice_with_and",
            .filter = " -@num_field_1.5:[1.0 1.4]  -@num_field_2.0:[3 +inf] ",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  NOT{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "  }\n"
                                       "  NOT{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_negate_twice_with_and_1",
            .filter = " -@num_field_1.5:[1.0 1.5]  -@num_field_2.0:[3 +inf] ",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "AND{\n"
                                       "  NOT{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "  }\n"
                                       "  NOT{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_negate_twice_with_and_2",
            .filter = " -@num_field_1.5:[1.0 1.4]  -@num_field_2.0:[2 +inf] ",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "AND{\n"
                                       "  NOT{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "  }\n"
                                       "  NOT{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_negate_twice_with_and_3",
            .filter = " -@num_field_1.5:[1.0 1.5]  -@num_field_2.0:[2 +inf] ",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "AND{\n"
                                       "  NOT{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "  }\n"
                                       "  NOT{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_negate_twice_with_or_1",
            .filter = " -@num_field_1.5:[1.0 1.4] | -@num_field_2.0:[2 +inf] ",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "OR{\n"
                                       "  NOT{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "  }\n"
                                       "  NOT{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_negate_twice_with_or_2",
            .filter = " -@num_field_1.5:[1.0 1.6] | -@num_field_2.0:[3 +inf] ",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "OR{\n"
                                       "  NOT{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "  }\n"
                                       "  NOT{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_negate_twice_with_or_3",
            .filter = " -@num_field_1.5:[1.0 1.5] | -@num_field_2.0:[2 +inf] ",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "OR{\n"
                                       "  NOT{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "  }\n"
                                       "  NOT{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_negate_2",
            .filter = " @num_field_1.5:[1.0 1.5]  -@num_field_2.0:[5 +inf] ",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "  NOT{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_negate_3",
            .filter = " @num_field_1.5:[1.0 1.4]  @num_field_2.0:[3 +inf] ",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "AND{\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "  NUMERIC(num_field_2.0)\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_negate_4",
            .filter = " -(@num_field_1.5:[1.0 1.4]  @num_field_2.0:[3 +inf]) ",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "NOT{\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_negate_5",
            .filter =
                " - ( - (@num_field_1.5:[1.0 1.4]  @num_field_2.0:[3 +inf]) )",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "NOT{\n"
                                       "  NOT{\n"
                                       "    AND{\n"
                                       "      NUMERIC(num_field_1.5)\n"
                                       "      NUMERIC(num_field_2.0)\n"
                                       "    }\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_negate_6",
            .filter = " -(@num_field_1.5:[1.0 1.4] | @num_field_2.0:[3 +inf]) ",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "NOT{\n"
                                       "  OR{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_negate_7",
            .filter = " -(@num_field_1.5:[1.0,2] | @num_field_2.0:[3 +inf]) ",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "NOT{\n"
                                       "  OR{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_happy_path_or_1",
            .filter = " (@num_field_1.5:[1.0 1.5])",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "NUMERIC(num_field_1.5)\n",
        },
        {
            .test_name = "numeric_happy_path_or_2",
            .filter = " ( (@num_field_1.5:[1.0 1.5])  )",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "NUMERIC(num_field_1.5)\n",
        },
        {
            .test_name = "numeric_happy_path_or_3",
            .filter = "(@num_field_1.5:[5.0 6.5]) | (@num_field_1.5:[1.0 1.5])",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "OR{\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "}\n",
        },
        {
            .test_name = "numeric_happy_path_or_4",
            .filter = "( (   (@num_field_1.5:[5.0 6.5]) | (@num_field_1.5:[1.0 "
                      "1.5]) ) ) ",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "OR{\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "}\n",
        },
        {
            .test_name = "tag_happy_path_1",
            .filter = "@tag_field_1:{tag1}",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "TAG(tag_field_1)\n",
        },
        {
            .test_name = "tag_case_sensitive_1",
            .filter = "@tag_field_1:{Tag1}",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "TAG(tag_field_1)\n",
        },
        {
            .test_name = "tag_case_sensitive_2",
            .filter = "@tag_field_case_insensitive:{Tag1}",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "TAG(tag_field_case_insensitive)\n",
        },
        {
            .test_name = "tag_case_sensitive_3",
            .filter = "@tag_field_case_insensitive:{Tag0@Tag1}",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "TAG(tag_field_case_insensitive)\n",
        },
        {
            .test_name = "tag_case_sensitive_4",
            .filter = "@tag_field_case_insensitive:{Tag0@Tag5}",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "TAG(tag_field_case_insensitive)\n",
        },
        {
            .test_name = "tag_missing_key_1",
            .filter = "@tag_field_1:{tag1}",
            .create_success = true,
            .evaluate_success = false,
            .key = "missing_key2",
            .expected_tree_structure = "TAG(tag_field_1)\n",
        },
        {
            .test_name = "tag_happy_path_2",
            .filter = "@tag_field_1:{tag1 , tag2}",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "TAG(tag_field_1)\n",
        },
        {
            .test_name = "tag_happy_path_4",
            .filter = "@tag_field_with_space:{tag 1 , tag4}",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "TAG(tag_field_with_space)\n",
        },
        {
            .test_name = "tag_not_found_1",
            .filter = "@tag_field_1:{tag3 , tag4}",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "TAG(tag_field_1)\n",
        },
        {
            .test_name = "tag_not_found_2",
            .filter = "-@tag_field_with_space:{tag1 , tag 2}",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "NOT{\n"
                                       "  TAG(tag_field_with_space)\n"
                                       "}\n",
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
            .expected_tree_structure = "OR{\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "  NUMERIC(num_field_2.0)\n"
                                       "}\n",
        },
        {
            .test_name = "left_associative_2",
            .filter = "@num_field_2.0:[23 25] @num_field_2.0:[23 25] | "
                      "@num_field_2.0:[23 25]",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "OR{\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "  NUMERIC(num_field_2.0)\n"
                                       "}\n",
        },
        {
            .test_name = "left_associative_3",
            .filter = "@num_field_2.0:[0 2.5] @num_field_2.0:[23 25] | "
                      "@num_field_2.0:[-inf 2.5]",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "OR{\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "  NUMERIC(num_field_2.0)\n"
                                       "}\n",
        },
        {
            .test_name = "left_associative_4",
            .filter = "@num_field_2.0 : [23 25] @num_field_2.0:[0 2.5] | "
                      "@num_field_2.0:[23 25]",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "OR{\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "  NUMERIC(num_field_2.0)\n"
                                       "}\n",
        },
        {
            .test_name = "or_precedence_1",
            .filter = "@num_field_2.0 : [23 25] @num_field_2.0:[23 25] | "
                      "@num_field_2.0:[0 2.5]",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "OR{\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "  NUMERIC(num_field_2.0)\n"
                                       "}\n",
        },
        {
            .test_name = "or_precedence_2",
            .filter = "@num_field_2.0 : [23 25] @num_field_2.0:[23 25] | "
                      "@num_field_2.0:[0 2.5] @num_field_2.0:[0 2.5]",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "OR{\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "or_precedence_3",
            .filter = "@num_field_2.0 : [23 25] @num_field_2.0:[23 25] | "
                      "@num_field_2.0:[23 25] @num_field_2.0:[0 2.5]",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "OR{\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "or_precedence_4",
            .filter = "@num_field_2.0 : [23 25] @num_field_2.0:[23 25] | "
                      "@num_field_2.0:[0 2.5] @num_field_2.0:[23 25]",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "OR{\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "or_precedence_5",
            .filter = "@num_field_2.0 : [0 2.5] @num_field_2.0:[23 25] | "
                      "@num_field_2.0:[0 2.5] @num_field_2.0:[23 25]",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "OR{\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "or_precedence_6",
            .filter = "@num_field_2.0 : [23 25] @num_field_2.0:[0 2.5] | "
                      "@num_field_2.0:[0 2.5] @num_field_2.0:[23 25]",
            .create_success = true,
            .evaluate_success = false,
            .expected_tree_structure = "OR{\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "or_precedence_7",
            .filter = "@num_field_2.0 : [0 2.5] @num_field_2.0:[0 2.5] | "
                      "@num_field_2.0:[0 2.5] @num_field_2.0:[23 25]",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "OR{\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "exact_term",
            .filter = "@text_field1:word",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "TEXT-TERM(\"word\", field_mask=1)\n",
        },
        {
            .test_name = "exact_prefix",
            .filter = "@text_field1:word*",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "TEXT-PREFIX(\"word\", field_mask=1)\n",
        },
        {
            .test_name = "exact_suffix_supported",
            .filter = "@text_field1:*word",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "TEXT-SUFFIX(\"word\", field_mask=1)\n",
        },
        {
            .test_name = "exact_suffix_unsupported",
            .filter = "@text_field2:*word",
            .create_success = false,
            .create_expected_error_message =
                "Field does not support suffix search",
        },
        {
            .test_name = "exact_inffix",
            .filter = "@text_field1:*word*",
            .create_success = false,
            .create_expected_error_message = "Unsupported query operation",
        },
        {
            .test_name = "exact_fuzzy1",
            .filter = "@text_field1:%word%",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure =
                "TEXT-FUZZY(\"word\", distance=1, field_mask=1)\n",
        },
        {
            .test_name = "exact_fuzzy2",
            .filter = "@text_field1:%%word%%",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure =
                "TEXT-FUZZY(\"word\", distance=2, field_mask=1)\n",
        },
        {
            .test_name = "exact_fuzzy3",
            .filter = "@text_field1:%%%word%%%",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure =
                "TEXT-FUZZY(\"word\", distance=3, field_mask=1)\n",
        },
        {
            .test_name = "proximity1",
            .filter = "@text_field1:\"hello my name is\"",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "AND(slop=0, inorder=true){\n"
                                       "  TEXT-TERM(\"hello\", field_mask=1)\n"
                                       "  TEXT-TERM(\"my\", field_mask=1)\n"
                                       "  TEXT-TERM(\"name\", field_mask=1)\n"
                                       "  TEXT-TERM(\"is\", field_mask=1)\n"
                                       "}\n",
        },
        {
            .test_name = "proximity2",
            .filter = "@text_field1:hello @text_field2:my @text_field1:name "
                      "@text_field2:is",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  TEXT-TERM(\"hello\", field_mask=1)\n"
                                       "  TEXT-TERM(\"my\", field_mask=2)\n"
                                       "  TEXT-TERM(\"name\", field_mask=1)\n"
                                       "  TEXT-TERM(\"is\", field_mask=2)\n"
                                       "}\n",
        },
        {
            .test_name = "default_field_text",
            .filter = "Hello, how are you doing?",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  TEXT-TERM(\"hello\", field_mask=3)\n"
                                       "  TEXT-TERM(\"how\", field_mask=3)\n"
                                       "  TEXT-TERM(\"are\", field_mask=3)\n"
                                       "  TEXT-TERM(\"you\", field_mask=3)\n"
                                       "  TEXT-TERM(\"doing?\", field_mask=3)\n"
                                       "}\n",
        },
        {
            .test_name = "default_field_exact_phrase",
            .filter = "\"Hello, how are you doing?\"",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "AND(slop=0, inorder=true){\n"
                                       "  TEXT-TERM(\"hello\", field_mask=3)\n"
                                       "  TEXT-TERM(\"how\", field_mask=3)\n"
                                       "  TEXT-TERM(\"are\", field_mask=3)\n"
                                       "  TEXT-TERM(\"you\", field_mask=3)\n"
                                       "  TEXT-TERM(\"doing?\", field_mask=3)\n"
                                       "}\n",
        },
        {
            .test_name = "default_field_exact_phrase_with_punct",
            .filter = "\"Hello, h(ow a)re yo#u doi_n$g?\"",
            .create_success = true,
            .expected_tree_structure = "AND(slop=0, inorder=true){\n"
                                       "  TEXT-TERM(\"hello\", field_mask=3)\n"
                                       "  TEXT-TERM(\"h\", field_mask=3)\n"
                                       "  TEXT-TERM(\"ow\", field_mask=3)\n"
                                       "  TEXT-TERM(\"a\", field_mask=3)\n"
                                       "  TEXT-TERM(\"re\", field_mask=3)\n"
                                       "  TEXT-TERM(\"yo\", field_mask=3)\n"
                                       "  TEXT-TERM(\"u\", field_mask=3)\n"
                                       "  TEXT-TERM(\"doi_n\", field_mask=3)\n"
                                       "  TEXT-TERM(\"g?\", field_mask=3)\n"
                                       "}\n",
        },
        {
            .test_name = "default_field_with_escape1",
            .filter =
                "\"\\\\\\\\\\Hello, \\how \\\\are \\\\\\you \\\\\\\\doing?\"",
            .create_success = true,
            .expected_tree_structure =
                "AND(slop=0, inorder=true){\n"
                "  TEXT-TERM(\"\\\", field_mask=3)\n"
                "  TEXT-TERM(\"\\\", field_mask=3)\n"
                "  TEXT-TERM(\"hello\", field_mask=3)\n"
                "  TEXT-TERM(\"how\", field_mask=3)\n"
                "  TEXT-TERM(\"\\are\", field_mask=3)\n"
                "  TEXT-TERM(\"\\\", field_mask=3)\n"
                "  TEXT-TERM(\"you\", field_mask=3)\n"
                "  TEXT-TERM(\"\\\", field_mask=3)\n"
                "  TEXT-TERM(\"\\doing?\", field_mask=3)\n"
                "}\n",
        },
        {
            .test_name = "default_field_with_escape2",
            .filter = "\\\\\\\\\\Hello, \\how \\\\are \\\\\\you \\\\\\\\doing?",
            .create_success = true,
            .expected_tree_structure =
                "AND{\n"
                "  TEXT-TERM(\"\\\", field_mask=3)\n"
                "  TEXT-TERM(\"\\\", field_mask=3)\n"
                "  TEXT-TERM(\"hello\", field_mask=3)\n"
                "  TEXT-TERM(\"how\", field_mask=3)\n"
                "  TEXT-TERM(\"\\are\", field_mask=3)\n"
                "  TEXT-TERM(\"\\\", field_mask=3)\n"
                "  TEXT-TERM(\"you\", field_mask=3)\n"
                "  TEXT-TERM(\"\\\", field_mask=3)\n"
                "  TEXT-TERM(\"\\doing?\", field_mask=3)\n"
                "}\n",
        },
        {
            .test_name = "default_field_with_escape3",
            .filter = "Hel\\(lo, ho\\$w a\\*re yo\\{u do\\|ing?",
            .create_success = true,
            .expected_tree_structure =
                "AND{\n"
                "  TEXT-TERM(\"hel(lo\", field_mask=3)\n"
                "  TEXT-TERM(\"ho$w\", field_mask=3)\n"
                "  TEXT-TERM(\"a*r\", field_mask=3)\n"
                "  TEXT-TERM(\"yo{u\", field_mask=3)\n"
                "  TEXT-TERM(\"do|ing?\", field_mask=3)\n"
                "}\n",
        },
        {
            .test_name = "default_field_with_escape4",
            .filter = "\\\\\\\\\\(Hello, \\$how \\\\\\*are \\\\\\-you "
                      "\\\\\\\\\\%doing?",
            .create_success = true,
            .expected_tree_structure =
                "AND{\n"
                "  TEXT-TERM(\"\\\", field_mask=3)\n"
                "  TEXT-TERM(\"\\\", field_mask=3)\n"
                "  TEXT-TERM(\"(hello\", field_mask=3)\n"
                "  TEXT-TERM(\"$how\", field_mask=3)\n"
                "  TEXT-TERM(\"\\\", field_mask=3)\n"
                "  TEXT-TERM(\"*are\", field_mask=3)\n"
                "  TEXT-TERM(\"\\\", field_mask=3)\n"
                "  TEXT-TERM(\"-you\", field_mask=3)\n"
                "  TEXT-TERM(\"\\\", field_mask=3)\n"
                "  TEXT-TERM(\"\\\", field_mask=3)\n"
                "  TEXT-TERM(\"%doing?\", field_mask=3)\n"
                "}\n",
        },
        {
            .test_name = "default_field_with_escape5",
            .filter = "Hello, how are you\\% doing",
            .create_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  TEXT-TERM(\"hello\", field_mask=3)\n"
                                       "  TEXT-TERM(\"how\", field_mask=3)\n"
                                       "  TEXT-TERM(\"are\", field_mask=3)\n"
                                       "  TEXT-TERM(\"you%\", field_mask=3)\n"
                                       "  TEXT-TERM(\"do\", field_mask=3)\n"
                                       "}\n",
        },
        {
            .test_name = "default_field_with_escape6",
            .filter = "Hello, how are you\\\\\\\\\\% doing",
            .create_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  TEXT-TERM(\"hello\", field_mask=3)\n"
                                       "  TEXT-TERM(\"how\", field_mask=3)\n"
                                       "  TEXT-TERM(\"are\", field_mask=3)\n"
                                       "  TEXT-TERM(\"you\\\", field_mask=3)\n"
                                       "  TEXT-TERM(\"\\\", field_mask=3)\n"
                                       "  TEXT-TERM(\"%\", field_mask=3)\n"
                                       "  TEXT-TERM(\"do\", field_mask=3)\n"
                                       "}\n",
        },
        {
            .test_name = "default_field_with_escape_query_syntax",
            .filter =
                "Hello, how are you\\]\\[\\$\\}\\{\\;\\:\\)\\(\\| \\-doing",
            .create_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  TEXT-TERM(\"hello\", field_mask=3)\n"
                                       "  TEXT-TERM(\"how\", field_mask=3)\n"
                                       "  TEXT-TERM(\"are\", field_mask=3)\n"
                                       "  TEXT-TERM(\"you]\", field_mask=3)\n"
                                       "  TEXT-TERM(\"[\", field_mask=3)\n"
                                       "  TEXT-TERM(\"$\", field_mask=3)\n"
                                       "  TEXT-TERM(\"}\", field_mask=3)\n"
                                       "  TEXT-TERM(\"{\", field_mask=3)\n"
                                       "  TEXT-TERM(\";\", field_mask=3)\n"
                                       "  TEXT-TERM(\":\", field_mask=3)\n"
                                       "  TEXT-TERM(\")\", field_mask=3)\n"
                                       "  TEXT-TERM(\"(\", field_mask=3)\n"
                                       "  TEXT-TERM(\"|\", field_mask=3)\n"
                                       "  TEXT-TERM(\"-do\", field_mask=3)\n"
                                       "}\n",
        },
        {
            .test_name = "default_field_with_all_operations",
            .filter = "%Hllo%, how are *ou do* *oda*",
            .create_success = false,
            .create_expected_error_message = "Unsupported query operation",
        },
        {
            .test_name = "mixed_fulltext",
            .filter =
                "@text_field1:\"Advanced Neural Networking in plants\" | "
                "@text_field1:Advanced @text_field2:neu* @text_field1:network"
                "@num_field_2.0:[10 100] @text_field1:hello | "
                "@tag_field_1:{books} @text_field2:Neural | "
                "@text_field1:%%%word%%% @text_field2:network",
            .create_success = true,
            .expected_tree_structure =
                "OR{\n"
                "  AND(slop=0, inorder=true){\n"
                "    TEXT-TERM(\"advanced\", field_mask=1)\n"
                "    TEXT-TERM(\"neural\", field_mask=1)\n"
                "    TEXT-TERM(\"networking\", field_mask=1)\n"
                "    TEXT-TERM(\"in\", field_mask=1)\n"
                "    TEXT-TERM(\"plants\", field_mask=1)\n"
                "  }\n"
                "  AND{\n"
                "    TEXT-TERM(\"advanc\", field_mask=1)\n"
                "    TEXT-PREFIX(\"neu\", field_mask=2)\n"
                "    TEXT-TERM(\"network\", field_mask=1)\n"
                "    NUMERIC(num_field_2.0)\n"
                "    TEXT-TERM(\"hello\", field_mask=1)\n"
                "  }\n"
                "  AND{\n"
                "    TAG(tag_field_1)\n"
                "    TEXT-TERM(\"neural\", field_mask=2)\n"
                "  }\n"
                "  AND{\n"
                "    TEXT-FUZZY(\"word\", distance=3, field_mask=1)\n"
                "    TEXT-TERM(\"network\", field_mask=2)\n"
                "  }\n"
                "}\n",
        },
        {
            .test_name = "fuzzy_ignored_in_exact_phrase",
            .filter = "@text_field1:\" Advanced Neural %%%word%%%\"",
            .create_success = true,
            .expected_tree_structure =
                "AND(slop=0, inorder=true){\n"
                "  TEXT-TERM(\"advanced\", field_mask=1)\n"
                "  TEXT-TERM(\"neural\", field_mask=1)\n"
                "  TEXT-TERM(\"word\", field_mask=1)\n"
                "}\n",
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
            .test_name = "invalid_fuzzy5",
            .filter = "Hello, how are %%%  %%%",
            .create_success = false,
            .create_expected_error_message = "Invalid fuzzy '%' markers",
        },
        {
            .test_name = "invalid_fuzzy6",
            .filter = "Hello, how are %%%*%%%",
            .create_success = false,
            .create_expected_error_message = "Invalid fuzzy '%' markers",
        },
        {
            .test_name = "invalid_escape1",
            .filter =
                "\\\\\\\\\\(Hello, \\$how \\\\*are \\\\\\-you \\\\\\\\%doing?",
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
            .create_expected_error_message = "Unsupported query operation",
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
                "Unexpected character at position 41: `:`",
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
                "Unexpected character at position 26: `$`",
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
        {
            .test_name = "bad_filter_13",
            .filter = "hello{world",
            .create_success = false,
            .create_expected_error_message =
                "Unexpected character at position 6: `{`",
        },
        {
            .test_name = "bad_filter_14",
            .filter = "hello}world",
            .create_success = false,
            .create_expected_error_message =
                "Unexpected character at position 6: `}`",
        },
        {
            .test_name = "bad_filter_15",
            .filter = "hello$world",
            .create_success = false,
            .create_expected_error_message =
                "Unexpected character at position 6: `$`",
        },
        {
            .test_name = "bad_filter_16",
            .filter = "hello[world",
            .create_success = false,
            .create_expected_error_message =
                "Unexpected character at position 6: `[`",
        },
        {
            .test_name = "bad_filter_17",
            .filter = "hello]world",
            .create_success = false,
            .create_expected_error_message =
                "Unexpected character at position 6: `]`",
        },
        {
            .test_name = "bad_filter_18",
            .filter = "hello:world",
            .create_success = false,
            .create_expected_error_message =
                "Unexpected character at position 6: `:`",
        },
        {
            .test_name = "bad_filter_19",
            .filter = "hello;world",
            .create_success = false,
            .create_expected_error_message =
                "Unexpected character at position 6: `;`",
        },
        // Nested brackets test cases for AND operations
        {
            .test_name = "nested_brackets_and_1",
            .filter = "(@num_field_1.5:[1.0 2.0] @num_field_2.0:[1.0 3.0]) "
                      "@tag_field_1:{tag1}",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "  TAG(tag_field_1)\n"
                                       "}\n",
        },
        {
            .test_name = "nested_brackets_and_2",
            .filter = "(@num_field_1.5:[1.0 2.0] (@num_field_2.0:[1.0 3.0] "
                      "(@tag_field_1:{tag1} (@tag_field_1_2:{tag1,tag2} "
                      "(@num_field_1.5:[1.0 2.0] @num_field_2.0:[1.0 3.0]) "
                      "@tag_field_1:{tag1}))))",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    AND{\n"
                                       "      TAG(tag_field_1)\n"
                                       "      AND{\n"
                                       "        TAG(tag_field_1_2)\n"
                                       "        AND{\n"
                                       "          NUMERIC(num_field_1.5)\n"
                                       "          NUMERIC(num_field_2.0)\n"
                                       "        }\n"
                                       "        TAG(tag_field_1)\n"
                                       "      }\n"
                                       "    }\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "nested_brackets_and_3",
            .filter = "@num_field_1.5:[1.0 2.0] (@num_field_2.0:[1.0 3.0] "
                      "(@tag_field_1:{tag1} (@tag_field_1_2:{tag1,tag2} "
                      "(@num_field_1.5:[1.0 2.0] @num_field_2.0:[1.0 3.0]))))",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    AND{\n"
                                       "      TAG(tag_field_1)\n"
                                       "      AND{\n"
                                       "        TAG(tag_field_1_2)\n"
                                       "        AND{\n"
                                       "          NUMERIC(num_field_1.5)\n"
                                       "          NUMERIC(num_field_2.0)\n"
                                       "        }\n"
                                       "      }\n"
                                       "    }\n"
                                       "  }\n"
                                       "}\n",
        },
        // Nested brackets test cases for OR operations
        {
            .test_name = "nested_brackets_or_1",
            .filter = "(@num_field_1.5:[5.0 6.0] | (@num_field_2.0:[5.0 6.0] | "
                      "(@tag_field_1:{tag2} | (@tag_field_1_2:{tag3} | "
                      "(@num_field_1.5:[1.0 2.0] | @num_field_2.0:[1.0 3.0]) | "
                      "@tag_field_1:{tag1}))))",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "OR{\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "  OR{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    OR{\n"
                                       "      TAG(tag_field_1)\n"
                                       "      OR{\n"
                                       "        TAG(tag_field_1_2)\n"
                                       "        OR{\n"
                                       "          NUMERIC(num_field_1.5)\n"
                                       "          NUMERIC(num_field_2.0)\n"
                                       "        }\n"
                                       "        TAG(tag_field_1)\n"
                                       "      }\n"
                                       "    }\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "nested_brackets_or_2",
            .filter = "(@num_field_1.5:[5.0 6.0] | @num_field_2.0:[5.0 6.0]) | "
                      "(@tag_field_1:{tag2} | @tag_field_1_2:{tag3}) | "
                      "(@num_field_1.5:[1.0 2.0] | @num_field_2.0:[1.0 3.0])",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "OR{\n"
                                       "  OR{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "  OR{\n"
                                       "    TAG(tag_field_1)\n"
                                       "    TAG(tag_field_1_2)\n"
                                       "  }\n"
                                       "  OR{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "nested_brackets_or_3",
            .filter = "(@num_field_1.5:[5.0 6.0] | @num_field_2.0:[5.0 6.0]) | "
                      "(@tag_field_1:{tag2} | @tag_field_1_2:{tag3}) | "
                      "(@num_field_1.5:[1.0 2.0] | @num_field_2.0:[1.0 3.0]) |"
                      "(@tag_field_1:{tag2} | @tag_field_1_2:{tag3}) | "
                      "(@num_field_1.5:[1.0 2.0] | @num_field_2.0:[1.0 3.0])",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "OR{\n"
                                       "  OR{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "  OR{\n"
                                       "    TAG(tag_field_1)\n"
                                       "    TAG(tag_field_1_2)\n"
                                       "  }\n"
                                       "  OR{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "  OR{\n"
                                       "    TAG(tag_field_1)\n"
                                       "    TAG(tag_field_1_2)\n"
                                       "  }\n"
                                       "  OR{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "  }\n"
                                       "}\n",
        },
        // Mixed AND/OR with brackets
        {
            .test_name = "mixed_and_or_1",
            .filter = "@num_field_1.5:[1.0 2.0] @num_field_2.0:[1.0 3.0] "
                      "(@tag_field_1:{tag1} @tag_field_1_2:{tag1,tag2}) "
                      "@num_field_1.5:[1.0 2.0] | (@num_field_2.0:[1.0 3.0] | "
                      "@tag_field_1:{tag1})",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "OR{\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    AND{\n"
                                       "      TAG(tag_field_1)\n"
                                       "      TAG(tag_field_1_2)\n"
                                       "    }\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "  }\n"
                                       "  OR{\n"
                                       "    NUMERIC(num_field_2.0)\n"
                                       "    TAG(tag_field_1)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "mixed_and_or_2",
            .filter =
                "( @num_field_1.5:[5.0 6.0] (@num_field_2.0:[5.0 6.0] "
                "(@tag_field_1:{tag2} (@tag_field_1_2:{tag3} "
                "@num_field_1.5:[5.0 6.0]))) | ( @num_field_1.5:[1.0 2.0] "
                "(@num_field_2.0:[1.0 3.0] (@tag_field_1:{tag1} "
                "(@tag_field_1_2:{tag1,tag2} | @num_field_1.5:[1.0 2.0])))))",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "OR{\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "    AND{\n"
                                       "      NUMERIC(num_field_2.0)\n"
                                       "      AND{\n"
                                       "        TAG(tag_field_1)\n"
                                       "        AND{\n"
                                       "          TAG(tag_field_1_2)\n"
                                       "          NUMERIC(num_field_1.5)\n"
                                       "        }\n"
                                       "      }\n"
                                       "    }\n"
                                       "  }\n"
                                       "  AND{\n"
                                       "    NUMERIC(num_field_1.5)\n"
                                       "    AND{\n"
                                       "      NUMERIC(num_field_2.0)\n"
                                       "      AND{\n"
                                       "        TAG(tag_field_1)\n"
                                       "        OR{\n"
                                       "          TAG(tag_field_1_2)\n"
                                       "          NUMERIC(num_field_1.5)\n"
                                       "        }\n"
                                       "      }\n"
                                       "    }\n"
                                       "  }\n"
                                       "}\n",
        },
        // Edge case: Complex nested OR with multiple levels
        {
            .test_name = "complex_nested_or",
            .filter = "@num_field_1.5:[5.0 6.0] | @num_field_2.0:[5.0 6.0] | "
                      "@tag_field_1:{tag2} | @tag_field_1_2:{tag3} | "
                      "@num_field_1.5:[1.0 2.0]",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "OR{\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "  NUMERIC(num_field_2.0)\n"
                                       "  TAG(tag_field_1)\n"
                                       "  TAG(tag_field_1_2)\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "}\n",
        },
        // Edge case: Deeply nested AND with single element brackets
        {
            .test_name = "nested_single_brackets_1",
            .filter = "(@num_field_1.5:[1.0 2.0]) (@num_field_2.0:[1.0 3.0]) "
                      "(@tag_field_1:{tag1})",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  NUMERIC(num_field_1.5)\n"
                                       "  NUMERIC(num_field_2.0)\n"
                                       "  TAG(tag_field_1)\n"
                                       "}\n",
        },
        // Edge case: Mixed brackets with negation
        {
            .test_name = "mixed_brackets_with_negation",
            .filter = "-(@num_field_1.5:[5.0 6.0] @num_field_2.0:[5.0 6.0]) | "
                      "(@tag_field_1:{tag1} @tag_field_1_2:{tag1,tag2})",
            .create_success = true,
            .evaluate_success = true,
            .expected_tree_structure = "OR{\n"
                                       "  NOT{\n"
                                       "    AND{\n"
                                       "      NUMERIC(num_field_1.5)\n"
                                       "      NUMERIC(num_field_2.0)\n"
                                       "    }\n"
                                       "  }\n"
                                       "  AND{\n"
                                       "    TAG(tag_field_1)\n"
                                       "    TAG(tag_field_1_2)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "empty_brackets_with_content",
            .filter = "@num_field_1.5:[1.0 2.0] (@num_field_2.0:[1.0 3.0] () "
                      "@tag_field_1:{tag1})",
            .create_success = false,
            .create_expected_error_message =
                "Empty brackets detected at Position: 52",
        },
        {
            .test_name = "empty_brackets_with_or",
            .filter =
                "@num_field_1.5:[1.0 2.0] ( @num_field_2.0:[1.0 3.0] | ())",
            .create_success = false,
            .create_expected_error_message =
                "Empty brackets detected at Position: 55",
        },
        {
            .test_name = "empty_brackets_only",
            .filter = "()",
            .create_success = false,
            .create_expected_error_message =
                "Unexpected character at position 2: `)`",
        },
        {
            .test_name = "or_with_missing_left_operand",
            .filter = "@num_field_1.5:[1.0 2.0] ( | @tag_field_1:{tag1})",
            .create_success = false,
            .create_expected_error_message = "Missing OR term",
        },
        {
            .test_name = "or_with_missing_both_operands",
            .filter = "@num_field_1.5:[1.0 2.0] ( | )",
            .create_success = false,
            .create_expected_error_message = "Missing OR term",
        },
    }),
    [](const TestParamInfo<FilterTestCase> &info) {
      return info.param.test_name;
    });

}  // namespace
}  // namespace valkey_search
