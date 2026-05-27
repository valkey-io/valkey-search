/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <cstddef>
#include <string>

#include "absl/strings/string_view.h"
#include "gtest/gtest.h"
#include "src/query/search.h"

namespace valkey_search {
namespace {

using testing::TestParamInfo;
using testing::ValuesIn;

struct FindVectorDelimiterTestCase {
  std::string test_name;
  std::string query;
  bool should_find{false};
  // If should_find is true, the character after => should be '['.
};

class FindVectorDelimiterTest
    : public testing::TestWithParam<FindVectorDelimiterTestCase> {};

TEST_P(FindVectorDelimiterTest, Disambiguate) {
  const auto &test_case = GetParam();
  size_t pos = query::FindVectorDelimiter(test_case.query);

  if (!test_case.should_find) {
    EXPECT_EQ(pos, absl::string_view::npos)
        << "Should not find vector delimiter in: " << test_case.query;
    return;
  }

  ASSERT_NE(pos, absl::string_view::npos)
      << "Failed to find vector delimiter in: " << test_case.query;
  EXPECT_EQ(test_case.query.substr(pos, 2), "=>");
  // After => and optional whitespace, next char should be '['
  size_t after = pos + 2;
  while (after < test_case.query.size() &&
         std::isspace(test_case.query[after])) {
    after++;
  }
  ASSERT_LT(after, test_case.query.size());
  EXPECT_EQ(test_case.query[after], '[');
}

INSTANTIATE_TEST_SUITE_P(
    FindVectorDelimiterTests, FindVectorDelimiterTest,
    ValuesIn<FindVectorDelimiterTestCase>({
        // Property 3: => followed by [ is identified as vector delimiter
        {
            .test_name = "vector_delimiter_no_whitespace",
            .query = "(@title:dogs)=>[KNN 10 @vec $BLOB]",
            .should_find = true,
        },
        {
            .test_name = "vector_delimiter_one_space",
            .query = "(@title:dogs)=> [KNN 10 @vec $BLOB]",
            .should_find = true,
        },
        {
            .test_name = "vector_delimiter_multiple_spaces",
            .query = "(@title:dogs)=>   [KNN 10 @vec $BLOB]",
            .should_find = true,
        },
        {
            .test_name = "vector_delimiter_with_complex_prefilter",
            .query = "(@price:[10 100] @tag:{electronics})=>[KNN 5 @vec $B]",
            .should_find = true,
        },
        {
            .test_name = "vector_delimiter_wildcard_prefilter",
            .query = "*=>[KNN 10 @vec $BLOB]",
            .should_find = true,
        },
        // Property 3: => followed by { is NOT a vector delimiter
        {
            .test_name = "qma_block_not_vector_delimiter",
            .query = "(@title:{dogs}) => { $weight: 2.0; }",
            .should_find = false,
        },
        {
            .test_name = "qma_block_no_whitespace",
            .query = "(@title:{dogs})=>{ $weight: 5.0; }",
            .should_find = false,
        },
        {
            .test_name = "qma_block_multiple_spaces",
            .query = "(@title:{dogs})=>   { $weight: 1.5; }",
            .should_find = false,
        },
        {
            .test_name = "qma_only_no_vector",
            .query = "(@body:cats) => { $weight: 3.0; } (@title:{dogs})",
            .should_find = false,
        },
        // Property 3: Multiple => - only the one followed by [ is found
        {
            .test_name = "qma_then_vector",
            .query =
                "(@title:{dogs}) => { $weight: 2.0; } =>[KNN 10 @vec $BLOB]",
            .should_find = true,
        },
        {
            .test_name = "qma_then_vector_with_spaces",
            .query = "(@title:{dogs})=> { $weight: 2.0; } => [KNN 10 @vec "
                     "$BLOB]",
            .should_find = true,
        },
        // Property 4: Mixed QMA and vector - split is correct
        {
            .test_name = "multiple_qma_then_vector",
            .query = "(@title:dogs)=> { $weight: 2.0; } (@body:cats)=> { "
                     "$weight: 3.0; } =>[KNN 10 @vec $BLOB]",
            .should_find = true,
        },
        {
            .test_name = "complex_mixed_query",
            .query = "(@price:[1 100]) => { $weight: 5.0; } (@tag:{foo} | "
                     "@tag:{bar}) => { $weight: 1.5; } => [KNN 20 @vec $BLOB]",
            .should_find = true,
        },
        // Edge cases
        {
            .test_name = "no_arrow_at_all",
            .query = "(@title:{dogs}) (@body:cats)",
            .should_find = false,
        },
        {
            .test_name = "arrow_followed_by_text",
            .query = "(@title:{dogs}) => something",
            .should_find = false,
        },
        {
            .test_name = "empty_query",
            .query = "",
            .should_find = false,
        },
    }),
    [](const TestParamInfo<FindVectorDelimiterTestCase> &info) {
      return info.param.test_name;
    });

// Verify the split position puts QMA in pre-filter
TEST(FindVectorDelimiterSplitTest, QMABlockRemainsInPreFilter) {
  std::string query =
      "(@title:{dogs}) => { $weight: 5.0; } =>[KNN 10 @vec $BLOB]";
  size_t pos = query::FindVectorDelimiter(query);
  ASSERT_NE(pos, absl::string_view::npos);

  absl::string_view before_split = absl::string_view(query).substr(0, pos);
  absl::string_view after_split = absl::string_view(query).substr(pos + 2);

  // Pre-filter should contain the QMA block
  EXPECT_NE(before_split.find("$weight"), absl::string_view::npos);
  EXPECT_NE(before_split.find('{'), absl::string_view::npos);
  // Vector portion should contain [KNN
  EXPECT_NE(after_split.find("[KNN"), absl::string_view::npos);
}

TEST(FindVectorDelimiterSplitTest, MultipleQMABlocksInPreFilter) {
  std::string query =
      "(@title:dogs)=> { $weight: 2.0; } (@body:cats)=> { $weight: 3.0; } "
      "=> [KNN 10 @vec $BLOB]";
  size_t pos = query::FindVectorDelimiter(query);
  ASSERT_NE(pos, absl::string_view::npos);

  absl::string_view before_split = absl::string_view(query).substr(0, pos);

  // Count => occurrences in pre-filter - should be 2 (the QMA ones)
  size_t arrow_count = 0;
  size_t search_pos = 0;
  while ((search_pos = before_split.find("=>", search_pos)) !=
         absl::string_view::npos) {
    arrow_count++;
    search_pos += 2;
  }
  EXPECT_EQ(arrow_count, 2u);
}

}  // namespace
}  // namespace valkey_search
