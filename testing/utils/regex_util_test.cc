/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/regex_util.h"

#include "absl/status/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace valkey_search {
namespace utils {

namespace {

TEST(RegexUtilTest, ValidPattern) {
  auto regex = Regex::Compile("^idx_[0-9]+$");
  ASSERT_TRUE(regex.ok()) << regex.status();
  EXPECT_TRUE(regex->PartialMatch("idx_42"));
  EXPECT_FALSE(regex->PartialMatch("idx_a"));
}

TEST(RegexUtilTest, PartialMatch) {
  auto regex = Regex::Compile("schema");
  ASSERT_TRUE(regex.ok()) << regex.status();
  EXPECT_TRUE(regex->PartialMatch("my_schema_1"));
  EXPECT_FALSE(regex->PartialMatch("my_index"));
}

TEST(RegexUtilTest, InvalidPattern) {
  auto regex = Regex::Compile("(");
  EXPECT_EQ(regex.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(regex.status().message(),
              testing::HasSubstr("Invalid regular expression: "));
}

}  // namespace

}  // namespace utils
}  // namespace valkey_search
