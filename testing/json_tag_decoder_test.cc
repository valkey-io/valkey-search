/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/json_tag_decoder.h"

#include <optional>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace valkey_search::indexes {
namespace {

using Status = JsonTags::Status;

struct DecodeCase {
  std::string name;
  std::string json;
  std::optional<char> separator;
  Status status;
  std::vector<std::string> tags;
};

class JsonTagDecoderTest : public testing::TestWithParam<DecodeCase> {};

TEST_P(JsonTagDecoderTest, Decode) {
  const auto &c = GetParam();
  auto result = DecodeJsonTags(c.json, c.separator);
  EXPECT_EQ(result.status, c.status);
  EXPECT_THAT(result.tags, testing::ElementsAreArray(c.tags));
}

INSTANTIATE_TEST_SUITE_P(
    JsonTagDecoderTests, JsonTagDecoderTest,
    testing::ValuesIn<DecodeCase>({
        {"string", R"(["s"])", std::nullopt, Status::kOk, {"s"}},
        {"bool_true", "[true]", std::nullopt, Status::kOk, {"true"}},
        {"bool_false", "[false]", std::nullopt, Status::kOk, {"false"}},
        {"null", "[null]", std::nullopt, Status::kOk, {}},
        {"empty_string", R"([""])", std::nullopt, Status::kOk, {}},
        {"empty_array", "[[]]", std::nullopt, Status::kOk, {}},
        {"array", R"([["a","b"]])", std::nullopt, Status::kOk, {"a", "b"}},
        {"array_with_null",
         R"([["y",null]])",
         std::nullopt,
         Status::kOk,
         {"y"}},
        {"matches", R"(["a",null,"b"])", std::nullopt, Status::kOk, {"a", "b"}},
        {"whitespace_kept",
         R"([" a ","   "])",
         std::nullopt,
         Status::kOk,
         {" a ", "   "}},
        {"no_split_without_separator",
         R"(["a,b"])",
         std::nullopt,
         Status::kOk,
         {"a,b"}},
        {"split_and_trim", R"([" x , y ,"])", ',', Status::kOk, {"x", "y"}},
        {"split_each_element",
         R"([["a|b","c"]])",
         '|',
         Status::kOk,
         {"a", "b", "c"}},
        {"escapes",
         R"(["say \"hi\"","a\\b","\u0001","é"])",
         std::nullopt,
         Status::kOk,
         {"say \"hi\"", "a\\b", "\x01", "é"}},
        {"no_match", "[]", std::nullopt, Status::kMissing, {}},
        {"legacy_path", R"("s")", std::nullopt, Status::kMissing, {}},
        {"number", "[5]", std::nullopt, Status::kInvalid, {}},
        {"object", R"([{"k":"v"}])", std::nullopt, Status::kInvalid, {}},
        {"bool_in_array", "[[true]]", std::nullopt, Status::kInvalid, {}},
        {"number_in_array", R"([["x",7]])", std::nullopt, Status::kInvalid, {}},
        {"nested_array", R"([[["a"]]])", std::nullopt, Status::kInvalid, {}},
        {"bool_among_matches",
         R"(["s",true])",
         std::nullopt,
         Status::kInvalid,
         {}},
        {"array_among_matches",
         R"([["p"],["q"]])",
         std::nullopt,
         Status::kInvalid,
         {}},
        {"unterminated_string", R"(["a)", std::nullopt, Status::kInvalid, {}},
        {"trailing_comma", R"(["a",])", std::nullopt, Status::kInvalid, {}},
        {"unclosed", R"(["a")", std::nullopt, Status::kInvalid, {}},
    }),
    [](const testing::TestParamInfo<DecodeCase> &info) {
      return info.param.name;
    });

TEST(JsonFirstMatchTest, Decode) {
  struct Case {
    std::string json;
    Status status;
    std::optional<std::string> value;
  };
  for (const auto &c : std::vector<Case>{
           {R"(["s"])", Status::kOk, "s"},
           {R"(["say \"hi\"","b"])", Status::kOk, R"(say "hi")"},
           {R"([["a","x\"y"]])", Status::kOk, R"(["a","x\"y"])"},
           {R"([{"k":"v"}])", Status::kOk, R"({"k":"v"})"},
           {"[true]", Status::kOk, "1"},
           {"[false]", Status::kOk, "0"},
           {R"([null,"a"])", Status::kOk, std::nullopt},
           {"[]", Status::kMissing, std::nullopt},
           {R"(["a)", Status::kInvalid, std::nullopt},
       }) {
    auto result = DecodeJsonFirstMatch(c.json);
    EXPECT_EQ(result.status, c.status) << c.json;
    EXPECT_EQ(result.value, c.value) << c.json;
  }
}

}  // namespace
}  // namespace valkey_search::indexes
