/*
 * Copyright (c) 2025, ValkeySearch contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "vmsdk/src/command_parser.h"

#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "gtest/gtest.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

namespace {

constexpr absl::string_view kTestParam1{"TEST_PARAM1"};
constexpr absl::string_view kTestParam2{"TEST_PARAM2"};
constexpr absl::string_view kDefaultParam{"DEFAULT_PARAM"};
constexpr absl::string_view kIntParam{"INT_PARAM"};
constexpr absl::string_view kEnumParam{"ENUM_PARAM"};
constexpr absl::string_view kStringParam{"STRING_PARAM"};
constexpr absl::string_view kFlagParam{"FLAG_PARAM"};

enum class EnumTest { kEnum1, kEnum2, kEnum3 };

using testing::TestParamInfo;
using testing::ValuesIn;

const absl::flat_hash_map<absl::string_view, EnumTest> kEnumTestByStr = {
    {"ENUM1", EnumTest::kEnum1},
    {"ENUM2", EnumTest::kEnum2},
    {"ENUM3", EnumTest::kEnum3}};

struct TestParameters {
  absl::string_view test_param1;
  absl::string_view test_param2;
  absl::string_view default_param{"default_param"};
  int int_param;
  EnumTest enum_param;
  absl::string_view string_param;
  bool flag_param{false};
};

struct KeyValueParseTestCase {
  std::string test_name;
  bool success{false};
  absl::string_view params_str;
  absl::string_view test_param1{"param1"};
  absl::string_view test_param2{"param2"};
  absl::string_view default_param{"default_param"};
  int int_param{0};
  EnumTest enum_param{EnumTest::kEnum1};
  bool flag_param{false};
  absl::string_view string_param;
  std::string expected_err_msg;
};

class CBTestCommandParser {
 public:
  CBTestCommandParser() {
    param_parsers_.AddParamParser(
        kTestParam1, GENERATE_VALUE_PARSER(TestParameters, test_param1));
    param_parsers_.AddParamParser(
        kTestParam2, GENERATE_VALUE_PARSER(TestParameters, test_param2));
    param_parsers_.AddParamParser(
        kDefaultParam, GENERATE_VALUE_PARSER(TestParameters, default_param));
    param_parsers_.AddParamParser(
        kIntParam, GENERATE_VALUE_PARSER(TestParameters, int_param));
    param_parsers_.AddParamParser(
        kEnumParam,
        GENERATE_ENUM_PARSER(TestParameters, enum_param, kEnumTestByStr));
    param_parsers_.AddParamParser(
        kFlagParam, GENERATE_FLAG_PARSER(TestParameters, flag_param));
    param_parsers_.AddParamParser(
        kStringParam, GENERATE_VALUE_PARSER(TestParameters, string_param));
  }
  virtual ~CBTestCommandParser() = default;

  absl::StatusOr<TestParameters> Parse(RedisModuleString **argv, int argc) {
    TestParameters test_params;
    ArgsIterator itr{argv, argc};
    VMSDK_RETURN_IF_ERROR(param_parsers_.Parse(test_params, itr));

    return test_params;
  }

 private:
  KeyValueParser<TestParameters> param_parsers_;
};

class KeyValueParserTest
    : public vmsdk::RedisTestWithParam<KeyValueParseTestCase> {};

TEST_P(KeyValueParserTest, ParseParams) {
  const KeyValueParseTestCase &test_case = GetParam();
  static CBTestCommandParser parser;
  auto args = ToRedisStringVector(test_case.params_str);
  auto params = parser.Parse(&args[0], args.size());
  EXPECT_EQ(params.ok(), test_case.success);
  if (params.ok()) {
    EXPECT_EQ(params.value().test_param1, test_case.test_param1);
    EXPECT_EQ(params.value().test_param2, test_case.test_param2);
    EXPECT_EQ(params.value().int_param, test_case.int_param);
    EXPECT_EQ(params.value().enum_param, test_case.enum_param);
    EXPECT_EQ(params.value().flag_param, test_case.flag_param);
    EXPECT_EQ(params.value().string_param, test_case.string_param);
    EXPECT_EQ(params.value().default_param, test_case.default_param);
  } else {
    std::cout << "params parsing error message: " << params.status().message()
              << "\n";
    if (!test_case.expected_err_msg.empty()) {
      EXPECT_EQ(params.status().message(), test_case.expected_err_msg);
    }
  }
  for (const auto &arg : args) {
    TestRedisModule_FreeString(nullptr, arg);
  }
}

INSTANTIATE_TEST_SUITE_P(
    CommandParserTest, KeyValueParserTest,
    ValuesIn<KeyValueParseTestCase>({
        {.test_name = "happy_path",
         .success = true,
         .params_str = "TEST_PARAM1 param1 TEST_PARAM2 param2 INT_PARAM 5 "
                       "ENUM_PARAM ENUM1 STRING_PARAM test",
         .test_param1 = "param1",
         .test_param2 = "param2",
         .int_param = 5,
         .enum_param = EnumTest::kEnum1,
         .string_param = "test"},
        {.test_name = "unexpected_TEST_PARAM3",
         .success = false,
         .params_str = "TEST_PARAM3 param1 TEST_PARAM2 param2 INT_PARAM 5 "
                       "ENUM_PARAM ENUM1 STRING_PARAM test"},
        {.test_name = "happy_path_enum2",
         .success = true,
         .params_str = "TEST_PARAM1 param1 TEST_PARAM2 param2 INT_PARAM 5 "
                       "ENUM_PARAM ENUM2 STRING_PARAM test",
         .test_param1 = "param1",
         .test_param2 = "param2",
         .int_param = 5,
         .enum_param = EnumTest::kEnum2,
         .string_param = "test"},
        {.test_name = "unexpected_HELLO_WORLD",
         .success = false,
         .params_str =
             "HELLO_WORLD TEST_PARAM1 param1 TEST_PARAM2 param2 INT_PARAM 5 "
             "ENUM_PARAM ENUM2 STRING_PARAM test"},
        {.test_name = "happy_path_1",
         .success = true,
         .params_str = "TEST_PARAM1   param1.   TEST_PARAM2   "
                       "param2.   INT_PARAM 5 "
                       "ENUM_PARAM   ENUM2 STRING_PARAM   test",
         .test_param1 = "param1.",
         .test_param2 = "param2.",
         .int_param = 5,
         .enum_param = EnumTest::kEnum2,
         .string_param = "test"},
        {.test_name = "unexpected_TEST_PARAM1AZ",
         .success = false,
         .params_str = "TEST_PARAM1AZ param1 FLAG_PARAM TEST_PARAM2 param2 "
                       "INT_PARAM hello "
                       "ENUM_PARAM ENUM2 STRING_PARAM test"},
    }),
    [](const TestParamInfo<KeyValueParseTestCase> &info) {
      return info.param.test_name;
    });

enum class DummyEnum { k100, k200, k500, kNONE };
absl::flat_hash_map<absl::string_view, DummyEnum> kDummyEnumByStr = {
    {"100", DummyEnum::k100},
    {"200", DummyEnum::k200},
    {"500", DummyEnum::k500}};

struct ParseParamsTestCase {
  std::string test_name;
  bool expected_success_as_str{false};
  bool expected_success_as_int{false};
  bool expected_success_as_enum{false};
  std::string expected_err_msg_str;
  std::string expected_err_msg_int;
  std::string expected_err_msg_enum;
  absl::string_view args_str;
  bool is_mandatory{true};
  absl::string_view parse_key;
  bool expected_parse_result{true};
  absl::string_view expected_parsed_value{"-1"};
  bool expected_iterator_last{false};
};

class ParseParamsTest : public vmsdk::RedisTestWithParam<ParseParamsTestCase> {
};

template <typename T>
void DoTest(bool expected_success, const std::string &expected_err_msg,
            const T expected_parsed_value, const ParseParamsTestCase &test_case,
            auto parse_func) {
  auto args = ToRedisStringVector(test_case.args_str);
  ArgsIterator itr{args.data(), static_cast<int>(args.size())};
  T parsed_value;
  auto res = parse_func(test_case.parse_key, test_case.is_mandatory, itr,
                        parsed_value);
  EXPECT_EQ(expected_success, res.ok());
  if (res.ok()) {
    EXPECT_EQ(test_case.expected_parse_result, res.value());
    if (res.value()) {
      EXPECT_EQ(expected_parsed_value, parsed_value);
    }
  } else {
    if (!expected_err_msg.empty()) {
      EXPECT_EQ(expected_err_msg, res.status().message());
    }
  }
  auto redis_str = itr.Get();
  EXPECT_EQ(!test_case.expected_iterator_last, redis_str.ok());
  for (const auto &arg : args) {
    TestRedisModule_FreeString(nullptr, arg);
  }
}

template <typename T>
absl::StatusOr<bool> ParseFunc(absl::string_view key, bool is_mandatory,
                               ArgsIterator &itr, T &parsed_value) {
  return ParseParam(key, is_mandatory, itr, parsed_value);
}

TEST_P(ParseParamsTest, ParseParams) {
  const ParseParamsTestCase &test_case = GetParam();
  DoTest<absl::string_view>(
      test_case.expected_success_as_str, test_case.expected_err_msg_str,
      test_case.expected_parsed_value, test_case, ParseFunc<absl::string_view>);
  DoTest<std::optional<absl::string_view>>(
      test_case.expected_success_as_str, test_case.expected_err_msg_str,
      test_case.expected_parsed_value, test_case,
      ParseFunc<std::optional<absl::string_view>>);
  auto expected_parsed_int = -1;
  if (test_case.expected_success_as_int) {
    expected_parsed_int = To<int>(test_case.expected_parsed_value).value();
  }
  DoTest<int>(test_case.expected_success_as_int, test_case.expected_err_msg_int,
              expected_parsed_int, test_case, ParseFunc<int>);
  DoTest<std::optional<int>>(
      test_case.expected_success_as_int, test_case.expected_err_msg_int,
      expected_parsed_int, test_case, ParseFunc<std::optional<int>>);

  auto expected_parsed_enum = DummyEnum::kNONE;
  if (test_case.expected_success_as_enum) {
    expected_parsed_enum = kDummyEnumByStr[test_case.expected_parsed_value];
  }
  auto parse_func_enum = [](absl::string_view key, bool is_mandatory,
                            ArgsIterator &itr, DummyEnum &parsed_value) {
    return ParseParam(key, is_mandatory, itr, parsed_value, kDummyEnumByStr);
  };
  DoTest<DummyEnum>(test_case.expected_success_as_enum,
                    test_case.expected_err_msg_enum, expected_parsed_enum,
                    test_case, parse_func_enum);
}

INSTANTIATE_TEST_SUITE_P(
    CommandParserTests, ParseParamsTest,
    ValuesIn<ParseParamsTestCase>({
        {
            .test_name = "happy_path",
            .expected_success_as_str = true,
            .expected_success_as_int = true,
            .expected_success_as_enum = true,
            .args_str = "MY_KEY 100",
            .is_mandatory = true,
            .parse_key = "MY_KEY",
            .expected_parse_result = true,
            .expected_parsed_value = "100",
            .expected_iterator_last = true,
        },
        {
            .test_name = "happy_path_key_case",
            .expected_success_as_str = true,
            .expected_success_as_int = true,
            .expected_success_as_enum = true,
            .args_str = "my_key_test 200 aa",
            .is_mandatory = true,
            .parse_key = "MY_KEY_TEST",
            .expected_parse_result = true,
            .expected_parsed_value = "200",
        },
        {
            .test_name = "key_not_match_mandatory_true",
            .expected_success_as_str = false,
            .expected_success_as_int = false,
            .expected_success_as_enum = false,
            .expected_err_msg_int = "Unknown argument `MY_KEY1` at position 0",
            .args_str = "MY_KEY1 1 2 3 VAL",
            .is_mandatory = true,
            .parse_key = "MY_KEY",
        },
        {
            .test_name = "key_not_match_mandatory_false",
            .expected_success_as_str = true,
            .expected_success_as_int = true,
            .expected_success_as_enum = true,
            .args_str = "MY_KEY 1 2 3 VAL",
            .is_mandatory = false,
            .parse_key = "TEST_MY_KEY",
            .expected_parse_result = false,
        },
        {
            .test_name = "fail_int_cast",
            .expected_success_as_str = true,
            .expected_success_as_int = false,
            .expected_success_as_enum = false,
            .expected_err_msg_int = "Bad arguments for TEST_MY_KEY: `A500` is "
                                    "not a valid numeric value",
            .expected_err_msg_enum =
                "Bad arguments for TEST_MY_KEY: Unknown argument `A500`",
            .args_str = "TEST_MY_KEY A500 bb",
            .is_mandatory = true,
            .parse_key = "TEST_MY_KEY",
            .expected_parse_result = true,
            .expected_parsed_value = "A500",
        },
        {
            .test_name = "missing_value_mandatory_true",
            .expected_success_as_str = false,
            .expected_success_as_int = false,
            .expected_success_as_enum = false,
            .expected_err_msg_str =
                "Bad arguments for TEST_MY_KEY: Missing argument",
            .expected_err_msg_int =
                "Bad arguments for TEST_MY_KEY: Missing argument",
            .expected_err_msg_enum =
                "Bad arguments for TEST_MY_KEY: Missing argument",
            .args_str = "TEST_MY_KEY",
            .is_mandatory = true,
            .parse_key = "TEST_MY_KEY",
            .expected_iterator_last = true,
        },
        {
            .test_name = "missing_value_mandatory_false",
            .expected_success_as_str = false,
            .expected_success_as_int = false,
            .expected_success_as_enum = false,
            .args_str = "TEST_MY_KEY",
            .is_mandatory = false,
            .parse_key = "TEST_MY_KEY",
            .expected_iterator_last = true,
        },
    }),
    [](const TestParamInfo<ParseParamsTestCase> &info) {
      return info.param.test_name;
    });

}  // namespace

}  // namespace vmsdk
