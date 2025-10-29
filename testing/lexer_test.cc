/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/lexer.h"

#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "gtest/gtest.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/text_index.h"
#include "testing/common.h"

namespace valkey_search::indexes::text {

// Test case structure for parameterized tests
struct LexerTestCase {
  std::string input;
  std::vector<std::string> expected;
  bool stemming_enabled = true;
  uint32_t min_stem_size = 3;
  std::string custom_punctuation = "";  // empty = use default punctuation
  std::string description;
};

class LexerTest : public ::testing::Test {
 protected:
  std::unique_ptr<Lexer> CreateLexer(
      const std::string& punctuation,
      const std::vector<std::string> stop_words) {
    return std::make_unique<Lexer>(
        data_model::LANGUAGE_ENGLISH, punctuation,
        stop_words);  // We only support english for now
  }

  const std::string default_punctuation_ =
      " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";
  const std::vector<std::string> default_stop_words_ = {"the", "and", "or"};
  bool default_stemming_enabled_ = true;
  uint32_t default_min_stem_size_ = 3;

  std::unique_ptr<Lexer> lexer_ =
      CreateLexer(default_punctuation_, default_stop_words_);
};

// Parameterized test for comprehensive tokenization testing
class LexerParameterizedTest
    : public LexerTest,
      public ::testing::WithParamInterface<LexerTestCase> {};

TEST_P(LexerParameterizedTest, TokenizeTest) {
  const auto& test_case = GetParam();

  if (!test_case.custom_punctuation.empty()) {
    lexer_ = CreateLexer(test_case.custom_punctuation, default_stop_words_);
  }

  auto result = lexer_->Tokenize(test_case.input, test_case.stemming_enabled,
                                 test_case.min_stem_size);

  ASSERT_TRUE(result.ok()) << "Test case: " << test_case.description;
  EXPECT_EQ(*result, test_case.expected)
      << "Test case: " << test_case.description;
}

INSTANTIATE_TEST_SUITE_P(
    AllTokenizationTests, LexerParameterizedTest,
    ::testing::Values(
        // Core tokenization functionality
        LexerTestCase{"", {}, true, 3, "", "Empty string returns no words"},
        LexerTestCase{"   \t\n!@#$%^&*()   ",
                      {},
                      true,
                      3,
                      "",
                      "Only punctuation returns no words"},
        LexerTestCase{"hello,world!this-is_a.test",
                      {"hello", "world", "this", "is", "a", "test"},
                      true,
                      3,
                      "",
                      "Default punctuation handling"},
        LexerTestCase{"hello,world!this-is_a.test",
                      {"hello", "world!this-is_a.test"},
                      true,
                      3,
                      " ,",
                      "Custom punctuation handling"},
        LexerTestCase{"HELLO World miXeD",
                      {"hello", "world", "mixed"},
                      false,
                      3,
                      "",
                      "Case conversion"},
        LexerTestCase{"hello 世界 test café",
                      {"hello", "世界", "test", "café"},
                      true,
                      3,
                      "",
                      "UTF-8 support"},
        LexerTestCase{
            "a b c", {"a", "b", "c"}, true, 3, "", "Single character words"},
        LexerTestCase{"hello\tworld\ntest",
                      {"hello", "world", "test"},
                      true,
                      3,
                      "",
                      "Tabs and newlines"},
        LexerTestCase{"running jumping",
                      {"run", "jump"},
                      true,
                      3,
                      "",
                      "Stemming enabled"},
        LexerTestCase{"running jumping",
                      {"running", "jumping"},
                      false,
                      3,
                      "",
                      "Stemming disabled"},
        LexerTestCase{"run running",
                      {"run", "running"},
                      true,
                      10,
                      "",
                      "Min stem size prevents stemming"},
        LexerTestCase{"hello🙂world",
                      {"hello🙂world"},
                      true,
                      3,
                      "",
                      "Non-ASCII punctuation handling"},

        // Stop word filtering test cases
        LexerTestCase{"the cat and dog",
                      {"cat", "dog"},
                      true,
                      3,
                      "",
                      "Stop words filtered out"},
        LexerTestCase{
            "the and or", {}, true, 3, "", "All stop words filtered out"}));

// Separate tests for error cases and special scenarios
TEST_F(LexerTest, InvalidUTF8) {
  std::string invalid_utf8 = "hello \xFF\xFE world";
  auto result = lexer_->Tokenize(invalid_utf8, default_stemming_enabled_,
                                 default_min_stem_size_);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(result.status().message(), "Invalid UTF-8");
}

TEST_F(LexerTest, LongWord) {
  std::string long_word(1000, 'a');
  auto result = lexer_->Tokenize(long_word, default_stemming_enabled_,
                                 default_min_stem_size_);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({long_word}));
}

// Test empty stop words set behavior
TEST_F(LexerTest, EmptyStopWordsHandling) {
  // Create Lexer with no stop words
  lexer_ = CreateLexer(default_punctuation_, {});

  // Test tokenization with empty stop words - all words preserved
  auto result = lexer_->Tokenize(
      "Hello, world! TESTING 123 with-dashes and/or symbols", true, 3);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result,
            std::vector<std::string>({"hello", "world", "test", "123", "with",
                                      "dash", "and", "or", "symbol"}));
}

}  // namespace valkey_search::indexes::text
