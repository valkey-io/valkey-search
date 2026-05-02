/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/lexer.h"

#include <algorithm>
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

  valkey_search::indexes::text::InProgressStemMap stem_mappings;
  auto result = lexer_->Tokenize(
      test_case.input, test_case.stemming_enabled, test_case.min_stem_size,
      test_case.stemming_enabled ? &stem_mappings : nullptr);

  ASSERT_TRUE(result.ok()) << "Test case: " << test_case.description;
  std::vector<std::string> result_vector(result->begin(), result->end());
  EXPECT_EQ(result_vector, test_case.expected)
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
                      {"running", "jumping"},
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
  valkey_search::indexes::text::InProgressStemMap stem_mappings;
  auto result = lexer_->Tokenize(invalid_utf8, default_stemming_enabled_,
                                 default_min_stem_size_, &stem_mappings);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(result.status().message(), "Invalid UTF-8");
}

TEST_F(LexerTest, LongWord) {
  std::string long_word(1000, 'a');
  valkey_search::indexes::text::InProgressStemMap stem_mappings;
  auto result = lexer_->Tokenize(long_word, default_stemming_enabled_,
                                 default_min_stem_size_, &stem_mappings);
  ASSERT_TRUE(result.ok());
  std::vector<std::string> result_vector(result->begin(), result->end());
  EXPECT_EQ(result_vector, std::vector<std::string>({long_word}));
}

// Test empty stop words set behavior
TEST_F(LexerTest, EmptyStopWordsHandling) {
  // Create Lexer with no stop words
  lexer_ = CreateLexer(default_punctuation_, {});

  // Test tokenization with empty stop words - all words preserved (original,
  // not stemmed)
  valkey_search::indexes::text::InProgressStemMap stem_mappings;
  auto result =
      lexer_->Tokenize("Hello, world! TESTING 123 with-dashes and/or symbols",
                       true, 3, &stem_mappings);

  ASSERT_TRUE(result.ok());
  std::vector<std::string> result_vector(result->begin(), result->end());
  EXPECT_EQ(result_vector, std::vector<std::string>(
                               {"hello", "world", "testing", "123", "with",
                                "dashes", "and", "or", "symbols"}));
}

// Stem tree tests - verify stem mappings are populated correctly
TEST_F(LexerTest, StemMappingsBasic) {
  valkey_search::indexes::text::InProgressStemMap stem_mappings;

  auto result =
      lexer_->Tokenize("running jumps happily", true, 3, &stem_mappings);

  ASSERT_TRUE(result.ok());
  // Original words (case-folded, not stemmed)
  std::vector<std::string> result_vector(result->begin(), result->end());
  EXPECT_EQ(result_vector,
            std::vector<std::string>({"running", "jumps", "happily"}));

  // Verify stem mappings: stemmed form -> original words
  EXPECT_EQ(stem_mappings.size(),
            3);  // All three words stem to different forms
  EXPECT_TRUE(stem_mappings.contains("run"));
  EXPECT_TRUE(std::find(stem_mappings["run"].begin(),
                        stem_mappings["run"].end(),
                        "running") != stem_mappings["run"].end());
  EXPECT_TRUE(stem_mappings.contains("jump"));
  EXPECT_TRUE(std::find(stem_mappings["jump"].begin(),
                        stem_mappings["jump"].end(),
                        "jumps") != stem_mappings["jump"].end());
  EXPECT_TRUE(stem_mappings.contains("happili"));
  EXPECT_TRUE(std::find(stem_mappings["happili"].begin(),
                        stem_mappings["happili"].end(),
                        "happily") != stem_mappings["happili"].end());
}

TEST_F(LexerTest, StemMappingsMultipleWordsToSameStem) {
  valkey_search::indexes::text::InProgressStemMap stem_mappings;

  auto result = lexer_->Tokenize("running runs", true, 3, &stem_mappings);

  ASSERT_TRUE(result.ok());
  // Original words (case-folded, not stemmed)
  std::vector<std::string> result_vector(result->begin(), result->end());
  EXPECT_EQ(result_vector, std::vector<std::string>({"running", "runs"}));

  // Both words should map to the same stem "run"
  EXPECT_EQ(stem_mappings.size(), 1);
  EXPECT_TRUE(stem_mappings.contains("run"));
  EXPECT_EQ(stem_mappings["run"].size(), 2);  // Both words map to "run"
  EXPECT_TRUE(std::find(stem_mappings["run"].begin(),
                        stem_mappings["run"].end(),
                        "running") != stem_mappings["run"].end());
  EXPECT_TRUE(std::find(stem_mappings["run"].begin(),
                        stem_mappings["run"].end(),
                        "runs") != stem_mappings["run"].end());
}

TEST_F(LexerTest, StemMappingsNoStemmingWhenDisabled) {
  valkey_search::indexes::text::InProgressStemMap stem_mappings;

  auto result =
      lexer_->Tokenize("running jumps happily", false, 3, &stem_mappings);

  ASSERT_TRUE(result.ok());
  // Original words (not stemmed)
  std::vector<std::string> result_vector(result->begin(), result->end());
  EXPECT_EQ(result_vector,
            std::vector<std::string>({"running", "jumps", "happily"}));

  // No stem mappings when stemming is disabled
  EXPECT_TRUE(stem_mappings.empty());
}

}  // namespace valkey_search::indexes::text
