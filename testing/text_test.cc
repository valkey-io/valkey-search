/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "gtest/gtest.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/text_index.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"

namespace valkey_search::indexes {

// Test case structure for comprehensive text indexing validation
struct TextIndexTestCase {
  std::string input_text;
  std::vector<std::string> expected_tokens;
  std::map<std::string, int>
      expected_frequencies_positional;  // token -> frequency in positional mode
  std::map<std::string, int>
      expected_frequencies_boolean;  // token -> frequency in boolean mode
  int expected_total_documents = 1;
  bool should_succeed = true;
  bool stemming_enabled = true;
  bool with_offsets = false;            // whether to use positional mode
  std::string custom_punctuation = "";  // empty = use default
  std::string description;
};

class TextTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create default text index schema for testing
    std::vector<std::string> empty_stop_words;
    auto index_schema_proto = CreateIndexSchemaProtoWithTextProperties(
        data_model::LANGUAGE_ENGLISH,
        " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",  // Default punctuation
        false,                                        // with_offsets
        empty_stop_words);
    text_index_schema_ =
        std::make_shared<text::TextIndexSchema>(index_schema_proto);

    // Create default TextIndex prototype
    text_index_proto_ = std::make_unique<data_model::TextIndex>();
    text_index_proto_->set_no_stem(!stemming_enabled_);

    // Create Text instance
    text_index_ =
        std::make_unique<Text>(*text_index_proto_, text_index_schema_);

    // Default configuration
    stemming_enabled_ = true;
  }

  // Helper to create custom schema with specific settings
  std::shared_ptr<text::TextIndexSchema> CreateCustomSchema(
      const std::string& punctuation = "", bool stemming = true,
      const std::vector<std::string>& stop_words = {},
      bool with_offsets = false) {
    std::string punct = punctuation.empty()
                            ? " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"
                            : punctuation;
    auto index_schema_proto = CreateIndexSchemaProtoWithTextProperties(
        data_model::LANGUAGE_ENGLISH, punct, with_offsets, stop_words);
    return std::make_shared<text::TextIndexSchema>(index_schema_proto);
  }

  // Helper to check if a token exists in the prefix tree
  bool TokenExists(const std::string& token,
                   std::shared_ptr<text::TextIndexSchema> schema = nullptr) {
    auto active_schema = schema ? schema : text_index_schema_;
    auto iter = active_schema->GetTextIndex()->prefix_.GetWordIterator(token);
    return !iter.Done();
  }

  // Helper to get postings for a token
  std::shared_ptr<text::Postings> GetPostingsForToken(
      const std::string& token,
      std::shared_ptr<text::TextIndexSchema> schema = nullptr) {
    auto active_schema = schema ? schema : text_index_schema_;
    auto iter = active_schema->GetTextIndex()->prefix_.GetWordIterator(token);
    if (iter.Done()) {
      return nullptr;
    }
    return iter.GetTarget();
  }

  // Validate that the index structure matches expected results
  void ValidateIndexStructure(
      const TextIndexTestCase& test_case,
      std::shared_ptr<text::TextIndexSchema> schema = nullptr) {
    // Validate each expected token exists with correct properties
    for (const auto& token : test_case.expected_tokens) {
      EXPECT_TRUE(TokenExists(token, schema))
          << "Token '" << token
          << "' should exist in index for: " << test_case.description;

      auto postings = GetPostingsForToken(token, schema);
      ASSERT_NE(postings, nullptr)
          << "Postings should exist for token '" << token
          << "' in: " << test_case.description;

      EXPECT_EQ(postings->GetKeyCount(), test_case.expected_total_documents)
          << "Document count mismatch for token '" << token
          << "' in: " << test_case.description;

      // Choose the appropriate frequency map based on the mode
      const auto& expected_frequencies =
          test_case.with_offsets ? test_case.expected_frequencies_positional
                                 : test_case.expected_frequencies_boolean;

      // Validate term frequency if specified
      auto freq_it = expected_frequencies.find(token);
      if (freq_it != expected_frequencies.end()) {
        EXPECT_EQ(postings->GetTotalTermFrequency(), freq_it->second)
            << "Term frequency mismatch for token '" << token
            << "' in: " << test_case.description;
      }
    }
  }

  std::shared_ptr<text::TextIndexSchema> text_index_schema_;
  std::unique_ptr<data_model::TextIndex> text_index_proto_;
  std::unique_ptr<Text> text_index_;
  bool stemming_enabled_;
};

// Parameterized test class for systematic index validation
class TextIndexParameterizedTest
    : public TextTest,
      public ::testing::WithParamInterface<TextIndexTestCase> {};

TEST_P(TextIndexParameterizedTest, ValidateIndexStructure) {
  const auto& test_case = GetParam();

  std::shared_ptr<text::TextIndexSchema> active_schema = text_index_schema_;

  // Use custom schema if specified or if with_offsets differs from default
  if (!test_case.custom_punctuation.empty() || test_case.with_offsets) {
    active_schema = CreateCustomSchema(test_case.custom_punctuation,
                                       test_case.stemming_enabled, {},
                                       test_case.with_offsets);
    text_index_proto_->set_no_stem(!test_case.stemming_enabled);
    text_index_ = std::make_unique<Text>(*text_index_proto_, active_schema);
  }

  auto key = StringInternStore::Intern("test_key");

  // Add record and verify it succeeds if expected
  auto result = text_index_->AddRecord(key, test_case.input_text);
  if (test_case.should_succeed) {
    ASSERT_TRUE(result.ok()) << "Test case: " << test_case.description;
    EXPECT_TRUE(result.value()) << "Test case: " << test_case.description;

    // Validate the index structure matches expectations
    ValidateIndexStructure(test_case, active_schema);
  } else {
    EXPECT_FALSE(result.ok())
        << "Test case should fail: " << test_case.description;
  }
}

INSTANTIATE_TEST_SUITE_P(
    AllIndexValidationTests, TextIndexParameterizedTest,
    ::testing::Values(
        TextIndexTestCase{
            "hello world",
            {"hello", "world"},
            {{"hello", 1}, {"world", 1}},  // positional frequencies
            {{"hello", 1}, {"world", 1}},  // boolean frequencies
            1,
            true,
            true,
            false,
            "",
            "Basic two-word document tokenization"},
        TextIndexTestCase{"hello,world!test.document",
                          {"hello", "world", "test", "document"},
                          {{"hello", 1},
                           {"world", 1},
                           {"test", 1},
                           {"document", 1}},  // positional
                          {{"hello", 1},
                           {"world", 1},
                           {"test", 1},
                           {"document", 1}},  // boolean
                          1,
                          true,
                          true,
                          false,
                          "",
                          "Punctuation separates tokens correctly"},
        TextIndexTestCase{
            "hello hello world hello test",
            {"hello", "world", "test"},
            {{"hello", 3},
             {"world", 1},
             {"test", 1}},  // positional: actual frequencies
            {{"hello", 1},
             {"world", 1},
             {"test", 1}},  // boolean: presence only
            1,
            true,
            true,
            true,
            "",  // with_offsets = true
            "Term frequency calculation accuracy with positional mode"},
        TextIndexTestCase{"",
                          {},
                          {},  // positional
                          {},  // boolean
                          1,
                          true,
                          true,
                          false,
                          "",
                          "Empty document handling"},
        TextIndexTestCase{"   \t\n\r  ",
                          {},
                          {},  // positional
                          {},  // boolean
                          1,
                          true,
                          true,
                          false,
                          "",
                          "Whitespace-only document handling"},
        TextIndexTestCase{
            "Hello WORLD Test",
            {"hello", "world", "test"},  // Assuming case normalization
            {{"hello", 1}, {"world", 1}, {"test", 1}},  // positional
            {{"hello", 1}, {"world", 1}, {"test", 1}},  // boolean
            1,
            true,
            true,
            false,
            "",
            "Case sensitivity in tokenization"},
        TextIndexTestCase{
            "Hello мир 世界 test",
            {"hello", "test"},            // Unicode handling may vary by lexer
            {{"hello", 1}, {"test", 1}},  // positional
            {{"hello", 1}, {"test", 1}},  // boolean
            1,
            true,
            true,
            false,
            "",
            "Unicode text handling"},
        TextIndexTestCase{
            "hello,world!test.document",
            {"hello", "world!test.docu"},  // Custom punctuation: only space and
                                           // comma (stemmed)
            {{"hello", 1}, {"world!test.docu", 1}},  // positional
            {{"hello", 1}, {"world!test.docu", 1}},  // boolean
            1,
            true,
            true,
            false,
            " ,",
            "Custom punctuation handling"},
        TextIndexTestCase{"a b c",
                          {"a", "b", "c"},
                          {{"a", 1}, {"b", 1}, {"c", 1}},  // positional
                          {{"a", 1}, {"b", 1}, {"c", 1}},  // boolean
                          1,
                          true,
                          true,
                          true,
                          "",  // with_offsets = true
                          "Single character tokens with positional mode"},
        TextIndexTestCase{
            "hello\tworld\ntest",
            {"hello", "world", "test"},
            {{"hello", 1}, {"world", 1}, {"test", 1}},  // positional
            {{"hello", 1}, {"world", 1}, {"test", 1}},  // boolean
            1,
            true,
            true,
            false,
            "",
            "Tabs and newlines as separators"}));

// Separate test for large document processing (non-parameterized due to
// complexity)
TEST_F(TextTest, LargeDocumentTokenization) {
  auto key = StringInternStore::Intern("large_key");

  // Create a document with many repeated words
  std::string data;
  for (int i = 0; i < 1000; ++i) {
    data += "word" + std::to_string(i % 10) + " ";
  }

  auto result = text_index_->AddRecord(key, data);
  ASSERT_TRUE(result.ok()) << result.status();

  // Should create tokens for word0 through word9
  for (int i = 0; i < 10; ++i) {
    std::string token = "word" + std::to_string(i);
    EXPECT_TRUE(TokenExists(token)) << "Token " << token << " should exist";

    auto postings = GetPostingsForToken(token);
    ASSERT_NE(postings, nullptr);
    EXPECT_EQ(postings->GetKeyCount(), 1);  // One document
    // In boolean mode (with_offsets=false), frequency is 1 regardless of actual
    // count
    EXPECT_EQ(postings->GetTotalTermFrequency(), 1);
  }
}

// Multi-document test (non-parameterized due to complexity)
TEST_F(TextTest, MultipleDocumentsShareTokens) {
  auto key1 = StringInternStore::Intern("doc1");
  auto key2 = StringInternStore::Intern("doc2");

  // Add documents with overlapping terms
  auto result1 = text_index_->AddRecord(key1, "hello world");
  auto result2 = text_index_->AddRecord(key2, "hello test");

  ASSERT_TRUE(result1.ok()) << result1.status();
  ASSERT_TRUE(result2.ok()) << result2.status();

  // "hello" should appear in both documents
  auto hello_postings = GetPostingsForToken("hello");
  ASSERT_NE(hello_postings, nullptr);
  EXPECT_EQ(hello_postings->GetKeyCount(), 2);

  // "world" should only appear in doc1
  auto world_postings = GetPostingsForToken("world");
  ASSERT_NE(world_postings, nullptr);
  EXPECT_EQ(world_postings->GetKeyCount(), 1);

  // "test" should only appear in doc2
  auto test_postings = GetPostingsForToken("test");
  ASSERT_NE(test_postings, nullptr);
  EXPECT_EQ(test_postings->GetKeyCount(), 1);
}

// Test stemming behavior (when enabled)
TEST_F(TextTest, StemmingBehavior) {
  // Create schema with stemming enabled
  std::vector<std::string> empty_stop_words;
  auto index_schema_proto = CreateIndexSchemaProtoWithTextProperties(
      data_model::LANGUAGE_ENGLISH, " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
      false,  // with_offsets
      empty_stop_words);
  auto stemming_schema =
      std::make_shared<text::TextIndexSchema>(index_schema_proto);

  data_model::TextIndex stem_proto;
  stem_proto.set_no_stem(false);  // Enable stemming

  auto stem_text_index = std::make_unique<Text>(stem_proto, stemming_schema);

  auto key = StringInternStore::Intern("stem_key");
  std::string data = "running runs runner";

  auto result = stem_text_index->AddRecord(key, data);
  ASSERT_TRUE(result.ok()) << result.status();

  // Stemming behavior depends on the stemmer implementation
  // This test ensures stemming doesn't break the indexing pipeline
  auto& prefix_tree = stemming_schema->GetTextIndex()->prefix_;

  // Should create some tokens (exact form depends on stemmer)
  bool has_tokens = false;
  // We can't easily iterate over all tokens, but we know at least some should
  // exist
  auto run_iter = prefix_tree.GetWordIterator("run");
  if (!run_iter.Done()) {
    has_tokens = true;
  }
  auto running_iter = prefix_tree.GetWordIterator("running");
  if (!running_iter.Done()) {
    has_tokens = true;
  }

  EXPECT_TRUE(has_tokens) << "Should create stemmed tokens";
}

TEST_F(TextTest, InvalidInputHandling) {
  auto key = StringInternStore::Intern("invalid_key");

  // Test with invalid UTF-8 would need proper invalid UTF-8 string
  // For now, test with very large document
  std::string very_large_doc(1000000, 'a');  // 1MB document

  auto result = text_index_->AddRecord(key, very_large_doc);
  // Should handle large documents gracefully
  EXPECT_TRUE(result.ok()) << "Should handle very large documents";
}

}  // namespace valkey_search::indexes
