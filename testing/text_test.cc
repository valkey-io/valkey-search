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
    text_index_schema_ = std::make_shared<text::TextIndexSchema>(
        data_model::LANGUAGE_ENGLISH,
        " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",  // Default punctuation
        false,                                        // with_offsets
        empty_stop_words);

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
    return std::make_shared<text::TextIndexSchema>(
        data_model::LANGUAGE_ENGLISH, punct, with_offsets, stop_words);
  }

  // Helper to check if a token exists in the prefix tree
  bool TokenExists(const std::string& token,
                   std::shared_ptr<text::TextIndexSchema> schema = nullptr) {
    auto active_schema = schema ? schema : text_index_schema_;
    auto iter =
        active_schema->GetTextIndex()->GetPrefix().GetWordIterator(token);
    return !iter.Done();
  }

  // Helper to get postings for a token
  std::shared_ptr<text::Postings> GetPostingsForToken(
      const std::string& token,
      std::shared_ptr<text::TextIndexSchema> schema = nullptr) {
    auto active_schema = schema ? schema : text_index_schema_;
    auto iter =
        active_schema->GetTextIndex()->GetPrefix().GetWordIterator(token);
    if (iter.Done()) {
      return nullptr;
    }
    return iter.GetTarget();
  }

  // Stages a single Text attribute update from the key and then commits the key
  // update to the schema-level text index structures.
  void AddRecordAndCommitKey(Text* text_index, const InternedStringPtr& key,
                             absl::string_view data,
                             std::shared_ptr<text::TextIndexSchema> schema) {
    auto result = text_index->AddRecord(key, data);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_TRUE(result.value());
    schema->CommitKeyData(key);
  }

  // Adds the record to the default text index
  void AddRecordAndCommitKey(
      const InternedStringPtr& key, absl::string_view data,
      std::shared_ptr<text::TextIndexSchema> schema = nullptr) {
    auto active_schema = schema ? schema : text_index_schema_;
    AddRecordAndCommitKey(text_index_.get(), key, data, active_schema);
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

  if (test_case.should_succeed) {
    AddRecordAndCommitKey(key, test_case.input_text, active_schema);
    // Validate the index structure matches expectations
    ValidateIndexStructure(test_case, active_schema);
  } else {
    // For failure cases, test directly without the helper
    auto result = text_index_->AddRecord(key, test_case.input_text);
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

  AddRecordAndCommitKey(key, data);

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
  AddRecordAndCommitKey(key1, "hello world");
  AddRecordAndCommitKey(key2, "hello test");

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
  auto stemming_schema = std::make_shared<text::TextIndexSchema>(
      data_model::LANGUAGE_ENGLISH, " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
      false,  // with_offsets
      empty_stop_words);

  data_model::TextIndex stem_proto;
  stem_proto.set_no_stem(false);  // Enable stemming

  auto stem_text_index = std::make_unique<Text>(stem_proto, stemming_schema);

  auto key = StringInternStore::Intern("stem_key");
  std::string data = "running runs runner";

  AddRecordAndCommitKey(stem_text_index.get(), key, data, stemming_schema);

  // Stemming behavior depends on the stemmer implementation
  // This test ensures stemming doesn't break the indexing pipeline
  auto& prefix_tree = stemming_schema->GetTextIndex()->GetPrefix();

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

// Test TEXT fetcher size estimation for different predicate types
TEST_F(TextTest, FetcherSizeEstimation) {
  // Setup: Create documents with overlapping terms
  auto key1 = StringInternStore::Intern("doc:1");
  auto key2 = StringInternStore::Intern("doc:2");
  auto key3 = StringInternStore::Intern("doc:3");
  AddRecordAndCommitKey(key1, "test document one");
  AddRecordAndCommitKey(key2, "test document two");
  AddRecordAndCommitKey(key3, "another document");
  // Create predicates
  auto field_mask = 1ULL;  // Field mask for testing

  // Test 1: TermPredicate size estimation
  {
    query::TermPredicate term_pred(text_index_schema_, field_mask, "test",
                                   false);
    // Perform query and validate
    auto fetcher_ptr = term_pred.Search(false);
    auto* fetcher = static_cast<Text::EntriesFetcher*>(fetcher_ptr);
    EXPECT_EQ(fetcher->Size(), 2)
        << "TermPredicate for 'test' should estimate 2 documents";
    delete fetcher;
  }
  // Test 2: PrefixPredicate size estimation
  {
    query::PrefixPredicate prefix_pred(text_index_schema_, field_mask, "doc");
    // Perform query and validate
    auto fetcher_ptr = prefix_pred.Search(false);
    auto* fetcher = static_cast<Text::EntriesFetcher*>(fetcher_ptr);
    EXPECT_EQ(fetcher->Size(), 3)
        << "PrefixPredicate for 'doc' should estimate 3 documents";
    delete fetcher;
  }
  // Test 3: SuffixPredicate size estimation (requires suffix trie)
  {
    // Create index for suffix trie enabled
    std::vector<std::string> empty_stop_words;
    auto suffix_schema = std::make_shared<text::TextIndexSchema>(
        data_model::LANGUAGE_ENGLISH,
        " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~", false, empty_stop_words);
    data_model::TextIndex suffix_proto;
    suffix_proto.set_with_suffix_trie(true);
    auto suffix_text_index =
        std::make_unique<Text>(suffix_proto, suffix_schema);
    // Add docs
    AddRecordAndCommitKey(suffix_text_index.get(), key1, "test document one",
                          suffix_schema);
    AddRecordAndCommitKey(suffix_text_index.get(), key2, "test document two",
                          suffix_schema);
    AddRecordAndCommitKey(suffix_text_index.get(), key3, "another document",
                          suffix_schema);
    // Perform query and validate
    query::SuffixPredicate suffix_pred(suffix_schema, field_mask, "ment");
    auto fetcher_ptr = suffix_pred.Search(false);
    auto* fetcher = static_cast<Text::EntriesFetcher*>(fetcher_ptr);
    EXPECT_EQ(fetcher->Size(), 3)
        << "SuffixPredicate for 'ment' should estimate 3 documents";
    delete fetcher;
  }
  // Test 4: Non-existent term
  {
    query::TermPredicate term_pred(text_index_schema_, field_mask,
                                   "nonexistent", false);
    auto fetcher_ptr = term_pred.Search(false);
    auto* fetcher = static_cast<Text::EntriesFetcher*>(fetcher_ptr);
    EXPECT_EQ(fetcher->Size(), 0)
        << "Non-existent term should estimate 0 documents";
    delete fetcher;
  }
}
// Validates deduplication: 100 matching words in 1 document should return
// size=1, not 100.
TEST_F(TextTest, FetcherSizeEstimationWithPrefixDeduplication) {
  auto key = StringInternStore::Intern("doc:pathological");
  auto field_mask = 1ULL;
  // Create document with 100 words starting with "test"
  std::string data;
  for (int i = 0; i < 100; ++i) {
    data += "test" + std::to_string(i) + " ";
  }
  AddRecordAndCommitKey(key, data);
  query::PrefixPredicate prefix_pred(text_index_schema_, field_mask, "test");
  auto fetcher_ptr = prefix_pred.Search(false);
  auto* fetcher = static_cast<Text::EntriesFetcher*>(fetcher_ptr);
  EXPECT_EQ(fetcher->Size(), 1)
      << "Fetcher size should count unique documents, not word occurrences";
  delete fetcher;
}

}  // namespace valkey_search::indexes
