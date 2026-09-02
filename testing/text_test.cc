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

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "gtest/gtest.h"
#include "src/index_schema.pb.h"
#include "src/indexes/scoring/scorer.h"
#include "src/indexes/text/invasive_ptr.h"
#include "src/indexes/text/text_index.h"
#include "src/query/predicate.h"
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
        empty_stop_words,
        4);  // min_stem_size

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
      const std::string &punctuation = "", bool stemming = true,
      const std::vector<std::string> &stop_words = {},
      bool with_offsets = false) {
    std::string punct = punctuation.empty()
                            ? " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"
                            : punctuation;
    return std::make_shared<text::TextIndexSchema>(
        data_model::LANGUAGE_ENGLISH, punct, with_offsets, stop_words, 4);
  }

  // Helper to check if a token exists in the prefix tree
  bool TokenExists(const std::string &token,
                   std::shared_ptr<text::TextIndexSchema> schema = nullptr) {
    auto active_schema = schema ? schema : text_index_schema_;
    auto iter =
        active_schema->GetTextIndex()->GetPrefix().GetWordIterator(token);
    return !iter.Done();
  }

  // Helper to get postings for a token
  text::InvasivePtr<text::Postings> GetPostingsForToken(
      const std::string &token,
      std::shared_ptr<text::TextIndexSchema> schema = nullptr) {
    auto active_schema = schema ? schema : text_index_schema_;
    auto iter =
        active_schema->GetTextIndex()->GetPrefix().GetWordIterator(token);
    if (iter.Done()) {
      return nullptr;
    }
    return iter.GetPostingsTarget();
  }

  // Stages a single Text attribute update from the key and then commits the key
  // update to the schema-level text index structures.
  void AddRecordAndCommitKey(Text *text_index, const InternedStringPtr &key,
                             absl::string_view data,
                             std::shared_ptr<text::TextIndexSchema> schema) {
    auto result = text_index->AddRecord(key, data);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(result.value(), indexes::RecordResult::kAdded);
    schema->CommitKeyData(key);
  }

  // Adds the record to the default text index
  void AddRecordAndCommitKey(
      const InternedStringPtr &key, absl::string_view data,
      std::shared_ptr<text::TextIndexSchema> schema = nullptr) {
    auto active_schema = schema ? schema : text_index_schema_;
    AddRecordAndCommitKey(text_index_.get(), key, data, active_schema);
  }

  // Validate that the index structure matches expected results
  void ValidateIndexStructure(
      const TextIndexTestCase &test_case,
      std::shared_ptr<text::TextIndexSchema> schema = nullptr) {
    // Validate each expected token exists with correct properties
    for (const auto &token : test_case.expected_tokens) {
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
      const auto &expected_frequencies =
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
  const auto &test_case = GetParam();

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
      empty_stop_words,
      4);  // min_stem_size

  data_model::TextIndex stem_proto;
  stem_proto.set_no_stem(false);  // Enable stemming

  auto stem_text_index = std::make_unique<Text>(stem_proto, stemming_schema);

  auto key = StringInternStore::Intern("stem_key");
  std::string data = "running runs runner";

  AddRecordAndCommitKey(stem_text_index.get(), key, data, stemming_schema);

  // Stemming behavior depends on the stemmer implementation
  // This test ensures stemming doesn't break the indexing pipeline
  auto &prefix_tree = stemming_schema->GetTextIndex()->GetPrefix();

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

// Builds a scored stem query iterator over a fresh stemming schema + corpus.
class StemScoringTest : public TextTest {
 protected:
  std::unique_ptr<text::TextIterator> BuildScoredStemQuery(
      std::shared_ptr<text::TextIndexSchema> schema, Text *text,
      const std::string &query) {
    query::TermPredicate pred(schema, /*field_mask=*/~0ULL, query,
                              /*exact=*/false);
    pred.SetScorer(scoring::GetScorer(scoring::ScorerType::kBm25Std));
    return pred.BuildTextIterator(schema->GetTextIndex(), ~0ULL,
                                  /*require_positions=*/false);
  }

  std::pair<std::shared_ptr<text::TextIndexSchema>, std::unique_ptr<Text>>
  MakeStemIndex() {
    auto schema = CreateCustomSchema("", /*stemming=*/true);
    data_model::TextIndex proto;
    proto.set_no_stem(false);
    auto text = std::make_unique<Text>(proto, schema);
    schema->SetStemTextFieldMask(1ULL << text->GetTextFieldNumber());
    return {schema, std::move(text)};
  }
};

// A stemmed query sums three BM25 leaves per the industry standard: the exact
// word, the stem root literal, and the stem inflection group. Values verified
// against the industry-standard reference.
TEST_F(StemScoringTest, ThreeLeafScoresMatchStandard) {
  auto [schema, text] = MakeStemIndex();
  AddRecordAndCommitKey(text.get(), StringInternStore::Intern("d1"), "running",
                        schema);
  AddRecordAndCommitKey(text.get(), StringInternStore::Intern("d2"), "runs",
                        schema);
  AddRecordAndCommitKey(text.get(), StringInternStore::Intern("d3"), "run",
                        schema);

  auto iter = BuildScoredStemQuery(schema, text.get(), "running");
  absl::flat_hash_map<std::string, float> scores;
  while (!iter->DoneKeys()) {
    scores[std::string(iter->CurrentKey()->Str())] = iter->GetScore();
    iter->NextKey();
  }

  ASSERT_EQ(scores.size(), 3u);
  // d1 "running": exact leaf (idf 0.98) + stem leaf (idf 0.47, dt=2 distinct).
  EXPECT_NEAR(scores["d1"], 1.450833f, 1e-3f);
  // d3 "run": scored only on the stem root literal leaf (its own idf 0.98).
  EXPECT_NEAR(scores["d3"], 0.980829f, 1e-3f);
  // d2 "runs": scored only on the stem inflection leaf.
  EXPECT_NEAR(scores["d2"], 0.470004f, 1e-3f);
  // exact-form doc outranks the root-literal doc, which outranks the
  // inflection.
  EXPECT_GT(scores["d1"], scores["d3"]);
  EXPECT_GT(scores["d3"], scores["d2"]);
}

// When the exact surface word is absent, matches still score via the stem group
// alone (the has_original_ == false path); no exact leaf, finite positive
// score.
TEST_F(StemScoringTest, MissingOriginalWordScoresStemOnly) {
  auto [schema, text] = MakeStemIndex();
  AddRecordAndCommitKey(text.get(), StringInternStore::Intern("d1"), "runs",
                        schema);

  auto iter = BuildScoredStemQuery(schema, text.get(), "running");
  ASSERT_FALSE(iter->DoneKeys());
  const float score = iter->GetScore();
  iter->NextKey();
  EXPECT_TRUE(iter->DoneKeys());
  EXPECT_GT(score, 0.0f);
}

}  // namespace valkey_search::indexes
