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

#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "gtest/gtest.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/fuzzy.h"
#include "src/indexes/text/invasive_ptr.h"
#include "src/indexes/text/punctuation.h"
#include "src/indexes/text/stop_words.h"
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
      const std::string& punctuation = "", bool stemming = true,
      const std::vector<std::string>& stop_words = {},
      bool with_offsets = false) {
    std::string punct = punctuation.empty()
                            ? " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"
                            : punctuation;
    return std::make_shared<text::TextIndexSchema>(
        data_model::LANGUAGE_ENGLISH, punct, with_offsets, stop_words, 4);
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
  text::InvasivePtr<text::Postings> GetPostingsForToken(
      const std::string& token,
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
            {"hello",
             "test"},  // Unicode handling may vary by LanguageProcessor
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

// Fuzzy must decode UTF-8 across radix-tree edge boundaries. Two Arabic words
// sharing only their first byte (0xD8) cause that byte to live on its own
// edge, with the second byte of each word starting the next edge. A walker
// that decodes per-edge sees two invalid bytes instead of one Arabic code
// point and miscounts the edit distance.
TEST_F(TextTest, FuzzySearchAcrossMultiByteEdgeSplit) {
  AddRecordAndCommitKey(StringInternStore::Intern("doc:1"), "بالعالم");
  AddRecordAndCommitKey(StringInternStore::Intern("doc:2"), "اختبار");

  const auto& tree = text_index_schema_->GetTextIndex()->GetPrefix();
  auto results = text::FuzzySearch::Search(tree, "بالعالم", /*max_distance=*/0,
                                           /*max_words=*/100);
  ASSERT_EQ(results.size(), 1u);
  EXPECT_EQ(results[0].GetKey()->Str(), "doc:1");
}

// Edit distance is in code points, not bytes. "¡hola" and "hola" differ by
// one inserted code point (¡ = U+00A1, 2 bytes). Distance 1 must match;
// distance 0 must not.
TEST_F(TextTest, FuzzySearchCodePointDistance) {
  AddRecordAndCommitKey(StringInternStore::Intern("doc:1"), "¡hola");

  const auto& tree = text_index_schema_->GetTextIndex()->GetPrefix();
  auto exact = text::FuzzySearch::Search(tree, "hola", /*max_distance=*/0,
                                         /*max_words=*/100);
  EXPECT_EQ(exact.size(), 0u);

  auto fuzzy = text::FuzzySearch::Search(tree, "hola", /*max_distance=*/1,
                                         /*max_words=*/100);
  ASSERT_EQ(fuzzy.size(), 1u);
  EXPECT_EQ(fuzzy[0].GetKey()->Str(), "doc:1");
}

// Damerau-Levenshtein transposition counts as a single edit, and must operate
// on code points. "café" vs "caéf" swaps the last two code points (é =
// U+00E9, 2 bytes; f = 1 byte). A byte-wise DP would see a multi-byte
// scramble; the code-point DP sees one transposition. Distance 1 matches,
// distance 0 does not.
TEST_F(TextTest, FuzzySearchMultiByteTransposition) {
  AddRecordAndCommitKey(StringInternStore::Intern("doc:1"), "café");

  const auto& tree = text_index_schema_->GetTextIndex()->GetPrefix();
  auto exact = text::FuzzySearch::Search(tree, "caéf", /*max_distance=*/0,
                                         /*max_words=*/100);
  EXPECT_EQ(exact.size(), 0u);

  auto fuzzy = text::FuzzySearch::Search(tree, "caéf", /*max_distance=*/1,
                                         /*max_words=*/100);
  ASSERT_EQ(fuzzy.size(), 1u);
  EXPECT_EQ(fuzzy[0].GetKey()->Str(), "doc:1");
}

// 3-byte code points that share a multi-byte prefix exercise the Rax
// edge-split decode path. ぁ (U+3041, E3 81 81) and あ (U+3042, E3 81 82)
// share the first two bytes E3 81, so the radix tree splits the shared prefix
// onto its own edge and the differing third byte starts the next edge. The
// fuzzy walker must reassemble the full 3-byte code point across that split
// rather than decoding partial bytes per edge.
TEST_F(TextTest, FuzzySearchAcrossThreeByteEdgeSplit) {
  AddRecordAndCommitKey(StringInternStore::Intern("doc:1"), "ぁ");
  AddRecordAndCommitKey(StringInternStore::Intern("doc:2"), "あ");

  const auto& tree = text_index_schema_->GetTextIndex()->GetPrefix();
  auto results = text::FuzzySearch::Search(tree, "ぁ", /*max_distance=*/0,
                                           /*max_words=*/100);
  ASSERT_EQ(results.size(), 1u);
  EXPECT_EQ(results[0].GetKey()->Str(), "doc:1");
}

// ==========================================================================
// Scenario 1: Transposition straddling an edge split.
// "café" and "cafx" share prefix "caf"; the tree splits edges there.
// Querying "caéf" (transposition of é and f) forces the two swapped code
// points to land on opposite sides of the edge boundary. The prev_tree_cp
// carry and partial-byte buffering must combine correctly to detect that
// a single transposition (distance 1) suffices.
TEST_F(TextTest, FuzzyTranspositionAcrossEdgeSplit) {
  // Insert two words that share prefix "caf" so the tree creates an edge
  // split at that boundary. "café" has é (2-byte) after "caf".
  AddRecordAndCommitKey(StringInternStore::Intern("doc:1"), "café");
  AddRecordAndCommitKey(StringInternStore::Intern("doc:2"), "cafx");

  const auto& tree = text_index_schema_->GetTextIndex()->GetPrefix();

  // "caéf" is "café" with the last two code points transposed.
  // The transposition crosses the edge split at "caf"|"é..." vs "caf"|"x".
  auto exact = text::FuzzySearch::Search(tree, "caéf", /*max_distance=*/0,
                                         /*max_words=*/100);
  EXPECT_EQ(exact.size(), 0u);

  auto fuzzy = text::FuzzySearch::Search(tree, "caéf", /*max_distance=*/1,
                                         /*max_words=*/100);
  ASSERT_EQ(fuzzy.size(), 1u);
  EXPECT_EQ(fuzzy[0].GetKey()->Str(), "doc:1");
}

// ==========================================================================
// Scenario 2: 4-byte code point (emoji) split across edges.
// Two emoji that share a 3-byte prefix force the radix tree to split the
// 4-byte sequence. The fuzzy walker must reassemble the full 4-byte code
// point across the edge boundary.
TEST_F(TextTest, FuzzySearchAcrossFourByteEdgeSplit) {
  // 😀 = F0 9F 98 80, 😁 = F0 9F 98 81
  // They share first 3 bytes F0 9F 98; the tree splits there.
  AddRecordAndCommitKey(StringInternStore::Intern("doc:1"),
                        "\xF0\x9F\x98\x80");  // 😀
  AddRecordAndCommitKey(StringInternStore::Intern("doc:2"),
                        "\xF0\x9F\x98\x81");  // 😁

  const auto& tree = text_index_schema_->GetTextIndex()->GetPrefix();

  // Exact match for 😀 at distance 0
  auto results = text::FuzzySearch::Search(
      tree, "\xF0\x9F\x98\x80", /*max_distance=*/0, /*max_words=*/100);
  ASSERT_EQ(results.size(), 1u);
  EXPECT_EQ(results[0].GetKey()->Str(), "doc:1");

  // Distance 1: searching for 😀 should find both 😀 (exact) and 😁
  // (1 substitution in code point space)
  auto fuzzy = text::FuzzySearch::Search(tree, "\xF0\x9F\x98\x80",
                                         /*max_distance=*/1, /*max_words=*/100);
  ASSERT_EQ(fuzzy.size(), 2u);
}

// ==========================================================================
// Scenario 3: Pruning on a partial-only edge.
// When an edge contains only partial bytes of a multi-byte sequence (the
// shared prefix of two code points), the min_dist init-from-prev must still
// prune correctly: if the subtree can't possibly satisfy max_distance, the
// walker must not descend.
TEST_F(TextTest, FuzzyPruningOnPartialEdge) {
  // Insert two 3-byte chars that share 2 bytes: ぁ (E3 81 81) and あ (E3 81 82)
  // plus a totally different word so the tree has a branch to prune.
  AddRecordAndCommitKey(StringInternStore::Intern("doc:ja1"), "ぁ");
  AddRecordAndCommitKey(StringInternStore::Intern("doc:ja2"), "あ");
  AddRecordAndCommitKey(StringInternStore::Intern("doc:en"), "xyz");

  const auto& tree = text_index_schema_->GetTextIndex()->GetPrefix();

  // Searching "ぁ" at distance 0 — "xyz" subtree should be pruned
  // (completely different code points, distance would be >> 0).
  auto results = text::FuzzySearch::Search(tree, "ぁ", /*max_distance=*/0,
                                           /*max_words=*/100);
  ASSERT_EQ(results.size(), 1u);
  EXPECT_EQ(results[0].GetKey()->Str(), "doc:ja1");

  // At distance 1, "あ" is reachable (1 substitution) but "xyz" is still
  // unreachable (3 substitutions for a 1-cp query = distance 3).
  auto fuzzy = text::FuzzySearch::Search(tree, "ぁ", /*max_distance=*/1,
                                         /*max_words=*/100);
  ASSERT_EQ(fuzzy.size(), 2u);
  // Verify "xyz" is not in results
  for (const auto& r : fuzzy) {
    EXPECT_NE(r.GetKey()->Str(), "doc:en");
  }
}

// ==========================================================================
// Scenario 4: max_distance > 1 with multibyte characters.
// All previous fuzzy multibyte tests use distance 0/1. This exercises
// distance 2 with multi-byte code points to ensure the DP matrix handles
// larger edit distances correctly across byte boundaries.
TEST_F(TextTest, FuzzyMultiByteDistance2) {
  // "München" — contains ü (U+00FC, 2 bytes)
  AddRecordAndCommitKey(StringInternStore::Intern("doc:1"), "münchen");

  const auto& tree = text_index_schema_->GetTextIndex()->GetPrefix();

  // "munchen" differs by: ü→u (1 substitution). Distance 1 should match.
  auto d1 = text::FuzzySearch::Search(tree, "munchen", /*max_distance=*/1,
                                      /*max_words=*/100);
  ASSERT_EQ(d1.size(), 1u);
  EXPECT_EQ(d1[0].GetKey()->Str(), "doc:1");

  // "munchn" differs by: ü→u (1 sub) + deletion of 'e'. Distance 2 matches.
  auto d2_match = text::FuzzySearch::Search(tree, "munchn", /*max_distance=*/2,
                                            /*max_words=*/100);
  ASSERT_EQ(d2_match.size(), 1u);
  EXPECT_EQ(d2_match[0].GetKey()->Str(), "doc:1");

  // Same query at distance 1 should NOT match (needs 2 edits).
  auto d1_miss = text::FuzzySearch::Search(tree, "munchn", /*max_distance=*/1,
                                           /*max_words=*/100);
  EXPECT_EQ(d1_miss.size(), 0u);

  // "mcn" needs 4 edits from "münchen" (delete ü, delete n, delete h, delete
  // e). Distance 2 should NOT match.
  auto d2_miss = text::FuzzySearch::Search(tree, "mcn", /*max_distance=*/2,
                                           /*max_words=*/100);
  EXPECT_EQ(d2_miss.size(), 0u);
}

// ==========================================================================
// Multi-language schema integration: verifies that non-English processors are
// wired correctly through TextIndexSchema (factory, stop words, punctuation,
// stem tree).

class TextMultiLanguageTest : public ::testing::Test {
 protected:
  std::shared_ptr<text::TextIndexSchema> CreateSchema(
      data_model::Language language) {
    return std::make_shared<text::TextIndexSchema>(
        language, text::GetDefaultPunctuation(language), false,
        text::GetDefaultStopWords(language), 4);
  }

  std::unique_ptr<Text> CreateTextIndex(
      std::shared_ptr<text::TextIndexSchema> schema, bool stemming = true) {
    data_model::TextIndex proto;
    proto.set_no_stem(!stemming);
    return std::make_unique<Text>(proto, schema);
  }

  void IndexDocument(Text* text_index,
                     std::shared_ptr<text::TextIndexSchema> schema,
                     const std::string& key_name, absl::string_view data) {
    auto key = StringInternStore::Intern(key_name);
    auto result = text_index->AddRecord(key, data);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_TRUE(result.value());
    schema->CommitKeyData(key);
  }

  bool TokenExists(std::shared_ptr<text::TextIndexSchema> schema,
                   const std::string& token) {
    auto iter = schema->GetTextIndex()->GetPrefix().GetWordIterator(token);
    return !iter.Done();
  }
};

TEST_F(TextMultiLanguageTest, NonEnglishStopWordsFiltered) {
  auto schema = CreateSchema(data_model::LANGUAGE_FRENCH);
  auto text_index = CreateTextIndex(schema);

  IndexDocument(text_index.get(), schema, "doc:1", "je suis dans la maison");

  EXPECT_FALSE(TokenExists(schema, "je"));
  EXPECT_FALSE(TokenExists(schema, "dans"));
  EXPECT_FALSE(TokenExists(schema, "la"));
  EXPECT_TRUE(TokenExists(schema, "maison"));
}

TEST_F(TextMultiLanguageTest, NonAsciiPunctuationSplitting) {
  auto schema = CreateSchema(data_model::LANGUAGE_ARABIC);
  auto text_index = CreateTextIndex(schema, false);

  IndexDocument(text_index.get(), schema, "doc:1", "مرحبا\xD8\x8Cعالم");

  EXPECT_TRUE(TokenExists(schema, "مرحبا"));
  EXPECT_TRUE(TokenExists(schema, "عالم"));
}

TEST_F(TextMultiLanguageTest, StemExpansionNonEnglish) {
  auto schema = CreateSchema(data_model::LANGUAGE_FRENCH);
  schema->SetStemTextFieldMask(1);
  auto text_index = CreateTextIndex(schema);

  IndexDocument(text_index.get(), schema, "doc:1", "continuellement");
  IndexDocument(text_index.get(), schema, "doc:2", "continuelle");

  absl::InlinedVector<absl::string_view, text::kStemVariantsInlineCapacity>
      words;
  schema->GetAllStemVariants("continuellement", words, 1, false);
  EXPECT_GE(words.size(), 1u);
}

TEST_F(TextMultiLanguageTest, DeleteCleansNonEnglishStemTree) {
  auto schema = CreateSchema(data_model::LANGUAGE_FRENCH);
  schema->SetStemTextFieldMask(1);
  auto text_index = CreateTextIndex(schema);

  auto key = StringInternStore::Intern("doc:1");
  {
    auto result = text_index->AddRecord(key, "continuellement");
    ASSERT_TRUE(result.ok());
    schema->CommitKeyData(key);
  }

  EXPECT_TRUE(TokenExists(schema, "continuellement"));

  schema->DeleteKeyData(key);

  EXPECT_FALSE(TokenExists(schema, "continuellement"));
}

}  // namespace valkey_search::indexes
