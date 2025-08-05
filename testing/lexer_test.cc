/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/lexer.h"

#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "gtest/gtest.h"

namespace valkey_search::text {

class LexerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create schema proto with test values
    data_model::IndexSchema schema_proto;
    schema_proto.set_punctuation(" \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~");
    schema_proto.set_with_offsets(true);
    schema_proto.set_nostem(false);
    schema_proto.set_language(data_model::LANGUAGE_ENGLISH);
    schema_proto.set_min_stem_size(4);
    
    // For stop words, use common English stop words
    // Properly add each stop word to the repeated field
    std::vector<std::string> stop_words = {
      "a", "is", "the", "an", "and", "are", "as", "at", "be", "but", "by", "for",
      "if", "in", "into", "it", "no", "not", "of", "on", "or", "such", "that", "their",
      "then", "there", "these", "they", "this", "to", "was", "will", "with"
    };
    for (const auto& word : stop_words) {
      schema_proto.add_stop_words(word);
    }
    
    // Create lexer with the schema proto
    lexer_ = std::make_unique<Lexer>(schema_proto);
  }

  std::unique_ptr<Lexer> lexer_;
};

TEST_F(LexerTest, BasicTokenization) {
  auto result = lexer_->ProcessString("hello world");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 2);
  
  EXPECT_EQ(words[0].word, "hello");
  EXPECT_EQ(words[0].location.start, 0);
  EXPECT_EQ(words[0].location.end, 5);
  
  EXPECT_EQ(words[1].word, "world");
  EXPECT_EQ(words[1].location.start, 6);
  EXPECT_EQ(words[1].location.end, 11);
}

TEST_F(LexerTest, EmptyStringReturnsNoWords) {
  auto result = lexer_->ProcessString("");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  EXPECT_EQ(words.size(), 0);
}

TEST_F(LexerTest, OnlyPunctuationReturnsNoWords) {
  auto result = lexer_->ProcessString("   \t\n!@#$%^&*()   ");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  EXPECT_EQ(words.size(), 0);
}

TEST_F(LexerTest, DefaultPunctuationHandling) {
  auto result = lexer_->ProcessString("hello,world!this-is_a.test");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 6);
  
  EXPECT_EQ(words[0].word, "hello");
  EXPECT_EQ(words[1].word, "world");
  EXPECT_EQ(words[2].word, "this");
  EXPECT_EQ(words[3].word, "is");
  EXPECT_EQ(words[4].word, "a");
  EXPECT_EQ(words[5].word, "test");
}

TEST_F(LexerTest, CustomPunctuation) {
  ASSERT_TRUE(lexer_->SetPunctuation(" ,").ok());
  
  auto result = lexer_->ProcessString("hello,world!this-is_a.test");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 2);
  
  EXPECT_EQ(words[0].word, "hello");
  EXPECT_EQ(words[1].word, "world!this-is_a.test");
}

TEST_F(LexerTest, EscapeSequences) {
  auto result = lexer_->ProcessString("hello\\,world test\\!ing");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 2);
  
  EXPECT_EQ(words[0].word, "hello,world");
  EXPECT_EQ(words[1].word, "test!ing");
}

TEST_F(LexerTest, EscapeAtEndOfString) {
  auto result = lexer_->ProcessString("hello\\");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 1);
  EXPECT_EQ(words[0].word, "hello");
}

TEST_F(LexerTest, EscapeBackslash) {
  auto result = lexer_->ProcessString("hello\\\\world");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 1);
  EXPECT_EQ(words[0].word, "hello\\world");
}

TEST_F(LexerTest, MultipleEscapesInWord) {
  auto result = lexer_->ProcessString("a\\,b\\!c\\@d");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 1);
  EXPECT_EQ(words[0].word, "a,b!c@d");
}

TEST_F(LexerTest, EscapeNonPunctuation) {
  auto result = lexer_->ProcessString("hello\\aworld");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 1);
  EXPECT_EQ(words[0].word, "helloaworld");
}

TEST_F(LexerTest, UTF8Support) {
  auto result = lexer_->ProcessString("hello 世界 test café");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 4);
  
  EXPECT_EQ(words[0].word, "hello");
  EXPECT_EQ(words[1].word, "世界");
  EXPECT_EQ(words[2].word, "test");
  EXPECT_EQ(words[3].word, "café");
  
  // Check processed terms (with case conversion)
  const auto& terms = result->GetProcessedTerms();
  ASSERT_EQ(terms.size(), 4);
  EXPECT_EQ(terms[0], "hello");
  EXPECT_EQ(terms[1], "世界");  // Non-ASCII should remain unchanged
  EXPECT_EQ(terms[2], "test");
  EXPECT_EQ(terms[3], "café");  // Accented characters should remain unchanged
}

TEST_F(LexerTest, InvalidUTF8) {
  // Invalid UTF-8 sequence
  std::string invalid_utf8 = "hello \xFF\xFE world";
  auto result = lexer_->ProcessString(invalid_utf8);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_F(LexerTest, CaseConversion) {
  // Test with case conversion enabled (default)
  auto result = lexer_->ProcessString("HELLO World miXeD");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 3);
  // Original words should be unchanged
  EXPECT_EQ(words[0].word, "HELLO");
  EXPECT_EQ(words[1].word, "World");
  EXPECT_EQ(words[2].word, "miXeD");
  
  // Processed terms should be lowercase
  const auto& terms = result->GetProcessedTerms();
  ASSERT_EQ(terms.size(), 3);
  EXPECT_EQ(terms[0], "hello");
  EXPECT_EQ(terms[1], "world");
  EXPECT_EQ(terms[2], "mixed");
}

TEST_F(LexerTest, DisableCaseConversion) {
  // Disable case conversion
  lexer_->SetCaseConversionEnabled(false);
  EXPECT_FALSE(lexer_->IsCaseConversionEnabled());
  
  auto result = lexer_->ProcessString("HELLO World miXeD");
  ASSERT_TRUE(result.ok()) << result.status();
  
  // Processed terms should preserve original case
  const auto& terms = result->GetProcessedTerms();
  ASSERT_EQ(terms.size(), 3);
  EXPECT_EQ(terms[0], "HELLO");
  EXPECT_EQ(terms[1], "World");
  EXPECT_EQ(terms[2], "miXeD");
  
  // Re-enable for subsequent tests
  lexer_->SetCaseConversionEnabled(true);
}

TEST_F(LexerTest, PositionTracking) {
  auto result = lexer_->ProcessString("hello world");
  ASSERT_TRUE(result.ok()) << result.status();
  
  // Check that positions are tracked
  EXPECT_TRUE(result->HasPositions());
  
  const auto& positions = result->GetPositions();
  ASSERT_EQ(positions.size(), 2);
  EXPECT_EQ(positions[0], 0);
  EXPECT_EQ(positions[1], 1);
}

TEST_F(LexerTest, LeadingAndTrailingWhitespace) {
  auto result = lexer_->ProcessString("   hello world   ");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 2);
  
  EXPECT_EQ(words[0].word, "hello");
  EXPECT_EQ(words[0].location.start, 3);
  EXPECT_EQ(words[0].location.end, 8);
  
  EXPECT_EQ(words[1].word, "world");
  EXPECT_EQ(words[1].location.start, 9);
  EXPECT_EQ(words[1].location.end, 14);
}

TEST_F(LexerTest, MultipleSpacesBetweenWords) {
  auto result = lexer_->ProcessString("hello    world");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 2);
  
  EXPECT_EQ(words[0].word, "hello");
  EXPECT_EQ(words[1].word, "world");
  EXPECT_EQ(words[1].location.start, 9);
}

TEST_F(LexerTest, GetAndSetPunctuation) {
  // Test default punctuation
  std::string default_punct = lexer_->GetPunctuation();
  EXPECT_FALSE(default_punct.empty());
  
  // Test setting custom punctuation
  ASSERT_TRUE(lexer_->SetPunctuation(".,!?").ok());
  EXPECT_EQ(lexer_->GetPunctuation(), ".,!?");
  
  // Test that setting works
  auto result = lexer_->ProcessString("hello,world.test!now?please");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 5);
  EXPECT_EQ(words[0].word, "hello");
  EXPECT_EQ(words[1].word, "world");
  EXPECT_EQ(words[2].word, "test");
  EXPECT_EQ(words[3].word, "now");
  EXPECT_EQ(words[4].word, "please");
}

TEST_F(LexerTest, FieldSpecificOverrides) {
  // Create a text index with field-specific overrides
  data_model::TextIndex text_index_proto;
  text_index_proto.set_nostem(true);  // Override schema-level setting
  text_index_proto.set_min_stem_size(6);  // Override schema-level setting
  
  // Create schema proto with defaults
  data_model::IndexSchema schema_proto;
  schema_proto.set_punctuation(" \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~");
  schema_proto.set_nostem(false);  // Will be overridden by field
  schema_proto.set_min_stem_size(4);  // Will be overridden by field
  
  // Create lexer with both protos
  Lexer field_lexer(text_index_proto, schema_proto);
  
  // Test that field-specific overrides were applied
  EXPECT_FALSE(field_lexer.IsCaseConversionEnabled());  // nostem=true disables case conversion
  EXPECT_FALSE(field_lexer.IsStemmingEnabled());
  EXPECT_EQ(field_lexer.GetMinStemSize(), 6);
}

TEST_F(LexerTest, NonASCIIPunctuationRejected) {
  auto status = lexer_->SetPunctuation(".,!?🙂");
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
}

TEST_F(LexerTest, SingleCharacterWords) {
  auto result = lexer_->ProcessString("a b c");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 3);
  
  EXPECT_EQ(words[0].word, "a");
  EXPECT_EQ(words[1].word, "b");
  EXPECT_EQ(words[2].word, "c");
}

TEST_F(LexerTest, LongWord) {
  std::string long_word(1000, 'a');
  auto result = lexer_->ProcessString(long_word);
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 1);
  EXPECT_EQ(words[0].word, long_word);
  EXPECT_EQ(words[0].location.start, 0);
  EXPECT_EQ(words[0].location.end, 1000);
}

TEST_F(LexerTest, MixedEscapedAndNormalWords) {
  auto result = lexer_->ProcessString("normal esc\\,aped normal2");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 3);
  
  EXPECT_EQ(words[0].word, "normal");
  EXPECT_EQ(words[1].word, "esc,aped");
  EXPECT_EQ(words[2].word, "normal2");
}

TEST_F(LexerTest, OnlyEscapedCharacters) {
  auto result = lexer_->ProcessString("\\,\\!\\@\\#");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 1);
  EXPECT_EQ(words[0].word, ",!@#");
}

TEST_F(LexerTest, TabsAndNewlines) {
  auto result = lexer_->ProcessString("hello\tworld\ntest");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 3);
  
  EXPECT_EQ(words[0].word, "hello");
  EXPECT_EQ(words[1].word, "world");
  EXPECT_EQ(words[2].word, "test");
}

// Test position tracking with escaped characters
TEST_F(LexerTest, EscapedCharacterPositions) {
  auto result = lexer_->ProcessString("abc\\,def ghi");
  ASSERT_TRUE(result.ok()) << result.status();
  
  const auto& words = result->GetWords();
  ASSERT_EQ(words.size(), 2);
  
  EXPECT_EQ(words[0].word, "abc,def");
  EXPECT_EQ(words[0].location.start, 0);
  EXPECT_EQ(words[0].location.end, 8);  // Position after 'def'
  
  EXPECT_EQ(words[1].word, "ghi");
  EXPECT_EQ(words[1].location.start, 9);
  EXPECT_EQ(words[1].location.end, 12);
}

}  // namespace valkey_search::text
