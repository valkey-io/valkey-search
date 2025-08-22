/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/lexer.h"
#include "src/indexes/text/text_index.h"
#include "src/index_schema.pb.h"

#include <string>
#include <vector>
#include <memory>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "gtest/gtest.h"

namespace valkey_search::indexes::text {

using Lexer = valkey_search::indexes::text::Lexer;

class LexerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    lexer_ = std::make_unique<Lexer>();
    
    // Create TextIndexSchema to get real bitmap (tests real integration)
    std::vector<std::string> stop_words = {"the", "and", "or"};
    text_schema_ = std::make_shared<TextIndexSchema>(
        data_model::LANGUAGE_ENGLISH,
        " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
        true,
        stop_words
    );

    language_ = "english";
    stemming_enabled_ = true;
    min_stem_size_ = 3;
  }

  std::shared_ptr<TextIndexSchema> text_schema_;

  // Helper method to create a custom TextIndexSchema with specific punctuation
  std::shared_ptr<TextIndexSchema> CreateCustomTextSchema(const std::string& punctuation) {
    std::vector<std::string> stop_words = {"the", "and", "or"};
    return std::make_shared<TextIndexSchema>(
        data_model::LANGUAGE_ENGLISH,
        punctuation,
        true,
        stop_words
    );
  }

  std::unique_ptr<Lexer> lexer_;
  std::string language_;
  bool stemming_enabled_;
  uint32_t min_stem_size_;
};

TEST_F(LexerTest, EmptyStringReturnsNoWords) {
  auto result = lexer_->Tokenize("", text_schema_->GetPunctuationBitmap(), text_schema_->GetStemmer(), stemming_enabled_, min_stem_size_);
  ASSERT_TRUE(result.ok());
  const auto& words = *result;
  EXPECT_TRUE(words.empty());
}

TEST_F(LexerTest, OnlyPunctuationReturnsNoWords) {
  auto result = lexer_->Tokenize("   \t\n!@#$%^&*()   ", text_schema_->GetPunctuationBitmap(), text_schema_->GetStemmer(), stemming_enabled_, min_stem_size_);
  ASSERT_TRUE(result.ok());
  const auto& words = *result;
  EXPECT_TRUE(words.empty());
}

TEST_F(LexerTest, DefaultPunctuationHandling) {
  auto result = lexer_->Tokenize("hello,world!this-is_a.test", text_schema_->GetPunctuationBitmap(), text_schema_->GetStemmer(), stemming_enabled_, min_stem_size_);
  ASSERT_TRUE(result.ok());
  const auto& words = *result;
  EXPECT_EQ(words.size(), 6);
  EXPECT_EQ(words[0], "hello");
  EXPECT_EQ(words[1], "world");
  EXPECT_EQ(words[2], "this");
  EXPECT_EQ(words[3], "is");
  EXPECT_EQ(words[4], "a");
  EXPECT_EQ(words[5], "test");
}

TEST_F(LexerTest, CustomPunctuationHandling) {
  auto custom_schema = CreateCustomTextSchema(" ,");

  auto result = lexer_->Tokenize("hello,world!this-is_a.test", custom_schema->GetPunctuationBitmap(), custom_schema->GetStemmer(), stemming_enabled_, min_stem_size_);
  ASSERT_TRUE(result.ok());
  
  const auto& words = *result;
  EXPECT_EQ(words.size(), 2);
  EXPECT_EQ(words[0], "hello");
  EXPECT_EQ(words[1], "world!this-is_a.test");
}

TEST_F(LexerTest, CaseConversion) {
  auto result = lexer_->Tokenize("HELLO World miXeD", text_schema_->GetPunctuationBitmap(), text_schema_->GetStemmer(), false, min_stem_size_);
  ASSERT_TRUE(result.ok());
  const auto& words = *result;
  ASSERT_EQ(words.size(), 3);
  EXPECT_EQ(words[0], "hello");
  EXPECT_EQ(words[1], "world");
  EXPECT_EQ(words[2], "mixed");
}

TEST_F(LexerTest, UTF8Support) {
  auto result = lexer_->Tokenize("hello 世界 test café", text_schema_->GetPunctuationBitmap(), text_schema_->GetStemmer(), stemming_enabled_, min_stem_size_);
  ASSERT_TRUE(result.ok());
  const auto& words = *result;
  ASSERT_EQ(words.size(), 4);
  EXPECT_EQ(words[0], "hello");
  EXPECT_EQ(words[1], "世界");
  EXPECT_EQ(words[2], "test");
  EXPECT_EQ(words[3], "café");
}

TEST_F(LexerTest, InvalidUTF8) {
  std::string invalid_utf8 = "hello \xFF\xFE world";
  auto result = lexer_->Tokenize(invalid_utf8, text_schema_->GetPunctuationBitmap(), text_schema_->GetStemmer(), stemming_enabled_, min_stem_size_);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(result.status().message(), "Invalid UTF-8");
}

TEST_F(LexerTest, SingleCharacterWords) {
  auto result = lexer_->Tokenize("a b c", text_schema_->GetPunctuationBitmap(), text_schema_->GetStemmer(), stemming_enabled_, min_stem_size_);
  ASSERT_TRUE(result.ok());
  const auto& words = *result;
  ASSERT_EQ(words.size(), 3);
  EXPECT_EQ(words[0], "a");
  EXPECT_EQ(words[1], "b");
  EXPECT_EQ(words[2], "c");
}

TEST_F(LexerTest, LongWord) {
  std::string long_word(1000, 'a');
  auto result = lexer_->Tokenize(long_word, text_schema_->GetPunctuationBitmap(), text_schema_->GetStemmer(), stemming_enabled_, min_stem_size_);
  ASSERT_TRUE(result.ok());
  const auto& words = *result;
  ASSERT_EQ(words.size(), 1);
  EXPECT_EQ(words[0], long_word);
}

TEST_F(LexerTest, TabsAndNewlines) {
  auto result = lexer_->Tokenize("hello\tworld\ntest", text_schema_->GetPunctuationBitmap(), text_schema_->GetStemmer(), stemming_enabled_, min_stem_size_);
  ASSERT_TRUE(result.ok());
  const auto& words = *result;
  ASSERT_EQ(words.size(), 3);
  EXPECT_EQ(words[0], "hello");
  EXPECT_EQ(words[1], "world");
  EXPECT_EQ(words[2], "test");
}

TEST_F(LexerTest, StemmingEnabled) {
  auto result = lexer_->Tokenize("running jumping", text_schema_->GetPunctuationBitmap(), text_schema_->GetStemmer(), true, 3);
  ASSERT_TRUE(result.ok());
  const auto& words = *result;
  ASSERT_EQ(words.size(), 2);
  EXPECT_EQ(words[0], "run");
  EXPECT_EQ(words[1], "jump");
}

TEST_F(LexerTest, StemmingDisabled) {
  auto result = lexer_->Tokenize("running jumping", text_schema_->GetPunctuationBitmap(), text_schema_->GetStemmer(), false, 3);
  ASSERT_TRUE(result.ok());
  const auto& words = *result;
  ASSERT_EQ(words.size(), 2);
  EXPECT_EQ(words[0], "running");
  EXPECT_EQ(words[1], "jumping");
}

TEST_F(LexerTest, MinStemSize) {
  auto result = lexer_->Tokenize("run running", text_schema_->GetPunctuationBitmap(), text_schema_->GetStemmer(), true, 10);
  ASSERT_TRUE(result.ok());
  const auto& words = *result;
  ASSERT_EQ(words.size(), 2);
  EXPECT_EQ(words[0], "run");
  EXPECT_EQ(words[1], "running");
}

TEST_F(LexerTest, NonASCIIPunctuationHandling) {
  auto result = lexer_->Tokenize("hello🙂world", text_schema_->GetPunctuationBitmap(), text_schema_->GetStemmer(), stemming_enabled_, min_stem_size_);
  ASSERT_TRUE(result.ok());
  const auto& words = *result;
  EXPECT_EQ(words.size(), 1);
  EXPECT_EQ(words[0], "hello🙂world");
}

}  // namespace valkey_search::indexes::text
