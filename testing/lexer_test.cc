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
    lexer_ = std::make_unique<Lexer>();
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
}

TEST_F(LexerTest, InvalidUTF8) {
  // Invalid UTF-8 sequence
  std::string invalid_utf8 = "hello \xFF\xFE world";
  auto result = lexer_->ProcessString(invalid_utf8);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
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
