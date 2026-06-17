/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/snowball_processor.h"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/language_processor.h"
#include "src/indexes/text/lexer.h"

namespace valkey_search::indexes::text {

class SnowballProcessorTest : public ::testing::Test {
 protected:
  std::shared_ptr<LanguageProcessor> processor_ =
      LanguageProcessor::Create(data_model::LANGUAGE_ENGLISH);
};

// Factory tests

TEST_F(SnowballProcessorTest, FactoryReturnsNonNull) {
  ASSERT_NE(processor_, nullptr);
}

TEST_F(SnowballProcessorTest, SupportsStemming) {
  EXPECT_TRUE(processor_->SupportsStemming());
}

TEST_F(SnowballProcessorTest, DefaultPunctuationNotEmpty) {
  EXPECT_FALSE(processor_->DefaultPunctuation().empty());
}

TEST_F(SnowballProcessorTest, FactoryUnspecifiedLanguageReturnsNonNull) {
  auto processor = LanguageProcessor::Create(data_model::LANGUAGE_UNSPECIFIED);
  EXPECT_NE(processor, nullptr);
}

// StemWordInPlace tests — mirrors Lexer stemming behavior

TEST_F(SnowballProcessorTest, StemWordInPlace_RunningToRun) {
  std::string word = "running";
  processor_->StemWordInPlace(word, 3);
  EXPECT_EQ(word, "run");
}

TEST_F(SnowballProcessorTest, StemWordInPlace_JumpsToJump) {
  std::string word = "jumps";
  processor_->StemWordInPlace(word, 3);
  EXPECT_EQ(word, "jump");
}

TEST_F(SnowballProcessorTest, StemWordInPlace_HappilyToHappili) {
  std::string word = "happily";
  processor_->StemWordInPlace(word, 3);
  EXPECT_EQ(word, "happili");
}

TEST_F(SnowballProcessorTest, StemWordInPlace_MinStemSizePrevents) {
  // From lexer_test: "run running" with min_stem_size=10 prevents stemming
  std::string word = "running";
  processor_->StemWordInPlace(word, 10);
  EXPECT_EQ(word, "running");
}

TEST_F(SnowballProcessorTest, StemWordInPlace_AlreadyStemmed) {
  std::string word = "run";
  processor_->StemWordInPlace(word, 3);
  EXPECT_EQ(word, "run");
}

// BuildStemMap tests — mirrors LexerTest::StemMappingsBasic

TEST_F(SnowballProcessorTest, BuildStemMap_Basic) {
  // From lexer_test: "running jumps happily" with stemming enabled
  std::vector<std::string> tokens = {"running", "jumps", "happily"};
  InProgressStemMap stem_mappings;

  processor_->BuildStemMap(tokens, 3, stem_mappings);

  EXPECT_EQ(stem_mappings.size(), 3);
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

// Mirrors LexerTest::StemMappingsMultipleWordsToSameStem

TEST_F(SnowballProcessorTest, BuildStemMap_MultipleWordsToSameStem) {
  // From lexer_test: "running runs" both map to "run"
  std::vector<std::string> tokens = {"running", "runs"};
  InProgressStemMap stem_mappings;

  processor_->BuildStemMap(tokens, 3, stem_mappings);

  EXPECT_EQ(stem_mappings.size(), 1);
  EXPECT_TRUE(stem_mappings.contains("run"));
  EXPECT_EQ(stem_mappings["run"].size(), 2);
  EXPECT_TRUE(std::find(stem_mappings["run"].begin(),
                        stem_mappings["run"].end(),
                        "running") != stem_mappings["run"].end());
  EXPECT_TRUE(std::find(stem_mappings["run"].begin(),
                        stem_mappings["run"].end(),
                        "runs") != stem_mappings["run"].end());
}

// Mirrors LexerTest::StemMappingsNoStemmingWhenDisabled
// BuildStemMap with min_stem_size larger than all tokens produces no mappings

TEST_F(SnowballProcessorTest, BuildStemMap_MinStemSizePreventsAllMappings) {
  std::vector<std::string> tokens = {"running", "jumps", "happily"};
  InProgressStemMap stem_mappings;

  processor_->BuildStemMap(tokens, 100, stem_mappings);

  EXPECT_TRUE(stem_mappings.empty());
}

}  // namespace valkey_search::indexes::text
