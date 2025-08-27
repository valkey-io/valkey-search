/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "gtest/gtest.h"
#include "src/indexes/text.h"
#include "src/indexes/text/text_index.h"
#include "src/index_schema.pb.h"
#include "src/utils/string_interning.h"

namespace valkey_search {
namespace indexes {
namespace text {

class TextIndexSchemaTest : public ::testing::Test {
 protected:
  std::shared_ptr<TextIndexSchema> CreateSchema() {
    std::vector<std::string> empty_stop_words;
    return std::make_shared<TextIndexSchema>(
        data_model::LANGUAGE_ENGLISH,
        " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
        false,
        empty_stop_words
    );
  }
};

TEST_F(TextIndexSchemaTest, FieldAllocationAcrossMultipleTexts) {
  // Test that TextIndexSchema properly manages field number allocation
  // across multiple Text instances - this is the key schema responsibility
  auto schema = CreateSchema();
  
  // Initial state - no fields allocated
  EXPECT_EQ(0, schema->num_text_fields_);
  
  // Create multiple Text instances sharing the same schema
  data_model::TextIndex proto;
  
  auto text1 = std::make_shared<Text>(proto, schema);
  EXPECT_EQ(1, schema->num_text_fields_);
  
  auto text2 = std::make_shared<Text>(proto, schema);
  EXPECT_EQ(2, schema->num_text_fields_);
  
  auto text3 = std::make_shared<Text>(proto, schema);
  EXPECT_EQ(3, schema->num_text_fields_);
  
  // Schema correctly tracks field allocation for posting list identification
}

TEST_F(TextIndexSchemaTest, TextIndexSchemaInitialization) {
  // Test with custom configuration including stop words
  std::vector<std::string> stop_words = {"the", "and", "or", "in", "on"};
  auto custom_schema = std::make_shared<TextIndexSchema>(
      data_model::LANGUAGE_ENGLISH,
      " \t\n.,!?;:", // custom punctuation
      true,          // stemming enabled
      stop_words
  );
  
  // Verify field allocation starts at zero
  EXPECT_EQ(0, custom_schema->num_text_fields_);
  
  // Verify punctuation bitmap is properly set
  const auto& punctuation_bitmap = custom_schema->GetPunctuationBitmap();
  EXPECT_GT(punctuation_bitmap.count(), 0); // At least some punctuation characters are set
  
  // Verify stop words are properly configured
  const auto& stop_set = custom_schema->GetStopWordsSet();
  EXPECT_EQ(5, stop_set.size());
  EXPECT_TRUE(stop_set.count("the"));
  EXPECT_TRUE(stop_set.count("and"));
  EXPECT_TRUE(stop_set.count("or"));
  EXPECT_TRUE(stop_set.count("in"));
  EXPECT_TRUE(stop_set.count("on"));
  EXPECT_FALSE(stop_set.count("hello"));
  
  // Verify stemmer lazy initialization and reuse
  auto stemmer1 = custom_schema->GetStemmer();
  EXPECT_NE(nullptr, stemmer1);
  
  auto stemmer2 = custom_schema->GetStemmer();
  EXPECT_EQ(stemmer1, stemmer2); // Same instance reused
  
  // Test empty stop words configuration
  std::vector<std::string> no_stop_words;
  auto empty_schema = std::make_shared<TextIndexSchema>(
      data_model::LANGUAGE_ENGLISH,
      " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
      true,
      no_stop_words
  );
  
  EXPECT_TRUE(empty_schema->GetStopWordsSet().empty());
  EXPECT_EQ(0, empty_schema->num_text_fields_);
}

}  // namespace text
}  // namespace indexes
}  // namespace valkey_search
