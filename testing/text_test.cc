/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text.h"
#include "src/indexes/text/text_index.h"
#include "src/index_schema.pb.h"

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "gtest/gtest.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes {

class TextTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create default text index schema for testing
    std::vector<std::string> empty_stop_words;
    text_index_schema_ = std::make_shared<text::TextIndexSchema>(
        data_model::LANGUAGE_ENGLISH,
        " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",  // Default punctuation
        false,  // with_offsets
        empty_stop_words
    );
    
    // Create default TextIndex prototype
    text_index_proto_ = std::make_unique<data_model::TextIndex>();
    
    // Create Text instance
    text_index_ = std::make_unique<Text>(*text_index_proto_, text_index_schema_);
  }

  std::shared_ptr<text::TextIndexSchema> text_index_schema_;
  std::unique_ptr<data_model::TextIndex> text_index_proto_;
  std::unique_ptr<Text> text_index_;
};

TEST_F(TextTest, BasicAddRecord) {
  auto key = valkey_search::StringInternStore::Intern("test_key");
  std::string data = "Hello world, this is a test document.";
  
  // Add record should succeed
  auto result = text_index_->AddRecord(key, data);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_TRUE(result.value());
}

TEST_F(TextTest, EmptyRecord) {
  auto key = valkey_search::StringInternStore::Intern("empty_key");
  std::string data = "";
  
  // Adding an empty record should still succeed
  auto result = text_index_->AddRecord(key, data);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_TRUE(result.value());
}

TEST_F(TextTest, LargeDocument) {
  auto key = valkey_search::StringInternStore::Intern("large_doc_key");
  std::string data(10000, 'a');  // 10KB document of just 'a's
  
  // Large document should still succeed
  auto result = text_index_->AddRecord(key, data);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_TRUE(result.value());
}

TEST_F(TextTest, UnicodeSupport) {
  auto key = valkey_search::StringInternStore::Intern("unicode_key");
  std::string data = "こんにちは 世界 Привет мир Hello world";
  
  // Unicode text should be handled correctly
  auto result = text_index_->AddRecord(key, data);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_TRUE(result.value());
}

}  // namespace valkey_search::indexes
