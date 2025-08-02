/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text.h"
#include "src/indexes/text/lexer.h"

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "gtest/gtest.h"
#include "src/utils/string_interning.h"

namespace valkey_search::text {

class TextFieldIndexTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a text index prototype with various settings
    default_text_index_proto_ = std::make_unique<data_model::TextIndex>();
    
    // Create a text index prototype with suffix tree enabled
    suffix_tree_text_index_proto_ = std::make_unique<data_model::TextIndex>();
    suffix_tree_text_index_proto_->set_suffix_tree(true);
    
    // Create a text index prototype with no stemming
    nostem_text_index_proto_ = std::make_unique<data_model::TextIndex>();
    nostem_text_index_proto_->set_nostem(true);
    
    // Create a text index prototype with custom min stem size
    min_stem_text_index_proto_ = std::make_unique<data_model::TextIndex>();
    min_stem_text_index_proto_->set_min_stem_size(5);  // Different from default
    
    // Create TextFieldIndex instances
    default_text_field_index_ = std::make_unique<TextFieldIndex>(*default_text_index_proto_);
    suffix_tree_text_field_index_ = std::make_unique<TextFieldIndex>(*suffix_tree_text_index_proto_);
    nostem_text_field_index_ = std::make_unique<TextFieldIndex>(*nostem_text_index_proto_);
    min_stem_text_field_index_ = std::make_unique<TextFieldIndex>(*min_stem_text_index_proto_);
  }

  std::unique_ptr<data_model::TextIndex> default_text_index_proto_;
  std::unique_ptr<data_model::TextIndex> suffix_tree_text_index_proto_;
  std::unique_ptr<data_model::TextIndex> nostem_text_index_proto_;
  std::unique_ptr<data_model::TextIndex> min_stem_text_index_proto_;
  
  std::unique_ptr<TextFieldIndex> default_text_field_index_;
  std::unique_ptr<TextFieldIndex> suffix_tree_text_field_index_;
  std::unique_ptr<TextFieldIndex> nostem_text_field_index_;
  std::unique_ptr<TextFieldIndex> min_stem_text_field_index_;
};

TEST_F(TextFieldIndexTest, BasicAddRecord) {
  auto key = valkey_search::StringInternStore::Intern("test_key");
  std::string data = "Hello world, this is a test document.";
  
  // Add record should succeed
  auto result = default_text_field_index_->AddRecord(key, data);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_TRUE(result.value());
}

TEST_F(TextFieldIndexTest, EmptyRecord) {
  auto key = valkey_search::StringInternStore::Intern("empty_key");
  std::string data = "";
  
  // Adding an empty record should still succeed
  auto result = default_text_field_index_->AddRecord(key, data);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_TRUE(result.value());
}

TEST_F(TextFieldIndexTest, InvalidUTF8) {
  auto key = valkey_search::StringInternStore::Intern("invalid_key");
  std::string data = "Hello \xFF\xFE world";  // Invalid UTF-8 sequence
  
  // Adding invalid UTF-8 should fail with an InvalidArgument error
  auto result = default_text_field_index_->AddRecord(key, data);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_F(TextFieldIndexTest, SuffixTreeConfig) {
  auto key = valkey_search::StringInternStore::Intern("test_key");
  std::string data = "Hello world";
  
  // Add record should succeed for both default and suffix tree enabled indices
  auto default_result = default_text_field_index_->AddRecord(key, data);
  auto suffix_result = suffix_tree_text_field_index_->AddRecord(key, data);
  
  ASSERT_TRUE(default_result.ok()) << default_result.status();
  ASSERT_TRUE(suffix_result.ok()) << suffix_result.status();
  
  EXPECT_TRUE(default_result.value());
  EXPECT_TRUE(suffix_result.value());
}

TEST_F(TextFieldIndexTest, NostemConfig) {
  auto key = valkey_search::StringInternStore::Intern("test_key");
  std::string data = "Running jumps working";  // Words that could be stemmed
  
  // Add record should succeed for both default and no stemming indices
  auto default_result = default_text_field_index_->AddRecord(key, data);
  auto nostem_result = nostem_text_field_index_->AddRecord(key, data);
  
  ASSERT_TRUE(default_result.ok()) << default_result.status();
  ASSERT_TRUE(nostem_result.ok()) << nostem_result.status();
  
  EXPECT_TRUE(default_result.value());
  EXPECT_TRUE(nostem_result.value());
}

TEST_F(TextFieldIndexTest, MinStemSizeConfig) {
  auto key = valkey_search::StringInternStore::Intern("test_key");
  std::string data = "run running jump jumping walk walking";
  
  // Add record should succeed for both default and custom min stem size indices
  auto default_result = default_text_field_index_->AddRecord(key, data);
  auto min_stem_result = min_stem_text_field_index_->AddRecord(key, data);
  
  ASSERT_TRUE(default_result.ok()) << default_result.status();
  ASSERT_TRUE(min_stem_result.ok()) << min_stem_result.status();
  
  EXPECT_TRUE(default_result.value());
  EXPECT_TRUE(min_stem_result.value());
}

TEST_F(TextFieldIndexTest, LargeDocument) {
  auto key = valkey_search::StringInternStore::Intern("large_doc_key");
  std::string data(10000, 'a');  // 10KB document of just 'a's
  
  // Large document should still succeed
  auto result = default_text_field_index_->AddRecord(key, data);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_TRUE(result.value());
}

TEST_F(TextFieldIndexTest, UnicodeSupport) {
  auto key = valkey_search::StringInternStore::Intern("unicode_key");
  std::string data = "こんにちは 世界 Привет мир Hello world";
  
  // Unicode text should be handled correctly
  auto result = default_text_field_index_->AddRecord(key, data);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_TRUE(result.value());
}

TEST_F(TextFieldIndexTest, SchemaPunctuationConfig) {
  // Create a text index with custom schema punctuation
  data_model::TextIndex text_index_proto;
  std::string custom_punctuation = "@#$";  // Only these characters will be treated as punctuation
  
  // Create TextFieldIndex with custom schema punctuation
  TextFieldIndex custom_punct_index(text_index_proto, custom_punctuation);
  
  auto key = valkey_search::StringInternStore::Intern("punct_test_key");
  // This string has default punctuation (spaces, commas) and custom punctuation (@#$)
  std::string data = "hello,world this@is#a$test";
  
  // Add record should succeed
  auto result = custom_punct_index.AddRecord(key, data);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_TRUE(result.value());
  
  // TODO: Team implements storage, add assertions to verify:
  // 1. "hello,world" is stored as one token (comma isn't punctuation)
  // 2. "this", "is", "a", "test" are separate tokens (@ # $ are punctuation)
}

}  // namespace valkey_search::text
