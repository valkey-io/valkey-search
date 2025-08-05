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
  
  // Create a schema with custom punctuation
  auto schema_proto = std::make_unique<data_model::IndexSchema>();
  schema_proto->set_punctuation("@#$");  // Only these characters will be treated as punctuation
  
  // Create TextFieldIndex with custom schema
  TextFieldIndex custom_punct_index(text_index_proto, schema_proto.get());
  
  auto key = StringInternStore::Intern("punct_test_key");
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

TEST_F(TextFieldIndexTest, CaseConversionEnabled) {
  auto key = valkey_search::StringInternStore::Intern("case_test_key");
  std::string data = "HELLO World miXeD";
  
  // Default TextFieldIndex should have case conversion enabled
  auto result = default_text_field_index_->AddRecord(key, data);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_TRUE(result.value());
  
  // With our enhanced Lexer, words should be processed to lowercase
  // TODO: When storage is implemented, verify terms are stored as lowercase
}

TEST_F(TextFieldIndexTest, CaseConversionDisabled) {
  auto key = valkey_search::StringInternStore::Intern("case_test_key");
  std::string data = "HELLO World miXeD";
  
  // nostem_text_field_index_ should have case conversion disabled
  auto result = nostem_text_field_index_->AddRecord(key, data);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_TRUE(result.value());
  
  // With nostem, case conversion should be disabled
  // TODO: When storage is implemented, verify terms are stored with original case
}

TEST_F(TextFieldIndexTest, MultipleFieldsWithDifferentConfigs) {
  // Create an index with two text fields having different configurations
  data_model::TextIndex field1_proto;
  field1_proto.set_suffix_tree(true);  // First field uses suffix tree
  field1_proto.set_nostem(false);      // First field uses stemming

  data_model::TextIndex field2_proto;
  field2_proto.set_suffix_tree(false); // Second field doesn't use suffix tree
  field2_proto.set_nostem(true);       // Second field has stemming disabled

  // Create a common schema
  auto schema_proto = std::make_unique<data_model::IndexSchema>();
  schema_proto->set_with_offsets(true);
  schema_proto->set_punctuation(",. ");  // Common punctuation

  // Create TextFieldIndex instances for each field
  TextFieldIndex field1_index(field1_proto, schema_proto.get());
  TextFieldIndex field2_index(field2_proto, schema_proto.get());

  // Test with same content for both fields
  auto key1 = StringInternStore::Intern("key1");
  auto key2 = StringInternStore::Intern("key2");
  std::string data = "Running,Walking Quickly";

  // Add records to both fields
  auto result1 = field1_index.AddRecord(key1, data);
  auto result2 = field2_index.AddRecord(key2, data);

  ASSERT_TRUE(result1.ok()) << result1.status();
  ASSERT_TRUE(result2.ok()) << result2.status();
  EXPECT_TRUE(result1.value());
  EXPECT_TRUE(result2.value());

  // TODO: Once storage is implemented, verify:
  // 1. field1 would stem "Running" to "run" and store in suffix tree
  // 2. field2 would keep "Running" as is and not use suffix tree
  // 3. Both fields would handle punctuation consistently
}

TEST_F(TextFieldIndexTest, FieldIdentifierCorrectlyStored) {
  // Create text index prototypes
  data_model::TextIndex text_index_proto;
  
  // Create schema prototypes with different names
  auto title_schema_proto = std::make_unique<data_model::IndexSchema>();
  title_schema_proto->set_name("title_field");
  
  auto desc_schema_proto = std::make_unique<data_model::IndexSchema>();
  desc_schema_proto->set_name("description_field");
  
  // Create TextFieldIndex with field identifiers passed through constructor
  TextFieldIndex title_index(text_index_proto, title_schema_proto.get(), "title_field");
  TextFieldIndex desc_index(text_index_proto, desc_schema_proto.get(), "description_field");
  TextFieldIndex no_id_index(text_index_proto);
  
  // Verify field identifiers are correctly stored
  EXPECT_EQ(title_index.GetFieldIdentifier(), "title_field");
  EXPECT_EQ(desc_index.GetFieldIdentifier(), "description_field");
  EXPECT_EQ(no_id_index.GetFieldIdentifier(), ""); // Empty for default constructor
  
  // Test basic functionality with field identifiers
  auto key = StringInternStore::Intern("test_key");
  EXPECT_TRUE(title_index.AddRecord(key, "test content").ok());
  EXPECT_TRUE(desc_index.AddRecord(key, "test description").ok());
}

TEST_F(TextFieldIndexTest, LanguageAndFieldPropagation) {
  // Create text index prototypes
  data_model::TextIndex text_index_proto;
  
  // Create schema prototypes with language and field name
  auto schema_proto = std::make_unique<data_model::IndexSchema>();
  schema_proto->set_name("content_field");
  schema_proto->set_language(data_model::LANGUAGE_ENGLISH);
  
  // Create another schema with different language
  auto schema_proto2 = std::make_unique<data_model::IndexSchema>();
  schema_proto2->set_name("tag_field");
  schema_proto2->set_language(data_model::LANGUAGE_ENGLISH); // Future: use different language once supported
  schema_proto2->set_nostem(true); // Disable stemming
  
  // Create TextFieldIndex instances with field identifiers
  TextFieldIndex content_index(text_index_proto, schema_proto.get(), "content_field");
  TextFieldIndex tag_index(text_index_proto, schema_proto2.get(), "tag_field");
  
  // Verify field identifiers and language settings propagated
  EXPECT_EQ(content_index.GetFieldIdentifier(), "content_field");
  EXPECT_EQ(tag_index.GetFieldIdentifier(), "tag_field");
  
  // Basic functionality with different language settings
  auto key = StringInternStore::Intern("test_key");
  auto content_result = content_index.AddRecord(key, "running jumping swimming");
  auto tag_result = tag_index.AddRecord(key, "run jump swim");
  
  ASSERT_TRUE(content_result.ok());
  ASSERT_TRUE(tag_result.ok());
  EXPECT_TRUE(content_result.value());
  EXPECT_TRUE(tag_result.value());
  
  // Future: Add assertions for language-specific stemming once implemented
}

}  // namespace valkey_search::text
