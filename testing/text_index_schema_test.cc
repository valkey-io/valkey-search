/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "gtest/gtest.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text.h"
#include "src/indexes/text/text_index.h"
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
        " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~", false, empty_stop_words);
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

TEST_F(TextIndexSchemaTest, SchemaEnablesIndexing) {
  auto schema = CreateSchema();
  data_model::TextIndex proto;
  Text field(proto, schema);

  EXPECT_EQ(nullptr, schema->stemmer_);

  auto key = StringInternStore::Intern("test_key");
  auto result = field.AddRecord(key, "hello world");

  EXPECT_TRUE(result.ok());
  EXPECT_TRUE(*result);

  EXPECT_NE(nullptr, schema->stemmer_);
  EXPECT_EQ(schema->stemmer_, schema->GetStemmer());  // Reused
}

}  // namespace text
}  // namespace indexes
}  // namespace valkey_search
