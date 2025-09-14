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
#include "testing/common.h"

namespace valkey_search {
namespace indexes {
namespace text {

class TextIndexSchemaTest : public ::testing::Test {
 protected:
  std::shared_ptr<TextIndexSchema> CreateSchema() {
    std::vector<std::string> empty_stop_words;
    auto proto = CreateIndexSchemaProtoWithTextProperties(
        data_model::LANGUAGE_ENGLISH,
        " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
        false,
        empty_stop_words);
    return std::make_shared<TextIndexSchema>(proto);
  }
};

TEST_F(TextIndexSchemaTest, FieldAllocationAcrossMultipleTexts) {
  // Test that TextIndexSchema properly manages field number allocation
  // across multiple Text instances - this is the key schema responsibility
  auto schema = CreateSchema();

  // Initial state - no fields allocated
  EXPECT_EQ(0, schema->GetNumTextFields());

  // Create multiple Text instances sharing the same schema
  data_model::TextIndex proto;

  auto text1 = std::make_shared<Text>(proto, schema);
  EXPECT_EQ(1, schema->GetNumTextFields());

  auto text2 = std::make_shared<Text>(proto, schema);
  EXPECT_EQ(2, schema->GetNumTextFields());

  auto text3 = std::make_shared<Text>(proto, schema);
  EXPECT_EQ(3, schema->GetNumTextFields());

  // Schema correctly tracks field allocation for posting list identification
}

}  // namespace text
}  // namespace indexes
}  // namespace valkey_search
