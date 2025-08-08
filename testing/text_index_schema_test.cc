/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/indexes/text.h"
#include "src/indexes/text/text_index.h"

namespace valkey_search {
namespace indexes {
namespace text {

class TextIndexSchemaTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(TextIndexSchemaTest, TextConstructorAllocation) {
  // Create a TextIndexSchema instance
  auto text_index_schema = std::make_shared<text::TextIndexSchema>();

  // Create text indexes and verify the number of text fields is incremented in
  // the text index schema
  data_model::TextIndex text_proto;
  std::make_shared<Text>(text_proto, text_index_schema);
  EXPECT_EQ(1, text_index_schema->num_text_fields_);
  std::make_shared<Text>(text_proto, text_index_schema);
  EXPECT_EQ(2, text_index_schema->num_text_fields_);
  std::make_shared<Text>(text_proto, text_index_schema);
  EXPECT_EQ(3, text_index_schema->num_text_fields_);
}

}  // namespace text
}  // namespace indexes
}  // namespace valkey_search
