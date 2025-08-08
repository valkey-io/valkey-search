/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/indexes/text/text_index.h"
#include "src/indexes/text.h"

namespace valkey_search {
namespace indexes {
namespace {

class TextIndexSchemaTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(TextIndexSchemaTest, BasicFieldNumbering) {
  // Create a TextIndexSchema instance
  auto text_index_schema = std::make_shared<text::TextIndexSchema>();
  
  // Initially, no text fields allocated
  EXPECT_EQ(text_index_schema->num_text_fields_, 0);

  // Manually allocate field numbers and verify sequential allocation
  EXPECT_EQ(0, text_index_schema->AllocateTextFieldNumber());
  EXPECT_EQ(1, text_index_schema->AllocateTextFieldNumber());
  EXPECT_EQ(2, text_index_schema->AllocateTextFieldNumber());
  EXPECT_EQ(3, text_index_schema->num_text_fields_);
}

TEST_F(TextIndexSchemaTest, TextConstructorAllocation) {
  // Create a TextIndexSchema instance
  auto text_index_schema = std::make_shared<text::TextIndexSchema>();
  
  // Create text indexes and verify they get unique field numbers
  data_model::TextIndex text_proto1;
  auto text_index1 = std::make_shared<Text>(text_proto1, text_index_schema);
  EXPECT_EQ(1, text_index_schema->num_text_fields_);
  
  data_model::TextIndex text_proto2;
  auto text_index2 = std::make_shared<Text>(text_proto2, text_index_schema);
  EXPECT_EQ(2, text_index_schema->num_text_fields_);
  
  data_model::TextIndex text_proto3;
  auto text_index3 = std::make_shared<Text>(text_proto3, text_index_schema);
  EXPECT_EQ(3, text_index_schema->num_text_fields_);
}

}  // namespace
}  // namespace indexes
}  // namespace valkey_search
