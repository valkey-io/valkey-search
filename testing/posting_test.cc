/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/posting.h"

#include "gtest/gtest.h"

namespace valkey_search::text {

class PostingTest : public testing::Test {
 protected:
  void SetUp() override {
    // Create postings with different configurations for testing
    boolean_postings_ = new Postings(false, 3);  // Boolean search, 3 fields
    positional_postings_ = new Postings(true, 5); // Positional search, 5 fields
  }
  
  void TearDown() override {
    delete boolean_postings_;
    delete positional_postings_;
  }
  
  Postings* boolean_postings_;
  Postings* positional_postings_;
};

// Test FieldMask factory and basic operations
TEST_F(PostingTest, FieldMaskFactory) {
  // Test single field mask
  auto mask1 = FieldMask::Create(1);
  EXPECT_EQ(mask1->MaxFields(), 1);
  
  // Test byte field mask
  auto mask8 = FieldMask::Create(8);
  EXPECT_EQ(mask8->MaxFields(), 8);
  
  // Test uint64 field mask
  auto mask64 = FieldMask::Create(64);
  EXPECT_EQ(mask64->MaxFields(), 64);
  
  // Test error cases
  EXPECT_THROW(FieldMask::Create(0), std::invalid_argument);
  EXPECT_THROW(FieldMask::Create(65), std::invalid_argument);
}

TEST_F(PostingTest, FieldMaskBasicOperations) {
  auto mask = FieldMask::Create(5);
  
  // Initially no fields set
  EXPECT_EQ(mask->CountSetFields(), 0);
  EXPECT_FALSE(mask->HasField(0));
  EXPECT_FALSE(mask->HasField(4));
  
  // Set some fields
  mask->SetField(0);
  mask->SetField(2);
  mask->SetField(4);
  
  EXPECT_EQ(mask->CountSetFields(), 3);
  EXPECT_TRUE(mask->HasField(0));
  EXPECT_FALSE(mask->HasField(1));
  EXPECT_TRUE(mask->HasField(2));
  EXPECT_FALSE(mask->HasField(3));
  EXPECT_TRUE(mask->HasField(4));
  
  // Clear a field
  mask->ClearField(2);
  EXPECT_EQ(mask->CountSetFields(), 2);
  EXPECT_FALSE(mask->HasField(2));
  
  // Set all fields
  mask->SetAllFields();
  EXPECT_EQ(mask->CountSetFields(), 5);
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_TRUE(mask->HasField(i));
  }
  
  // Clear all fields
  mask->ClearAllFields();
  EXPECT_EQ(mask->CountSetFields(), 0);
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_FALSE(mask->HasField(i));
  }
}

TEST_F(PostingTest, FieldMaskClone) {
  auto original = FieldMask::Create(3);
  original->SetField(0);
  original->SetField(2);
  
  auto clone = original->Clone();
  EXPECT_EQ(clone->CountSetFields(), 2);
  EXPECT_TRUE(clone->HasField(0));
  EXPECT_FALSE(clone->HasField(1));
  EXPECT_TRUE(clone->HasField(2));
  
  // Verify independence
  original->SetField(1);
  EXPECT_FALSE(clone->HasField(1));
}

TEST_F(PostingTest, PostingEmptyOperations) {
  EXPECT_TRUE(boolean_postings_->IsEmpty());
  EXPECT_EQ(boolean_postings_->GetKeyCount(), 0);
  EXPECT_EQ(boolean_postings_->GetPostingCount(), 0);
  EXPECT_EQ(boolean_postings_->GetTotalTermFrequency(), 0);
}

TEST_F(PostingTest, BooleanSearchSetKey) {
  // Test valid boolean search mode
  boolean_postings_->SetKey("doc1");
  boolean_postings_->SetKey("doc2");
  
  EXPECT_FALSE(boolean_postings_->IsEmpty());
  EXPECT_EQ(boolean_postings_->GetKeyCount(), 2);
  EXPECT_EQ(boolean_postings_->GetPostingCount(), 2); // One position per key
  
  // Test error when using SetKey in positional mode
  EXPECT_THROW(positional_postings_->SetKey("doc1"), std::invalid_argument);
}

TEST_F(PostingTest, AddPositionForField) {
  // Add positions for different documents and fields
  positional_postings_->AddPositionForField("doc1", 10, 0); // field 0, position 10
  positional_postings_->AddPositionForField("doc1", 20, 1); // field 1, position 20
  positional_postings_->AddPositionForField("doc1", 10, 2); // field 2, position 10 (same position, different field)
  
  EXPECT_EQ(positional_postings_->GetKeyCount(), 1);
  EXPECT_EQ(positional_postings_->GetPostingCount(), 2); // Two unique positions (10, 20)
  EXPECT_EQ(positional_postings_->GetTotalTermFrequency(), 3); // Three field occurrences total
  
  // Add second document
  positional_postings_->AddPositionForField("doc2", 5, 0);
  positional_postings_->AddPositionForField("doc2", 15, 0);
  
  EXPECT_EQ(positional_postings_->GetKeyCount(), 2);
  EXPECT_EQ(positional_postings_->GetPostingCount(), 4); // Two positions per document
  EXPECT_EQ(positional_postings_->GetTotalTermFrequency(), 5); // Five field occurrences total
  
  // Test field index bounds
  EXPECT_THROW(positional_postings_->AddPositionForField("doc3", 1, 5), std::out_of_range);
}

TEST_F(PostingTest, SetKeyWithFieldPositions) {
  // Test batch position setting (replace mode)
  std::vector<std::pair<Position, size_t>> positions = {
    {10, 0}, {10, 1}, {20, 2}, {30, 0}
  };
  
  positional_postings_->SetKeyWithFieldPositions("doc1", positions);
  
  EXPECT_EQ(positional_postings_->GetKeyCount(), 1);
  EXPECT_EQ(positional_postings_->GetPostingCount(), 3); // Three unique positions (10, 20, 30)
  EXPECT_EQ(positional_postings_->GetTotalTermFrequency(), 4); // Four field occurrences
  
  // Test replace behavior - should clear previous positions
  std::vector<std::pair<Position, size_t>> new_positions = {{5, 1}};
  positional_postings_->SetKeyWithFieldPositions("doc1", positions);
  positional_postings_->SetKeyWithFieldPositions("doc1", new_positions);
  
  EXPECT_EQ(positional_postings_->GetPostingCount(), 1); // Only one position remains
  EXPECT_EQ(positional_postings_->GetTotalTermFrequency(), 1); // Only one field occurrence
}

TEST_F(PostingTest, UpdateKeyWithFieldPositions) {
  // Set initial positions
  std::vector<std::pair<Position, size_t>> initial_positions = {{10, 0}, {20, 1}};
  positional_postings_->SetKeyWithFieldPositions("doc1", initial_positions);
  
  EXPECT_EQ(positional_postings_->GetPostingCount(), 2);
  EXPECT_EQ(positional_postings_->GetTotalTermFrequency(), 2);
  
  // Update with additional positions (merge mode)
  std::vector<std::pair<Position, size_t>> additional_positions = {{30, 2}, {10, 1}};
  positional_postings_->UpdateKeyWithFieldPositions("doc1", additional_positions);
  
  EXPECT_EQ(positional_postings_->GetPostingCount(), 3); // Three unique positions (10, 20, 30)
  EXPECT_EQ(positional_postings_->GetTotalTermFrequency(), 4); // position 10 now has 2 fields, others have 1 each
}

TEST_F(PostingTest, RemoveKey) {
  // Add some data
  positional_postings_->AddPositionForField("doc1", 10, 0);
  positional_postings_->AddPositionForField("doc2", 20, 1);
  
  EXPECT_EQ(positional_postings_->GetKeyCount(), 2);
  
  // Remove one key
  positional_postings_->RemoveKey("doc1");
  EXPECT_EQ(positional_postings_->GetKeyCount(), 1);
  EXPECT_EQ(positional_postings_->GetPostingCount(), 1);
  
  // Remove non-existent key (should be no-op)
  positional_postings_->RemoveKey("nonexistent");
  EXPECT_EQ(positional_postings_->GetKeyCount(), 1);
  
  // Remove last key
  positional_postings_->RemoveKey("doc2");
  EXPECT_TRUE(positional_postings_->IsEmpty());
}

TEST_F(PostingTest, CopyConstructorAndAssignment) {
  // Set up original posting
  positional_postings_->AddPositionForField("doc1", 10, 0);
  positional_postings_->AddPositionForField("doc1", 20, 1);
  
  // Test copy constructor
  Postings copy_constructed(*positional_postings_);
  EXPECT_EQ(copy_constructed.GetKeyCount(), 1);
  EXPECT_EQ(copy_constructed.GetPostingCount(), 2);
  EXPECT_EQ(copy_constructed.GetTotalTermFrequency(), 2);
  
  // Test independence
  positional_postings_->AddPositionForField("doc2", 30, 2);
  EXPECT_EQ(copy_constructed.GetKeyCount(), 1); // Should not change
  EXPECT_EQ(positional_postings_->GetKeyCount(), 2); // Original should change
  
  // Test copy assignment
  Postings copy_assigned(false, 3);
  copy_assigned = *positional_postings_;
  EXPECT_EQ(copy_assigned.GetKeyCount(), 2);
  EXPECT_EQ(copy_assigned.GetPostingCount(), 3);
  
  // Test self-assignment
  copy_assigned = copy_assigned;
  EXPECT_EQ(copy_assigned.GetKeyCount(), 2);
}

TEST_F(PostingTest, ErrorHandling) {
  // Test field index validation
  EXPECT_THROW(positional_postings_->AddPositionForField("doc1", 1, 5), std::out_of_range);
  EXPECT_THROW(positional_postings_->AddPositionForField("doc1", 1, SIZE_MAX), std::out_of_range);
  
  // Test batch operations field validation
  std::vector<std::pair<Position, size_t>> invalid_positions = {{10, 10}};
  EXPECT_THROW(positional_postings_->SetKeyWithFieldPositions("doc1", invalid_positions), std::out_of_range);
  EXPECT_THROW(positional_postings_->UpdateKeyWithFieldPositions("doc1", invalid_positions), std::out_of_range);
  
  // Test FieldMask bounds
  auto mask = FieldMask::Create(3);
  EXPECT_THROW(mask->SetField(3), std::out_of_range);
  EXPECT_THROW(mask->ClearField(3), std::out_of_range);
  EXPECT_FALSE(mask->HasField(3)); // Out of range should return false, not throw
}

TEST_F(PostingTest, LargeScaleOperations) {
  // Test with many documents and positions
  for (int doc = 0; doc < 100; ++doc) {
    std::string key = "doc" + std::to_string(doc);
    for (int pos = 0; pos < 10; ++pos) {
      positional_postings_->AddPositionForField(key, pos * 10, pos % 5);
    }
  }
  
  EXPECT_EQ(positional_postings_->GetKeyCount(), 100);
  EXPECT_EQ(positional_postings_->GetPostingCount(), 1000); // 100 docs * 10 positions each
  EXPECT_EQ(positional_postings_->GetTotalTermFrequency(), 1000); // One field per position
}

}  // namespace valkey_search::text
