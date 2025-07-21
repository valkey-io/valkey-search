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

TEST_F(PostingTest, PostingEmptyOperations) {
  EXPECT_TRUE(boolean_postings_->IsEmpty());
  EXPECT_EQ(boolean_postings_->GetKeyCount(), 0);
  EXPECT_EQ(boolean_postings_->GetPostingCount(), 0);
  EXPECT_EQ(boolean_postings_->GetTotalTermFrequency(), 0);
}

TEST_F(PostingTest, BooleanSearchInsertPosting) {
  // Test boolean search mode - positions are ignored
  boolean_postings_->InsertPosting("doc1", 0);        // field 0, position ignored
  boolean_postings_->InsertPosting("doc1", 1, 100);   // field 1, position 100 ignored
  boolean_postings_->InsertPosting("doc2", 2);        // field 2, position ignored
  
  EXPECT_FALSE(boolean_postings_->IsEmpty());
  EXPECT_EQ(boolean_postings_->GetKeyCount(), 2);
  EXPECT_EQ(boolean_postings_->GetPostingCount(), 2); // One position per key (always position 0)
  EXPECT_EQ(boolean_postings_->GetTotalTermFrequency(), 3); // Three field occurrences total
}

TEST_F(PostingTest, PositionalSearchInsertPosting) {
  // Test positional search mode - positions are respected
  positional_postings_->InsertPosting("doc1", 0, 10); // field 0, position 10
  positional_postings_->InsertPosting("doc1", 1, 20); // field 1, position 20
  positional_postings_->InsertPosting("doc1", 2, 10); // field 2, position 10 (same position, different field)
  
  EXPECT_EQ(positional_postings_->GetKeyCount(), 1);
  EXPECT_EQ(positional_postings_->GetPostingCount(), 2); // Two unique positions (10, 20)
  EXPECT_EQ(positional_postings_->GetTotalTermFrequency(), 3); // Three field occurrences total
  
  // Add second document
  positional_postings_->InsertPosting("doc2", 0, 5);
  positional_postings_->InsertPosting("doc2", 0, 15);
  
  EXPECT_EQ(positional_postings_->GetKeyCount(), 2);
  EXPECT_EQ(positional_postings_->GetPostingCount(), 4); // Two positions per document
  EXPECT_EQ(positional_postings_->GetTotalTermFrequency(), 5); // Five field occurrences total
}

TEST_F(PostingTest, InsertPostingDefaultPosition) {
  // Test that default position works correctly for boolean postings
  boolean_postings_->InsertPosting("doc1", 0); // Default position ignored in boolean mode
  boolean_postings_->InsertPosting("doc1", 1); // Default position ignored in boolean mode
  
  EXPECT_EQ(boolean_postings_->GetKeyCount(), 1);
  EXPECT_EQ(boolean_postings_->GetPostingCount(), 1); // Only one position (0)
  EXPECT_EQ(boolean_postings_->GetTotalTermFrequency(), 2); // Two field occurrences at position 0
}

TEST_F(PostingTest, InsertPostingFieldValidation) {
  // Test field index bounds
  EXPECT_THROW(positional_postings_->InsertPosting("doc1", 5, 10), std::out_of_range);
  EXPECT_THROW(boolean_postings_->InsertPosting("doc1", 3), std::out_of_range);
}


TEST_F(PostingTest, RemoveKey) {
  // Add some data
  positional_postings_->InsertPosting("doc1", 0, 10);
  positional_postings_->InsertPosting("doc2", 1, 20);
  
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
  positional_postings_->InsertPosting("doc1", 0, 10);
  positional_postings_->InsertPosting("doc1", 1, 20);
  
  // Test copy constructor
  Postings copy_constructed(*positional_postings_);
  EXPECT_EQ(copy_constructed.GetKeyCount(), 1);
  EXPECT_EQ(copy_constructed.GetPostingCount(), 2);
  EXPECT_EQ(copy_constructed.GetTotalTermFrequency(), 2);
  
  // Test independence
  positional_postings_->InsertPosting("doc2", 2, 30);
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
  EXPECT_THROW(positional_postings_->InsertPosting("doc1", 5, 1), std::out_of_range);
  EXPECT_THROW(positional_postings_->InsertPosting("doc1", SIZE_MAX, 1), std::out_of_range);
  
  // Test position validation in positional mode
  EXPECT_THROW(positional_postings_->InsertPosting("doc1", 0), std::invalid_argument);
}

TEST_F(PostingTest, LargeScaleOperations) {
  // Test with many documents and positions
  for (int doc = 0; doc < 100; ++doc) {
    std::string key = "doc" + std::to_string(doc);
    for (int pos = 0; pos < 10; ++pos) {
      positional_postings_->InsertPosting(key, pos % 5, pos * 10);
    }
  }
  
  EXPECT_EQ(positional_postings_->GetKeyCount(), 100);
  EXPECT_EQ(positional_postings_->GetPostingCount(), 1000); // 100 docs * 10 positions each
  EXPECT_EQ(positional_postings_->GetTotalTermFrequency(), 1000); // One field per position
}

TEST_F(PostingTest, SingleFieldOptimization) {
  // Test posting with single field (uses SingleFieldMask optimization internally)
  Postings single_field_posting(true, 1);  // 1 field only
  
  // Add some postings - all will use field 0
  single_field_posting.InsertPosting("doc1", 0, 10);
  single_field_posting.InsertPosting("doc1", 0, 20);
  single_field_posting.InsertPosting("doc2", 0, 5);
  
  // Verify posting works correctly with single field optimization
  EXPECT_EQ(single_field_posting.GetKeyCount(), 2);
  EXPECT_EQ(single_field_posting.GetPostingCount(), 3);
  EXPECT_EQ(single_field_posting.GetTotalTermFrequency(), 3);
  
  // Test that field index validation still works
  EXPECT_THROW(single_field_posting.InsertPosting("doc3", 1, 1), std::out_of_range);
}

TEST_F(PostingTest, BooleanVsPositionalBehavior) {
  // Test that boolean and positional modes behave differently for positions
  
  // Boolean mode: positions ignored, all stored at position 0
  boolean_postings_->InsertPosting("doc1", 0, 100);  // position 100 ignored
  boolean_postings_->InsertPosting("doc1", 1, 200);  // position 200 ignored
  boolean_postings_->InsertPosting("doc1", 2, 300);  // position 300 ignored
  
  EXPECT_EQ(boolean_postings_->GetPostingCount(), 1); // All at position 0
  EXPECT_EQ(boolean_postings_->GetTotalTermFrequency(), 3); // Three fields
  
  // Positional mode: positions respected
  positional_postings_->InsertPosting("doc1", 0, 100);
  positional_postings_->InsertPosting("doc1", 1, 200);
  positional_postings_->InsertPosting("doc1", 2, 300);
  
  EXPECT_EQ(positional_postings_->GetPostingCount(), 3); // Three different positions
  EXPECT_EQ(positional_postings_->GetTotalTermFrequency(), 3); // Three fields
}

TEST_F(PostingTest, MultipleInsertPostingCalls) {
  // Test multiple InsertPosting calls on same document
  positional_postings_->InsertPosting("doc1", 0, 10);
  positional_postings_->InsertPosting("doc1", 1, 20);
  positional_postings_->InsertPosting("doc1", 2, 30);
  positional_postings_->InsertPosting("doc1", 1, 10); // Add field 1 to position 10
  
  EXPECT_EQ(positional_postings_->GetPostingCount(), 3); // Three unique positions (10, 20, 30)
  EXPECT_EQ(positional_postings_->GetTotalTermFrequency(), 4); // Four field occurrences
}

TEST_F(PostingTest, KeyIteratorBasic) {
  // Add some test data
  positional_postings_->InsertPosting("doc1", 0, 10);
  positional_postings_->InsertPosting("doc2", 1, 20);
  positional_postings_->InsertPosting("doc3", 2, 30);
  
  // Test key iteration
  auto key_iter = positional_postings_->GetKeyIterator();
  
  EXPECT_TRUE(key_iter.IsValid());
  EXPECT_EQ(key_iter.GetKey(), "doc1");  // Keys should be in sorted order
  
  key_iter.NextKey();
  EXPECT_TRUE(key_iter.IsValid());
  EXPECT_EQ(key_iter.GetKey(), "doc2");
  
  key_iter.NextKey();
  EXPECT_TRUE(key_iter.IsValid());
  EXPECT_EQ(key_iter.GetKey(), "doc3");
  
  key_iter.NextKey();
  EXPECT_FALSE(key_iter.IsValid());  // End of iteration
}

TEST_F(PostingTest, KeyIteratorSkipForward) {
  // Add test data
  positional_postings_->InsertPosting("doc1", 0, 10);
  positional_postings_->InsertPosting("doc3", 1, 20);
  positional_postings_->InsertPosting("doc5", 2, 30);
  
  auto key_iter = positional_postings_->GetKeyIterator();
  
  // Skip to exact match
  EXPECT_TRUE(key_iter.SkipForwardKey("doc3"));
  EXPECT_TRUE(key_iter.IsValid());
  EXPECT_EQ(key_iter.GetKey(), "doc3");
  
  // Skip to non-existent key (should land on next greater key)
  EXPECT_FALSE(key_iter.SkipForwardKey("doc4"));
  EXPECT_TRUE(key_iter.IsValid());
  EXPECT_EQ(key_iter.GetKey(), "doc5");
  
  // Skip beyond all keys
  EXPECT_FALSE(key_iter.SkipForwardKey("doc9"));
  EXPECT_FALSE(key_iter.IsValid());
}

TEST_F(PostingTest, PositionIteratorBasic) {
  // Add test data with multiple positions for one key
  positional_postings_->InsertPosting("doc1", 0, 10);
  positional_postings_->InsertPosting("doc1", 1, 20);
  positional_postings_->InsertPosting("doc1", 2, 30);
  
  // Get key iterator and position iterator
  auto key_iter = positional_postings_->GetKeyIterator();
  EXPECT_TRUE(key_iter.IsValid());
  EXPECT_EQ(key_iter.GetKey(), "doc1");
  
  auto pos_iter = key_iter.GetPositionIterator();
  
  // Test position iteration
  EXPECT_TRUE(pos_iter.IsValid());
  EXPECT_EQ(pos_iter.GetPosition(), 10);
  EXPECT_EQ(pos_iter.GetFieldMask(), 1ULL);  // Field 0 set (bit 0)
  
  pos_iter.NextPosition();
  EXPECT_TRUE(pos_iter.IsValid());
  EXPECT_EQ(pos_iter.GetPosition(), 20);
  EXPECT_EQ(pos_iter.GetFieldMask(), 2ULL);  // Field 1 set (bit 1)
  
  pos_iter.NextPosition();
  EXPECT_TRUE(pos_iter.IsValid());
  EXPECT_EQ(pos_iter.GetPosition(), 30);
  EXPECT_EQ(pos_iter.GetFieldMask(), 4ULL);  // Field 2 set (bit 2)
  
  pos_iter.NextPosition();
  EXPECT_FALSE(pos_iter.IsValid());  // End of iteration
}

TEST_F(PostingTest, PositionIteratorSkipForward) {
  // Add test data with gaps in positions
  positional_postings_->InsertPosting("doc1", 0, 10);
  positional_postings_->InsertPosting("doc1", 1, 30);
  positional_postings_->InsertPosting("doc1", 2, 50);
  
  auto key_iter = positional_postings_->GetKeyIterator();
  auto pos_iter = key_iter.GetPositionIterator();
  
  // Skip to exact position match
  EXPECT_TRUE(pos_iter.SkipForwardPosition(30));
  EXPECT_TRUE(pos_iter.IsValid());
  EXPECT_EQ(pos_iter.GetPosition(), 30);
  EXPECT_EQ(pos_iter.GetFieldMask(), 2ULL);
  
  // Skip to non-existent position (should land on next greater position)
  EXPECT_FALSE(pos_iter.SkipForwardPosition(40));
  EXPECT_TRUE(pos_iter.IsValid());
  EXPECT_EQ(pos_iter.GetPosition(), 50);
  
  // Skip beyond all positions
  EXPECT_FALSE(pos_iter.SkipForwardPosition(100));
  EXPECT_FALSE(pos_iter.IsValid());
}

TEST_F(PostingTest, IteratorWithMultipleFields) {
  // Test position with multiple fields set
  positional_postings_->InsertPosting("doc1", 0, 10);  // Field 0 at position 10
  positional_postings_->InsertPosting("doc1", 2, 10);  // Field 2 at position 10
  positional_postings_->InsertPosting("doc1", 1, 20);  // Field 1 at position 20
  
  auto key_iter = positional_postings_->GetKeyIterator();
  auto pos_iter = key_iter.GetPositionIterator();
  
  // First position should have fields 0 and 2 set (bits 0 and 2)
  EXPECT_TRUE(pos_iter.IsValid());
  EXPECT_EQ(pos_iter.GetPosition(), 10);
  EXPECT_EQ(pos_iter.GetFieldMask(), 5ULL);  // Binary: 101 (fields 0 and 2)
  
  pos_iter.NextPosition();
  EXPECT_TRUE(pos_iter.IsValid());
  EXPECT_EQ(pos_iter.GetPosition(), 20);
  EXPECT_EQ(pos_iter.GetFieldMask(), 2ULL);  // Binary: 010 (field 1)
}

TEST_F(PostingTest, EmptyPostingIterators) {
  // Test iterators on empty posting
  auto key_iter = positional_postings_->GetKeyIterator();
  EXPECT_FALSE(key_iter.IsValid());
  
  auto pos_iter = key_iter.GetPositionIterator();
  EXPECT_FALSE(pos_iter.IsValid());
}

}  // namespace valkey_search::text
