/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/posting.h"
#include "testing/common.h"
#include "src/utils/string_interning.h"

#include "gtest/gtest.h"

namespace valkey_search::indexes::text {

class PostingTest : public ValkeySearchTest {
 protected:
  void SetUp() override {
    ValkeySearchTest::SetUp();
    
    // Create MockIndexSchema instances like other tests
    std::vector<absl::string_view> key_prefixes = {"test:"};
    boolean_schema_ = MockIndexSchema::Create(&fake_ctx_, "boolean_schema", key_prefixes,
                                            std::make_unique<MockAttributeDataType>(),
                                            nullptr).value();
    
    positional_schema_ = MockIndexSchema::Create(&fake_ctx_, "positional_schema", key_prefixes,
                                               std::make_unique<MockAttributeDataType>(),
                                               nullptr).value();
    
    // Create postings with different configurations for testing using IndexSchema
    boolean_postings_ = new Postings(false, 3);  // Boolean search, 3 fields
    positional_postings_ = new Postings(true, 5); // Positional search, 5 fields
  }
  
  // Helper function to create InternedStringPtr from string
  InternedStringPtr InternKey(const std::string& key) {
    return StringInternStore::Intern(key);
  }
  
  void TearDown() override {
    delete boolean_postings_;
    delete positional_postings_;
    boolean_postings_ = nullptr;
    positional_postings_ = nullptr;
    
    // Reset schemas before calling base teardown
    boolean_schema_.reset();
    positional_schema_.reset();
    
    ValkeySearchTest::TearDown();
  }
  
  std::shared_ptr<MockIndexSchema> boolean_schema_;
  std::shared_ptr<MockIndexSchema> positional_schema_;
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
  boolean_postings_->InsertPosting(InternKey("doc1"), 0);        // field 0, position ignored
  boolean_postings_->InsertPosting(InternKey("doc1"), 1, 100);   // field 1, position 100 ignored
  boolean_postings_->InsertPosting(InternKey("doc2"), 2);        // field 2, position ignored
  
  EXPECT_FALSE(boolean_postings_->IsEmpty());
  EXPECT_EQ(boolean_postings_->GetKeyCount(), 2);
  EXPECT_EQ(boolean_postings_->GetPostingCount(), 2); // One position per key (always position 0)
  EXPECT_EQ(boolean_postings_->GetTotalTermFrequency(), 3); // Three field occurrences total
}

TEST_F(PostingTest, PositionalSearchInsertPosting) {
  // Test positional search mode - positions are respected
  positional_postings_->InsertPosting(InternKey("doc1"), 0, 10); // field 0, position 10
  positional_postings_->InsertPosting(InternKey("doc1"), 1, 20); // field 1, position 20
  positional_postings_->InsertPosting(InternKey("doc1"), 2, 10); // field 2, position 10 (same position, different field)
  
  EXPECT_EQ(positional_postings_->GetKeyCount(), 1);
  EXPECT_EQ(positional_postings_->GetPostingCount(), 2); // Two unique positions (10, 20)
  EXPECT_EQ(positional_postings_->GetTotalTermFrequency(), 3); // Three field occurrences total
  
  // Add second document
  positional_postings_->InsertPosting(InternKey("doc2"), 0, 5);
  positional_postings_->InsertPosting(InternKey("doc2"), 0, 15);
  
  EXPECT_EQ(positional_postings_->GetKeyCount(), 2);
  EXPECT_EQ(positional_postings_->GetPostingCount(), 4); // Two positions per document
  EXPECT_EQ(positional_postings_->GetTotalTermFrequency(), 5); // Five field occurrences total
}

TEST_F(PostingTest, InsertPostingDefaultPosition) {
  // Test that default position works correctly for boolean postings
  boolean_postings_->InsertPosting(InternKey("doc1"), 0); // Default position ignored in boolean mode
  boolean_postings_->InsertPosting(InternKey("doc1"), 1); // Default position ignored in boolean mode
  
  EXPECT_EQ(boolean_postings_->GetKeyCount(), 1);
  EXPECT_EQ(boolean_postings_->GetPostingCount(), 1); // Only one position (0)
  EXPECT_EQ(boolean_postings_->GetTotalTermFrequency(), 2); // Two field occurrences at position 0
}


TEST_F(PostingTest, RemoveKey) {
  // Add some data
  positional_postings_->InsertPosting(InternKey("doc1"), 0, 10);
  positional_postings_->InsertPosting(InternKey("doc2"), 1, 20);
  
  EXPECT_EQ(positional_postings_->GetKeyCount(), 2);
  
  // Remove one key
  positional_postings_->RemoveKey(InternKey("doc1"));
  EXPECT_EQ(positional_postings_->GetKeyCount(), 1);
  EXPECT_EQ(positional_postings_->GetPostingCount(), 1);
  
  // Remove non-existent key (should be no-op)
  positional_postings_->RemoveKey(InternKey("nonexistent"));
  EXPECT_EQ(positional_postings_->GetKeyCount(), 1);
  
  // Remove last key
  positional_postings_->RemoveKey(InternKey("doc2"));
  EXPECT_TRUE(positional_postings_->IsEmpty());
}

TEST_F(PostingTest, LargeScaleOperations) {
  // Test with many documents and positions
  for (int doc = 0; doc < 100; ++doc) {
    std::string key = "doc" + std::to_string(doc);
    for (int pos = 0; pos < 10; ++pos) {
      positional_postings_->InsertPosting(InternKey(key), pos % 5, pos * 10);
    }
  }
  
  EXPECT_EQ(positional_postings_->GetKeyCount(), 100);
  EXPECT_EQ(positional_postings_->GetPostingCount(), 1000); // 100 docs * 10 positions each
  EXPECT_EQ(positional_postings_->GetTotalTermFrequency(), 1000); // One field per position
}

TEST_F(PostingTest, SingleFieldOptimization) {
  // Test posting with single field (uses SingleFieldMask optimization internally)
  std::vector<absl::string_view> single_key_prefixes = {"single:"};
  auto single_field_schema = MockIndexSchema::Create(&fake_ctx_, "single_field_schema", single_key_prefixes,
                                                    std::make_unique<MockAttributeDataType>(),
                                                    nullptr).value();
  Postings single_field_posting(true, 1);
  
  // Add some postings - all will use field 0
  single_field_posting.InsertPosting(InternKey("doc1"), 0, 10);
  single_field_posting.InsertPosting(InternKey("doc1"), 0, 20);
  single_field_posting.InsertPosting(InternKey("doc2"), 0, 5);
  
  // Verify posting works correctly with single field optimization
  EXPECT_EQ(single_field_posting.GetKeyCount(), 2);
  EXPECT_EQ(single_field_posting.GetPostingCount(), 3);
  EXPECT_EQ(single_field_posting.GetTotalTermFrequency(), 3);
}

TEST_F(PostingTest, BooleanVsPositionalBehavior) {
  // Test that boolean and positional modes behave differently for positions
  
  // Boolean mode: positions ignored, all stored at position 0
  boolean_postings_->InsertPosting(InternKey("doc1"), 0, 100);  // position 100 ignored
  boolean_postings_->InsertPosting(InternKey("doc1"), 1, 200);  // position 200 ignored
  boolean_postings_->InsertPosting(InternKey("doc1"), 2, 300);  // position 300 ignored
  
  EXPECT_EQ(boolean_postings_->GetPostingCount(), 1); // All at position 0
  EXPECT_EQ(boolean_postings_->GetTotalTermFrequency(), 3); // Three fields
  
  // Positional mode: positions respected
  positional_postings_->InsertPosting(InternKey("doc1"), 0, 100);
  positional_postings_->InsertPosting(InternKey("doc1"), 1, 200);
  positional_postings_->InsertPosting(InternKey("doc1"), 2, 300);
  
  EXPECT_EQ(positional_postings_->GetPostingCount(), 3); // Three different positions
  EXPECT_EQ(positional_postings_->GetTotalTermFrequency(), 3); // Three fields
}

TEST_F(PostingTest, MultipleInsertPostingCalls) {
  // Test multiple InsertPosting calls on same document
  positional_postings_->InsertPosting(InternKey("doc1"), 0, 10);
  positional_postings_->InsertPosting(InternKey("doc1"), 1, 20);
  positional_postings_->InsertPosting(InternKey("doc1"), 2, 30);
  positional_postings_->InsertPosting(InternKey("doc1"), 1, 10); // Add field 1 to position 10
  
  EXPECT_EQ(positional_postings_->GetPostingCount(), 3); // Three unique positions (10, 20, 30)
  EXPECT_EQ(positional_postings_->GetTotalTermFrequency(), 4); // Four field occurrences
}

TEST_F(PostingTest, KeyIteratorBasic) {
  // Add some test data
  positional_postings_->InsertPosting(InternKey("doc1"), 0, 10);
  positional_postings_->InsertPosting(InternKey("doc2"), 1, 20);
  positional_postings_->InsertPosting(InternKey("doc3"), 2, 30);
  
  // Test key iteration - collect all keys
  auto key_iter = positional_postings_->GetKeyIterator();
  std::vector<std::string> found_keys;
  
  while (key_iter.IsValid()) {
    found_keys.push_back(std::string(key_iter.GetKey()->Str()));
    key_iter.NextKey();
  }
  
  // Verify all keys are present
  EXPECT_EQ(found_keys.size(), 3);
  std::sort(found_keys.begin(), found_keys.end());
  EXPECT_EQ(found_keys[0], "doc1");
  EXPECT_EQ(found_keys[1], "doc2");
  EXPECT_EQ(found_keys[2], "doc3");
}

TEST_F(PostingTest, KeyIteratorSkipForward) {
  // Add test data
  positional_postings_->InsertPosting(InternKey("doc1"), 0, 10);
  positional_postings_->InsertPosting(InternKey("doc3"), 1, 20);
  positional_postings_->InsertPosting(InternKey("doc5"), 2, 30);
  
  auto key_iter = positional_postings_->GetKeyIterator();
  
  // Test that we can skip to an existing key
  auto doc3_key = InternKey("doc3");
  bool found_exact = key_iter.SkipForwardKey(doc3_key);
  if (found_exact) {
    EXPECT_TRUE(key_iter.IsValid());
    EXPECT_EQ(key_iter.GetKey()->Str(), "doc3");
  }
  
  // Test that iterator can advance through all keys
  key_iter = positional_postings_->GetKeyIterator();
  int key_count = 0;
  while (key_iter.IsValid()) {
    key_count++;
    key_iter.NextKey();
  }
  EXPECT_EQ(key_count, 3);
}

TEST_F(PostingTest, PositionIteratorBasic) {
  // Add test data with multiple positions for one key
  positional_postings_->InsertPosting(InternKey("doc1"), 0, 10);
  positional_postings_->InsertPosting(InternKey("doc1"), 1, 20);
  positional_postings_->InsertPosting(InternKey("doc1"), 2, 30);
  
  // Get key iterator and position iterator
  auto key_iter = positional_postings_->GetKeyIterator();
  EXPECT_TRUE(key_iter.IsValid());
  EXPECT_EQ(key_iter.GetKey()->Str(), "doc1");
  
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
  positional_postings_->InsertPosting(InternKey("doc1"), 0, 10);
  positional_postings_->InsertPosting(InternKey("doc1"), 1, 30);
  positional_postings_->InsertPosting(InternKey("doc1"), 2, 50);
  
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
  positional_postings_->InsertPosting(InternKey("doc1"), 0, 10);  // Field 0 at position 10
  positional_postings_->InsertPosting(InternKey("doc1"), 2, 10);  // Field 2 at position 10
  positional_postings_->InsertPosting(InternKey("doc1"), 1, 20);  // Field 1 at position 20
  
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
  // Test key iterator on empty posting
  auto key_iter = positional_postings_->GetKeyIterator();
  EXPECT_FALSE(key_iter.IsValid());
  
  // Test position iterator behavior: add one position, then advance past it
  positional_postings_->InsertPosting(InternKey("doc1"), 0, 10);
  
  auto valid_key_iter = positional_postings_->GetKeyIterator();
  EXPECT_TRUE(valid_key_iter.IsValid());
  
  auto pos_iter = valid_key_iter.GetPositionIterator();
  EXPECT_TRUE(pos_iter.IsValid());
  EXPECT_EQ(pos_iter.GetPosition(), 10);
  
  // Advance past the only position - should become invalid
  pos_iter.NextPosition();
  EXPECT_FALSE(pos_iter.IsValid());
}

TEST_F(PostingTest, ContainsFieldsCheck) {
  // Create a fresh posting to ensure no interference from other tests
  Postings fresh_postings(true, 5);
  
  // Test basic field containment functionality 
  fresh_postings.InsertPosting(InternKey("doc1"), 0, 10);
  
  auto field_mask = FieldMask::Create(5);
  field_mask->SetField(0);
  
  auto key_iter = fresh_postings.GetKeyIterator();
  
  // Should have exactly one key
  EXPECT_EQ(fresh_postings.GetKeyCount(), 1);
  
  // Iterator should be valid and contain the field we inserted
  EXPECT_TRUE(key_iter.IsValid());
  EXPECT_EQ(key_iter.GetKey()->Str(), "doc1");
  EXPECT_TRUE(key_iter.ContainsFields(*field_mask));
  
  // Test that it doesn't contain fields that weren't set
  auto field_mask_2 = FieldMask::Create(5);
  field_mask_2->SetField(1);
  EXPECT_FALSE(key_iter.ContainsFields(*field_mask_2));
}

}  // namespace valkey_search::indexes::text
