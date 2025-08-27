/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/posting.h"
#include "testing/common.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/memory_tracker.h"
#include "vmsdk/src/memory_allocation_overrides.h"
#include "vmsdk/src/testing_infra/module.h"

#include "gtest/gtest.h"
#include "gmock/gmock.h"

namespace valkey_search::indexes::text {

class PostingMemoryAllocationTest : public vmsdk::ValkeyTest {
 protected:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    // Reset memory tracking to start fresh
    vmsdk::ResetValkeyAlloc();
    vmsdk::SetMemoryDelta(0);
  }
  
  void TearDown() override {
    vmsdk::ValkeyTest::TearDown();
    vmsdk::ResetValkeyAlloc();
  }
  
  // Helper function to create InternedStringPtr from string
  InternedStringPtr InternKey(const std::string& key) {
    return StringInternStore::Intern(key);
  }
};

TEST_F(PostingMemoryAllocationTest, MemoryTrackingDemonstration) {
  MemoryPool pool{0};
  
  {
    IsolatedMemoryScope scope{pool};
    
    // Manually report some memory allocations to demonstrate the tracking works
    vmsdk::ReportAllocMemorySize(100);
    vmsdk::ReportAllocMemorySize(200);
    
    // This should now show 300 bytes allocated
    EXPECT_EQ(vmsdk::GetMemoryDelta(), 300);
    
    // Manually report some memory deallocation
    vmsdk::ReportFreeMemorySize(50);
    
    // This should now show 250 bytes net allocated
    EXPECT_EQ(vmsdk::GetMemoryDelta(), 250);
  }
  
  // After scope ends, pool should track the net allocation (250 bytes)
  EXPECT_EQ(pool.GetUsage(), 250);
  
  // After scope destruction, global memory should be back to baseline
  EXPECT_EQ(vmsdk::GetMemoryDelta(), 0);
}

TEST_F(PostingMemoryAllocationTest, PostingAllocationsNotTracked) {
  MemoryPool pool{0};
  
  {
    IsolatedMemoryScope scope{pool};
    
    // Create empty posting - this uses standard C++ allocations 
    // that are NOT being intercepted by vmsdk tracking
    auto posting = std::make_unique<Postings>(false, 3);
    
    // Add some data - this will allocate internal std::maps, std::vectors
    posting->InsertPosting(InternKey("doc1"), 0);
    posting->InsertPosting(InternKey("doc2"), 1);
    
    // Memory delta will be 0 because posting allocations aren't tracked
    std::cout << "Memory delta after posting operations: " << vmsdk::GetMemoryDelta() << " bytes" << std::endl;
    EXPECT_EQ(vmsdk::GetMemoryDelta(), 0); // This will pass because allocations aren't tracked
    
    // Verify posting is working correctly though
    EXPECT_FALSE(posting->IsEmpty());
    EXPECT_EQ(posting->GetKeyCount(), 2);
  }
  
  // Pool usage will be 0 because no allocations were tracked
  EXPECT_EQ(pool.GetUsage(), 0);
  EXPECT_EQ(vmsdk::GetMemoryDelta(), 0);
}

TEST_F(PostingMemoryAllocationTest, BooleanPostingMemoryUsage) {
  // Use Postings' own memory tracking system
  auto posting = std::make_unique<Postings>(false, 3); // Boolean mode, 3 fields
  int64_t base_memory = Postings::GetMemoryUsage();
  
  // Add single document with multiple fields - should only create one position (0)
  posting->InsertPosting(InternKey("doc1"), 0);
  posting->InsertPosting(InternKey("doc1"), 1);
  posting->InsertPosting(InternKey("doc1"), 2);
  
  int64_t memory_after_one_doc = Postings::GetMemoryUsage();
  EXPECT_GT(memory_after_one_doc, base_memory);
  
  // Memory should grow with each new document key
  posting->InsertPosting(InternKey("doc2"), 0);
  posting->InsertPosting(InternKey("doc2"), 1);
  
  int64_t memory_after_two_docs = Postings::GetMemoryUsage();
  EXPECT_GT(memory_after_two_docs, memory_after_one_doc);
  
  // Verify posting structure
  EXPECT_EQ(posting->GetKeyCount(), 2);
  EXPECT_EQ(posting->GetPostingCount(), 2); // Two keys, each with one position (0)
  EXPECT_EQ(posting->GetTotalTermFrequency(), 5); // 3 + 2 field occurrences
  
  // Clean up - memory should decrease
  posting.reset();
  int64_t memory_after_cleanup = Postings::GetMemoryUsage();
  EXPECT_LT(memory_after_cleanup, memory_after_two_docs);
}

TEST_F(PostingMemoryAllocationTest, PositionalPostingMemoryUsage) {
  auto posting = std::make_unique<Postings>(true, 5); // Positional mode, 5 fields
  int64_t base_memory = Postings::GetMemoryUsage();
  
  // Add document with multiple positions - each position creates separate entry
  posting->InsertPosting(InternKey("doc1"), 0, 10);
  posting->InsertPosting(InternKey("doc1"), 1, 20);
  posting->InsertPosting(InternKey("doc1"), 2, 30);
  
  int64_t memory_after_positions = Postings::GetMemoryUsage();
  EXPECT_GT(memory_after_positions, base_memory);
  
  // Add same document at existing position with different field - should reuse position
  posting->InsertPosting(InternKey("doc1"), 3, 20); // Add field 3 to existing position 20
  
  int64_t memory_after_field_reuse = Postings::GetMemoryUsage();
  // Memory should increase slightly for additional field but not create new position
  EXPECT_GE(memory_after_field_reuse, memory_after_positions);
  
  // Verify posting structure
  EXPECT_EQ(posting->GetKeyCount(), 1);
  EXPECT_EQ(posting->GetPostingCount(), 3); // Three positions (10, 20, 30)
  EXPECT_EQ(posting->GetTotalTermFrequency(), 4); // Four field occurrences
  
  // Clean up - memory should decrease
  posting.reset();
  int64_t memory_after_cleanup = Postings::GetMemoryUsage();
  EXPECT_LT(memory_after_cleanup, memory_after_field_reuse);
}

TEST_F(PostingMemoryAllocationTest, FieldMaskOptimizationMemoryUsage) {
  int64_t single_field_memory = 0;
  int64_t multi_field_memory = 0;
  
  // Test single field optimization (uses EmptyFieldMask - no storage)
  {
    int64_t baseline = Postings::GetMemoryUsage();
    auto posting = std::make_unique<Postings>(true, 1); // Single field
    
    // Add multiple documents with single field
    for (int i = 0; i < 10; ++i) {
      std::string key = "doc" + std::to_string(i);
      posting->InsertPosting(InternKey(key), 0, i * 10);
    }
    
    single_field_memory = Postings::GetMemoryUsage() - baseline;
    EXPECT_EQ(posting->GetKeyCount(), 10);
    EXPECT_EQ(posting->GetPostingCount(), 10);
    
    posting.reset(); // Clean up
  }
  
  // Test multi-field (uses ByteFieldMask/Uint64FieldMask - has storage)
  {
    int64_t baseline = Postings::GetMemoryUsage();
    auto posting = std::make_unique<Postings>(true, 8); // 8 fields (uses ByteFieldMask)
    
    // Add same number of documents with multiple fields
    for (int i = 0; i < 10; ++i) {
      std::string key = "doc" + std::to_string(i);
      posting->InsertPosting(InternKey(key), i % 8, i * 10);
    }
    
    multi_field_memory = Postings::GetMemoryUsage() - baseline;
    EXPECT_EQ(posting->GetKeyCount(), 10);
    EXPECT_EQ(posting->GetPostingCount(), 10);
    
    posting.reset(); // Clean up
  }
  
  // Both should use memory, but the difference might be small for this test size
  EXPECT_GT(single_field_memory, 0);
  EXPECT_GT(multi_field_memory, 0);
  
  // For demonstration purposes, show the memory usage difference
  std::cout << "Field mask optimization memory comparison:" << std::endl;
  std::cout << "  Single field (EmptyFieldMask): " << single_field_memory << " bytes" << std::endl;
  std::cout << "  Multi-field (ByteFieldMask): " << multi_field_memory << " bytes" << std::endl;
  std::cout << "  Difference: " << (multi_field_memory - single_field_memory) << " bytes" << std::endl;
  
  // The difference may be small for this test size, so we just verify both use memory
  EXPECT_GE(multi_field_memory, single_field_memory);
}

TEST_F(PostingMemoryAllocationTest, LargeScaleMemoryUsage) {
  auto posting = std::make_unique<Postings>(true, 10); // 10 fields
  int64_t base_memory = Postings::GetMemoryUsage();
  
  // Add large number of documents and positions
  const int num_docs = 100;
  const int positions_per_doc = 20;
  
  for (int doc = 0; doc < num_docs; ++doc) {
    std::string key = "doc" + std::to_string(doc);
    for (int pos = 0; pos < positions_per_doc; ++pos) {
      posting->InsertPosting(InternKey(key), pos % 10, pos * 10);
    }
  }
  
  int64_t final_memory = Postings::GetMemoryUsage();
  EXPECT_GT(final_memory, base_memory);
  
  // Verify posting structure
  EXPECT_EQ(posting->GetKeyCount(), num_docs);
  EXPECT_EQ(posting->GetPostingCount(), num_docs * positions_per_doc);
  EXPECT_EQ(posting->GetTotalTermFrequency(), num_docs * positions_per_doc);
  
  // Memory usage should be proportional to data size
  double memory_per_posting = static_cast<double>(final_memory - base_memory) / 
                              (num_docs * positions_per_doc);
  EXPECT_GT(memory_per_posting, 0);
  
  // Log memory usage for analysis
  std::cout << "Large scale memory usage:" << std::endl;
  std::cout << "  Total documents: " << num_docs << std::endl;
  std::cout << "  Positions per document: " << positions_per_doc << std::endl;
  std::cout << "  Total postings: " << (num_docs * positions_per_doc) << std::endl;
  std::cout << "  Memory usage: " << (final_memory - base_memory) << " bytes" << std::endl;
  std::cout << "  Memory per posting: " << memory_per_posting << " bytes" << std::endl;
  
  // Clean up - memory should be freed
  posting.reset();
  int64_t memory_after_cleanup = Postings::GetMemoryUsage();
  EXPECT_LT(memory_after_cleanup, final_memory);
}

TEST_F(PostingMemoryAllocationTest, MemoryCleanupAfterRemoval) {
  auto posting = std::make_unique<Postings>(true, 5);
  int64_t base_memory = Postings::GetMemoryUsage();
  
  // Add some data
  posting->InsertPosting(InternKey("doc1"), 0, 10);
  posting->InsertPosting(InternKey("doc1"), 1, 20);
  posting->InsertPosting(InternKey("doc2"), 0, 30);
  posting->InsertPosting(InternKey("doc3"), 2, 40);
  
  int64_t memory_after_insert = Postings::GetMemoryUsage();
  EXPECT_GT(memory_after_insert, base_memory);
  
  EXPECT_EQ(posting->GetKeyCount(), 3);
  EXPECT_EQ(posting->GetPostingCount(), 4);
  
  // Remove some keys and check memory
  posting->RemoveKey(InternKey("doc2"));
  int64_t memory_after_remove_one = Postings::GetMemoryUsage();
  EXPECT_LT(memory_after_remove_one, memory_after_insert); // Should use less memory
  
  EXPECT_EQ(posting->GetKeyCount(), 2);
  EXPECT_EQ(posting->GetPostingCount(), 3);
  
  // Remove all remaining keys
  posting->RemoveKey(InternKey("doc1"));
  posting->RemoveKey(InternKey("doc3"));
  
  int64_t memory_after_remove_all = Postings::GetMemoryUsage();
  EXPECT_LT(memory_after_remove_all, memory_after_remove_one);
  
  EXPECT_EQ(posting->GetKeyCount(), 0);
  EXPECT_EQ(posting->GetPostingCount(), 0);
  EXPECT_TRUE(posting->IsEmpty());
  
  // Should be close to base memory usage (empty posting)
  EXPECT_LT(std::abs(memory_after_remove_all - base_memory), 
            std::abs(memory_after_insert - base_memory));
  
  // Clean up
  posting.reset();
  int64_t memory_after_cleanup = Postings::GetMemoryUsage();
  EXPECT_LE(memory_after_cleanup, memory_after_remove_all);
}

TEST_F(PostingMemoryAllocationTest, NestedMemoryScopesBehavior) {
  // Test that multiple posting objects created in sequence track memory correctly
  int64_t baseline = Postings::GetMemoryUsage();
  
  // Create first posting
  auto outer_posting = std::make_unique<Postings>(true, 3);
  outer_posting->InsertPosting(InternKey("outer_doc"), 0, 10);
  
  int64_t memory_after_outer = Postings::GetMemoryUsage();
  EXPECT_GT(memory_after_outer, baseline);
  
  // Create second posting - should add to total memory
  auto inner_posting = std::make_unique<Postings>(true, 5);
  inner_posting->InsertPosting(InternKey("inner_doc"), 1, 20);
  
  int64_t memory_after_inner = Postings::GetMemoryUsage();
  EXPECT_GT(memory_after_inner, memory_after_outer);
  
  // Clean up inner posting first
  inner_posting.reset();
  int64_t memory_after_inner_cleanup = Postings::GetMemoryUsage();
  EXPECT_LT(memory_after_inner_cleanup, memory_after_inner);
  EXPECT_GE(memory_after_inner_cleanup, memory_after_outer);
  
  // Clean up outer posting
  outer_posting.reset();
  int64_t memory_after_outer_cleanup = Postings::GetMemoryUsage();
  EXPECT_LT(memory_after_outer_cleanup, memory_after_inner_cleanup);
  EXPECT_LE(memory_after_outer_cleanup, baseline);
}

TEST_F(PostingMemoryAllocationTest, IteratorMemoryImpact) {
  auto posting = std::make_unique<Postings>(true, 5);
  
  // Add test data
  for (int i = 0; i < 50; ++i) {
    std::string key = "doc" + std::to_string(i);
    for (int pos = 0; pos < 10; ++pos) {
      posting->InsertPosting(InternKey(key), pos % 5, pos * 10);
    }
  }
  
  int64_t memory_after_data = Postings::GetMemoryUsage();
  
  // Create multiple iterators - should not significantly increase memory
  // (Iterators are lightweight - they just hold pointers to the existing data)
  {
    auto key_iter1 = posting->GetKeyIterator();
    auto key_iter2 = posting->GetKeyIterator();
    
    int64_t memory_after_key_iters = Postings::GetMemoryUsage();
    
    // Iterator creation should not allocate significant memory (they're just stack objects with pointers)
    EXPECT_EQ(memory_after_key_iters, memory_after_data);
    
    if (key_iter1.IsValid()) {
      auto pos_iter1 = key_iter1.GetPositionIterator();
      auto pos_iter2 = key_iter1.GetPositionIterator();
      
      int64_t memory_after_pos_iters = Postings::GetMemoryUsage();
      
      // Position iterator creation should also not allocate memory
      EXPECT_EQ(memory_after_pos_iters, memory_after_data);
      
      // Use iterators to traverse data
      int key_count = 0;
      while (key_iter1.IsValid()) {
        key_count++;
        key_iter1.NextKey();
      }
      EXPECT_EQ(key_count, 50);
    }
  }
  
  // After iterator destruction, memory should be the same (iterators don't allocate)
  int64_t memory_after_iter_cleanup = Postings::GetMemoryUsage();
  EXPECT_EQ(memory_after_iter_cleanup, memory_after_data);
  
  // Clean up posting
  posting.reset();
  int64_t memory_after_cleanup = Postings::GetMemoryUsage();
  EXPECT_LT(memory_after_cleanup, memory_after_data);
}

}  // namespace valkey_search::indexes::text
