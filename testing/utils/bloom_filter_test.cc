/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/utils/bloom_filter.h"

#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace valkey_search::utils {

TEST(BloomFilterTest, BasicInsertAndQuery) {
  BloomFilter filter(1024, 3);
  
  EXPECT_FALSE(filter.MayContain("hello"));
  filter.Insert("hello");
  EXPECT_TRUE(filter.MayContain("hello"));
  
  EXPECT_FALSE(filter.MayContain("world"));
  filter.Insert("world");
  EXPECT_TRUE(filter.MayContain("world"));
}

TEST(BloomFilterTest, Clear) {
  BloomFilter filter(1024, 3);
  
  filter.Insert("test");
  EXPECT_TRUE(filter.MayContain("test"));
  
  filter.Clear();
  EXPECT_FALSE(filter.MayContain("test"));
}

TEST(BloomFilterTest, MultipleItems) {
  BloomFilter filter(4096, 4);
  
  std::vector<std::string> items = {"apple", "banana", "cherry", "date", "elderberry"};
  
  for (const auto& item : items) {
    filter.Insert(item);
  }
  
  for (const auto& item : items) {
    EXPECT_TRUE(filter.MayContain(item));
  }
}

TEST(BloomFilterTest, ConcurrentInserts) {
  BloomFilter filter(8192, 4);
  constexpr int kNumThreads = 4;
  constexpr int kItemsPerThread = 100;
  
  std::vector<std::thread> threads;
  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&filter, t]() {
      for (int i = 0; i < kItemsPerThread; ++i) {
        filter.Insert("thread" + std::to_string(t) + "_item" + std::to_string(i));
      }
    });
  }
  
  for (auto& thread : threads) {
    thread.join();
  }
  
  for (int t = 0; t < kNumThreads; ++t) {
    for (int i = 0; i < kItemsPerThread; ++i) {
      EXPECT_TRUE(filter.MayContain("thread" + std::to_string(t) + "_item" + std::to_string(i)));
    }
  }
}

TEST(BloomFilterTest, ConcurrentReads) {
  BloomFilter filter(4096, 3);
  
  std::vector<std::string> items = {"item1", "item2", "item3", "item4", "item5"};
  for (const auto& item : items) {
    filter.Insert(item);
  }
  
  constexpr int kNumThreads = 4;
  std::vector<std::thread> threads;
  
  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&filter, &items]() {
      for (int i = 0; i < 100; ++i) {
        for (const auto& item : items) {
          EXPECT_TRUE(filter.MayContain(item));
        }
      }
    });
  }
  
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST(BloomFilterTest, CreateOptimal) {
  // Create filter for 1000 items with 1% false positive rate
  auto filter = BloomFilter::CreateOptimal(1000, 0.01);
  
  // Verify reasonable sizing: ~9585 bits for 1000 items at 1% FP rate
  EXPECT_GT(filter.NumBits(), 8000);
  EXPECT_LT(filter.NumBits(), 12000);
  
  // Optimal hash count for 1% FP is ~7
  EXPECT_GE(filter.NumHashes(), 5);
  EXPECT_LE(filter.NumHashes(), 10);
  
  // Verify functionality
  filter.Insert("test_item");
  EXPECT_TRUE(filter.MayContain("test_item"));
  EXPECT_FALSE(filter.MayContain("not_inserted"));
}

TEST(BloomFilterTest, CreateOptimalEdgeCases) {
  // Zero capacity should be handled
  auto filter1 = BloomFilter::CreateOptimal(0, 0.01);
  EXPECT_GE(filter1.NumBits(), 64);
  
  // Invalid FP rate should be clamped
  auto filter2 = BloomFilter::CreateOptimal(100, 0.0);
  EXPECT_GE(filter2.NumBits(), 64);
  
  auto filter3 = BloomFilter::CreateOptimal(100, 1.5);
  EXPECT_GE(filter3.NumBits(), 64);
}

}  // namespace valkey_search::utils
