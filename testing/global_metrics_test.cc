/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "src/commands/ft_create_parser.h"
#include "src/indexes/global_metrics.h"
#include "src/indexes/metric_types.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"
#include "vmsdk/src/testing_infra/module.h"

namespace valkey_search {

class GlobalMetricsTest : public ValkeySearchTest {
 protected:
  void SetUp() override {
    ValkeySearchTest::SetUp();
    // Reset global metrics to ensure clean state
    auto& stats = indexes::GlobalIndexStats::Instance();
    // Clear any existing metrics by getting all and resetting
    auto all_metrics = stats.GetAllMetrics();
    for (const auto& [type, count] : all_metrics) {
      // Reset counters to 0
      while (stats.GetCount(type) > 0) {
        stats.Decr(type, 1);
      }
    }
  }
};

// Sanity test to verify basic global metrics functionality
TEST_F(GlobalMetricsTest, BasicMetricsIncrement) {
  auto& stats = indexes::GlobalIndexStats::Instance();
  
  // Test basic increment/decrement
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kInternedStrings), 0);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMemoryMarkedDeleted), 0);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kInternedStringsMarkedDeleted), 0);
  
  stats.Incr(indexes::MetricType::kInternedStrings, 5);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kInternedStrings), 5);
  
  stats.Incr(indexes::MetricType::kVectorsMemoryMarkedDeleted, 2);
  stats.Incr(indexes::MetricType::kInternedStringsMarkedDeleted, 1);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMemoryMarkedDeleted), 2);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kInternedStringsMarkedDeleted), 1);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kInternedStrings), 5); // Count unchanged
  
  stats.Decr(indexes::MetricType::kVectorsMemoryMarkedDeleted, 1);
  stats.Decr(indexes::MetricType::kInternedStringsMarkedDeleted, 1);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMemoryMarkedDeleted), 1);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kInternedStringsMarkedDeleted), 0);
  
  stats.Decr(indexes::MetricType::kInternedStrings, 3);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kInternedStrings), 2);
}

// Test interned string allocation and deallocation metrics
TEST_F(GlobalMetricsTest, InternedStringAllocationMetrics) {
  auto& stats = indexes::GlobalIndexStats::Instance();

  std::string test_str = "test_vector_key_123";
  auto interned_str = StringInternStore::Intern(test_str, nullptr, indexes::MetricType::kVectorsMemory);
  
  // Verify metrics were updated
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kInternedStrings), 1);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kInternedStringsMemory), test_str.length());
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMemory), test_str.length());
  
  // Increment use count to set it to 1 (from initial 0xFFFF)
  indexes::OnInternedStringIncrUsed(interned_str);
  
  // Mark as unused (should trigger mark_deleted when use_count reaches 0)
  bool marked = indexes::OnInternedStringMarkUnused(interned_str);
  EXPECT_TRUE(marked);
  
  // Verify mark_deleted metrics
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMemoryMarkedDeleted), test_str.length());
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kInternedStringsMarkedDeleted), 1);
  
  // Unmark deleted (reuse)
  bool unmarked = indexes::OnInternedStringIncrUsed(interned_str);
  EXPECT_TRUE(unmarked);

  // Verify mark_deleted metrics are reduced
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMemoryMarkedDeleted), 0);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kInternedStringsMarkedDeleted), 0);
}

// Test use count behavior for shared interned strings
TEST_F(GlobalMetricsTest, SharedInternedStringUseCount) {
  auto& stats = indexes::GlobalIndexStats::Instance();
  
  std::string shared_key = "shared_vector_key";
  auto interned_str = StringInternStore::Intern(shared_key, nullptr, indexes::MetricType::kVectorsMemory);
  
  // Increment use count multiple times (simulate multiple indexes using the same key)
  // First call sets use_count from 0xFFFF to 1, subsequent calls increment it
  indexes::OnInternedStringIncrUsed(interned_str);  // use_count = 1
  indexes::OnInternedStringIncrUsed(interned_str);  // use_count = 2
  indexes::OnInternedStringIncrUsed(interned_str);  // use_count = 3
  
  // Mark unused once - should not trigger mark_deleted yet (use_count = 2)
  indexes::OnInternedStringMarkUnused(interned_str);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMemoryMarkedDeleted), 0);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kInternedStringsMarkedDeleted), 0);
  
  // Mark unused again - should not trigger mark_deleted yet (use_count = 1)
  indexes::OnInternedStringMarkUnused(interned_str);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMemoryMarkedDeleted), 0);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kInternedStringsMarkedDeleted), 0);
  
  // Mark unused one more time - now use_count should reach 0 and trigger mark_deleted
  indexes::OnInternedStringMarkUnused(interned_str);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMemoryMarkedDeleted), shared_key.length());
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kInternedStringsMarkedDeleted), 1);

  // Test unmarking deleted (reuse)
  indexes::OnInternedStringIncrUsed(interned_str);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMemoryMarkedDeleted), 0);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kInternedStringsMarkedDeleted), 0);
}

// Test GetAllMetrics functionality
TEST_F(GlobalMetricsTest, GetAllMetrics) {
  auto& stats = indexes::GlobalIndexStats::Instance();
  
  // Add some metrics
  stats.Incr(indexes::MetricType::kInternedStrings, 10);
  stats.Incr(indexes::MetricType::kHnswNodes, 5);
  stats.Incr(indexes::MetricType::kVectorsMemoryMarkedDeleted, 3);
  stats.Incr(indexes::MetricType::kInternedStringsMarkedDeleted, 1);
  stats.Incr(indexes::MetricType::kHnswNodesMarkedDeleted, 2);
  
  auto all_metrics = stats.GetAllMetrics();
  
  // Verify the metrics are correctly returned
  EXPECT_EQ(all_metrics[indexes::MetricType::kInternedStrings], 10);
  EXPECT_EQ(all_metrics[indexes::MetricType::kHnswNodes], 5);
  EXPECT_EQ(all_metrics[indexes::MetricType::kVectorsMemoryMarkedDeleted], 3);
  EXPECT_EQ(all_metrics[indexes::MetricType::kInternedStringsMarkedDeleted], 1);
  EXPECT_EQ(all_metrics[indexes::MetricType::kHnswNodesMarkedDeleted], 2);
}


}  // namespace valkey_search
