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

class GlobalMetricsTest : public ValkeySearchTest {};

// Sanity test to verify basic global metrics functionality
TEST_F(GlobalMetricsTest, BasicMetricsIncrement) {
  auto& stats = indexes::GlobalIndexStats::Instance();
  
  // Test basic string interning and metrics
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kInternedStrings), 0);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMemoryMarkedDeleted), 0);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMarkedDeleted), 0);
  
  // Create some interned strings
  auto str1 = StringInternStore::Intern("test_vector_1", StringType::VECTOR);
  StringInternStore::Instance().SetDeleteMark(str1->Str().data(), str1->Str().length(), false);
  auto str2 = StringInternStore::Intern("test_tag_1", StringType::TAG);
  auto str3 = StringInternStore::Intern("test_key_1", StringType::KEY);
  auto str4 = StringInternStore::Intern("test_vector_2", StringType::VECTOR);
  auto str5 = StringInternStore::Intern("test_tag_2", StringType::TAG);
  
  // Should have 5 total interned strings
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kInternedStrings), 5);

  StringInternStore::Instance().SetDeleteMark(str1->Str().data(), str1->Str().length(), true); // This should mark str1 as deleted
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMarkedDeleted), 1);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMemoryMarkedDeleted), str1->Str().length());
  
  StringInternStore::Instance().SetDeleteMark(str1->Str().data(), str1->Str().length(), false); // This should unmark str1 as deleted
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMarkedDeleted), 0);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMemoryMarkedDeleted), 0);
}

// Test use count behavior for shared interned strings
TEST_F(GlobalMetricsTest, SharedInternedStringUseCount) {
  auto& stats = indexes::GlobalIndexStats::Instance();
  
  std::string shared_key = "shared_vector_key";
  auto interned_str = StringInternStore::Intern(shared_key, StringType::VECTOR);
  
  // Increment use count multiple times (simulate multiple indexes using the same key)
  StringInternStore::Instance().SetDeleteMark(interned_str->Str().data(), interned_str->Str().length(), false);  // use_count = 1
  StringInternStore::Instance().SetDeleteMark(interned_str->Str().data(), interned_str->Str().length(), false);  // use_count = 2
  StringInternStore::Instance().SetDeleteMark(interned_str->Str().data(), interned_str->Str().length(), false);  // use_count = 3
  
  // Mark unused once - should not trigger mark_deleted yet (use_count = 2)
  StringInternStore::Instance().SetDeleteMark(interned_str->Str().data(), interned_str->Str().length(), true);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMemoryMarkedDeleted), 0);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMarkedDeleted), 0);
  
  // Mark unused again - should not trigger mark_deleted yet (use_count = 1)
  StringInternStore::Instance().SetDeleteMark(interned_str->Str().data(), interned_str->Str().length(), true);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMemoryMarkedDeleted), 0);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMarkedDeleted), 0);
  
  // Mark unused one more time - now use_count should reach 0 and trigger mark_deleted
  StringInternStore::Instance().SetDeleteMark(interned_str->Str().data(), interned_str->Str().length(), true);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMemoryMarkedDeleted), shared_key.length());
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMarkedDeleted), 1);

  // Test unmarking deleted (reuse)
  StringInternStore::Instance().SetDeleteMark(interned_str->Str().data(), interned_str->Str().length(), false);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMemoryMarkedDeleted), 0);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMarkedDeleted), 0);
}

// Test GetCount functionality for various metrics
TEST_F(GlobalMetricsTest, GetCount) {
  auto& stats = indexes::GlobalIndexStats::Instance();
  
  // Create actual interned strings to test metrics
  std::vector<std::shared_ptr<InternedString>> strings;
  for (int i = 0; i < 10; ++i) {
    strings.push_back(StringInternStore::Intern("test_vector_" + std::to_string(i), StringType::VECTOR));
  }
  StringInternStore::Instance().SetDeleteMark(strings[0]->Str().data(), strings[0]->Str().length(), false);
  StringInternStore::Instance().SetDeleteMark(strings[0]->Str().data(), strings[0]->Str().length(), true);

  stats.Incr(indexes::MetricType::kHnswNodes, 5);
  stats.Incr(indexes::MetricType::kHnswNodesMarkedDeleted, 2);

  // Test individual GetCount calls
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kInternedStrings), 10);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kHnswNodes), 5);
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kVectorsMarkedDeleted), 1);
  EXPECT_GT(stats.GetCount(indexes::MetricType::kVectorsMemoryMarkedDeleted), 0); // Should have some memory
  EXPECT_EQ(stats.GetCount(indexes::MetricType::kHnswNodesMarkedDeleted), 2);
}


}  // namespace valkey_search
