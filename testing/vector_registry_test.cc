/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 *
 */

#include "src/vector_registry.h"

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "src/indexes/vector_base.h"
#include "src/utils/allocator.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"

namespace valkey_search {

class VectorRegistryTest : public ValkeySearchTest {
 protected:
  // Helper to call Track under a writer lock, passing the test allocator.
  std::shared_ptr<indexes::VectorRecord> Track(VectorRegistry &registry,
                                               const InternedStringPtr &key,
                                               absl::string_view attribute,
                                               absl::string_view vector,
                                               Allocator *allocator,
                                               bool *out_assigned = nullptr) {
    size_t old_size = registry.tracked_vectors_.size();
    auto shared_record = registry.DedupConstruct(
        key, attribute, vector,
        indexes::CalcMagnitude(reinterpret_cast<const float *>(vector.data()),
                               vector.size() / sizeof(float)),
        allocator);
    if (out_assigned) {
      *out_assigned = (registry.tracked_vectors_.size() > old_size);
    }
    return shared_record;
  }

  // Helper to calculate TrackedSize (number of entries in the map) under a
  // reader lock.
  size_t TrackedSize(VectorRegistry &registry) {
    return registry.tracked_vectors_.size();
  }

  // Helper to manually set the shared_externally flag on an entry.
  void SetSharedExternally(VectorRegistry &registry,
                           const InternedStringPtr &key,
                           absl::string_view attribute, bool value) {
    auto &map = registry.tracked_vectors_;
    auto it = map.find(std::make_pair(key, std::string(attribute)));
    if (it != map.end()) {
      it->second.shared_externally = value ? it->second.record.lock() : nullptr;
    }
  }
};

TEST_F(VectorRegistryTest, TrackNewRecord) {
  UniqueFixedSizeAllocatorPtr allocator = CREATE_UNIQUE_PTR(
      FixedSizeAllocator, sizeof(indexes::VectorRecord) + 4 * sizeof(float),
      true);
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("doc_key_1");
  std::string attr = "vec_attr_1";

  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  absl::string_view vec_view(reinterpret_cast<const char *>(vec_data.data()),
                             vec_data.size() * sizeof(float));

  bool assigned = false;
  {
    auto record =
        Track(registry, key, attr, vec_view, allocator.get(), &assigned);
    EXPECT_NE(record, nullptr);
    EXPECT_TRUE(assigned);

    // Verify vector content
    absl::string_view stored_vec(record->GetRawVector(), vec_view.size());
    EXPECT_EQ(stored_vec, vec_view);

    float expected_magnitude = std::sqrt(1.0f + 4.0f + 9.0f + 16.0f);
    EXPECT_NEAR(record->GetReciprocalMagnitude(), 1.0f / expected_magnitude,
                1e-6f);
  }
  EXPECT_EQ(TrackedSize(registry), 1);
}
TEST_F(VectorRegistryTest, TrackSharesSameRecord) {
  UniqueFixedSizeAllocatorPtr allocator = CREATE_UNIQUE_PTR(
      FixedSizeAllocator, sizeof(indexes::VectorRecord) + 4 * sizeof(float),
      true);
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("doc_key_1");
  std::string attr = "vec_attr_1";

  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  absl::string_view vec_view(reinterpret_cast<const char *>(vec_data.data()),
                             vec_data.size() * sizeof(float));

  bool assigned1 = false;
  bool assigned2 = true;
  {
    auto record1 =
        Track(registry, key, attr, vec_view, allocator.get(), &assigned1);
    auto record2 =
        Track(registry, key, attr, vec_view, allocator.get(), &assigned2);

    // They must point to the exact same raw buffer
    EXPECT_EQ(record1->GetRawVector(), record2->GetRawVector());
    EXPECT_TRUE(assigned1);
    EXPECT_FALSE(assigned2);  // skipped
  }
  EXPECT_EQ(TrackedSize(registry), 1);
}

TEST_F(VectorRegistryTest, UntrackOnlyEasesIfNotShared) {
  UniqueFixedSizeAllocatorPtr allocator = CREATE_UNIQUE_PTR(
      FixedSizeAllocator, sizeof(indexes::VectorRecord) + 4 * sizeof(float),
      true);
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("doc_key_1");
  std::string attr = "vec_attr_1";
  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  absl::string_view vec_view(reinterpret_cast<const char *>(vec_data.data()),
                             vec_data.size() * sizeof(float));
  EXPECT_EQ(TrackedSize(registry), 0);
  auto record = Track(registry, key, attr, vec_view, allocator.get());
  EXPECT_NE(record, nullptr);
  EXPECT_EQ(TrackedSize(registry), 1);

  SetSharedExternally(registry, key, attr, true);
  registry.Untrack(key, attr, false);
  EXPECT_EQ(TrackedSize(registry), 1);

  SetSharedExternally(registry, key, attr, false);

  registry.Untrack(key, attr, false);
  EXPECT_EQ(TrackedSize(registry), 0);
}

TEST_F(VectorRegistryTest, StatsAreUpdated) {
  UniqueFixedSizeAllocatorPtr allocator = CREATE_UNIQUE_PTR(
      FixedSizeAllocator, sizeof(indexes::VectorRecord) + 4 * sizeof(float),
      true);
  auto &registry = VectorRegistry::Instance();
  registry.Reset();
  
  auto key = StringInternStore::Intern("doc_key_stats");
  std::string attr = "vec_attr_stats";

  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  absl::string_view vec_view(reinterpret_cast<const char *>(vec_data.data()),
                             vec_data.size() * sizeof(float));

  size_t misses_before = registry.GetStats().deduplication_misses;
  size_t hits_before = registry.GetStats().deduplication_hits;
  size_t memory_saved_before = registry.GetStats().memory_saved_bytes;
  
  // First track -> miss
  auto record1 = registry.DedupConstruct(key, attr, vec_view, 1.0f, allocator.get());
  
  EXPECT_EQ(registry.GetStats().deduplication_misses, misses_before + 1);
  EXPECT_EQ(registry.GetStats().deduplication_hits, hits_before);
  EXPECT_EQ(registry.GetStats().memory_saved_bytes, memory_saved_before);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  
  // Second track with SAME data -> hit
  auto record2 = registry.DedupConstruct(key, attr, vec_view, 1.0f, allocator.get());
  
  EXPECT_EQ(registry.GetStats().deduplication_misses, misses_before + 1);
  EXPECT_EQ(registry.GetStats().deduplication_hits, hits_before + 1);
  EXPECT_EQ(registry.GetStats().memory_saved_bytes, memory_saved_before + vec_view.size());
  
  // Third track with DIFFERENT data -> miss, and overrides old record
  std::vector<float> vec_data2 = {2.0f, 3.0f, 4.0f, 5.0f};
  absl::string_view vec_view2(reinterpret_cast<const char *>(vec_data2.data()),
                              vec_data2.size() * sizeof(float));
                              
  auto record3 = registry.DedupConstruct(key, attr, vec_view2, 1.0f, allocator.get());
  
  EXPECT_EQ(registry.GetStats().deduplication_misses, misses_before + 2);
  EXPECT_EQ(registry.GetStats().deduplication_hits, hits_before + 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

TEST_F(VectorRegistryTest, UntrackKeepsWeakPtrIfShared) {
  UniqueFixedSizeAllocatorPtr allocator = CREATE_UNIQUE_PTR(
      FixedSizeAllocator, sizeof(indexes::VectorRecord) + 4 * sizeof(float),
      true);
  auto &registry = VectorRegistry::Instance();
  registry.Reset();
  
  auto key = StringInternStore::Intern("doc_key_untrack");
  std::string attr = "vec_attr_untrack";

  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  absl::string_view vec_view(reinterpret_cast<const char *>(vec_data.data()),
                             vec_data.size() * sizeof(float));

  auto record = registry.DedupConstruct(key, attr, vec_view, 1.0f, allocator.get());
  
  // Fake EngineShare by manually setting shared_externally
  SetSharedExternally(registry, key, attr, true);
  
  // Untrack(attribute_deleted=false)
  registry.Untrack(key, attr, false);
  
  // The entry should still be there, AND the weak_ptr should still be valid!
  EXPECT_EQ(TrackedSize(registry), 1);
  
  // If we try to DedupConstruct the same data, it should be a hit (weak_ptr was NOT reset)!
  size_t hits_before = registry.GetStats().deduplication_hits;
  auto record2 = registry.DedupConstruct(key, attr, vec_view, 1.0f, allocator.get());
  EXPECT_EQ(registry.GetStats().deduplication_hits, hits_before + 1);
  
  SetSharedExternally(registry, key, attr, false);
}

TEST_F(VectorRegistryTest, UntrackDeletesIfAttributeDeleted) {
  UniqueFixedSizeAllocatorPtr allocator = CREATE_UNIQUE_PTR(
      FixedSizeAllocator, sizeof(indexes::VectorRecord) + 4 * sizeof(float),
      true);
  auto &registry = VectorRegistry::Instance();
  registry.Reset();
  
  auto key = StringInternStore::Intern("doc_key_delete");
  std::string attr = "vec_attr_delete";

  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  absl::string_view vec_view(reinterpret_cast<const char *>(vec_data.data()),
                             vec_data.size() * sizeof(float));

  auto record = registry.DedupConstruct(key, attr, vec_view, 1.0f, allocator.get());
  
  SetSharedExternally(registry, key, attr, true);
  
  // Erase it by passing attribute_deleted=true
  registry.Untrack(key, attr, true);
  
  // The entry should be gone entirely
  EXPECT_EQ(TrackedSize(registry), 0);
}

}  // namespace valkey_search
