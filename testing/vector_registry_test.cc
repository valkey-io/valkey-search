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

  void SetHashRegistrationSupported(VectorRegistry &registry, bool value) {
    registry.hash_registration_supported_ = value;
  }

  bool GetHashRegistrationSupported(VectorRegistry &registry) {
    return registry.hash_registration_supported_;
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

  bool old_hash_reg = GetHashRegistrationSupported(registry);
  SetHashRegistrationSupported(registry, false);

  auto record = Track(registry, key, attr, vec_view, allocator.get());
  EXPECT_NE(record, nullptr);
  EXPECT_EQ(TrackedSize(registry), 1);

  registry.UntrackExpired(key, attr);
  EXPECT_EQ(TrackedSize(registry), 1);

  SetHashRegistrationSupported(registry, old_hash_reg);
}

TEST_F(VectorRegistryTest, StatsAreUpdated) {
  UniqueFixedSizeAllocatorPtr allocator = CREATE_UNIQUE_PTR(
      FixedSizeAllocator, sizeof(indexes::VectorRecord) + 4 * sizeof(float),
      true);
  auto &registry = VectorRegistry::Instance();

  auto key = StringInternStore::Intern("doc_key_stats");
  std::string attr = "vec_attr_stats";

  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  absl::string_view vec_view(reinterpret_cast<const char *>(vec_data.data()),
                             vec_data.size() * sizeof(float));

  auto misses_before = registry.GetStats().deduplication_misses.GetTotal();
  auto hits_before = registry.GetStats().deduplication_hits.GetTotal();

  // First track -> miss
  auto record1 =
      registry.DedupConstruct(key, attr, vec_view, 1.0f, allocator.get());

  EXPECT_EQ(registry.GetStats().deduplication_misses.GetTotal(),
            misses_before + 1);
  EXPECT_EQ(registry.GetStats().deduplication_hits.GetTotal(), hits_before);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Second track with SAME data -> hit
  auto record2 =
      registry.DedupConstruct(key, attr, vec_view, 1.0f, allocator.get());

  EXPECT_EQ(registry.GetStats().deduplication_misses.GetTotal(),
            misses_before + 1);
  EXPECT_EQ(registry.GetStats().deduplication_hits.GetTotal(), hits_before + 1);

  // Third track with DIFFERENT data -> miss, and overrides old record
  std::vector<float> vec_data2 = {2.0f, 3.0f, 4.0f, 5.0f};
  absl::string_view vec_view2(reinterpret_cast<const char *>(vec_data2.data()),
                              vec_data2.size() * sizeof(float));

  auto record3 =
      registry.DedupConstruct(key, attr, vec_view2, 1.0f, allocator.get());

  EXPECT_EQ(registry.GetStats().deduplication_misses.GetTotal(),
            misses_before + 2);
  EXPECT_EQ(registry.GetStats().deduplication_hits.GetTotal(), hits_before + 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

TEST_F(VectorRegistryTest, UntrackKeepsWeakPtrIfShared) {
  UniqueFixedSizeAllocatorPtr allocator = CREATE_UNIQUE_PTR(
      FixedSizeAllocator, sizeof(indexes::VectorRecord) + 4 * sizeof(float),
      true);
  auto &registry = VectorRegistry::Instance();

  auto key = StringInternStore::Intern("doc_key_untrack");
  std::string attr = "vec_attr_untrack";

  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  absl::string_view vec_view(reinterpret_cast<const char *>(vec_data.data()),
                             vec_data.size() * sizeof(float));

  auto record =
      registry.DedupConstruct(key, attr, vec_view, 1.0f, allocator.get());

  // Untrack(attribute_deleted=false)
  registry.UntrackExpired(key, attr);

  // The entry should still be there, AND the weak_ptr should still be valid!
  EXPECT_EQ(TrackedSize(registry), 1);

  // If we try to DedupConstruct the same data, it should be a hit (weak_ptr was
  // NOT reset)!
  auto hits_before = registry.GetStats().deduplication_hits.GetTotal();
  auto record2 =
      registry.DedupConstruct(key, attr, vec_view, 1.0f, allocator.get());
  EXPECT_EQ(registry.GetStats().deduplication_hits.GetTotal(), hits_before + 1);
}

TEST_F(VectorRegistryTest, UntrackDeletesIfAttributeDeleted) {
  UniqueFixedSizeAllocatorPtr allocator = CREATE_UNIQUE_PTR(
      FixedSizeAllocator, sizeof(indexes::VectorRecord) + 4 * sizeof(float),
      true);
  auto &registry = VectorRegistry::Instance();

  auto key = StringInternStore::Intern("doc_key_delete");
  std::string attr = "vec_attr_delete";

  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  absl::string_view vec_view(reinterpret_cast<const char *>(vec_data.data()),
                             vec_data.size() * sizeof(float));

  auto record =
      registry.DedupConstruct(key, attr, vec_view, 1.0f, allocator.get());

  // Erase it by untracking
  registry.Untrack(key, attr);

  // The entry should be gone entirely
  EXPECT_EQ(TrackedSize(registry), 0);
}

TEST_F(VectorRegistryTest, EngineShareTest) {
  auto allocator = CREATE_UNIQUE_PTR(
      FixedSizeAllocator, sizeof(indexes::VectorRecord) + 4 * sizeof(float),
      true);
  auto &registry = VectorRegistry::Instance();

  auto key = StringInternStore::Intern("doc_key_share");
  std::string attr = "vec_attr_share";
  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  absl::string_view vec_view(reinterpret_cast<const char *>(vec_data.data()),
                             vec_data.size() * sizeof(float));

  auto record =
      registry.DedupConstruct(key, attr, vec_view, 1.0f, allocator.get());
  EXPECT_NE(record, nullptr);

  // By default it should not be pending
  EXPECT_EQ(registry.GetStats().hash_extern_errors.GetTotal(), 0);

  bool old_hash_reg = GetHashRegistrationSupported(registry);
  SetHashRegistrationSupported(registry, true);

  std::vector<std::pair<ValkeyModuleEventLoopOneShotFunc, void *>> pending_cbs;
  EXPECT_CALL(*kMockValkeyModule, EventLoopAddOneShot(testing::_, testing::_))
      .WillOnce([&pending_cbs](ValkeyModuleEventLoopOneShotFunc callback,
                               void *data) -> int {
        pending_cbs.push_back({callback, data});
        return 0;
      });

  bool success = false;
  std::thread t([&]() {
    success = registry.EngineShare(
        key, attr, record, vec_view.size(),
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  });
  t.join();
  EXPECT_TRUE(success);

  for (auto &p : pending_cbs) {
    p.first(p.second);
  }

  SetHashRegistrationSupported(registry, old_hash_reg);
}

TEST_F(VectorRegistryTest, UntrackExpiredRemovesWhenUnreferenced) {
  UniqueFixedSizeAllocatorPtr allocator = CREATE_UNIQUE_PTR(
      FixedSizeAllocator, sizeof(indexes::VectorRecord) + 4 * sizeof(float),
      true);
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("doc_key_expire");

  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  absl::string_view vec_view(reinterpret_cast<const char *>(vec_data.data()),
                             16);

  bool old_hash_reg = GetHashRegistrationSupported(registry);
  SetHashRegistrationSupported(registry, false);

  auto record =
      registry.DedupConstruct(key, "attr", vec_view, 1.0f, allocator.get());
  EXPECT_EQ(TrackedSize(registry), 1);

  // Drop our strong reference so the weak_ptr expires
  record.reset();

  // Because the weak_ptr is expired, this should successfully erase it
  registry.UntrackExpired(key, "attr");
  EXPECT_EQ(TrackedSize(registry), 0);

  SetHashRegistrationSupported(registry, old_hash_reg);
}

TEST_F(VectorRegistryTest, EngineShareIgnoresExpiredPendingVectors) {
  UniqueFixedSizeAllocatorPtr allocator = CREATE_UNIQUE_PTR(
      FixedSizeAllocator, sizeof(indexes::VectorRecord) + 4 * sizeof(float),
      true);
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("doc_key_async_expire");
  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  absl::string_view vec_view(reinterpret_cast<const char *>(vec_data.data()),
                             16);

  bool old_hash_reg = GetHashRegistrationSupported(registry);
  SetHashRegistrationSupported(registry, true);

  // Capture the pending callback
  std::vector<std::pair<ValkeyModuleEventLoopOneShotFunc, void *>> pending_cbs;
  EXPECT_CALL(*kMockValkeyModule, EventLoopAddOneShot(testing::_, testing::_))
      .WillOnce(
          [&pending_cbs](ValkeyModuleEventLoopOneShotFunc cb, void *data) {
            pending_cbs.push_back({cb, data});
            return 0;
          });

  {
    auto record =
        registry.DedupConstruct(key, "attr", vec_view, 1.0f, allocator.get());
    registry.EngineShare(
        key, "attr", record, vec_view.size(),
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  }
  // 'record' goes out of scope and expires here!

  // Now simulate the main thread executing the async callback
  for (auto &p : pending_cbs) {
    p.first(p.second);
  }

  // It should safely ignore the expired vector and not crash.
  EXPECT_EQ(registry.GetStats().hash_extern_errors.GetTotal(), 0);

  SetHashRegistrationSupported(registry, old_hash_reg);
}

TEST_F(VectorRegistryTest, UntrackForcefullyRemoves) {
  UniqueFixedSizeAllocatorPtr allocator = CREATE_UNIQUE_PTR(
      FixedSizeAllocator, sizeof(indexes::VectorRecord) + 4 * sizeof(float),
      true);
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("doc_key_force_remove");

  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  absl::string_view vec_view(reinterpret_cast<const char *>(vec_data.data()),
                             16);

  auto record =
      registry.DedupConstruct(key, "attr", vec_view, 1.0f, allocator.get());

  // Even though 'record' is still alive and holding a strong reference,
  // Untrack should forcefully erase it from the registry.
  registry.Untrack(key, "attr");

  EXPECT_EQ(TrackedSize(registry), 0);
  EXPECT_NE(record, nullptr);
}

TEST_F(VectorRegistryTest, EngineShareNoOpWhenHashRegistrationNotSupported) {
  UniqueFixedSizeAllocatorPtr allocator = CREATE_UNIQUE_PTR(
      FixedSizeAllocator, sizeof(indexes::VectorRecord) + 4 * sizeof(float),
      true);
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("doc_key_no_hash_reg");

  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  absl::string_view vec_view(reinterpret_cast<const char *>(vec_data.data()),
                             16);

  // Set hash_registration_supported_ to false
  bool old_hash_reg = GetHashRegistrationSupported(registry);
  SetHashRegistrationSupported(registry, false);

  auto record =
      registry.DedupConstruct(key, "attr", vec_view, 1.0f, allocator.get());

  // Ensure no one-shots are lingering
  kMockValkeyModule->one_shots.clear();

  // Calling EngineShare should return false and NOT enqueue anything
  bool success = registry.EngineShare(
      key, "attr", record, vec_view.size(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  EXPECT_FALSE(success);

  // We can verify it was a no-op because no one-shots were queued to the mock
  // module
  EXPECT_EQ(kMockValkeyModule->one_shots.size(), 0);

  SetHashRegistrationSupported(registry, old_hash_reg);
}

}  // namespace valkey_search
