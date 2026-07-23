/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/vector_registry.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/indexes/vector_base.h"
#include "src/indexes/vector_hnsw.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "testing/common.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search {

class VectorRegistryTest : public ValkeySearchTest {
 protected:
  void SetHashRegistrationSupported(VectorRegistry &registry, bool supported) {
    registry.hash_vector_sharing_ = supported;
  }
  bool GetHashRegistrationSupported(const VectorRegistry &registry) const {
    return registry.hash_vector_sharing_;
  }
  void InitRegistry(VectorRegistry &registry, ValkeyModuleCtx *ctx) {
    registry.Init(ctx);
  }
};

TEST_F(VectorRegistryTest, LookupRecordHitsAndMisses) {
  auto &registry = VectorRegistry::Instance();
  auto key1 = StringInternStore::Intern("key1");
  auto attr1 = StringInternStore::Intern("attr1");

  // Initial lookup should miss.
  auto [rec_miss, size_miss] = registry.LookupRecord(key1, attr1);
  EXPECT_EQ(rec_miss, nullptr);
  EXPECT_EQ(size_miss, 0);
  EXPECT_EQ(registry.GetStats().lookup_record_misses.GetTotal(), 1);
  EXPECT_EQ(registry.GetStats().lookup_record_hits.GetTotal(), 0);

  // Track a record.
  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  auto tracked_rec =
      registry.Track(key1, attr1, valkey_vec.get(), nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  ASSERT_NE(tracked_rec, nullptr);

  // Second lookup should hit.
  auto [rec_hit, size_hit] = registry.LookupRecord(key1, attr1);
  EXPECT_NE(rec_hit, nullptr);
  EXPECT_EQ(rec_hit, tracked_rec);
  EXPECT_EQ(size_hit, vec_str.size());
  EXPECT_EQ(registry.GetStats().lookup_record_hits.GetTotal(), 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

TEST_F(VectorRegistryTest, TrackDeduplication) {
  auto &registry = VectorRegistry::Instance();
  auto key1 = StringInternStore::Intern("key1");
  auto attr1 = StringInternStore::Intern("attr1");

  std::vector<float> vec_data = {0.5f, 1.5f, 2.5f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  auto rec1 =
      registry.Track(key1, attr1, valkey_vec.get(), nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  ASSERT_NE(rec1, nullptr);

  // Tracking the exact same data again for the same key and attribute returns
  // same instance.
  auto valkey_vec_dup = vmsdk::MakeUniqueValkeyString(vec_str);
  auto rec2 =
      registry.Track(key1, attr1, valkey_vec_dup.get(), nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  EXPECT_EQ(rec1, rec2);

  // Tracking modified data creates a new instance.
  std::vector<float> vec_data_mod = {0.5f, 1.5f, 9.9f};
  std::string vec_str_mod(reinterpret_cast<const char *>(vec_data_mod.data()),
                          vec_data_mod.size() * sizeof(float));
  auto valkey_vec_mod = vmsdk::MakeUniqueValkeyString(vec_str_mod);
  auto rec3 =
      registry.Track(key1, attr1, valkey_vec_mod.get(), nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  EXPECT_NE(rec1, rec3);
}

TEST_F(VectorRegistryTest, TrackUntrackWithNullptr) {
  auto &registry = VectorRegistry::Instance();
  auto key1 = StringInternStore::Intern("key1");
  auto attr1 = StringInternStore::Intern("attr1");

  std::vector<float> vec_data = {1.0f, 0.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  registry.Track(key1, attr1, valkey_vec.get(), nullptr,
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Untrack by passing nullptr vector.
  auto res =
      registry.Track(key1, attr1, nullptr, nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  EXPECT_EQ(res, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);

  auto [rec, size] = registry.LookupRecord(key1, attr1);
  EXPECT_EQ(rec, nullptr);
}

TEST_F(VectorRegistryTest, UntrackIfUnused) {
  auto &registry = VectorRegistry::Instance();
  auto key1 = StringInternStore::Intern("key1");
  auto attr1 = StringInternStore::Intern("attr1");

  std::vector<float> vec_data = {2.0f, 3.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  auto rec1 =
      registry.Track(key1, attr1, valkey_vec.get(), nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);

  // Local reference 'rec1' still exists, so use_count > 1.
  registry.UntrackIfUnused(key1, attr1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Drop local reference.
  rec1.reset();

  // Now registry holds the last reference (use_count == 1).
  registry.UntrackIfUnused(key1, attr1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, BatchUntrackIfUnused) {
  auto &registry = VectorRegistry::Instance();
  auto key1 = StringInternStore::Intern("key1");
  auto key2 = StringInternStore::Intern("key2");
  auto attr1 = StringInternStore::Intern("attr1");

  std::vector<float> vec_data = {1.0f, 1.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec1 = vmsdk::MakeUniqueValkeyString(vec_str);
  auto valkey_vec2 = vmsdk::MakeUniqueValkeyString(vec_str);

  auto rec1 =
      registry.Track(key1, attr1, valkey_vec1.get(), nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  auto rec2 =
      registry.Track(key2, attr1, valkey_vec2.get(), nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);

  EXPECT_EQ(registry.GetStats().entry_cnt, 2);

  // Keep rec1 held locally (use_count > 1), drop rec2 (use_count == 1).
  rec2.reset();

  InternedStringHashMap<indexes::TrackedKeyMetadata> batch_keys;
  batch_keys[key1] = {};
  batch_keys[key2] = {};

  registry.BatchUntrackIfUnused(attr1, std::move(batch_keys));

  // key1 remains because it has external reference; key2 is untracked.
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  auto [rec_k1, sz_k1] = registry.LookupRecord(key1, attr1);
  EXPECT_NE(rec_k1, nullptr);

  auto [rec_k2, sz_k2] = registry.LookupRecord(key2, attr1);
  EXPECT_EQ(rec_k2, nullptr);
}

TEST_F(VectorRegistryTest, TrackAttributeDataTypeJson) {
  auto &registry = VectorRegistry::Instance();
  auto key1 = StringInternStore::Intern("key_json");
  auto attr1 = StringInternStore::Intern("attr_json");

  std::vector<float> vec_data = {4.0f, 5.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  auto initial_hash_sharing_hits =
      registry.GetStats().hash_sharing_hits.GetTotal();

  auto rec =
      registry.Track(key1, attr1, valkey_vec.get(), nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON);
  ASSERT_NE(rec, nullptr);

  // JSON attributes track memory records but skip hash string reference
  // sharing.
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(),
            initial_hash_sharing_hits);

  auto [rec_lookup, size_lookup] = registry.LookupRecord(key1, attr1);
  EXPECT_EQ(rec_lookup, rec);
  EXPECT_EQ(size_lookup, vec_str.size());
}

TEST_F(VectorRegistryTest, HashSetStringRefNotAvailable) {
  auto &registry = VectorRegistry::Instance();
  auto key1 = StringInternStore::Intern("key_no_ref");
  auto attr1 = StringInternStore::Intern("attr_no_ref");

  // Temporarily disable hash registration support flag.
  bool original_supported = GetHashRegistrationSupported(registry);
  SetHashRegistrationSupported(registry, false);

  std::vector<float> vec_data = {7.0f, 8.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  auto initial_hash_sharing_hits =
      registry.GetStats().hash_sharing_hits.GetTotal();

  auto rec =
      registry.Track(key1, attr1, valkey_vec.get(), nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  ASSERT_NE(rec, nullptr);

  // Engine hash sharing is skipped when API support flag is false.
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(),
            initial_hash_sharing_hits);

  rec.reset();
  registry.UntrackIfUnused(key1, attr1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);

  // Restore original support state.
  SetHashRegistrationSupported(registry, original_supported);
}

TEST_F(VectorRegistryTest, VectorSharingDisabled) {
  auto &registry = VectorRegistry::Instance();
  auto key1 = StringInternStore::Intern("key_disabled");
  auto attr1 = StringInternStore::Intern("attr_disabled");

  // Disable vector sharing configuration option.
  auto &enable_sharing =
      const_cast<vmsdk::config::Boolean &>(options::GetEnableVectorSharing());
  VMSDK_EXPECT_OK(enable_sharing.SetValue(false));
  bool original_supported = GetHashRegistrationSupported(registry);
  InitRegistry(registry, &fake_ctx_);

  std::vector<float> vec_data = {1.2f, 3.4f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  auto initial_hash_sharing_hits =
      registry.GetStats().hash_sharing_hits.GetTotal();

  auto rec =
      registry.Track(key1, attr1, valkey_vec.get(), nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  ASSERT_NE(rec, nullptr);

  // When vector sharing option is disabled, hash sharing hits stay unchanged.
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(),
            initial_hash_sharing_hits);

  // Restore option and support state.
  VMSDK_EXPECT_OK(enable_sharing.SetValue(true));
  SetHashRegistrationSupported(registry, original_supported);
}

TEST_F(VectorRegistryTest, ShareWithValkeyHashOpenKeyFails) {
  auto &registry = VectorRegistry::Instance();
  SetHashRegistrationSupported(registry, true);
  auto key = StringInternStore::Intern("key_open_fail");
  auto attr1 = StringInternStore::Intern("attr1");
  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(nullptr));

  auto initial_hits = registry.GetStats().hash_sharing_hits.GetTotal();
  auto rec =
      registry.Track(key, attr1, valkey_vec.get(), nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  ASSERT_NE(rec, nullptr);
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(), initial_hits);
}

TEST_F(VectorRegistryTest, ShareWithValkeyHashHasStringRefFails) {
  auto &registry = VectorRegistry::Instance();
  SetHashRegistrationSupported(registry, true);
  auto key = StringInternStore::Intern("key_has_ref_fail");
  auto attr1 = StringInternStore::Intern("attr1");
  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(testing::Return(VALKEYMODULE_ERR));

  auto initial_hits = registry.GetStats().hash_sharing_hits.GetTotal();
  auto rec =
      registry.Track(key, attr1, valkey_vec.get(), nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  ASSERT_NE(rec, nullptr);
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(), initial_hits);
}

TEST_F(VectorRegistryTest, ShareWithValkeyHashGetNullRecord) {
  auto &registry = VectorRegistry::Instance();
  SetHashRegistrationSupported(registry, true);
  auto key = StringInternStore::Intern("key_get_null");
  auto attr1 = StringInternStore::Intern("attr1");
  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(testing::_, VALKEYMODULE_HASH_NONE, testing::_,
                      testing::An<ValkeyModuleString **>(),
                      testing::TypedEq<void *>(nullptr)))
      .WillOnce(testing::DoAll(testing::SetArgPointee<3>(nullptr),
                               testing::Return(VALKEYMODULE_OK)));

  auto initial_hits = registry.GetStats().hash_sharing_hits.GetTotal();
  auto rec =
      registry.Track(key, attr1, valkey_vec.get(), nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  ASSERT_NE(rec, nullptr);
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(), initial_hits);
}

TEST_F(VectorRegistryTest, ShareWithValkeyHashGetPayloadMismatch) {
  auto &registry = VectorRegistry::Instance();
  SetHashRegistrationSupported(registry, true);
  auto key = StringInternStore::Intern("key_payload_mismatch");
  auto attr1 = StringInternStore::Intern("attr1");
  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  std::vector<float> diff_data = {9.9f, 9.9f};
  std::string diff_str(reinterpret_cast<const char *>(diff_data.data()),
                       diff_data.size() * sizeof(float));
  auto diff_valkey_vec = vmsdk::MakeUniqueValkeyString(diff_str);

  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(testing::_, VALKEYMODULE_HASH_NONE, testing::_,
                      testing::An<ValkeyModuleString **>(),
                      testing::TypedEq<void *>(nullptr)))
      .WillOnce(
          testing::DoAll(testing::SetArgPointee<3>(diff_valkey_vec.release()),
                         testing::Return(VALKEYMODULE_OK)));

  auto initial_hits = registry.GetStats().hash_sharing_hits.GetTotal();
  auto rec =
      registry.Track(key, attr1, valkey_vec.get(), nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  ASSERT_NE(rec, nullptr);
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(), initial_hits);
}

TEST_F(VectorRegistryTest, ShareWithValkeyHashSetStringRefFails) {
  auto &registry = VectorRegistry::Instance();
  SetHashRegistrationSupported(registry, true);
  auto key = StringInternStore::Intern("key_set_ref_fail");
  auto attr1 = StringInternStore::Intern("attr1");
  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);
  auto match_valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(testing::_, VALKEYMODULE_HASH_NONE, testing::_,
                      testing::An<ValkeyModuleString **>(),
                      testing::TypedEq<void *>(nullptr)))
      .WillOnce(
          testing::DoAll(testing::SetArgPointee<3>(match_valkey_vec.release()),
                         testing::Return(VALKEYMODULE_OK)));
  EXPECT_CALL(*kMockValkeyModule,
              HashSetStringRef(testing::_, testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(VALKEYMODULE_ERR));

  auto initial_errors = registry.GetStats().hash_sharing_errors.GetTotal();
  auto rec =
      registry.Track(key, attr1, valkey_vec.get(), nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  ASSERT_NE(rec, nullptr);
  EXPECT_EQ(registry.GetStats().hash_sharing_errors.GetTotal(),
            initial_errors + 1);
}

TEST_F(VectorRegistryTest, ShareWithValkeyHashSuccess) {
  auto &registry = VectorRegistry::Instance();
  SetHashRegistrationSupported(registry, true);
  auto key = StringInternStore::Intern("key_success");
  auto attr1 = StringInternStore::Intern("attr1");
  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);
  auto match_valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(testing::_, VALKEYMODULE_HASH_NONE, testing::_,
                      testing::An<ValkeyModuleString **>(),
                      testing::TypedEq<void *>(nullptr)))
      .WillOnce(
          testing::DoAll(testing::SetArgPointee<3>(match_valkey_vec.release()),
                         testing::Return(VALKEYMODULE_OK)));
  EXPECT_CALL(*kMockValkeyModule,
              HashSetStringRef(testing::_, testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(VALKEYMODULE_OK));

  auto initial_hits = registry.GetStats().hash_sharing_hits.GetTotal();
  auto rec =
      registry.Track(key, attr1, valkey_vec.get(), nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  ASSERT_NE(rec, nullptr);
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(), initial_hits + 1);
}

TEST_F(VectorRegistryTest, ShareWithValkeyHashIdenticalVectorReTrack) {
  auto &registry = VectorRegistry::Instance();
  SetHashRegistrationSupported(registry, true);
  auto key = StringInternStore::Intern("key_identical_retrack");
  auto attr1 = StringInternStore::Intern("attr1");
  std::vector<float> vec_data = {3.14f, 2.71f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));

  auto valkey_vec1 = vmsdk::MakeUniqueValkeyString(vec_str);

  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .Times(2)
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .Times(2)
      .WillRepeatedly(testing::Return(VALKEYMODULE_OK));
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(testing::_, VALKEYMODULE_HASH_NONE, testing::_,
                      testing::An<ValkeyModuleString **>(),
                      testing::TypedEq<void *>(nullptr)))
      .Times(2)
      .WillRepeatedly([&vec_str](ValkeyModuleKey *, int, const char *,
                                 ValkeyModuleString **value_out, void *) {
        *value_out = vmsdk::MakeUniqueValkeyString(vec_str).release();
        return VALKEYMODULE_OK;
      });
  EXPECT_CALL(*kMockValkeyModule,
              HashSetStringRef(testing::_, testing::_, testing::_, testing::_))
      .Times(2)
      .WillRepeatedly(testing::Return(VALKEYMODULE_OK));

  auto initial_hits = registry.GetStats().hash_sharing_hits.GetTotal();

  // First Track call creates the VectorRecord and shares with Valkey Hash.
  auto rec1 =
      registry.Track(key, attr1, valkey_vec1.get(), nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  ASSERT_NE(rec1, nullptr);
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(), initial_hits + 1);

  // Second Track call with exact same vector data reuses existing VectorRecord
  // and STILL invokes ShareWithValkeyHash to ensure vector record is shared
  // with engine.
  auto valkey_vec2 = vmsdk::MakeUniqueValkeyString(vec_str);
  auto rec2 =
      registry.Track(key, attr1, valkey_vec2.get(), nullptr,
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  ASSERT_NE(rec2, nullptr);
  EXPECT_EQ(rec1, rec2);
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(), initial_hits + 2);
}

TEST_F(VectorRegistryTest,
       HnswVectorIndexReferenceCountOnIngestionAndMutation) {
  auto &registry = VectorRegistry::Instance();
  InitRegistry(registry, &fake_ctx_);

  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();

  auto dimensions = 100;
  auto hnsw_index = indexes::VectorHNSW<float>::Create(
      CreateHNSWVectorIndexProto(dimensions, data_model::DISTANCE_METRIC_COSINE,
                                 1000, 10, 300, 30),
      "vector", data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  ASSERT_TRUE(hnsw_index.ok());

  std::vector<absl::string_view> key_prefixes = {"prefix:"};
  auto index_schema_or = CreateIndexSchema(
      "vector_schema", &fake_ctx_, &mutations_thread_pool, &key_prefixes);
  ASSERT_TRUE(index_schema_or.ok());
  const auto &index_schema = index_schema_or.value();
  VMSDK_EXPECT_OK(index_schema->AddIndex("vector", "vector", *hnsw_index));

  auto key = StringInternStore::Intern("prefix:1");
  auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key->Str().data());
  auto attr_interned = StringInternStore::Intern("vector");

  // 1. Prepare initial vector data (100 float values for HNSW index)
  std::vector<float> vec_data(100, 1.0f);
  std::string vec_bytes(reinterpret_cast<const char *>(vec_data.data()),
                        vec_data.size() * sizeof(float));
  ValkeyModuleString *valkey_vec_str =
      vmsdk::MakeUniqueValkeyString(vec_bytes).release();

  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(TestValkeyModule_KeyTypeDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule,
              KeyType(vmsdk::ValkeyModuleKeyIsForString(key->Str())))
      .WillRepeatedly(testing::Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);

  EXPECT_CALL(*kMockValkeyModule,
              HashGet(vmsdk::ValkeyModuleKeyIsForString(key->Str()),
                      VALKEYMODULE_HASH_CFIELDS, testing::StrEq("vector"),
                      testing::An<ValkeyModuleString **>(),
                      testing::TypedEq<void *>(nullptr)))
      .WillOnce([valkey_vec_str](ValkeyModuleKey *, int, const char *,
                                 ValkeyModuleString **value_out, void *) {
        *value_out = valkey_vec_str;
        return VALKEYMODULE_OK;
      });

  // Ingestion of mutated key with attribute indexed as HNSW vector index
  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                       "hset", key_valkey_str.get());
  WaitWorkerTasksAreCompleted(mutations_thread_pool);

  // Reference count in vector registry should be 2 (1 in registry + 1 in HNSW
  // index). LookupRecord handle adds 1, resulting in use_count of 3.
  auto [rec1, size1] = registry.LookupRecord(key, attr_interned);
  ASSERT_NE(rec1, nullptr);
  EXPECT_EQ(rec1.use_count(), 3);
  EXPECT_EQ(size1, vec_bytes.size());
  EXPECT_EQ(absl::string_view(rec1->GetRawVector(), size1), vec_bytes);

  // 2. Modify vector attribute on key with new vector data
  std::vector<float> vec_data_mod(100, 2.0f);
  std::string vec_bytes_mod(reinterpret_cast<const char *>(vec_data_mod.data()),
                            vec_data_mod.size() * sizeof(float));
  ValkeyModuleString *valkey_vec_str_mod =
      vmsdk::MakeUniqueValkeyString(vec_bytes_mod).release();

  EXPECT_CALL(*kMockValkeyModule,
              HashGet(vmsdk::ValkeyModuleKeyIsForString(key->Str()),
                      VALKEYMODULE_HASH_CFIELDS, testing::StrEq("vector"),
                      testing::An<ValkeyModuleString **>(),
                      testing::TypedEq<void *>(nullptr)))
      .WillOnce([valkey_vec_str_mod](ValkeyModuleKey *, int, const char *,
                                     ValkeyModuleString **value_out, void *) {
        *value_out = valkey_vec_str_mod;
        return VALKEYMODULE_OK;
      });

  rec1.reset();  // Release local lookup handle

  // Ingestion completed for modified vector attribute
  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                       "hset", key_valkey_str.get());
  WaitWorkerTasksAreCompleted(mutations_thread_pool);

  // Vector registry indicates reference count of 2 again upon ingestion
  // completed, and matching modified vector payload
  auto [rec2, size2] = registry.LookupRecord(key, attr_interned);
  ASSERT_NE(rec2, nullptr);
  EXPECT_EQ(rec2.use_count(), 3);
  EXPECT_EQ(size2, vec_bytes_mod.size());
  EXPECT_EQ(absl::string_view(rec2->GetRawVector(), size2), vec_bytes_mod);
}

}  // namespace valkey_search
