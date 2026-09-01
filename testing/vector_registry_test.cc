/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/vector_registry.h"

#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search {

using ::testing::Return;

class VectorRegistryTest : public ValkeySearchTest {
 protected:
  void SetUp() override {
    ValkeySearchTest::SetUp();
    SetHashRegistrationSupported(VectorRegistry::Instance(), true);
  }
  void SetHashRegistrationSupported(VectorRegistry &registry, bool supported) {
    registry.hash_vector_sharing_ = supported;
  }
  void InitRegistry(VectorRegistry &registry, ValkeyModuleCtx *ctx) {
    registry.Init(ctx);
  }
};

TEST_F(VectorRegistryTest, DedupOrConstructBasicAndDedup) {
  auto &registry = VectorRegistry::Instance();
  auto key1 = StringInternStore::Intern("key1");
  auto attr1 = StringInternStore::Intern("attr1");
  MockIndex index1(4, attr1->Str(), 0);
  MockIndex index2(4, attr1->Str(), 0);

  EXPECT_EQ(registry.GetStats().entry_cnt, 0);

  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  // 1. Initial DedupOrConstruct constructs a new record
  auto rec1 = registry.DedupOrConstruct(
      key1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index1);
  EXPECT_NE(rec1, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().dedup_cnt.GetTotal(), 0);

  // 2. DedupOrConstruct with identical content deduplicates and returns
  // matching record
  auto rec2 = registry.DedupOrConstruct(
      key1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index2);
  EXPECT_EQ(rec1, rec2);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().dedup_cnt.GetTotal(), 1);
}

TEST_F(VectorRegistryTest, PayloadChangeReplacesRecord) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("key_mutate");
  auto attr = StringInternStore::Intern("attr");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec1 = {1.0f, 2.0f};
  std::string vec1_str(reinterpret_cast<const char *>(vec1.data()),
                       vec1.size() * sizeof(float));
  auto valkey_vec1 = vmsdk::MakeUniqueValkeyString(vec1_str);

  auto rec1 = registry.DedupOrConstruct(
      key, valkey_vec1.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec1, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().dedup_cnt.GetTotal(), 0);

  // Ingest mutated payload
  std::vector<float> vec2 = {3.0f, 4.0f};
  std::string vec2_str(reinterpret_cast<const char *>(vec2.data()),
                       vec2.size() * sizeof(float));
  auto valkey_vec2 = vmsdk::MakeUniqueValkeyString(vec2_str);

  auto rec2 = registry.DedupOrConstruct(
      key, valkey_vec2.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec2, nullptr);
  EXPECT_NE(rec1, rec2);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().dedup_cnt.GetTotal(), 0);

  // Subsequent call with vec2 deduplicates
  auto rec2_dup = registry.DedupOrConstruct(
      key, valkey_vec2.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_EQ(rec2, rec2_dup);
  EXPECT_EQ(registry.GetStats().dedup_cnt.GetTotal(), 1);
}

TEST_F(VectorRegistryTest, DedupOrConstructWithNullptr) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("key_null");
  auto attr = StringInternStore::Intern("attr");
  MockIndex index(2, attr->Str(), 0);

  auto rec = registry.DedupOrConstruct(
      key, nullptr, data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0,
      &index);
  EXPECT_EQ(rec, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, DedupOrConstructWithInvalidSize) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("key_bad_size");
  auto attr = StringInternStore::Intern("attr");
  MockIndex index(2, attr->Str(), 0);

  // Index expects 2 floats (8 bytes), provide 3 floats (12 bytes)
  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  auto rec = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec, nullptr);
  EXPECT_EQ(rec.size, vec_str.size());
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, OverwriteWithNullptrErasesTrackedRecord) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("key_overwrite_null");
  auto attr = StringInternStore::Intern("attr");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  auto rec = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Overwriting with nullptr erases from tracked_vectors_
  auto rec_null = registry.DedupOrConstruct(
      key, nullptr, data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0,
      &index);
  EXPECT_EQ(rec_null, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, OverwriteWithEmptyStringErasesTrackedRecord) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("key_overwrite_empty");
  auto attr = StringInternStore::Intern("attr");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  auto rec = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Overwriting with empty string erases from tracked_vectors_
  auto empty_vec = vmsdk::MakeUniqueValkeyString("");
  auto rec_empty = registry.DedupOrConstruct(
      key, empty_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_EQ(rec_empty, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, OverwriteWithInvalidSizeRetainsWhenShared) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("key_shared_invalid_size");
  auto attr = StringInternStore::Intern("attr");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  auto rec = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // When field is currently shared (HashHasStringRef == 1), should NOT erase
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(Return(1));

  std::vector<float> bad_data = {1.0f, 2.0f, 3.0f};
  std::string bad_str(reinterpret_cast<const char *>(bad_data.data()),
                      bad_data.size() * sizeof(float));
  auto bad_vec = vmsdk::MakeUniqueValkeyString(bad_str);

  auto rec_bad = registry.DedupOrConstruct(
      key, bad_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec_bad, nullptr);
  EXPECT_EQ(rec_bad.size, bad_str.size());
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

TEST_F(VectorRegistryTest, OverwriteWithInvalidSizeErasesWhenNonShared) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("key_non_shared_invalid_size");
  auto attr = StringInternStore::Intern("attr");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  auto rec = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // When field is NOT shared (HashHasStringRef == 0), ShouldEraseTrackedRecord
  // erases
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(Return(0));

  std::vector<float> bad_data = {1.0f, 2.0f, 3.0f};
  std::string bad_str(reinterpret_cast<const char *>(bad_data.data()),
                      bad_data.size() * sizeof(float));
  auto bad_vec = vmsdk::MakeUniqueValkeyString(bad_str);

  auto rec_bad = registry.DedupOrConstruct(
      key, bad_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec_bad, nullptr);
  EXPECT_EQ(rec_bad.size, bad_str.size());
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, JsonVectorTrackingAndDeduplication) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("json_doc1");
  auto attr = StringInternStore::Intern("json_vec");
  MockIndex index(4, attr->Str(), 0);

  std::vector<float> vec_data = {1.5f, 2.5f, 3.5f, 4.5f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  // JSON attributes track and deduplicate in VectorRegistry
  auto rec1 = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON, 0, &index);
  EXPECT_NE(rec1, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(), 0);

  auto rec2 = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON, 0, &index);
  EXPECT_EQ(rec1, rec2);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().dedup_cnt.GetTotal(), 1);
  // Hash sharing is not attempted for JSON
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(), 0);

  // Overwriting JSON key with invalid size evicts from tracked_vectors_
  std::vector<float> bad_data = {1.0f, 2.0f};
  std::string bad_str(reinterpret_cast<const char *>(bad_data.data()),
                      bad_data.size() * sizeof(float));
  auto bad_vec = vmsdk::MakeUniqueValkeyString(bad_str);
  auto rec_bad = registry.DedupOrConstruct(
      key, bad_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON, 0, &index);
  EXPECT_NE(rec_bad, nullptr);
  EXPECT_EQ(rec_bad.size, bad_str.size());
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, ShareWithValkeyAlreadySharedNoOp) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("key_already_shared");
  auto attr = StringInternStore::Intern("attr");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
  // HashHasStringRef returns 1 (already holding string ref)
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(Return(1));
  // HashSetStringRef must NOT be called

  auto rec = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(), 0);
}

TEST_F(VectorRegistryTest, VectorSharingDisabled) {
  auto &registry = VectorRegistry::Instance();
  SetHashRegistrationSupported(registry, false);

  auto key = StringInternStore::Intern("key_disabled");
  auto attr = StringInternStore::Intern("attr");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  // When sharing is disabled, DedupOrConstruct still tracks and deduplicates in
  // VectorRegistry, but does not share the string reference with Valkey.
  auto rec1 = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec1, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(), 0);

  auto rec2 = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec2, nullptr);
  EXPECT_EQ(rec1, rec2);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().dedup_cnt.GetTotal(), 1);
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(), 0);
}

TEST_F(VectorRegistryTest, ShareWithValkeyOpenKeyFails) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("key_open_fail");
  auto attr = StringInternStore::Intern("attr");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillOnce(Return(nullptr));

  auto rec = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(), 0);
}

TEST_F(VectorRegistryTest, ShareWithValkeyHasStringRefFails) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("key_no_strref");
  auto attr = StringInternStore::Intern("attr");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(Return(VALKEYMODULE_ERR));

  auto rec = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(), 0);
}

TEST_F(VectorRegistryTest, ShareWithValkeySetStringRefFails) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("key_set_fail");
  auto attr = StringInternStore::Intern("attr");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(Return(VALKEYMODULE_OK));
  EXPECT_CALL(*kMockValkeyModule,
              HashSetStringRef(testing::_, testing::_, testing::_, testing::_))
      .WillOnce(Return(VALKEYMODULE_ERR));

  auto rec = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().hash_sharing_errors.GetTotal(), 1);
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(), 0);
}

TEST_F(VectorRegistryTest, ShareWithValkeySuccess) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("key_share_ok");
  auto attr = StringInternStore::Intern("attr");
  MockIndex index(4, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(Return(VALKEYMODULE_OK));
  EXPECT_CALL(*kMockValkeyModule,
              HashSetStringRef(testing::_, testing::_, testing::_, testing::_))
      .WillOnce(Return(VALKEYMODULE_OK));

  auto rec = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().hash_sharing_hits.GetTotal(), 1);
  EXPECT_EQ(registry.GetStats().hash_sharing_errors.GetTotal(), 0);
}

TEST_F(VectorRegistryTest, NoCollisionBetweenDifferentDBs) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("common_key");
  auto attr = StringInternStore::Intern("attr");
  MockIndex index_db0(2, attr->Str(), 0);
  MockIndex index_db1(2, attr->Str(), 1);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  auto rec_db0 = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index_db0);
  auto rec_db1 = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 1, &index_db1);

  EXPECT_NE(rec_db0, nullptr);
  EXPECT_NE(rec_db1, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 2);
}

TEST_F(VectorRegistryTest, ForceHashSharingErrorFallback) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("key_forced_err");
  auto attr = StringInternStore::Intern("attr");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  VMSDK_EXPECT_OK(vmsdk::debug::ControlledSet("ForceHashSharingError", "1"));
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(Return(VALKEYMODULE_OK));

  auto rec = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().hash_sharing_errors.GetTotal(), 1);
  VMSDK_EXPECT_OK(vmsdk::debug::ControlledSet("ForceHashSharingError", "0"));
}

TEST_F(VectorRegistryTest, MultipleIndexesShareRecordViaDedup) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("multi_idx_key");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index1(4, attr->Str(), 0);
  MockIndex index2(4, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_OK));
  EXPECT_CALL(*kMockValkeyModule,
              HashSetStringRef(testing::_, testing::_, testing::_, testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_OK));

  // 1. First index creates/tracks the record
  auto rec1 = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index1);
  ASSERT_NE(rec1, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().dedup_cnt.GetTotal(), 0);

  // 2. Second index dedups and gets the exact same record pointer
  auto rec2 = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index2);
  ASSERT_NE(rec2, nullptr);
  EXPECT_EQ(rec1, rec2);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().dedup_cnt.GetTotal(), 1);

  // 3. First index drops its reference; second index's record remains fully
  // valid
  rec1.vector_record.reset();
  EXPECT_NE(rec2, nullptr);
  EXPECT_EQ(rec2, vec_str);
}

TEST_F(VectorRegistryTest, SameKeyDifferentAttributes) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("doc_multi_attr");
  auto attr1 = StringInternStore::Intern("vec_title");
  auto attr2 = StringInternStore::Intern("vec_body");
  MockIndex index_title(2, attr1->Str(), 0);
  MockIndex index_body(2, attr2->Str(), 0);

  std::vector<float> vec1_data = {1.0f, 2.0f};
  std::string vec1_str(reinterpret_cast<const char *>(vec1_data.data()),
                       vec1_data.size() * sizeof(float));
  auto valkey_vec1 = vmsdk::MakeUniqueValkeyString(vec1_str);

  std::vector<float> vec2_data = {3.0f, 4.0f};
  std::string vec2_str(reinterpret_cast<const char *>(vec2_data.data()),
                       vec2_data.size() * sizeof(float));
  auto valkey_vec2 = vmsdk::MakeUniqueValkeyString(vec2_str);

  auto rec1 = registry.DedupOrConstruct(
      key, valkey_vec1.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index_title);
  auto rec2 = registry.DedupOrConstruct(
      key, valkey_vec2.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index_body);

  EXPECT_NE(rec1, nullptr);
  EXPECT_NE(rec2, nullptr);
  EXPECT_NE(rec1, rec2);
  EXPECT_EQ(registry.GetStats().entry_cnt, 2);
}

TEST_F(VectorRegistryTest,
       DifferentVectorDimensionsReplacesRecordWithoutOverread) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("doc_resize");
  auto attr = StringInternStore::Intern("vec_field");
  MockIndex index_small(2, attr->Str(), 0);
  MockIndex index_large(8, attr->Str(), 0);

  std::vector<float> small_vec = {1.0f, 2.0f};
  std::string small_vec_str(reinterpret_cast<const char *>(small_vec.data()),
                            small_vec.size() * sizeof(float));
  auto valkey_small = vmsdk::MakeUniqueValkeyString(small_vec_str);

  auto rec_small = registry.DedupOrConstruct(
      key, valkey_small.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index_small);
  ASSERT_NE(rec_small, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Ingest larger vector into same key & attribute
  std::vector<float> large_vec = {1.0f, 2.0f, 3.0f, 4.0f,
                                  5.0f, 6.0f, 7.0f, 8.0f};
  std::string large_vec_str(reinterpret_cast<const char *>(large_vec.data()),
                            large_vec.size() * sizeof(float));
  auto valkey_large = vmsdk::MakeUniqueValkeyString(large_vec_str);

  auto rec_large = registry.DedupOrConstruct(
      key, valkey_large.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index_large);
  ASSERT_NE(rec_large, nullptr);
  EXPECT_NE(rec_small, rec_large);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().dedup_cnt.GetTotal(), 0);
}

TEST_F(VectorRegistryTest, FlushDBPreservesOtherDBEntries) {
  auto &registry = VectorRegistry::Instance();
  auto key1 = StringInternStore::Intern("doc1");
  auto key2 = StringInternStore::Intern("doc2");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index_db0(2, attr->Str(), 0);
  MockIndex index_db1(2, attr->Str(), 1);

  std::vector<float> vec = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec.data()),
                      vec.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  registry.DedupOrConstruct(
      key1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index_db0);
  registry.DedupOrConstruct(
      key2, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 1, &index_db1);

  EXPECT_EQ(registry.GetStats().entry_cnt, 2);

  // Flush DB 0 only
  ValkeyModuleFlushInfo flush_db0{.version = 1, .sync = 0, .dbnum = 0};
  registry.OnFlushDB(&flush_db0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Flush DB 1
  ValkeyModuleFlushInfo flush_db1{.version = 1, .sync = 0, .dbnum = 1};
  registry.OnFlushDB(&flush_db1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, FlushDBAllClearsAllEntries) {
  auto &registry = VectorRegistry::Instance();
  auto key1 = StringInternStore::Intern("doc1");
  auto key2 = StringInternStore::Intern("doc2");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index_db0(2, attr->Str(), 0);
  MockIndex index_db1(2, attr->Str(), 1);

  std::vector<float> vec = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec.data()),
                      vec.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  registry.DedupOrConstruct(
      key1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index_db0);
  registry.DedupOrConstruct(
      key2, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 1, &index_db1);

  EXPECT_EQ(registry.GetStats().entry_cnt, 2);

  // Flush ALL databases (dbnum = -1)
  ValkeyModuleFlushInfo flush_all{.version = 1, .sync = 0, .dbnum = -1};
  registry.OnFlushDB(&flush_all);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, SwapDBExchangesDBEntries) {
  auto &registry = VectorRegistry::Instance();
  auto key1 = StringInternStore::Intern("doc1");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index_db0(2, attr->Str(), 0);

  std::vector<float> vec = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec.data()),
                      vec.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  registry.DedupOrConstruct(
      key1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index_db0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Swap DB 0 and DB 1
  ValkeyModuleSwapDbInfo swap_info{
      .version = 1, .dbnum_first = 0, .dbnum_second = 1};
  registry.OnSwapDB(&swap_info);

  // Flush DB 0 - the entry has moved to DB 1, so flushing DB 0 preserves it
  ValkeyModuleFlushInfo flush_db0{.version = 1, .sync = 0, .dbnum = 0};
  registry.OnFlushDB(&flush_db0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Flush DB 1 - cleans up the swapped entry
  ValkeyModuleFlushInfo flush_db1{.version = 1, .sync = 0, .dbnum = 1};
  registry.OnFlushDB(&flush_db1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, RenameToIndexedKeyClaimsPendingRecord) {
  auto &registry = VectorRegistry::Instance();
  auto key_src = StringInternStore::Intern("doc_src");
  auto key_dst = vmsdk::MakeUniqueValkeyString("doc_dst");
  auto interned_dst = StringInternStore::Intern("doc_dst");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  auto rec_src = registry.DedupOrConstruct(
      key_src, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_NE(rec_src, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Move source key to destination key
  absl::flat_hash_map<InternedStringPtr, VectorRegistry::Action> actions = {
      {attr, VectorRegistry::Action::kMove}};
  registry.MoveKey(0, key_src, 0, key_dst.get(), actions);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Ingest destination key with matching vector payload (hits dedup)
  auto rec_dst = registry.DedupOrConstruct(
      interned_dst, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_NE(rec_dst, nullptr);
  EXPECT_EQ(rec_src.vector_record, rec_dst.vector_record);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

TEST_F(VectorRegistryTest, RenameToUnindexedKeyMaterializesVector) {
  auto &registry = VectorRegistry::Instance();
  auto key_src = StringInternStore::Intern("doc_src");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  auto rec_src = registry.DedupOrConstruct(
      key_src, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_NE(rec_src, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  auto key_dst = vmsdk::MakeUniqueValkeyString("outside:1");
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillOnce(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(Return(1));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .WillOnce(Return(VALKEYMODULE_OK));

  // Rename source key to unindexed key (to_unshare)
  absl::flat_hash_map<InternedStringPtr, VectorRegistry::Action> actions = {
      {attr, VectorRegistry::Action::kUnshare}};
  registry.MoveKey(0, key_src, 0, key_dst.get(), actions);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, RenameToUnindexedKeyNonHashOrMissingNoOp) {
  auto &registry = VectorRegistry::Instance();
  auto key_src = StringInternStore::Intern("doc_src");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  auto rec_src = registry.DedupOrConstruct(
      key_src, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_NE(rec_src, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  auto key_dst = vmsdk::MakeUniqueValkeyString("other_type:1");
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillOnce(Return(VALKEYMODULE_KEYTYPE_LIST));

  absl::flat_hash_map<InternedStringPtr, VectorRegistry::Action> actions = {
      {attr, VectorRegistry::Action::kUnshare}};
  registry.MoveKey(0, key_src, 0, key_dst.get(), actions);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, MoveAcrossDatabasesMovesRecord) {
  auto &registry = VectorRegistry::Instance();
  auto key_src = StringInternStore::Intern("doc_src");
  auto key_dst = vmsdk::MakeUniqueValkeyString("doc_dst");
  auto interned_dst = StringInternStore::Intern("doc_dst");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index_db0(2, attr->Str(), 0);
  MockIndex index_db1(2, attr->Str(), 1);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  auto rec_src = registry.DedupOrConstruct(
      key_src, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index_db0);
  ASSERT_NE(rec_src, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Move from db 0 to db 1
  absl::flat_hash_map<InternedStringPtr, VectorRegistry::Action> actions = {
      {attr, VectorRegistry::Action::kMove}};
  registry.MoveKey(0, key_src, 1, key_dst.get(), actions);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Ingest destination key in db 1
  auto rec_dst = registry.DedupOrConstruct(
      interned_dst, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 1, &index_db1);
  ASSERT_NE(rec_dst, nullptr);
  EXPECT_EQ(rec_src.vector_record, rec_dst.vector_record);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

TEST_F(VectorRegistryTest, MoveKeyMixedActionsInSingleBatch) {
  auto &registry = VectorRegistry::Instance();
  auto key_src = StringInternStore::Intern("doc_src");
  auto key_dst = vmsdk::MakeUniqueValkeyString("doc_dst");
  auto interned_dst = StringInternStore::Intern("doc_dst");
  auto attr1 = StringInternStore::Intern("vec1");
  auto attr2 = StringInternStore::Intern("vec2");
  MockIndex index1(2, attr1->Str(), 0);
  MockIndex index2(2, attr2->Str(), 0);

  std::vector<float> vec1_data = {1.0f, 2.0f};
  std::string vec1_str(reinterpret_cast<const char *>(vec1_data.data()),
                       vec1_data.size() * sizeof(float));
  auto valkey_vec1 = vmsdk::MakeUniqueValkeyString(vec1_str);

  std::vector<float> vec2_data = {3.0f, 4.0f};
  std::string vec2_str(reinterpret_cast<const char *>(vec2_data.data()),
                       vec2_data.size() * sizeof(float));
  auto valkey_vec2 = vmsdk::MakeUniqueValkeyString(vec2_str);

  auto rec1 = registry.DedupOrConstruct(
      key_src, valkey_vec1.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index1);
  auto rec2 = registry.DedupOrConstruct(
      key_src, valkey_vec2.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index2);
  ASSERT_NE(rec1, nullptr);
  ASSERT_NE(rec2, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 2);

  // Expect OpenKey for unsharing vec2
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly(Return(1));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .WillOnce(Return(VALKEYMODULE_OK));

  // Batch move: vec1 is moved, vec2 is unshared
  absl::flat_hash_map<InternedStringPtr, VectorRegistry::Action> actions = {
      {attr1, VectorRegistry::Action::kMove},
      {attr2, VectorRegistry::Action::kUnshare},
  };
  registry.MoveKey(0, key_src, 0, key_dst.get(), actions);

  // Entry count drops by 1 (vec2 was unshared and untracked, vec1 remains
  // tracked)
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Ingest destination key for vec1 claims tracked record
  auto rec_dst1 = registry.DedupOrConstruct(
      interned_dst, valkey_vec1.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index1);
  ASSERT_NE(rec_dst1, nullptr);
  EXPECT_EQ(rec1.vector_record, rec_dst1.vector_record);
}

TEST_F(VectorRegistryTest, MoveKeyDestinationOverwriteMove) {
  auto &registry = VectorRegistry::Instance();
  auto key_src = StringInternStore::Intern("doc_src");
  auto key_dst = vmsdk::MakeUniqueValkeyString("doc_dst");
  auto interned_dst = StringInternStore::Intern("doc_dst");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_src_data = {1.0f, 2.0f};
  std::string vec_src_str(reinterpret_cast<const char *>(vec_src_data.data()),
                          vec_src_data.size() * sizeof(float));
  auto valkey_src_vec = vmsdk::MakeUniqueValkeyString(vec_src_str);

  std::vector<float> vec_dst_data = {9.0f, 9.0f};
  std::string vec_dst_str(reinterpret_cast<const char *>(vec_dst_data.data()),
                          vec_dst_data.size() * sizeof(float));
  auto valkey_dst_vec = vmsdk::MakeUniqueValkeyString(vec_dst_str);

  auto rec_src = registry.DedupOrConstruct(
      key_src, valkey_src_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  auto rec_dst_old = registry.DedupOrConstruct(
      interned_dst, valkey_dst_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_EQ(registry.GetStats().entry_cnt, 2);

  // Overwrite doc_dst with doc_src
  absl::flat_hash_map<InternedStringPtr, VectorRegistry::Action> actions = {
      {attr, VectorRegistry::Action::kMove}};
  registry.MoveKey(0, key_src, 0, key_dst.get(), actions);

  // Still 1 entry after overwriting destination and removing source
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  auto rec_dst = registry.DedupOrConstruct(
      interned_dst, valkey_src_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_NE(rec_dst, nullptr);
  EXPECT_EQ(rec_src.vector_record, rec_dst.vector_record);
}

TEST_F(VectorRegistryTest, MoveKeyDestinationOverwriteUnshare) {
  auto &registry = VectorRegistry::Instance();
  auto key_src = StringInternStore::Intern("doc_src");
  auto key_dst = vmsdk::MakeUniqueValkeyString("doc_dst");
  auto interned_dst = StringInternStore::Intern("doc_dst");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  registry.DedupOrConstruct(
      key_src, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  registry.DedupOrConstruct(
      interned_dst, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_EQ(registry.GetStats().entry_cnt, 2);

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillOnce(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(Return(1));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .WillOnce(Return(VALKEYMODULE_OK));

  // Unsharing overwrites and purges destination tracked record
  absl::flat_hash_map<InternedStringPtr, VectorRegistry::Action> actions = {
      {attr, VectorRegistry::Action::kUnshare}};
  registry.MoveKey(0, key_src, 0, key_dst.get(), actions);

  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, MoveKeyEmptyActionsIsNoOp) {
  auto &registry = VectorRegistry::Instance();
  auto key_src = StringInternStore::Intern("doc_src");
  auto key_dst = vmsdk::MakeUniqueValkeyString("doc_dst");

  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .Times(0);
  registry.MoveKey(0, key_src, 0, key_dst.get(), {});
}

TEST_F(VectorRegistryTest, MoveKeyUntrackedSourceDbIsNoOp) {
  auto &registry = VectorRegistry::Instance();
  auto key_src = StringInternStore::Intern("doc_src");
  auto key_dst = vmsdk::MakeUniqueValkeyString("doc_dst");
  auto attr = StringInternStore::Intern("vec");

  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .Times(0);
  registry.MoveKey(999, key_src, 0, key_dst.get(),
                   {{attr, VectorRegistry::Action::kMove}});
}

TEST_F(VectorRegistryTest, MoveKeyUntrackedAttributeSkippedGracefully) {
  auto &registry = VectorRegistry::Instance();
  auto key_src = StringInternStore::Intern("doc_src");
  auto key_dst = vmsdk::MakeUniqueValkeyString("doc_dst");
  auto attr_tracked = StringInternStore::Intern("vec1");
  auto attr_untracked = StringInternStore::Intern("vec2");
  MockIndex index(2, attr_tracked->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  registry.DedupOrConstruct(
      key_src, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Request move for an attribute that is not tracked for doc_src
  absl::flat_hash_map<InternedStringPtr, VectorRegistry::Action> actions = {
      {attr_untracked, VectorRegistry::Action::kMove}};
  registry.MoveKey(0, key_src, 0, key_dst.get(), actions);

  // doc_src still has its tracked vec1
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

TEST_F(VectorRegistryTest, MoveKeySharingDisabledIsNoOp) {
  auto &registry = VectorRegistry::Instance();
  SetHashRegistrationSupported(registry, false);

  auto key_src = StringInternStore::Intern("doc_src");
  auto key_dst = vmsdk::MakeUniqueValkeyString("doc_dst");
  auto attr = StringInternStore::Intern("vec");

  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .Times(0);
  registry.MoveKey(0, key_src, 0, key_dst.get(),
                   {{attr, VectorRegistry::Action::kMove}});
}

TEST_F(VectorRegistryTest, RemoveIndexKeysRateLimiting) {
  auto &registry = VectorRegistry::Instance();
  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(2));

  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  std::vector<InternedStringPtr> keys;
  for (int i = 1; i <= 5; ++i) {
    auto key = StringInternStore::Intern(absl::StrCat("key", i));
    registry.DedupOrConstruct(
        key, valkey_vec.get(),
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
    keys.push_back(key);
  }
  EXPECT_EQ(registry.GetStats().entry_cnt, 5);

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly(Return(1));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .WillRepeatedly(Return(VALKEYMODULE_OK));

  // Dropping index with 5 keys, batch size = 2
  // Immediate slice: 2 keys processed, 3 remain pending
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 3);
  EXPECT_EQ(registry.GetStats().entry_cnt, 3);

  // Next cron tick processes up to 2 keys -> 1 remains pending
  size_t processed = registry.ProcessPendingUnshares(2);
  EXPECT_EQ(processed, 2);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Third cron tick processes the last key -> 0 remain pending
  processed = registry.ProcessPendingUnshares(2);
  EXPECT_EQ(processed, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);

  EXPECT_EQ(registry.ProcessPendingUnshares(2), 0);
  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1024));
}

TEST_F(VectorRegistryTest, RemoveIndexKeysOnServerCronCallback) {
  auto &registry = VectorRegistry::Instance();
  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(2));

  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  std::vector<InternedStringPtr> keys;
  for (int i = 1; i <= 3; ++i) {
    auto key = StringInternStore::Intern(absl::StrCat("key", i));
    registry.DedupOrConstruct(
        key, valkey_vec.get(),
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
    keys.push_back(key);
  }
  EXPECT_EQ(registry.GetStats().entry_cnt, 3);

  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Server cron triggers processing of the remaining key
  registry.OnServerCronCallback(nullptr, ValkeyModuleEvent_CronLoop, 0,
                                nullptr);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);

  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1024));
}

TEST_F(VectorRegistryTest, RemoveIndexKeysCancellationOnDedupOrConstruct) {
  auto &registry = VectorRegistry::Instance();
  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(2));

  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  std::vector<InternedStringPtr> keys;
  for (int i = 1; i <= 5; ++i) {
    auto key = StringInternStore::Intern(absl::StrCat("key", i));
    registry.DedupOrConstruct(
        key, valkey_vec.get(),
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
    keys.push_back(key);
  }

  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 3);

  // Re-insert one key that was pending unshare
  size_t before_dedup = registry.GetPendingUnsharesCount();
  for (const auto &k : keys) {
    registry.DedupOrConstruct(
        k, valkey_vec.get(),
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
    if (registry.GetPendingUnsharesCount() < before_dedup) {
      break;
    }
  }
  EXPECT_EQ(registry.GetPendingUnsharesCount(), before_dedup - 1);

  // Drain remaining pending unshares
  registry.ProcessPendingUnshares(10);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);

  // Re-inserted key remains tracked!
  EXPECT_GE(registry.GetStats().entry_cnt, 1);

  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1024));
}

TEST_F(VectorRegistryTest, RemoveIndexKeysCancellationOnErase) {
  auto &registry = VectorRegistry::Instance();
  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1));

  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  std::vector<InternedStringPtr> keys;
  for (int i = 1; i <= 3; ++i) {
    auto key = StringInternStore::Intern(absl::StrCat("k", i));
    registry.DedupOrConstruct(
        key, valkey_vec.get(),
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
    keys.push_back(key);
  }

  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 2);

  // Overwriting with nullptr cancels pending unshare and untracks
  size_t before_erase = registry.GetPendingUnsharesCount();
  for (const auto &k : keys) {
    registry.DedupOrConstruct(
        k, nullptr, data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0,
        &index);
    if (registry.GetPendingUnsharesCount() < before_erase) {
      break;
    }
  }
  EXPECT_EQ(registry.GetPendingUnsharesCount(), before_erase - 1);

  registry.ProcessPendingUnshares(10);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);

  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1024));
}

TEST_F(VectorRegistryTest, RemoveIndexKeysFlushDBClearsPending) {
  auto &registry = VectorRegistry::Instance();
  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1));

  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  auto k1 = StringInternStore::Intern("k1");
  auto k2 = StringInternStore::Intern("k2");
  registry.DedupOrConstruct(
      k1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  registry.DedupOrConstruct(
      k2, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);

  std::vector<InternedStringPtr> keys = {k1, k2};
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);

  ValkeyModuleFlushInfo flush_info{.version = 1, .sync = 0, .dbnum = 0};
  registry.OnFlushDB(&flush_info);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);

  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1024));
}

TEST_F(VectorRegistryTest, RemoveIndexKeysSwapDBMovesPending) {
  auto &registry = VectorRegistry::Instance();
  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1));

  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);

  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);

  auto k1 = StringInternStore::Intern("k1");
  auto k2 = StringInternStore::Intern("k2");
  registry.DedupOrConstruct(
      k1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  registry.DedupOrConstruct(
      k2, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);

  std::vector<InternedStringPtr> keys = {k1, k2};
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);

  ValkeyModuleSwapDbInfo swap_info{.dbnum_first = 0, .dbnum_second = 1};
  registry.OnSwapDB(&swap_info);

  // Pending count remains 1, now in DB 1
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);

  registry.ProcessPendingUnshares(10);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);

  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1024));
}

}  // namespace valkey_search
