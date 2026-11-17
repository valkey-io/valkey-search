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

#include "absl/strings/str_cat.h"
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
  bool TrackedNeedsMigration(VectorRegistry &registry, int db_num) {
    auto it = registry.tracked_vectors_.find(db_num);
    return it != registry.tracked_vectors_.end() &&
           it->second.keys.NeedsMigration();
  }
  vmsdk::UniqueValkeyString MakeFloatVectorString(
      const std::vector<float> &vec_data = {1.0f, 2.0f}) {
    std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                        vec_data.size() * sizeof(float));
    return vmsdk::MakeUniqueValkeyString(vec_str);
  }
  void ExpectMissingHashField(int open_key_flags) {
    EXPECT_CALL(*kMockValkeyModule,
                OpenKey(testing::_, testing::_, open_key_flags))
        .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
    if (open_key_flags & VALKEYMODULE_WRITE) {
      EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
          .Times(testing::AtMost(1))
          .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
    }
    EXPECT_CALL(*kMockValkeyModule,
                HashGet(testing::_, VALKEYMODULE_HASH_EXISTS, testing::_,
                        testing::An<int *>(), nullptr))
        .WillOnce(testing::DoAll(testing::SetArgPointee<3>(0),
                                 Return(VALKEYMODULE_OK)));
    EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
        .Times(0);
  }
  // Registers key's attr with a {1, 2} vector. The returned record holds a
  // reference, as an index tracking the key would.
  indexes::VectorRecordWithSize Register(absl::string_view key,
                                         const MockIndex &index,
                                         int db_num = 0) {
    auto vec = MakeFloatVectorString();
    return VectorRegistry::Instance().DedupOrConstruct(
        StringInternStore::Intern(key), vec.get(),
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, db_num,
        &index);
  }
  // Opening any key with the sweep's flags finds a hash.
  void ExpectSweepOpensHash() {
    EXPECT_CALL(*kMockValkeyModule,
                OpenKey(testing::_, testing::_,
                        VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
        .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
    EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
        .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  }
  void SendEvent(const char *event, absl::string_view key, int db_num = 0) {
    EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(testing::_))
        .WillRepeatedly(Return(db_num));
    auto key_str = vmsdk::MakeUniqueValkeyString(key);
    VectorRegistry::Instance().OnKeyspaceEvent(nullptr, event, key_str.get());
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
  EXPECT_EQ(registry.GetStats().dedup_cnt, 0);

  // 2. DedupOrConstruct with identical content deduplicates and returns
  // matching record
  auto rec2 = registry.DedupOrConstruct(
      key1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index2);
  EXPECT_EQ(rec1, rec2);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().dedup_cnt, 1);
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
  EXPECT_EQ(registry.GetStats().dedup_cnt, 0);

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
  EXPECT_EQ(registry.GetStats().dedup_cnt, 0);

  // Subsequent call with vec2 deduplicates
  auto rec2_dup = registry.DedupOrConstruct(
      key, valkey_vec2.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_EQ(rec2, rec2_dup);
  EXPECT_EQ(registry.GetStats().dedup_cnt, 1);
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
  EXPECT_EQ(registry.GetStats().hash_sharing_hits, 0);

  auto rec2 = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON, 0, &index);
  EXPECT_EQ(rec1, rec2);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().dedup_cnt, 1);
  // Hash sharing is not attempted for JSON
  EXPECT_EQ(registry.GetStats().hash_sharing_hits, 0);

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
  EXPECT_EQ(registry.GetStats().hash_sharing_hits, 0);
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
  EXPECT_EQ(registry.GetStats().hash_sharing_hits, 0);

  auto rec2 = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec2, nullptr);
  EXPECT_EQ(rec1, rec2);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().dedup_cnt, 1);
  EXPECT_EQ(registry.GetStats().hash_sharing_hits, 0);
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
  EXPECT_EQ(registry.GetStats().hash_sharing_hits, 0);
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
  EXPECT_EQ(registry.GetStats().hash_sharing_hits, 0);
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
  EXPECT_EQ(registry.GetStats().hash_sharing_errors, 1);
  EXPECT_EQ(registry.GetStats().hash_sharing_hits, 0);
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
  EXPECT_EQ(registry.GetStats().hash_sharing_hits, 1);
  EXPECT_EQ(registry.GetStats().hash_sharing_errors, 0);
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
  EXPECT_EQ(registry.GetStats().hash_sharing_errors, 1);
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
  EXPECT_EQ(registry.GetStats().dedup_cnt, 0);

  // 2. Second index dedups and gets the exact same record pointer
  auto rec2 = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index2);
  ASSERT_NE(rec2, nullptr);
  EXPECT_EQ(rec1, rec2);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().dedup_cnt, 1);

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
  EXPECT_EQ(registry.GetStats().dedup_cnt, 0);
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

TEST_F(VectorRegistryTest, RenameMovesEntryToDestination) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto rec_src = Register("doc_src", index);
  ASSERT_NE(rec_src, nullptr);

  // A move never opens the key: the hash moves with its shared field.
  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .Times(0);
  registry.MoveKey(0, "doc_src", 0, "doc_dst");
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  testing::Mock::VerifyAndClearExpectations(kMockValkeyModule);

  auto rec_dst = Register("doc_dst", index);
  EXPECT_EQ(rec_src.vector_record, rec_dst.vector_record);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

TEST_F(VectorRegistryTest, MoveAcrossDatabasesMovesRecord) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index_db0(2, attr->Str(), 0);
  MockIndex index_db1(2, attr->Str(), 1);
  auto rec_src = Register("doc_src", index_db0, 0);

  registry.MoveKey(0, "doc_src", 1, "doc_dst");
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  auto rec_dst = Register("doc_dst", index_db1, 1);
  EXPECT_EQ(rec_src.vector_record, rec_dst.vector_record);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

TEST_F(VectorRegistryTest, MoveKeyMovesEveryAttribute) {
  auto &registry = VectorRegistry::Instance();
  auto attr1 = StringInternStore::Intern("vec1");
  auto attr2 = StringInternStore::Intern("vec2");
  MockIndex index1(2, attr1->Str(), 0);
  MockIndex index2(2, attr2->Str(), 0);
  auto rec1 = Register("doc_src", index1);
  auto rec2 = Register("doc_src", index2);
  EXPECT_EQ(registry.GetStats().entry_cnt, 2);

  registry.MoveKey(0, "doc_src", 0, "doc_dst");
  EXPECT_EQ(registry.GetStats().entry_cnt, 2);
  EXPECT_EQ(Register("doc_dst", index1).vector_record, rec1.vector_record);
  EXPECT_EQ(Register("doc_dst", index2).vector_record, rec2.vector_record);
  EXPECT_EQ(registry.GetStats().entry_cnt, 2);
}

TEST_F(VectorRegistryTest, MoveKeyOverwriteDropsEveryDestinationEntry) {
  auto &registry = VectorRegistry::Instance();
  auto attr1 = StringInternStore::Intern("vec1");
  auto attr2 = StringInternStore::Intern("vec2");
  MockIndex index1(2, attr1->Str(), 0);
  MockIndex index2(2, attr2->Str(), 0);
  auto rec_src = Register("doc_src", index1);
  auto dst_vec = MakeFloatVectorString({9.0f, 9.0f});
  for (const auto *index : {&index1, &index2}) {
    registry.DedupOrConstruct(
        StringInternStore::Intern("doc_dst"), dst_vec.get(),
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, index);
  }
  EXPECT_EQ(registry.GetStats().entry_cnt, 3);

  // The overwritten destination hash had vec1 and vec2; only vec1 arrives.
  registry.MoveKey(0, "doc_src", 0, "doc_dst");
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(Register("doc_dst", index1).vector_record, rec_src.vector_record);
}

TEST_F(VectorRegistryTest, MoveUntrackedSourceOverUntrackedDestinationIsNoOp) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto rec = Register("other", index);

  registry.MoveKey(0, "doc_src", 0, "doc_dst");
  registry.MoveKey(999, "doc_src", 0, "doc_dst");
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

TEST_F(VectorRegistryTest, MoveUntrackedSourceDropsDestinationEntry) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto rec = Register("doc_dst", index);

  registry.MoveKey(0, "unindexed_src", 0, "doc_dst");
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, MoveKeyOntoItselfKeepsEntry) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto rec = Register("doc", index);

  registry.MoveKey(0, "doc", 0, "doc");
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

TEST_F(VectorRegistryTest, SweepKeepsReferencedEntries) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto rec = Register("doc", index);

  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_EQ(registry.SweepUnreferenced(10), 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

TEST_F(VectorRegistryTest, SweepCopiesBackSharedFieldAndErases) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  Register("doc", index);

  ExpectSweepOpensHash();
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(Return(1));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .WillOnce(Return(1));  // fields updated
  auto before = registry.GetStats().unshare_cnt;
  registry.SweepUnreferenced(10);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
  EXPECT_EQ(registry.GetStats().unshare_cnt, before + 1);
}

TEST_F(VectorRegistryTest, UnshareAttributesCopiesBackReferencedEntries) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("doc");
  auto vec_a = StringInternStore::Intern("vec_a");
  auto vec_b = StringInternStore::Intern("vec_b");
  MockIndex index_a(2, vec_a->Str(), 0);
  MockIndex index_b(2, vec_b->Str(), 0);
  // Held references stand for a deleted HNSW node: the sweep keeps both.
  auto rec_a = Register("doc", index_a);
  auto rec_b = Register("doc", index_b);
  ASSERT_EQ(registry.GetStats().entry_cnt, 2);

  ExpectSweepOpensHash();
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(Return(1));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .WillOnce(Return(1));  // fields updated
  auto before = registry.GetStats().unshare_cnt;
  std::vector<InternedStringPtr> attrs{vec_a};
  registry.UnshareAttributes(0, key, attrs);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().unshare_cnt, before + 1);
}

TEST_F(VectorRegistryTest, UnshareAttributesOfUntrackedKeyDoesNotOpenKey) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto rec = Register("doc", index);

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .Times(0);
  std::vector<InternedStringPtr> attrs{attr};
  registry.UnshareAttributes(0, StringInternStore::Intern("other"), attrs);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

TEST_F(VectorRegistryTest, SweepErasesUnsharedFieldWithoutCopy) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  Register("doc", index);

  ExpectSweepOpensHash();
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(Return(0));
  EXPECT_CALL(*kMockValkeyModule,
              HashSet(testing::_, testing::_, testing::_, testing::_, nullptr))
      .Times(0);
  registry.SweepUnreferenced(10);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, SweepCopyBackFailureKeepsEntry) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  Register("doc", index);

  ExpectSweepOpensHash();
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(Return(1));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .WillOnce(Return(0));  // no field updated
  registry.SweepUnreferenced(10);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

// An absent key may be logically expired and still hold the shared bytes.
TEST_F(VectorRegistryTest, SweepKeepsEntryWhenKeyAbsent) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  Register("doc", index);

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillOnce(Return(nullptr))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
  // A write open of an absent key returns an empty handle.
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillOnce(Return(VALKEYMODULE_KEYTYPE_EMPTY));
  registry.SweepUnreferenced(10);
  registry.SweepUnreferenced(10);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

// An absent field may be logically expired; HashHasStringRef must not run on
// it.
TEST_F(VectorRegistryTest, SweepKeepsEntryWhenFieldMissing) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  Register("doc", index);

  ExpectMissingHashField(VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE);
  EXPECT_CALL(*kMockValkeyModule,
              HashSet(testing::_, testing::_, testing::_, testing::_, nullptr))
      .Times(0);
  registry.SweepUnreferenced(10);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

TEST_F(VectorRegistryTest, SweepErasesWhenKeyReplacedByOtherType) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  Register("doc", index);

  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillOnce(Return(VALKEYMODULE_KEYTYPE_LIST));
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .Times(0);
  registry.SweepUnreferenced(10);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, SweepWithSharingDisabledErasesWithoutOpeningKey) {
  auto &registry = VectorRegistry::Instance();
  SetHashRegistrationSupported(registry, false);
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  Register("doc", index);

  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .Times(0);
  registry.SweepUnreferenced(10);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, SweepIsBatchedAndResumes) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  for (int i = 0; i < 5; ++i) {
    Register(absl::StrCat("key", i), index);
  }
  ExpectSweepOpensHash();
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly(Return(0));

  EXPECT_EQ(registry.SweepUnreferenced(2), 2);
  EXPECT_EQ(registry.GetStats().entry_cnt, 3);
  EXPECT_EQ(registry.SweepUnreferenced(2), 2);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.SweepUnreferenced(2), 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
  EXPECT_EQ(registry.SweepUnreferenced(2), 0);
}

// Entries skipped on one pass are reclaimed on a later one.
TEST_F(VectorRegistryTest, SweepRevisitsEntriesReleasedLater) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto held = Register("doc", index);
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly(Return(0));
  ExpectSweepOpensHash();

  registry.SweepUnreferenced(10);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  held = {};
  registry.SweepUnreferenced(10);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, ServerCronSweeps) {
  auto &registry = VectorRegistry::Instance();
  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(2));
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  for (int i = 0; i < 3; ++i) {
    Register(absl::StrCat("key", i), index);
  }
  ExpectSweepOpensHash();
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly(Return(0));

  registry.OnServerCronCallback(nullptr, ValkeyModuleEvent_CronLoop, 0,
                                nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  registry.OnServerCronCallback(nullptr, ValkeyModuleEvent_CronLoop, 0,
                                nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1024));
}

// Server cron drains every db's tables, with no query or ingest on this node.
TEST_F(VectorRegistryTest, ServerCronDrainsEveryDbsBackground) {
  auto &registry = VectorRegistry::Instance();
  MinimalHugeMapMigration minimal_migration;
  auto attr = StringInternStore::Intern("vec");
  std::vector<float> vec_data = {1.0f, 2.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);
  size_t entries = 0;
  // Held, as by an index, so the sweep keeps every entry.
  std::vector<indexes::VectorRecordWithSize> held;
  for (int db_num : {0, 1}) {
    MockIndex index(2, attr->Str(), db_num);
    for (int i = 0; i < 64 || !TrackedNeedsMigration(registry, db_num); ++i) {
      ASSERT_LT(i, 100000);
      held.push_back(registry.DedupOrConstruct(
          StringInternStore::Intern(absl::StrCat("key", i)), valkey_vec.get(),
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, db_num,
          &index));
      ++entries;
    }
  }
  int ticks = 0;
  while (TrackedNeedsMigration(registry, 0) ||
         TrackedNeedsMigration(registry, 1)) {
    ASSERT_LT(ticks++, 100000);
    registry.OnServerCronCallback(nullptr, ValkeyModuleEvent_CronLoop, 0,
                                  nullptr);
  }
  EXPECT_GT(ticks, 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, entries);
}

TEST_F(VectorRegistryTest, KeyRemovalEventsEraseEveryAttribute) {
  auto &registry = VectorRegistry::Instance();
  auto attr1 = StringInternStore::Intern("vec1");
  auto attr2 = StringInternStore::Intern("vec2");
  MockIndex index1(2, attr1->Str(), 0);
  MockIndex index2(2, attr2->Str(), 0);
  const std::vector<std::string> events = {"del", "expired", "evicted"};
  // Held by an index or not, the hash is gone.
  std::vector<indexes::VectorRecordWithSize> held;
  for (const auto &event : events) {
    held.push_back(Register(event, index1));
    held.push_back(Register(event, index2));
  }
  EXPECT_EQ(registry.GetStats().entry_cnt, 6);

  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .Times(0);
  size_t remaining = 6;
  for (const auto &event : events) {
    SendEvent(event.c_str(), event);
    remaining -= 2;
    EXPECT_EQ(registry.GetStats().entry_cnt, remaining) << event;
  }
}

TEST_F(VectorRegistryTest, EventErasesOnlyUnsharedFields) {
  auto &registry = VectorRegistry::Instance();
  auto attr1 = StringInternStore::Intern("vec1");
  auto attr2 = StringInternStore::Intern("vec2");
  MockIndex index1(2, attr1->Str(), 0);
  MockIndex index2(2, attr2->Str(), 0);
  auto rec1 = Register("doc", index1);
  auto rec2 = Register("doc", index2);

  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillOnce(Return(VALKEYMODULE_KEYTYPE_HASH));
  // vec1 is still shared, vec2 was overwritten.
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(Return(1))
      .WillOnce(Return(0))
      .WillRepeatedly(Return(1));
  SendEvent("hset", "doc");
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(Register("doc", index1).vector_record, rec1.vector_record);
}

TEST_F(VectorRegistryTest, FieldRemovalEventErasesMissingField) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  const std::vector<std::string> events = {"hset", "hdel", "hexpired"};
  std::vector<indexes::VectorRecordWithSize> held;
  held.reserve(events.size());
  for (const auto &event : events) {
    held.push_back(Register(event, index));
  }

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(testing::_, VALKEYMODULE_HASH_EXISTS, testing::_,
                      testing::An<int *>(), nullptr))
      .WillRepeatedly(testing::DoAll(testing::SetArgPointee<3>(0),
                                     Return(VALKEYMODULE_OK)));
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .Times(0);
  // Any other event may see a logically expired field as missing.
  SendEvent("hset", "hset");
  EXPECT_EQ(registry.GetStats().entry_cnt, 3);
  SendEvent("hdel", "hdel");
  EXPECT_EQ(registry.GetStats().entry_cnt, 2);
  SendEvent("hexpired", "hexpired");
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

TEST_F(VectorRegistryTest, EventOnAbsentOrNonHashKeyKeepsEntry) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto rec = Register("doc", index);

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ))
      .WillOnce(Return(nullptr))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillOnce(Return(VALKEYMODULE_KEYTYPE_STRING));
  SendEvent("hset", "doc");
  SendEvent("set", "doc");
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

TEST_F(VectorRegistryTest, EventForUntrackedKeyDoesNotOpenKey) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto rec = Register("doc", index);

  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .Times(0);
  SendEvent("hset", "other");
  SendEvent("hset", "doc", 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

TEST_F(VectorRegistryTest,
       ShareWithValkeySkipsMissingFieldWithoutCallingHashHasStringRef) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("k_hdel_share");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  ExpectMissingHashField(VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE);
  EXPECT_CALL(*kMockValkeyModule,
              HashSetStringRef(testing::_, testing::_, testing::_, testing::_))
      .Times(0);

  auto rec = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().hash_sharing_hits, 0);
  EXPECT_EQ(registry.GetStats().hash_sharing_errors, 0);
}

TEST_F(
    VectorRegistryTest,
    OverwriteWithInvalidSizeErasesWhenFieldMissingWithoutCallingHashHasStringRef) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("k_hdel_invalid_size");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  auto rec = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  ExpectMissingHashField(VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);

  auto bad_vec = MakeFloatVectorString({1.0f, 2.0f, 3.0f});
  auto rec_bad = registry.DedupOrConstruct(
      key, bad_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_NE(rec_bad, nullptr);
  EXPECT_EQ(rec_bad.size, 3 * sizeof(float));
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

}  // namespace valkey_search
