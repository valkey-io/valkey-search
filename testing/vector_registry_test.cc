/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/vector_registry.h"

#include <limits>
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
    ON_CALL(*kMockValkeyModule, KeyType(testing::_))
        .WillByDefault(Return(VALKEYMODULE_KEYTYPE_HASH));
  }
  void SetHashRegistrationSupported(VectorRegistry &registry, bool supported) {
    registry.hash_vector_sharing_ = supported;
  }
  void InitRegistry(VectorRegistry &registry, ValkeyModuleCtx *ctx) {
    registry.Init(ctx);
  }
  bool TrackedNeedsMigration(VectorRegistry &registry, int db_num) {
    auto it = registry.tracked_vectors_.find(db_num);
    return it != registry.tracked_vectors_.end() && it->second.NeedsMigration();
  }
  bool TrackedKeysNeedsMigration(VectorRegistry &registry, int db_num) {
    auto it = registry.tracked_keys_by_db_.find(db_num);
    return it != registry.tracked_keys_by_db_.end() &&
           it->second.NeedsMigration();
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

// A missing vector on a key that is still a hash may be a logically expired
// field that holds the shared bytes, so the record is kept. Once the key holds
// another type, nothing can reference it.
TEST_F(VectorRegistryTest, OverwriteWithNullptrRetainsRecordWhileKeyIsHash) {
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

  auto rec_null = registry.DedupOrConstruct(
      key, nullptr, data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0,
      &index);
  EXPECT_EQ(rec_null, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  ON_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillByDefault(Return(VALKEYMODULE_KEYTYPE_STRING));
  registry.DedupOrConstruct(
      key, nullptr, data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0,
      &index);
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
      .Times(2)
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .Times(2)
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .Times(2)
      .WillRepeatedly(Return(1));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .WillOnce(Return(1));

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
      .WillOnce(Return(1));

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
      .Times(2)
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .Times(2)
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .Times(2)
      .WillRepeatedly(Return(1));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .WillOnce(Return(1));

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
      .WillRepeatedly(Return(1));

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
  for (int db_num : {0, 1}) {
    MockIndex index(2, attr->Str(), db_num);
    for (int i = 0; i < 64 || !TrackedNeedsMigration(registry, db_num) ||
                    !TrackedKeysNeedsMigration(registry, db_num);
         ++i) {
      ASSERT_LT(i, 100000);
      registry.DedupOrConstruct(
          StringInternStore::Intern(absl::StrCat("key", i)), valkey_vec.get(),
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, db_num,
          &index);
      ++entries;
    }
  }
  int ticks = 0;
  while (TrackedNeedsMigration(registry, 0) ||
         TrackedNeedsMigration(registry, 1) ||
         TrackedKeysNeedsMigration(registry, 0) ||
         TrackedKeysNeedsMigration(registry, 1)) {
    ASSERT_LT(ticks++, 100000);
    registry.OnServerCronCallback(nullptr, ValkeyModuleEvent_CronLoop, 0,
                                  nullptr);
  }
  EXPECT_GT(ticks, 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, entries);
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

  // Overwriting with nullptr cancels the pending unshare. The key is still a
  // hash, so the record is kept until the key is deleted.
  size_t before_erase = registry.GetPendingUnsharesCount();
  InternedStringPtr cancelled;
  for (const auto &k : keys) {
    registry.DedupOrConstruct(
        k, nullptr, data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0,
        &index);
    if (registry.GetPendingUnsharesCount() < before_erase) {
      cancelled = k;
      break;
    }
  }
  ASSERT_TRUE(cancelled);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), before_erase - 1);

  registry.ProcessPendingUnshares(10);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  registry.OnKeyDeleted(0, cancelled->Str());
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

// A field that is not visible may be logically expired and still hold the
// reference, so the record is kept until the key is deleted.
TEST_F(VectorRegistryTest,
       UnshareWithValkeyRetainsRecordForMissingFieldUntilKeyDeleted) {
  auto &registry = VectorRegistry::Instance();
  auto k1 = StringInternStore::Intern("k_hdel_unshare");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      k1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  ExpectMissingHashField(VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE);
  EXPECT_CALL(*kMockValkeyModule,
              HashSet(testing::_, testing::_, testing::_, testing::_, nullptr))
      .Times(0);

  std::vector<InternedStringPtr> keys = {k1};
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  registry.OnKeyDeleted(0, k1->Str());
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

// If the copy fails the field still references the record, so it is kept.
TEST_F(VectorRegistryTest, FailedUnshareRetainsRecord) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("k_failed_unshare");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .Times(2)
      .WillRepeatedly(Return(1));
  EXPECT_CALL(*kMockValkeyModule,
              HashSet(testing::_, testing::_, testing::_, testing::_, nullptr))
      .WillOnce(Return(0));

  std::vector<InternedStringPtr> keys = {key};
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().unshare_errors, 1);
}

TEST_F(VectorRegistryTest, MoveKeyUnshareRetainsRecordForMissingField) {
  auto &registry = VectorRegistry::Instance();
  auto key_src = StringInternStore::Intern("doc_src_hdel");
  auto key_dst = vmsdk::MakeUniqueValkeyString("outside_dst_hdel");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      key_src, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  ExpectMissingHashField(VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE);
  EXPECT_CALL(*kMockValkeyModule,
              HashSet(testing::_, testing::_, testing::_, testing::_, nullptr))
      .Times(0);

  absl::flat_hash_map<InternedStringPtr, VectorRegistry::Action> actions = {
      {attr, VectorRegistry::Action::kUnshare}};
  registry.MoveKey(0, key_src, 0, key_dst.get(), actions);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_TRUE(registry.GetTrackedAttributes(0, key_src->Str()).empty());
  EXPECT_TRUE(
      registry.GetTrackedAttributes(0, "outside_dst_hdel").contains(attr));

  registry.OnKeyDeleted(0, "outside_dst_hdel");
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
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
    OverwriteWithInvalidSizeRetainsWhenFieldMissingWithoutCallingHashHasStringRef) {
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
  // A field that is not visible may be logically expired and still shared.
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

TEST_F(VectorRegistryTest, GetTrackedAttributesReturnsTrackedAttributesForKey) {
  auto &registry = VectorRegistry::Instance();
  auto key1 = StringInternStore::Intern("doc1");
  auto key2 = StringInternStore::Intern("doc2");
  auto attr1 = StringInternStore::Intern("vec1");
  auto attr2 = StringInternStore::Intern("vec2");
  MockIndex index1(2, attr1->Str(), 0);
  MockIndex index2(4, attr2->Str(), 0);

  EXPECT_TRUE(registry.GetTrackedAttributes(0, key1->Str()).empty());

  auto valkey_vec1 = MakeFloatVectorString({1.0f, 2.0f});
  auto valkey_vec2 = MakeFloatVectorString({1.0f, 2.0f, 3.0f, 4.0f});

  registry.DedupOrConstruct(
      key1, valkey_vec1.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index1);
  registry.DedupOrConstruct(
      key1, valkey_vec2.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index2);
  registry.DedupOrConstruct(
      key2, valkey_vec1.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index1);

  auto key1_attrs = registry.GetTrackedAttributes(0, key1->Str());
  EXPECT_EQ(key1_attrs.size(), 2);
  EXPECT_EQ(key1_attrs[attr1], 2 * sizeof(float));
  EXPECT_EQ(key1_attrs[attr2], 4 * sizeof(float));

  auto key2_attrs = registry.GetTrackedAttributes(0, key2->Str());
  EXPECT_EQ(key2_attrs.size(), 1);
  EXPECT_EQ(key2_attrs[attr1], 2 * sizeof(float));

  // Erasing one attribute from key1 updates GetTrackedAttributes. A missing
  // vector erases only once the key is no longer a hash.
  ON_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillByDefault(Return(VALKEYMODULE_KEYTYPE_STRING));
  registry.DedupOrConstruct(
      key1, nullptr, data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0,
      &index1);
  key1_attrs = registry.GetTrackedAttributes(0, key1->Str());
  EXPECT_EQ(key1_attrs.size(), 1);
  EXPECT_EQ(key1_attrs[attr2], 4 * sizeof(float));

  // Erasing the remaining attribute leaves an empty map
  registry.DedupOrConstruct(
      key1, nullptr, data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0,
      &index2);
  EXPECT_TRUE(registry.GetTrackedAttributes(0, key1->Str()).empty());
}

TEST_F(VectorRegistryTest,
       RenameWhilePendingUnshareUnsharesDestinationAndCancelsPending) {
  auto &registry = VectorRegistry::Instance();
  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1));

  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  std::vector<InternedStringPtr> keys;
  for (int i = 1; i <= 3; ++i) {
    auto key = StringInternStore::Intern(absl::StrCat("pending_k", i));
    registry.DedupOrConstruct(
        key, valkey_vec.get(),
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
    keys.push_back(key);
  }

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
      .WillRepeatedly(Return(1));

  // Drop index: 1 key processed synchronously, 2 remain pending unshare
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 2);
  EXPECT_EQ(registry.GetStats().entry_cnt, 2);

  // Find one key that is still awaiting unshare
  InternedStringPtr pending_key;
  for (const auto &k : keys) {
    if (!registry.GetTrackedAttributes(0, k->Str()).empty()) {
      pending_key = k;
      break;
    }
  }
  ASSERT_TRUE(pending_key);

  auto dst_key = vmsdk::MakeUniqueValkeyString("moved_pending_k");
  absl::flat_hash_map<InternedStringPtr, VectorRegistry::Action> actions;
  for (const auto &[tracked_attr, _] :
       registry.GetTrackedAttributes(0, pending_key->Str())) {
    actions.emplace(tracked_attr, VectorRegistry::Action::kUnshare);
  }
  EXPECT_CALL(*kMockValkeyModule,
              HashSet(testing::Truly([](ValkeyModuleKey *key_obj) {
                        return key_obj && key_obj->key == "moved_pending_k";
                      }),
                      VALKEYMODULE_HASH_NONE, testing::_,
                      testing::Truly([&](ValkeyModuleString *val) {
                        return vmsdk::ToStringView(val) ==
                               vmsdk::ToStringView(valkey_vec.get());
                      }),
                      nullptr))
      .WillOnce(Return(1));
  registry.MoveKey(0, pending_key, 0, dst_key.get(), actions);

  // Pending unshare for pending_key was cancelled and its vector was unshared
  // into dst_key immediately
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_TRUE(registry.GetTrackedAttributes(0, pending_key->Str()).empty());

  registry.ProcessPendingUnshares(10);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);

  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1024));
}

TEST_F(VectorRegistryTest,
       MoveKeyWithEmptyActionsErasesOverwrittenDestination) {
  auto &registry = VectorRegistry::Instance();
  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1));

  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  auto k1 = StringInternStore::Intern("dst_k1");
  auto k2 = StringInternStore::Intern("dst_k2");
  registry.DedupOrConstruct(
      k1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  registry.DedupOrConstruct(
      k2, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);

  std::vector<InternedStringPtr> keys = {k1, k2};
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  InternedStringPtr remaining_dst =
      !registry.GetTrackedAttributes(0, k1->Str()).empty() ? k1 : k2;
  auto untracked_src = StringInternStore::Intern("untracked_src");
  auto dst_valkey_str = vmsdk::MakeUniqueValkeyString(remaining_dst->Str());

  // Renaming an untracked key over remaining_dst purges remaining_dst's
  // tracked record and cancels its pending unshare.
  registry.MoveKey(0, untracked_src, 0, dst_valkey_str.get(), {});
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
  EXPECT_TRUE(registry.GetTrackedAttributes(0, remaining_dst->Str()).empty());

  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1024));
}

TEST_F(VectorRegistryTest,
       GetTrackedAttributesAndOnKeyDeletedDoNotInternUntrackedKeys) {
  auto &registry = VectorRegistry::Instance();
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  auto tracked_key = StringInternStore::Intern("tracked_for_delete");
  registry.DedupOrConstruct(
      tracked_key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  const size_t interned_before = StringInternStore::Instance().UniqueStrings();
  EXPECT_TRUE(
      registry.GetTrackedAttributes(0, "never_interned_probe_key").empty());
  registry.OnKeyDeleted(0, "never_interned_delete_key");
  EXPECT_EQ(StringInternStore::Instance().UniqueStrings(), interned_before);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  registry.OnKeyDeleted(0, "tracked_for_delete");
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
  EXPECT_TRUE(registry.GetTrackedAttributes(0, "tracked_for_delete").empty());
}

// A batch whose every item is deferred while other unshares wait outside it
// counts as stalled. The deferred items stay queued.
TEST_F(VectorRegistryTest, ProcessPendingUnsharesCountsStalledBatch) {
  auto &registry = VectorRegistry::Instance();
  const auto saved_batch = options::GetVectorUnshareBatchSize().GetValue();
  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1));
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  std::vector<InternedStringPtr> keys;
  std::vector<indexes::VectorRecordWithSize> held;
  for (int i = 0; i < 2; ++i) {
    auto key = StringInternStore::Intern(absl::StrCat("k_stall_", i));
    // Distinct payloads so the two keys do not share one record.
    auto vec = MakeFloatVectorString({1.0f, static_cast<float>(i)});
    held.push_back(registry.DedupOrConstruct(
        key, vec.get(), data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH,
        0, &index));
    keys.push_back(key);
  }
  const auto stalled = registry.GetStats().unshare_stalled_batches;

  // Both records are busy: the batch of one defers its item while the other
  // waits.
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 2);
  EXPECT_EQ(registry.GetStats().unshare_stalled_batches, stalled + 1);

  // A batch that holds the whole queue and defers everything is a stall too.
  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(2));
  EXPECT_EQ(registry.ProcessPendingUnshares(2), 2);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 2);
  EXPECT_EQ(registry.GetStats().unshare_stalled_batches, stalled + 2);

  held.clear();
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly(Return(1));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .Times(2)
      .WillRepeatedly(Return(1));
  EXPECT_EQ(registry.ProcessPendingUnshares(2), 2);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(saved_batch));
}

TEST_F(VectorRegistryTest, ProcessPendingUnsharesRetainsBusyRefCountRecord) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("k_busy_refcount");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  auto held_rec = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_NE(held_rec, nullptr);
  EXPECT_EQ(held_rec.vector_record.RefCount(), 2);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // While RefCount() > 1, RemoveIndexKeys / ProcessPendingUnshares must defer
  // the unshare rather than dropping it from pending_unshares_.
  std::vector<InternedStringPtr> keys = {key};
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Release the extra reference and retry ProcessPendingUnshares.
  held_rec.vector_record.reset();
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .Times(2)
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .Times(2)
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .Times(2)
      .WillRepeatedly(Return(1));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .WillOnce(Return(1));

  EXPECT_EQ(registry.ProcessPendingUnshares(10), 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest,
       ProcessPendingUnsharesRetainsLogicallyExpiredKeyUntilDeleted) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("k_expired");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Simulate a logically expired key: OpenKey with NOEFFECTS returns nullptr.
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillOnce(Return(nullptr));

  std::vector<InternedStringPtr> keys = {key};
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // NotifySubscribers cleans up the retained record via OnKeyDeleted when the
  // key is physically expired, even if another subscription matches.
  MockKeyspaceEventSubscription sub;
  MockAttributeDataType attr_type;
  std::vector<std::string> prefixes = {"k_"};
  EXPECT_CALL(*kMockValkeyModule,
              SubscribeToKeyspaceEvents(testing::_, testing::_, testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_OK));
  EXPECT_CALL(attr_type, GetValkeyEventTypes())
      .WillRepeatedly(Return(VALKEYMODULE_NOTIFY_ALL));
  EXPECT_CALL(sub, GetAttributeDataType())
      .WillRepeatedly(testing::ReturnRef(attr_type));
  EXPECT_CALL(sub, GetKeyPrefixes())
      .WillRepeatedly(testing::ReturnRef(prefixes));
  EXPECT_CALL(sub, OnKeyspaceNotification(testing::_, testing::_, testing::_,
                                          testing::_))
      .Times(1);
  VMSDK_EXPECT_OK(
      KeyspaceEventManager::Instance().InsertSubscription(&fake_ctx_, &sub));

  auto key_str = vmsdk::MakeUniqueValkeyString("k_expired");
  KeyspaceEventManager::Instance().NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_EXPIRED, "expired", key_str.get());
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);

  VMSDK_EXPECT_OK(KeyspaceEventManager::Instance().RemoveSubscription(&sub));
}

TEST_F(VectorRegistryTest,
       ProcessPendingUnsharesEmptyHandleWithLiveIndexIsRetainedNotDeferred) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("doc:expired_blue_green");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index1(2, attr->Str(), 0);
  MockIndex index2(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  // Both indexes share the same VectorRecord; index2 keeps a live reference
  // (RefCount == 2) and stays subscribed.
  registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index1);
  auto held_by_index2 = registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index2);
  ASSERT_NE(held_by_index2, nullptr);
  EXPECT_EQ(held_by_index2.vector_record.RefCount(), 2);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  MockKeyspaceEventSubscription sub2;
  HashAttributeDataType hash_attr_type;
  std::vector<std::string> prefixes = {"doc:"};
  sub2.vector_indexes_ = {&index2};
  sub2.db_num_ = 0;
  EXPECT_CALL(*kMockValkeyModule,
              SubscribeToKeyspaceEvents(testing::_, testing::_, testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_OK));
  EXPECT_CALL(sub2, GetAttributeDataType())
      .WillRepeatedly(testing::ReturnRef(hash_attr_type));
  EXPECT_CALL(sub2, GetKeyPrefixes())
      .WillRepeatedly(testing::ReturnRef(prefixes));
  VMSDK_EXPECT_OK(
      KeyspaceEventManager::Instance().InsertSubscription(&fake_ctx_, &sub2));

  // Real VM_OpenKey(NOEFFECTS | WRITE) on a logically expired key returns a
  // non-null handle with VALKEYMODULE_KEYTYPE_EMPTY.
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_EMPTY));

  std::vector<InternedStringPtr> keys = {key};
  registry.RemoveIndexKeys(0, attr, keys);

  // Must be retained and dropped from pending_unshares_, not re-queued in a
  // deferral spin loop.
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  VMSDK_EXPECT_OK(KeyspaceEventManager::Instance().RemoveSubscription(&sub2));
  held_by_index2.vector_record.reset();
  registry.OnKeyDeleted(0, key->Str());
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, ProcessPendingUnsharesBusyRecordDoesNotStarveQueue) {
  auto &registry = VectorRegistry::Instance();
  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1));

  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  auto key_a = StringInternStore::Intern("k_busy_starve_a");
  auto key_b = StringInternStore::Intern("k_free_starve_b");

  // Hold an extra reference on key_a (RefCount == 2); key_b is free
  // (RefCount == 1).
  auto held_a = registry.DedupOrConstruct(
      key_a, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_NE(held_a, nullptr);
  EXPECT_EQ(held_a.vector_record.RefCount(), 2);
  registry.DedupOrConstruct(
      key_b, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_EQ(registry.GetStats().entry_cnt, 2);

  bool key_b_unshared = false;
  bool key_a_unshared = false;
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
      .WillRepeatedly([&](ValkeyModuleKey *key_obj, int, ValkeyModuleString *,
                          ValkeyModuleString *, void *) {
        if (key_obj->key == key_b->Str()) {
          key_b_unshared = true;
        } else if (key_obj->key == key_a->Str()) {
          key_a_unshared = true;
        }
        return 1;
      });

  // RemoveIndexKeys runs the first ProcessPendingUnshares(1) batch.
  std::vector<InternedStringPtr> keys = {key_a, key_b};
  registry.RemoveIndexKeys(0, attr, keys);
  if (!key_b_unshared) {
    registry.ProcessPendingUnshares(1);
  }

  // key_b must be unshared within <= 2 calls of batch size 1, while key_a stays
  // pending.
  EXPECT_TRUE(key_b_unshared);
  EXPECT_FALSE(key_a_unshared);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Even after extra calls while key_a is still busy, key_a stays deferred
  // cleanly.
  registry.ProcessPendingUnshares(1);
  EXPECT_FALSE(key_a_unshared);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);

  // Also test with multiple busy keys plus a free key queued together so a busy
  // key is deferred ahead of the free key regardless of hash iteration order.
  std::vector<indexes::VectorRecordWithSize> extra_held;
  std::vector<InternedStringPtr> mixed_keys;
  for (int i = 0; i < 4; ++i) {
    auto k = StringInternStore::Intern(absl::StrCat("k_multi_busy_", i));
    extra_held.push_back(registry.DedupOrConstruct(
        k, valkey_vec.get(),
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index));
    mixed_keys.push_back(k);
  }
  auto key_c = StringInternStore::Intern("k_multi_free_c");
  registry.DedupOrConstruct(
      key_c, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  mixed_keys.push_back(key_c);

  bool key_c_unshared = false;
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .WillRepeatedly([&](ValkeyModuleKey *key_obj, int, ValkeyModuleString *,
                          ValkeyModuleString *, void *) {
        if (key_obj->key == key_c->Str()) {
          key_c_unshared = true;
        } else if (key_obj->key == key_a->Str()) {
          key_a_unshared = true;
        }
        return 1;
      });

  registry.RemoveIndexKeys(0, attr, mixed_keys);
  for (int i = 0; i < 4 && !key_c_unshared; ++i) {
    registry.ProcessPendingUnshares(1);
  }
  EXPECT_TRUE(key_c_unshared);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 5);

  // Releasing held_a allows the deferred key_a to be promoted and unshared.
  held_a.vector_record.reset();
  extra_held.clear();
  for (int i = 0; i < 6 && registry.GetPendingUnsharesCount() > 0; ++i) {
    registry.ProcessPendingUnshares(1);
  }
  EXPECT_TRUE(key_a_unshared);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);

  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1024));
}

TEST_F(VectorRegistryTest,
       DedupOrConstructNullptrRetainsLogicallyExpiredKeyUntilDeleted) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("k_expired_dedup_null");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ))
      .WillOnce(Return(nullptr));

  registry.DedupOrConstruct(
      key, nullptr, data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0,
      &index);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  registry.OnKeyDeleted(0, "k_expired_dedup_null");
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest, OnKeyDeletedCleansUpTrackedAndPendingEntries) {
  auto &registry = VectorRegistry::Instance();
  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1));

  auto attr1 = StringInternStore::Intern("vec1");
  auto attr2 = StringInternStore::Intern("vec2");
  MockIndex index1(2, attr1->Str(), 0);
  MockIndex index2(2, attr2->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  auto k1 = StringInternStore::Intern("k_del_multi_1");
  auto k2 = StringInternStore::Intern("k_del_multi_2");
  for (const auto &k : {k1, k2}) {
    registry.DedupOrConstruct(
        k, valkey_vec.get(),
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index1);
    registry.DedupOrConstruct(
        k, valkey_vec.get(),
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index2);
  }
  ASSERT_EQ(registry.GetStats().entry_cnt, 4);

  // Queue attr1 for unshare with batch_size=1 so one key remains pending for
  // attr1 while both remain tracked for attr2.
  std::vector<InternedStringPtr> keys = {k1, k2};
  registry.RemoveIndexKeys(0, attr1, keys);
  ASSERT_EQ(registry.GetPendingUnsharesCount(), 1);
  ASSERT_EQ(registry.GetStats().entry_cnt, 3);

  InternedStringPtr pending_key =
      registry.GetTrackedAttributes(0, k1->Str()).contains(attr1) ? k1 : k2;
  registry.OnKeyDeleted(0, pending_key->Str());

  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_TRUE(registry.GetTrackedAttributes(0, pending_key->Str()).empty());

  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1024));
}

TEST_F(VectorRegistryTest, UnshareWithValkeyPreservesFieldTtl) {
  auto &registry = VectorRegistry::Instance();
  auto k1 = StringInternStore::Intern("k_ttl_unshare");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      k1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  constexpr long long kExpiryMs = 1800000000000LL;  // NOLINT
  CallReplyArray arr;
  arr.push_back(CreateValkeyModuleCallReply(CallReplyInteger(kExpiryMs)));
  auto hpexpiretime_reply = CreateValkeyModuleCallReply(std::move(arr));

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .Times(2)
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .Times(2)
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .Times(2)
      .WillRepeatedly(Return(1));
  CallReplyArray arr2;
  arr2.push_back(CreateValkeyModuleCallReply(CallReplyInteger(1)));
  auto hpexpireat_reply = CreateValkeyModuleCallReply(std::move(arr2));
  {
    testing::InSequence seq;
    EXPECT_CALL(*kMockValkeyModule,
                Call(testing::_, testing::StrEq("HPEXPIRETIME"),
                     testing::StrEq("sccs"), testing::_,
                     testing::StrEq("FIELDS"), testing::StrEq("1"), testing::_))
        .WillOnce(Return(hpexpiretime_reply.get()));
    EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                            testing::_, testing::_, nullptr))
        .WillOnce(Return(1));
    EXPECT_CALL(
        *kMockValkeyModule,
        Call(testing::Matcher<ValkeyModuleCtx *>(testing::_),
             testing::Matcher<const char *>(testing::StrEq("HPEXPIREAT")),
             testing::Matcher<const char *>(testing::StrEq("slccs")),
             testing::Matcher<ValkeyModuleString *>(testing::_), kExpiryMs,
             testing::Matcher<const char *>(testing::StrEq("FIELDS")),
             testing::Matcher<const char *>(testing::StrEq("1")),
             testing::Matcher<ValkeyModuleString *>(testing::_)))
        .WillOnce(Return(hpexpireat_reply.get()));
  }

  std::vector<InternedStringPtr> keys = {k1};
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
  EXPECT_EQ(registry.GetStats().unshare_errors, 0);
}

TEST_F(VectorRegistryTest,
       CrossDbMoveKeyUnshareSelectsDestinationDbBeforeOpenKey) {
  auto &registry = VectorRegistry::Instance();
  auto key_src = StringInternStore::Intern("doc_src_db0");
  auto key_dst = vmsdk::MakeUniqueValkeyString("outside_dst_db3");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      key_src, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_EQ(registry.GetStats().entry_cnt, 1);

  CallReplyArray no_ttl_arr;
  no_ttl_arr.push_back(CreateValkeyModuleCallReply(CallReplyInteger(-1)));
  auto no_ttl_reply = CreateValkeyModuleCallReply(std::move(no_ttl_arr));

  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(testing::_))
      .WillRepeatedly(Return(0));
  {
    testing::InSequence seq;
    EXPECT_CALL(*kMockValkeyModule, SelectDb(testing::_, 3))
        .WillOnce(Return(VALKEYMODULE_OK));
    EXPECT_CALL(*kMockValkeyModule,
                OpenKey(testing::_, key_dst.get(),
                        VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
        .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
    EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
        .WillOnce(Return(VALKEYMODULE_KEYTYPE_HASH));
    EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
        .WillOnce(Return(1));
    EXPECT_CALL(*kMockValkeyModule,
                Call(testing::_, testing::StrEq("HPEXPIRETIME"),
                     testing::StrEq("sccs"), key_dst.get(),
                     testing::StrEq("FIELDS"), testing::StrEq("1"), testing::_))
        .WillOnce(Return(no_ttl_reply.get()));
    EXPECT_CALL(*kMockValkeyModule,
                OpenKey(testing::_, key_dst.get(),
                        VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
        .WillOnce(TestValkeyModule_OpenKeyDefaultImpl);
    EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
        .WillOnce(Return(VALKEYMODULE_KEYTYPE_HASH));
    EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
        .WillOnce(Return(1));
    EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                            testing::_, testing::_, nullptr))
        .WillOnce(Return(1));
    EXPECT_CALL(*kMockValkeyModule, SelectDb(testing::_, 0))
        .WillOnce(Return(VALKEYMODULE_OK));
  }

  absl::flat_hash_map<InternedStringPtr, VectorRegistry::Action> actions = {
      {attr, VectorRegistry::Action::kUnshare}};
  registry.MoveKey(0, key_src, 3, key_dst.get(), actions);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
  EXPECT_EQ(registry.GetStats().unshare_errors, 0);
}

TEST_F(VectorRegistryTest,
       ProcessPendingUnsharesSafeWhenHpexpiretimeTriggersReentryOnKeyDeleted) {
  auto &registry = VectorRegistry::Instance();
  auto k1 = StringInternStore::Intern("k_reentry_1");
  auto k2 = StringInternStore::Intern("k_reentry_2");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      k1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  registry.DedupOrConstruct(
      k2, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_EQ(registry.GetStats().entry_cnt, 2);

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
      .Times(0);
  // Even if HPEXPIRETIME triggers re-entrant OnKeyDeleted for both keys and
  // returns nullptr (causing ProcessPendingUnshares to consider deferring k1),
  // neither key is re-queued or dereferenced via invalidated iterators.
  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::_, testing::StrEq("HPEXPIRETIME"),
                   testing::StrEq("sccs"), testing::_, testing::StrEq("FIELDS"),
                   testing::StrEq("1"), testing::_))
      .WillOnce([&](ValkeyModuleCtx *, const char *, const char *,
                    ValkeyModuleString *, const char *, const char *,
                    ValkeyModuleString *) -> ValkeyModuleCallReply * {
        registry.OnKeyDeleted(0, k1->Str());
        registry.OnKeyDeleted(0, k2->Str());
        return nullptr;
      });

  std::vector<InternedStringPtr> keys = {k1, k2};
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest,
       UnshareWithValkeyDoesNotClearTtlWhenExpiryLookupFails) {
  auto &registry = VectorRegistry::Instance();
  auto k1 = StringInternStore::Intern("doc:1");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      k1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_EQ(registry.GetStats().entry_cnt, 1);

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly(Return(1));
  EXPECT_CALL(*kMockValkeyModule,
              HashSet(testing::_, testing::_, testing::_, testing::_, nullptr))
      .Times(0);
  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::Matcher<ValkeyModuleCtx *>(testing::_),
                   testing::Matcher<const char *>(testing::StrEq("HPEXPIREAT")),
                   testing::Matcher<const char *>(testing::_),
                   testing::Matcher<ValkeyModuleString *>(testing::_),
                   testing::_, testing::Matcher<const char *>(testing::_),
                   testing::Matcher<const char *>(testing::_),
                   testing::Matcher<ValkeyModuleString *>(testing::_)))
      .Times(0);

  auto err_reply = std::make_unique<ValkeyModuleCallReply>();
  err_reply->type = VALKEYMODULE_REPLY_ERROR;
  err_reply->msg = "MOVED 0 127.0.0.1:6379";

  // Case 1: HPEXPIRETIME returns nullptr (e.g. cluster replica redirection).
  // Case 2: HPEXPIRETIME returns an error reply.
  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::_, testing::StrEq("HPEXPIRETIME"),
                   testing::StrEq("sccs"), testing::_, testing::StrEq("FIELDS"),
                   testing::StrEq("1"), testing::_))
      .WillOnce(Return(nullptr))
      .WillOnce(Return(err_reply.get()));

  std::vector<InternedStringPtr> keys = {k1};
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  EXPECT_EQ(registry.GetStats().unshare_errors, 1);

  EXPECT_EQ(registry.ProcessPendingUnshares(1), 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  // Each record's lookup failure is counted once rather than on every retry.
  EXPECT_EQ(registry.GetStats().unshare_errors, 1);
}

TEST_F(VectorRegistryTest, MoveKeyFailsClosedAndDefersWhenExpiryLookupFails) {
  auto &registry = VectorRegistry::Instance();
  auto key_src = StringInternStore::Intern("doc:move_fail");
  auto key_dst = vmsdk::MakeUniqueValkeyString("outside:move_fail");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      key_src, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_EQ(registry.GetStats().entry_cnt, 1);

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly(Return(1));

  constexpr long long kExpiryMs = 1800000000000LL;  // NOLINT
  CallReplyArray expiry_arr;
  expiry_arr.push_back(
      CreateValkeyModuleCallReply(CallReplyInteger(kExpiryMs)));
  auto expiry_reply = CreateValkeyModuleCallReply(std::move(expiry_arr));

  CallReplyArray ok_arr;
  ok_arr.push_back(CreateValkeyModuleCallReply(CallReplyInteger(1)));
  auto hpexpireat_reply = CreateValkeyModuleCallReply(std::move(ok_arr));

  // First attempt (during MoveKey) fails HPEXPIRETIME -> fail closed.
  // Second attempt (during ProcessPendingUnshares after promotion) succeeds.
  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::_, testing::StrEq("HPEXPIRETIME"),
                   testing::StrEq("sccs"), testing::_, testing::StrEq("FIELDS"),
                   testing::StrEq("1"), testing::_))
      .WillOnce(Return(nullptr))
      .WillOnce(Return(expiry_reply.get()));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .WillOnce(Return(1));
  EXPECT_CALL(
      *kMockValkeyModule,
      Call(testing::Matcher<ValkeyModuleCtx *>(testing::_),
           testing::Matcher<const char *>(testing::StrEq("HPEXPIREAT")),
           testing::Matcher<const char *>(testing::StrEq("slccs")),
           testing::Matcher<ValkeyModuleString *>(testing::_), kExpiryMs,
           testing::Matcher<const char *>(testing::StrEq("FIELDS")),
           testing::Matcher<const char *>(testing::StrEq("1")),
           testing::Matcher<ValkeyModuleString *>(testing::_)))
      .WillOnce(Return(hpexpireat_reply.get()));

  absl::flat_hash_map<InternedStringPtr, VectorRegistry::Action> actions = {
      {attr, VectorRegistry::Action::kUnshare}};
  registry.MoveKey(0, key_src, 0, key_dst.get(), actions);

  // Record must remain tracked at destination and queued for retry.
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  EXPECT_EQ(registry.GetStats().unshare_errors, 1);
  EXPECT_TRUE(registry.GetTrackedAttributes(0, "doc:move_fail").empty());
  EXPECT_TRUE(
      registry.GetTrackedAttributes(0, "outside:move_fail").contains(attr));

  // Retry succeeds and drains the deferred unshare.
  EXPECT_EQ(registry.ProcessPendingUnshares(1), 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().unshare_errors, 1);
}

TEST_F(VectorRegistryTest,
       UnshareWithValkeySurfacesRestoreFailureAndPropagatesImmediateExpiry) {
  auto &registry = VectorRegistry::Instance();
  auto k1 = StringInternStore::Intern("doc:restore_err");
  auto k2 = StringInternStore::Intern("doc:restore_expired_hdel");
  auto k3 = StringInternStore::Intern("doc:restore_expired_del");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      k1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  registry.DedupOrConstruct(
      k2, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  registry.DedupOrConstruct(
      k3, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_EQ(registry.GetStats().entry_cnt, 3);

  constexpr long long kExpiryMs = 1800000000000LL;  // NOLINT
  CallReplyArray expiry_arr;
  expiry_arr.push_back(
      CreateValkeyModuleCallReply(CallReplyInteger(kExpiryMs)));
  auto expiry_reply = CreateValkeyModuleCallReply(std::move(expiry_arr));

  auto err_reply = std::make_unique<ValkeyModuleCallReply>();
  err_reply->type = VALKEYMODULE_REPLY_ERROR;
  err_reply->msg = "ERR restore failed";

  CallReplyArray immediate_expiry_arr;
  immediate_expiry_arr.push_back(
      CreateValkeyModuleCallReply(CallReplyInteger(2)));
  auto immediate_expiry_reply =
      CreateValkeyModuleCallReply(std::move(immediate_expiry_arr));

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ))
      .WillOnce(TestValkeyModule_OpenKeyDefaultImpl)
      .WillOnce(Return(nullptr));
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly(Return(1));
  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::_, testing::StrEq("HPEXPIRETIME"),
                   testing::StrEq("sccs"), testing::_, testing::StrEq("FIELDS"),
                   testing::StrEq("1"), testing::_))
      .Times(3)
      .WillRepeatedly(Return(expiry_reply.get()));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .Times(3)
      .WillRepeatedly(Return(1));
  EXPECT_CALL(
      *kMockValkeyModule,
      Call(testing::Matcher<ValkeyModuleCtx *>(testing::_),
           testing::Matcher<const char *>(testing::StrEq("HPEXPIREAT")),
           testing::Matcher<const char *>(testing::StrEq("slccs")),
           testing::Matcher<ValkeyModuleString *>(testing::_), kExpiryMs,
           testing::Matcher<const char *>(testing::StrEq("FIELDS")),
           testing::Matcher<const char *>(testing::StrEq("1")),
           testing::Matcher<ValkeyModuleString *>(testing::_)))
      .WillOnce(Return(err_reply.get()))
      .WillOnce(Return(immediate_expiry_reply.get()))
      .WillOnce(Return(immediate_expiry_reply.get()));
  EXPECT_CALL(*kMockValkeyModule, Replicate(testing::_, testing::StrEq("HDEL"),
                                            testing::StrEq("ss")))
      .WillOnce(Return(VALKEYMODULE_OK));
  EXPECT_CALL(*kMockValkeyModule,
              Replicate(testing::_, testing::StrEq("DEL"), testing::StrEq("s")))
      .WillOnce(Return(VALKEYMODULE_OK));

  std::vector<InternedStringPtr> keys1 = {k1};
  registry.RemoveIndexKeys(0, attr, keys1);
  EXPECT_EQ(registry.GetStats().unshare_errors, 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 2);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);

  std::vector<InternedStringPtr> keys2 = {k2};
  registry.RemoveIndexKeys(0, attr, keys2);
  EXPECT_EQ(registry.GetStats().unshare_errors, 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);

  std::vector<InternedStringPtr> keys3 = {k3};
  registry.RemoveIndexKeys(0, attr, keys3);
  EXPECT_EQ(registry.GetStats().unshare_errors, 1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
}

TEST_F(VectorRegistryTest, UnshareWithValkeySkipsHpexpireatWhenFieldHasNoTtl) {
  auto &registry = VectorRegistry::Instance();
  auto k1 = StringInternStore::Intern("doc:no_ttl");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      k1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_EQ(registry.GetStats().entry_cnt, 1);

  CallReplyArray no_ttl_arr;
  no_ttl_arr.push_back(CreateValkeyModuleCallReply(CallReplyInteger(-1)));
  auto no_ttl_reply = CreateValkeyModuleCallReply(std::move(no_ttl_arr));

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly(Return(1));
  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::_, testing::StrEq("HPEXPIRETIME"),
                   testing::StrEq("sccs"), testing::_, testing::StrEq("FIELDS"),
                   testing::StrEq("1"), testing::_))
      .WillOnce(Return(no_ttl_reply.get()));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .WillOnce(Return(1));
  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::Matcher<ValkeyModuleCtx *>(testing::_),
                   testing::Matcher<const char *>(testing::StrEq("HPEXPIREAT")),
                   testing::Matcher<const char *>(testing::_),
                   testing::Matcher<ValkeyModuleString *>(testing::_),
                   testing::_, testing::Matcher<const char *>(testing::_),
                   testing::Matcher<const char *>(testing::_),
                   testing::Matcher<ValkeyModuleString *>(testing::_)))
      .Times(0);

  std::vector<InternedStringPtr> keys = {k1};
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().unshare_errors, 0);
}

TEST_F(VectorRegistryTest,
       UnshareWithValkeySkipsHashSetWhenFieldExpiredDuringLookup) {
  auto &registry = VectorRegistry::Instance();
  auto k1 = StringInternStore::Intern("doc:expired_during_lookup");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      k1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_EQ(registry.GetStats().entry_cnt, 1);

  CallReplyArray missing_arr;
  missing_arr.push_back(CreateValkeyModuleCallReply(CallReplyInteger(-2)));
  auto missing_reply = CreateValkeyModuleCallReply(std::move(missing_arr));

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  // First GetHashFieldRefState (in ProcessPendingUnshares before HPEXPIRETIME)
  // sees the shared field; second GetHashFieldRefState (after the field became
  // logically expired during read-only HPEXPIRETIME on a primary) sees kMissing
  // and retains the record until OnKeyDeleted.
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(testing::_, VALKEYMODULE_HASH_EXISTS, testing::_,
                      testing::An<int *>(), nullptr))
      .WillOnce(
          testing::DoAll(testing::SetArgPointee<3>(1), Return(VALKEYMODULE_OK)))
      .WillOnce(testing::DoAll(testing::SetArgPointee<3>(0),
                               Return(VALKEYMODULE_OK)));
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillOnce(Return(1));
  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::_, testing::StrEq("HPEXPIRETIME"),
                   testing::StrEq("sccs"), testing::_, testing::StrEq("FIELDS"),
                   testing::StrEq("1"), testing::_))
      .WillOnce(Return(missing_reply.get()));
  EXPECT_CALL(*kMockValkeyModule,
              HashSet(testing::_, testing::_, testing::_, testing::_, nullptr))
      .Times(0);

  std::vector<InternedStringPtr> keys = {k1};
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().unshare_errors, 0);

  registry.OnKeyDeleted(0, k1->Str());
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest,
       UnshareWithValkeyDefersReplicaExpiredFieldStillHoldingSharedRef) {
  auto &registry = VectorRegistry::Instance();
  auto k1 = StringInternStore::Intern("doc:replica_expired_shared");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      k1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_EQ(registry.GetStats().entry_cnt, 1);

  // On a standalone replica whose per-field TTL passed wall-clock time before
  // the primary's HDEL replicated, HPEXPIRETIME returns -2 while the hash dict
  // entry still physically holds a shared reference into VectorRecord under
  // POLICY_IGNORE_EXPIRE. UnshareWithValkey must keep the record tracked and
  // deferred rather than calling HashSet with EXPIRY_NONE (which would clear
  // the expiration and resurrect the expired field on failover).
  CallReplyArray expired_arr;
  expired_arr.push_back(CreateValkeyModuleCallReply(CallReplyInteger(-2)));
  auto expired_reply = CreateValkeyModuleCallReply(std::move(expired_arr));

  int ctx_flags = VALKEYMODULE_CTX_FLAGS_SLAVE;
  bool field_exists = true;
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(testing::_))
      .WillRepeatedly([&](ValkeyModuleCtx *) { return ctx_flags; });
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(testing::_, VALKEYMODULE_HASH_EXISTS, testing::_,
                      testing::An<int *>(), nullptr))
      .WillRepeatedly(
          [&](ValkeyModuleKey *, int, const char *, int *exists_out, void *) {
            *exists_out = field_exists ? 1 : 0;
            return VALKEYMODULE_OK;
          });
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly([&](ValkeyModuleKey *, ValkeyModuleString *) {
        return field_exists ? 1 : 0;
      });
  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::_, testing::StrEq("HPEXPIRETIME"),
                   testing::StrEq("sccs"), testing::_, testing::StrEq("FIELDS"),
                   testing::StrEq("1"), testing::_))
      .WillOnce(Return(expired_reply.get()))
      .WillOnce([&](ValkeyModuleCtx *, const char *, const char *,
                    ValkeyModuleString *, const char *, const char *,
                    ValkeyModuleString *) {
        field_exists = false;
        return expired_reply.get();
      });
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .Times(0);
  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::Matcher<ValkeyModuleCtx *>(testing::_),
                   testing::Matcher<const char *>(testing::StrEq("HPEXPIREAT")),
                   testing::Matcher<const char *>(testing::_),
                   testing::Matcher<ValkeyModuleString *>(testing::_),
                   testing::_, testing::Matcher<const char *>(testing::_),
                   testing::Matcher<const char *>(testing::_),
                   testing::Matcher<ValkeyModuleString *>(testing::_)))
      .Times(0);

  std::vector<InternedStringPtr> keys = {k1};
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  EXPECT_EQ(registry.GetStats().unshare_errors, 0);

  // While still a replica, cron ticks do not retry under POLICY_KEEP_EXPIRED.
  for (int i = 0; i < 20; ++i) {
    registry.OnServerCronCallback(&fake_ctx_, ValkeyModuleEvent_CronLoop, 0,
                                  nullptr);
  }
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  EXPECT_EQ(registry.GetStats().unshare_errors, 0);

  // Promote replica to primary: HPEXPIRETIME returns -2 and HashGet hides the
  // logically expired field (field_exists becomes false), HashSet is skipped,
  // pending_unshares_ drains, and the record stays retained until OnKeyDeleted.
  ctx_flags = VALKEYMODULE_CTX_FLAGS_MASTER;
  for (int i = 0; i < 9; ++i) {
    registry.OnServerCronCallback(&fake_ctx_, ValkeyModuleEvent_CronLoop, 0,
                                  nullptr);
    EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  }
  registry.OnServerCronCallback(&fake_ctx_, ValkeyModuleEvent_CronLoop, 0,
                                nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().unshare_errors, 0);

  registry.OnKeyDeleted(0, k1->Str());
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest,
       ReplicaDeferredExpiredFieldDrainsOnReplicatedMultiFieldHdel) {
  auto &registry = VectorRegistry::Instance();
  auto k1 = StringInternStore::Intern("doc:replica_multifield_hdel");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      k1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_EQ(registry.GetStats().entry_cnt, 1);

  CallReplyArray expired_arr;
  expired_arr.push_back(CreateValkeyModuleCallReply(CallReplyInteger(-2)));
  auto expired_reply = CreateValkeyModuleCallReply(std::move(expired_arr));

  int ctx_flags =
      VALKEYMODULE_CTX_FLAGS_SLAVE | VALKEYMODULE_CTX_FLAGS_REPLICATED;
  bool field_exists = true;
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(testing::_))
      .WillRepeatedly([&](ValkeyModuleCtx *) { return ctx_flags; });
  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(testing::_, VALKEYMODULE_HASH_EXISTS, testing::_,
                      testing::An<int *>(), nullptr))
      .WillRepeatedly(
          [&](ValkeyModuleKey *, int, const char *, int *exists_out, void *) {
            *exists_out = field_exists ? 1 : 0;
            return VALKEYMODULE_OK;
          });
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly([&](ValkeyModuleKey *, ValkeyModuleString *) {
        return field_exists ? 1 : 0;
      });
  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::_, testing::StrEq("HPEXPIRETIME"),
                   testing::StrEq("sccs"), testing::_, testing::StrEq("FIELDS"),
                   testing::StrEq("1"), testing::_))
      .WillOnce(Return(expired_reply.get()));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .Times(0);

  std::vector<InternedStringPtr> keys = {k1};
  registry.RemoveIndexKeys(0, attr, keys);
  ASSERT_EQ(registry.GetStats().entry_cnt, 1);
  ASSERT_EQ(registry.GetPendingUnsharesCount(), 1);

  // Non-replicated replica context hides expired fields under
  // POLICY_KEEP_EXPIRED, so OnHashKeyMutated must ignore it rather than
  // freeing VectorRecord while the entry still holds a shared pointer.
  ctx_flags = VALKEYMODULE_CTX_FLAGS_SLAVE;
  field_exists = false;
  auto key_str = vmsdk::MakeUniqueValkeyString(k1->Str());
  registry.OnHashKeyMutated(&fake_ctx_, 0, key_str.get(), k1->Str(), "hset");
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);

  // When the primary's synthesized HDEL arrives on the replication stream
  // (CTX_FLAGS_REPLICATED) for a multi-field hash that stays alive,
  // OnHashKeyMutated sees physical absence and erases the deferred record.
  ctx_flags = VALKEYMODULE_CTX_FLAGS_SLAVE | VALKEYMODULE_CTX_FLAGS_REPLICATED;
  registry.OnHashKeyMutated(&fake_ctx_, 0, key_str.get(), k1->Str(), "hdel");
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().unshare_errors, 0);
}

TEST_F(VectorRegistryTest, UnshareWithValkeyFailsClosedWhenHashSetFails) {
  auto &registry = VectorRegistry::Instance();
  auto k1 = StringInternStore::Intern("doc:hashset_fail");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      k1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_EQ(registry.GetStats().entry_cnt, 1);

  constexpr long long kExpiryMs = 1800000000000LL;  // NOLINT
  CallReplyArray expiry_arr;
  expiry_arr.push_back(
      CreateValkeyModuleCallReply(CallReplyInteger(kExpiryMs)));
  auto expiry_reply = CreateValkeyModuleCallReply(std::move(expiry_arr));

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly(Return(1));
  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::_, testing::StrEq("HPEXPIRETIME"),
                   testing::StrEq("sccs"), testing::_, testing::StrEq("FIELDS"),
                   testing::StrEq("1"), testing::_))
      .WillOnce(Return(expiry_reply.get()));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .WillOnce(Return(0));
  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::Matcher<ValkeyModuleCtx *>(testing::_),
                   testing::Matcher<const char *>(testing::StrEq("HPEXPIREAT")),
                   testing::Matcher<const char *>(testing::_),
                   testing::Matcher<ValkeyModuleString *>(testing::_),
                   testing::_, testing::Matcher<const char *>(testing::_),
                   testing::Matcher<const char *>(testing::_),
                   testing::Matcher<ValkeyModuleString *>(testing::_)))
      .Times(0);

  std::vector<InternedStringPtr> keys = {k1};
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().unshare_errors, 1);

  registry.OnKeyDeleted(0, k1->Str());
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest,
       ProcessPendingUnsharesCapsReservationWhenBatchSizeIsUint32Max) {
  auto &registry = VectorRegistry::Instance();
  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1));

  auto k1 = StringInternStore::Intern("doc:max_batch_1");
  auto k2 = StringInternStore::Intern("doc:max_batch_2");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      k1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  registry.DedupOrConstruct(
      k2, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);

  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly(Return(1));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .WillRepeatedly(Return(1));

  std::vector<InternedStringPtr> keys = {k1, k2};
  registry.RemoveIndexKeys(0, attr, keys);
  ASSERT_EQ(registry.GetPendingUnsharesCount(), 1);

  EXPECT_EQ(
      registry.ProcessPendingUnshares(std::numeric_limits<uint32_t>::max()), 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);

  VMSDK_EXPECT_OK(options::GetVectorUnshareBatchSize().SetValue(1024));
}

TEST_F(VectorRegistryTest,
       ServerCronSkipsLookupFailedRetriesOnClusterReplicaUntilPromoted) {
  auto &registry = VectorRegistry::Instance();
  auto k1 = StringInternStore::Intern("doc:cluster_replica_ttl");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      k1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_EQ(registry.GetStats().entry_cnt, 1);

  int ctx_flags = VALKEYMODULE_CTX_FLAGS_CLUSTER | VALKEYMODULE_CTX_FLAGS_SLAVE;
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(testing::_))
      .WillRepeatedly([&](ValkeyModuleCtx *) { return ctx_flags; });
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly(Return(1));

  constexpr long long kExpiryMs = 1800000000000LL;  // NOLINT
  CallReplyArray expiry_arr;
  expiry_arr.push_back(
      CreateValkeyModuleCallReply(CallReplyInteger(kExpiryMs)));
  auto expiry_reply = CreateValkeyModuleCallReply(std::move(expiry_arr));

  CallReplyArray ok_arr;
  ok_arr.push_back(CreateValkeyModuleCallReply(CallReplyInteger(1)));
  auto hpexpireat_reply = CreateValkeyModuleCallReply(std::move(ok_arr));

  // Exactly two HPEXPIRETIME calls total:
  // 1. Initial RemoveIndexKeys on the cluster replica -> nullptr (deferred).
  // 2. Retry after role promotion to cluster primary -> succeeds.
  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::_, testing::StrEq("HPEXPIRETIME"),
                   testing::StrEq("sccs"), testing::_, testing::StrEq("FIELDS"),
                   testing::StrEq("1"), testing::_))
      .WillOnce(Return(nullptr))
      .WillOnce(Return(expiry_reply.get()));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .WillOnce(Return(1));
  EXPECT_CALL(
      *kMockValkeyModule,
      Call(testing::Matcher<ValkeyModuleCtx *>(testing::_),
           testing::Matcher<const char *>(testing::StrEq("HPEXPIREAT")),
           testing::Matcher<const char *>(testing::StrEq("slccs")),
           testing::Matcher<ValkeyModuleString *>(testing::_), kExpiryMs,
           testing::Matcher<const char *>(testing::StrEq("FIELDS")),
           testing::Matcher<const char *>(testing::StrEq("1")),
           testing::Matcher<ValkeyModuleString *>(testing::_)))
      .WillOnce(Return(hpexpireat_reply.get()));

  std::vector<InternedStringPtr> keys = {k1};
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  EXPECT_EQ(registry.GetStats().unshare_errors, 1);

  // While still a cluster replica, cron ticks do not retry lookup_failed keys.
  for (int i = 0; i < 25; ++i) {
    registry.OnServerCronCallback(&fake_ctx_, ValkeyModuleEvent_CronLoop, 0,
                                  nullptr);
  }
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  EXPECT_EQ(registry.GetStats().unshare_errors, 1);

  // Promote node to cluster primary; after backoff window, cron retries and
  // drains the deferred unshare while preserving the field TTL.
  ctx_flags = VALKEYMODULE_CTX_FLAGS_CLUSTER | VALKEYMODULE_CTX_FLAGS_MASTER;
  for (int i = 0; i < 9; ++i) {
    registry.OnServerCronCallback(&fake_ctx_, ValkeyModuleEvent_CronLoop, 0,
                                  nullptr);
    EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  }
  registry.OnServerCronCallback(&fake_ctx_, ValkeyModuleEvent_CronLoop, 0,
                                nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().unshare_errors, 1);
}

TEST_F(VectorRegistryTest,
       ServerCronBacksOffLookupFailedRetriesOnPrimaryAndCountsErrorOnce) {
  auto &registry = VectorRegistry::Instance();
  auto k1 = StringInternStore::Intern("doc:clusterdown_1");
  auto k2 = StringInternStore::Intern("doc:clusterdown_2");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      k1, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  registry.DedupOrConstruct(
      k2, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_EQ(registry.GetStats().entry_cnt, 2);

  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_CTX_FLAGS_CLUSTER |
                             VALKEYMODULE_CTX_FLAGS_MASTER));
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly(Return(1));

  auto err_reply = std::make_unique<ValkeyModuleCallReply>();
  err_reply->type = VALKEYMODULE_REPLY_ERROR;
  err_reply->msg = "CLUSTERDOWN The cluster is down";

  CallReplyArray no_ttl_arr;
  no_ttl_arr.push_back(CreateValkeyModuleCallReply(CallReplyInteger(-1)));
  auto no_ttl_reply = CreateValkeyModuleCallReply(std::move(no_ttl_arr));

  // Call sequence for HPEXPIRETIME:
  // 1. k1 initial drop -> CLUSTERDOWN (unshare_errors = 1).
  // 2. k2 drop -> succeeds immediately (RemoveIndexKeys does not re-probe k1).
  // 3. k1 first cron retry at tick 10 -> still CLUSTERDOWN (unshare_errors
  //    remains 1).
  // 4. k1 second cron retry at tick 20 -> succeeds and drains.
  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::_, testing::StrEq("HPEXPIRETIME"),
                   testing::StrEq("sccs"), testing::_, testing::StrEq("FIELDS"),
                   testing::StrEq("1"), testing::_))
      .WillOnce(Return(err_reply.get()))
      .WillOnce(Return(no_ttl_reply.get()))
      .WillOnce(Return(err_reply.get()))
      .WillOnce(Return(no_ttl_reply.get()));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .Times(2)
      .WillRepeatedly(Return(1));

  std::vector<InternedStringPtr> keys1 = {k1};
  registry.RemoveIndexKeys(0, attr, keys1);
  EXPECT_EQ(registry.GetStats().entry_cnt, 2);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  EXPECT_EQ(registry.GetStats().unshare_errors, 1);

  // Dropping k2 drains k2 immediately without retrying k1.
  std::vector<InternedStringPtr> keys2 = {k2};
  registry.RemoveIndexKeys(0, attr, keys2);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  EXPECT_EQ(registry.GetStats().unshare_errors, 1);

  for (int i = 0; i < 10; ++i) {
    registry.OnServerCronCallback(&fake_ctx_, ValkeyModuleEvent_CronLoop, 0,
                                  nullptr);
  }
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  EXPECT_EQ(registry.GetStats().unshare_errors, 1);

  for (int i = 0; i < 10; ++i) {
    registry.OnServerCronCallback(&fake_ctx_, ValkeyModuleEvent_CronLoop, 0,
                                  nullptr);
  }
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().unshare_errors, 1);
}

TEST_F(VectorRegistryTest,
       ReplicaAsyncRemoveIndexKeysDefersHiddenExpiredFieldUntilPromotion) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("doc:replica_async_drop");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_EQ(registry.GetStats().entry_cnt, 1);

  // When VectorBase::~VectorBase() runs via RunByMain on a replica (SLAVE set,
  // REPLICATED unset), Valkey's POLICY_KEEP_EXPIRED hides expired hash fields
  // from HashGet(VALKEYMODULE_HASH_EXISTS) even while the hashtable entry still
  // holds a stringRef. RemoveIndexKeys must keep the record tracked and
  // deferred until replicated HDEL or failover promotion.
  int ctx_flags = VALKEYMODULE_CTX_FLAGS_SLAVE;
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(testing::_))
      .WillRepeatedly(
          [&ctx_flags](ValkeyModuleCtx *) -> int { return ctx_flags; });
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(testing::_, VALKEYMODULE_HASH_EXISTS, testing::_,
                      testing::An<int *>(), nullptr))
      .WillRepeatedly(
          [](ValkeyModuleKey *, int, const char *, int *exists_out, void *) {
            *exists_out = 0;
            return VALKEYMODULE_OK;
          });
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .Times(0);

  std::vector<InternedStringPtr> keys = {key};
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  EXPECT_EQ(registry.GetStats().unshare_errors, 0);

  // Cron on replica leaves the deferred record untouched.
  for (int i = 0; i < 15; ++i) {
    registry.OnServerCronCallback(&fake_ctx_, ValkeyModuleEvent_CronLoop, 0,
                                  nullptr);
  }
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);

  // After promotion to primary, cron drains pending_unshares_ without calling
  // HashSet while retaining the record until OnKeyDeleted.
  ctx_flags = VALKEYMODULE_CTX_FLAGS_MASTER;
  for (int i = 0; i < 10; ++i) {
    registry.OnServerCronCallback(&fake_ctx_, ValkeyModuleEvent_CronLoop, 0,
                                  nullptr);
  }
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().unshare_errors, 0);

  registry.OnKeyDeleted(0, key->Str());
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest,
       UnshareWithValkeyDefersNearExpiryOnPrimaryWithoutCountingError) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("doc:primary_near_expiry");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_EQ(registry.GetStats().entry_cnt, 1);

  constexpr long long kNowMs = 1800000000000LL;    // NOLINT
  constexpr long long kNearExpiryMs = kNowMs + 3;  // NOLINT
  CallReplyArray near_arr;
  near_arr.push_back(
      CreateValkeyModuleCallReply(CallReplyInteger(kNearExpiryMs)));
  auto near_reply = CreateValkeyModuleCallReply(std::move(near_arr));

  bool field_exists = true;
  long long now_ms = kNowMs;
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_CTX_FLAGS_MASTER));
  EXPECT_CALL(*kMockValkeyModule, Milliseconds())
      .WillRepeatedly([&]() -> mstime_t { return now_ms; });
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, testing::_,
                      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(testing::_, VALKEYMODULE_HASH_EXISTS, testing::_,
                      testing::An<int *>(), nullptr))
      .WillRepeatedly(
          [&](ValkeyModuleKey *, int, const char *, int *exists_out, void *) {
            *exists_out = field_exists ? 1 : 0;
            return VALKEYMODULE_OK;
          });
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly([&](ValkeyModuleKey *, ValkeyModuleString *) {
        return field_exists ? 1 : 0;
      });
  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::_, testing::StrEq("HPEXPIRETIME"),
                   testing::StrEq("sccs"), testing::_, testing::StrEq("FIELDS"),
                   testing::StrEq("1"), testing::_))
      .WillOnce(Return(near_reply.get()));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .Times(0);
  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::Matcher<ValkeyModuleCtx *>(testing::_),
                   testing::Matcher<const char *>(testing::StrEq("HPEXPIREAT")),
                   testing::Matcher<const char *>(testing::_),
                   testing::Matcher<ValkeyModuleString *>(testing::_),
                   testing::_, testing::Matcher<const char *>(testing::_),
                   testing::Matcher<const char *>(testing::_),
                   testing::Matcher<ValkeyModuleString *>(testing::_)))
      .Times(0);

  std::vector<InternedStringPtr> keys = {key};
  registry.RemoveIndexKeys(0, attr, keys);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  EXPECT_EQ(registry.GetStats().unshare_errors, 0);

  // Once the timestamp passes on the primary, HashGet(HASH_EXISTS) hides the
  // logically expired field (kMissing) even before activeExpireCycle reaps the
  // dictEntry; the lookup_failed retry drains pending_unshares_ without calling
  // HashSet while retaining the record until OnKeyDeleted.
  now_ms = kNowMs + 20;
  field_exists = false;
  for (int i = 0; i < 10; ++i) {
    registry.OnServerCronCallback(&fake_ctx_, ValkeyModuleEvent_CronLoop, 0,
                                  nullptr);
  }
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().unshare_errors, 0);

  registry.OnKeyDeleted(0, key->Str());
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

TEST_F(VectorRegistryTest,
       ReplicaBackfillNullptrRetainsAndEnqueuesHiddenExpiredField) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("doc:replica_backfill_expired");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_EQ(registry.GetStats().entry_cnt, 1);

  int ctx_flags = VALKEYMODULE_CTX_FLAGS_SLAVE;
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(testing::_))
      .WillRepeatedly([&](ValkeyModuleCtx *) { return ctx_flags; });
  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(testing::_, VALKEYMODULE_HASH_EXISTS, testing::_,
                      testing::An<int *>(), nullptr))
      .WillRepeatedly(
          [](ValkeyModuleKey *, int, const char *, int *exists_out, void *) {
            *exists_out = 0;
            return VALKEYMODULE_OK;
          });

  // Replica cron backfill runs without REPLICATED, so POLICY_KEEP_EXPIRED hides
  // an expired hash field (vector == nullptr, HashGet exists == 0) while the
  // hashtable entry still holds a shared stringRef. DedupOrConstruct must keep
  // the record tracked and queue it in lookup_failed_deferred.
  auto rec = registry.DedupOrConstruct(
      key, nullptr, data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0,
      &index, &fake_ctx_);
  EXPECT_EQ(rec, nullptr);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);
  EXPECT_EQ(registry.GetStats().unshare_errors, 0);

  for (int i = 0; i < 15; ++i) {
    registry.OnServerCronCallback(&fake_ctx_, ValkeyModuleEvent_CronLoop, 0,
                                  nullptr);
  }
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 1);

  // Replicated HDEL on the live multi-field hash drains the deferred record.
  ctx_flags = VALKEYMODULE_CTX_FLAGS_SLAVE | VALKEYMODULE_CTX_FLAGS_REPLICATED;
  auto key_str = vmsdk::MakeUniqueValkeyString(key->Str());
  registry.OnHashKeyMutated(&fake_ctx_, 0, key_str.get(), key->Str(), "hdel");
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().unshare_errors, 0);
}

TEST_F(
    VectorRegistryTest,
    PrimaryHexpiredOnLookupFailedKeyCancelsPendingAndRetainsUntilKeyDeleted) {
  auto &registry = VectorRegistry::Instance();
  auto key = StringInternStore::Intern("doc:primary_other_field_hexpired");
  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  auto valkey_vec = MakeFloatVectorString();

  registry.DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  ASSERT_EQ(registry.GetStats().entry_cnt, 1);

  constexpr long long kNowMs = 1800000000000LL;    // NOLINT
  constexpr long long kNearExpiryMs = kNowMs + 2;  // NOLINT
  CallReplyArray near_arr;
  near_arr.push_back(
      CreateValkeyModuleCallReply(CallReplyInteger(kNearExpiryMs)));
  auto near_reply = CreateValkeyModuleCallReply(std::move(near_arr));

  bool field_exists = true;
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_CTX_FLAGS_MASTER));
  EXPECT_CALL(*kMockValkeyModule, Milliseconds())
      .WillRepeatedly(Return(kNowMs));
  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(testing::_, VALKEYMODULE_HASH_EXISTS, testing::_,
                      testing::An<int *>(), nullptr))
      .WillRepeatedly(
          [&](ValkeyModuleKey *, int, const char *, int *exists_out, void *) {
            *exists_out = field_exists ? 1 : 0;
            return VALKEYMODULE_OK;
          });
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .WillRepeatedly([&](ValkeyModuleKey *, ValkeyModuleString *) {
        return field_exists ? 1 : 0;
      });
  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::_, testing::StrEq("HPEXPIRETIME"),
                   testing::StrEq("sccs"), testing::_, testing::StrEq("FIELDS"),
                   testing::StrEq("1"), testing::_))
      .WillOnce(Return(near_reply.get()));
  EXPECT_CALL(*kMockValkeyModule, HashSet(testing::_, VALKEYMODULE_HASH_NONE,
                                          testing::_, testing::_, nullptr))
      .Times(0);

  std::vector<InternedStringPtr> keys = {key};
  registry.RemoveIndexKeys(0, attr, keys);
  ASSERT_EQ(registry.GetStats().entry_cnt, 1);
  ASSERT_EQ(registry.GetPendingUnsharesCount(), 1);

  // If "vec" is now logically expired (hidden by HashGet -> kMissing while its
  // dictEntry is not yet reaped) and a different field on the same hash expires
  // and fires "hexpired", OnHashKeyMutated must cancel the pending unshare
  // while keeping the VectorRecord alive until OnKeyDeleted.
  field_exists = false;
  auto key_str = vmsdk::MakeUniqueValkeyString(key->Str());
  registry.OnHashKeyMutated(&fake_ctx_, 0, key_str.get(), key->Str(),
                            "hexpired");
  EXPECT_EQ(registry.GetPendingUnsharesCount(), 0);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
  EXPECT_EQ(registry.GetStats().unshare_errors, 0);

  registry.OnKeyDeleted(0, key->Str());
  EXPECT_EQ(registry.GetStats().entry_cnt, 0);
}

}  // namespace valkey_search
