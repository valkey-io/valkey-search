/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_TESTING_COMMON_H_
#define VALKEYSEARCH_TESTING_COMMON_H_

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/attribute_data_type.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/metadata_manager.h"
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"
#include "src/keyspace_event_manager.h"
#include "src/query/search.h"
#include "src/rdb_serialization.h"
#include "src/schema_manager.h"
#include "src/server_events.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search.h"
#include "src/vector_registry.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
template <typename T, typename K>
class IndexTeser : public T {
 public:
  explicit IndexTeser(K proto) : T(K(proto)) {}
  // AddRecord/ModifyRecord return true iff the value was indexed (kAdded), so
  // existing tests that check success/skip via a bool keep working. Use the
  // *Result variants below to distinguish kMissing from kInvalidData.
  absl::StatusOr<bool> AddRecord(absl::string_view key,
                                 absl::string_view data) {
    auto interned_key = StringInternStore::Intern(key);
    auto res = T::AddRecord(interned_key,
                            AttributeData(vmsdk::MakeUniqueValkeyString(data)));
    if (!res.ok()) {
      return res.status();
    }
    return *res == indexes::RecordResult::kAdded;
  }
  absl::StatusOr<indexes::RecordResult> AddRecordResult(
      absl::string_view key, absl::string_view data) {
    auto interned_key = StringInternStore::Intern(key);
    return T::AddRecord(interned_key,
                        AttributeData(vmsdk::MakeUniqueValkeyString(data)));
  }
  absl::StatusOr<bool> RemoveRecord(
      absl::string_view key,
      indexes::DeletionType deletion_type = indexes::DeletionType::kNone) {
    auto interned_key = StringInternStore::Intern(key);
    return T::RemoveRecord(interned_key, deletion_type);
  }
  absl::StatusOr<bool> ModifyRecord(absl::string_view key,
                                    absl::string_view data) {
    auto interned_key = StringInternStore::Intern(key);
    auto res = T::ModifyRecord(
        interned_key, AttributeData(vmsdk::MakeUniqueValkeyString(data)));
    if (!res.ok()) {
      return res.status();
    }
    return *res == indexes::RecordResult::kAdded;
  }
  absl::StatusOr<indexes::RecordResult> ModifyRecordResult(
      absl::string_view key, absl::string_view data) {
    auto interned_key = StringInternStore::Intern(key);
    return T::ModifyRecord(interned_key,
                           AttributeData(vmsdk::MakeUniqueValkeyString(data)));
  }
  bool IsTracked(absl::string_view key) const {
    auto interned_key = StringInternStore::Intern(key);
    return T::IsTracked(interned_key);
  }
};

namespace testing_infra {

inline AttributeData MakeAttributeData(vmsdk::UniqueValkeyString str) {
  return AttributeData(std::move(str));
}

inline AttributeData MakeAttributeData(absl::string_view sv) {
  return AttributeData(vmsdk::MakeUniqueValkeyString(sv));
}

inline AttributeData MakeAttributeData(indexes::VectorBase &index,
                                       const InternedStringPtr &key,
                                       absl::string_view data) {
  auto valkey_str = vmsdk::MakeUniqueValkeyString(data);
  auto vec = VectorRegistry::Instance().DedupOrConstruct(
      key, valkey_str.get(), data_model::ATTRIBUTE_DATA_TYPE_HASH,
      index.GetDBNum(), &index);
  return AttributeData(std::move(vec));
}

inline absl::StatusOr<indexes::RecordResult> AddRecord(
    indexes::IndexBase &index, const InternedStringPtr &key,
    AttributeData &&data) {
  return index.AddRecord(key, std::move(data));
}

inline absl::StatusOr<indexes::RecordResult> AddRecord(
    indexes::IndexBase &index, const InternedStringPtr &key,
    vmsdk::UniqueValkeyString str) {
  return index.AddRecord(key, AttributeData(std::move(str)));
}

inline absl::StatusOr<indexes::RecordResult> AddRecord(
    indexes::IndexBase &index, const InternedStringPtr &key,
    absl::string_view data) {
  return index.AddRecord(key,
                         AttributeData(vmsdk::MakeUniqueValkeyString(data)));
}

inline absl::StatusOr<indexes::RecordResult> AddVectorRecord(
    indexes::VectorBase &index, const InternedStringPtr &key,
    absl::string_view data) {
  auto valkey_str = vmsdk::MakeUniqueValkeyString(data);
  auto vec = VectorRegistry::Instance().DedupOrConstruct(
      key, valkey_str.get(), data_model::ATTRIBUTE_DATA_TYPE_HASH,
      index.GetDBNum(), &index);
  return index.AddRecord(key, AttributeData(std::move(vec)));
}

inline absl::StatusOr<indexes::RecordResult> ModifyRecord(
    indexes::IndexBase &index, const InternedStringPtr &key,
    AttributeData &&data) {
  return index.ModifyRecord(key, std::move(data));
}

inline absl::StatusOr<indexes::RecordResult> ModifyRecord(
    indexes::IndexBase &index, const InternedStringPtr &key,
    vmsdk::UniqueValkeyString str) {
  return index.ModifyRecord(key, AttributeData(std::move(str)));
}

inline absl::StatusOr<indexes::RecordResult> ModifyRecord(
    indexes::IndexBase &index, const InternedStringPtr &key,
    absl::string_view data) {
  return index.ModifyRecord(key,
                            AttributeData(vmsdk::MakeUniqueValkeyString(data)));
}

inline absl::StatusOr<indexes::RecordResult> ModifyVectorRecord(
    indexes::VectorBase &index, const InternedStringPtr &key,
    absl::string_view data) {
  auto valkey_str = vmsdk::MakeUniqueValkeyString(data);
  auto vec = VectorRegistry::Instance().DedupOrConstruct(
      key, valkey_str.get(), data_model::ATTRIBUTE_DATA_TYPE_HASH,
      index.GetDBNum(), &index);
  return index.ModifyRecord(key, AttributeData(std::move(vec)));
}

}  // namespace testing_infra

class MockIndex : public indexes::VectorBase {
 public:
  MockIndex(int dimensions = 4, absl::string_view attribute_name = "vec",
            int db_num = 0)
      : indexes::VectorBase(indexes::IndexerType::kVector, dimensions,
                            data_model::ATTRIBUTE_DATA_TYPE_HASH,
                            attribute_name, db_num) {
    ON_CALL(*this, GetDataTypeSize)
        .WillByDefault(testing::Return(sizeof(float)));
  }
  MockIndex(indexes::IndexerType type, int dimensions = 4,
            absl::string_view attribute_name = "vec", int db_num = 0)
      : indexes::VectorBase(type, dimensions,
                            data_model::ATTRIBUTE_DATA_TYPE_HASH,
                            attribute_name, db_num) {
    ON_CALL(*this, GetDataTypeSize)
        .WillByDefault(testing::Return(sizeof(float)));
  }
  MOCK_METHOD(absl::StatusOr<indexes::RecordResult>, AddRecord,
              (const InternedStringPtr &key, AttributeData &&data), (override));
  MOCK_METHOD(absl::StatusOr<bool>, RemoveRecord,
              (const InternedStringPtr &key,
               indexes::DeletionType deletion_type),
              (override));
  MOCK_METHOD(absl::StatusOr<indexes::RecordResult>, ModifyRecord,
              (const InternedStringPtr &key, AttributeData &&data), (override));
  MOCK_METHOD(std::unique_ptr<data_model::Index>, ToProto, (),
              (const, override));
  MOCK_METHOD(int, RespondWithInfo, (ValkeyModuleCtx * ctx), (const, override));
  MOCK_METHOD(absl::Status, SaveIndex, (RDBChunkOutputStream chunked_out),
              (const, override));
  MOCK_METHOD((size_t), GetTrackedKeyCount, (), (const, override));
  MOCK_METHOD((size_t), GetUnTrackedKeyCount, (), (const, override));
  MOCK_METHOD(bool, IsTracked, (const InternedStringPtr &key),
              (const, override));
  MOCK_METHOD(bool, IsUnTracked, (const InternedStringPtr &key),
              (const, override));
  MOCK_METHOD(void, UnTrack, (const InternedStringPtr &key), (override));
  MOCK_METHOD(
      (absl::Status), ForEachTrackedKey,
      (absl::AnyInvocable<absl::Status(const InternedStringPtr &key)> fn),
      (const, override));
  MOCK_METHOD(
      (absl::Status), ForEachUnTrackedKey,
      (absl::AnyInvocable<absl::Status(const InternedStringPtr &key)> fn),
      (const, override));
  MOCK_METHOD(uint32_t, GetMutationWeight, (), (const, override));
  MOCK_METHOD(size_t, GetCapacity, (), (const, override));
  MOCK_METHOD(absl::Status, RemoveRecordImpl, (uint64_t internal_id),
              (override));
  absl::Status ModifyRecordImpl(
      uint64_t, std::shared_ptr<const indexes::VectorRecord> &&) override {
    return absl::OkStatus();
  }
  absl::Status AddRecordImpl(
      uint64_t, std::shared_ptr<const indexes::VectorRecord> &&) override {
    return absl::OkStatus();
  }
  MOCK_METHOD(int, RespondWithInfoImpl, (ValkeyModuleCtx * ctx),
              (const, override));
  MOCK_METHOD(size_t, GetDataTypeSize, (), (const, override));
  MOCK_METHOD(void, ToProtoImpl, (data_model::VectorIndex *),
              (const, override));
  absl::Status SaveIndexImpl(RDBChunkOutputStream) const override {
    return absl::OkStatus();
  }
  std::shared_ptr<const indexes::VectorRecord> &GetVectorLockFree(
      uint64_t) const override {
    static std::shared_ptr<const indexes::VectorRecord> p;
    return p;
  }
  std::shared_ptr<const indexes::VectorRecord> &GetVector(
      uint64_t) const override {
    static std::shared_ptr<const indexes::VectorRecord> p;
    return p;
  }
  float ComputeDistance(absl::string_view, const indexes::VectorRecord *,
                        float) const override {
    return 0.0f;
  }
  std::optional<hnswlib::tableint> GetAlgoIdLockFree(uint64_t) const override {
    return std::nullopt;
  }
};

class MockKeyspaceEventSubscription : public KeyspaceEventSubscription {
 public:
  MOCK_METHOD(AttributeDataType &, GetAttributeDataType, (), (override, const));
  MOCK_METHOD(const std::vector<std::string> &, GetKeyPrefixes, (),
              (override, const));
  MOCK_METHOD(void, OnKeyspaceNotification,
              (ValkeyModuleCtx * ctx, int type, const char *event,
               ValkeyModuleString *key),
              (override));
  std::vector<const indexes::VectorBase *> GetVectorIndexes() const override {
    return vector_indexes_;
  }
  bool IsInDB(int db_num) const override {
    return db_num_ == -1 || db_num_ == db_num;
  }
  std::vector<const indexes::VectorBase *> vector_indexes_;
  int db_num_{-1};
};

class MockAttributeDataType : public AttributeDataType {
 public:
  MockAttributeDataType() {
    ON_CALL(*this, IsProperType(testing::_))
        .WillByDefault(testing::Return(true));
  }
  MOCK_METHOD(absl::StatusOr<vmsdk::UniqueValkeyString>, GetAttribute,
              (ValkeyModuleCtx * ctx, ValkeyModuleKey *open_key,
               absl::string_view key, absl::string_view identifier),
              (override, const));
  MOCK_METHOD(int, GetValkeyEventTypes, (), (override, const));
  MOCK_METHOD((absl::StatusOr<RecordsMap>), FetchAllAttributes,
              (ValkeyModuleCtx * ctx,
               const std::optional<std::string> &query_attribute_name,
               ValkeyModuleKey *open_key, absl::string_view key,
               const absl::flat_hash_set<absl::string_view> &identifiers),
              (override, const));
  MOCK_METHOD((data_model::AttributeDataType), ToProto, (), (override, const));
  MOCK_METHOD((std::string), ToString, (), (override, const));
  MOCK_METHOD((bool), IsProperType, (ValkeyModuleKey * key), (override, const));
  MOCK_METHOD(bool, AttributesProvidedAsString, (), (override, const));
};

class FakeSafeRDB : public SafeRDB {
 public:
  FakeSafeRDB() = default;
  FakeSafeRDB(unsigned char dump_rdb[], size_t len) {
    buffer_.write((const char *)dump_rdb, len);
  }
  absl::StatusOr<size_t> LoadSizeT() override { return LoadPOD<size_t>(); }
  absl::StatusOr<unsigned int> LoadUnsigned() override {
    return LoadPOD<unsigned int>();
  }
  absl::StatusOr<int> LoadSigned() override { return LoadPOD<int>(); }
  absl::StatusOr<double> LoadDouble() override { return LoadPOD<double>(); }

  absl::StatusOr<vmsdk::UniqueValkeyString> LoadString() override {
    auto len = LoadPOD<size_t>();
    auto _str = std::make_unique<char[]>(len);
    buffer_.read(_str.get(), len);
    EXPECT_TRUE(buffer_);
    auto str = vmsdk::UniqueValkeyString(
        ValkeyModule_CreateString(nullptr, _str.get(), len));
    return str;
  }

  absl::Status SaveSizeT(size_t val) override {
    SavePOD(val);
    return absl::OkStatus();
  }
  absl::Status SaveUnsigned(unsigned int val) override {
    SavePOD(val);
    return absl::OkStatus();
  }
  absl::Status SaveSigned(int val) override {
    SavePOD(val);
    return absl::OkStatus();
  }
  absl::Status SaveDouble(double val) override {
    SavePOD(val);
    return absl::OkStatus();
  }

  absl::Status SaveStringBuffer(absl::string_view buf) override {
    SavePOD(buf.size());
    buffer_.write(buf.data(), buf.size());
    EXPECT_TRUE(buffer_);
    return absl::OkStatus();
  }

  std::stringstream buffer_;

 private:
  template <typename T>
  T LoadPOD() {
    T val;
    buffer_.read((char *)&val, sizeof(T));
    EXPECT_TRUE(buffer_);
    return val;
  }

  template <typename T>
  void SavePOD(const T val) {
    buffer_.write((char *)&val, sizeof(T));
    EXPECT_TRUE(buffer_);
  }
};

data_model::VectorIndex CreateHNSWVectorIndexProto(
    int dimensions, data_model::DistanceMetric distance_metric, int initial_cap,
    int m, int ef_construction, size_t ef_runtime);

data_model::VectorIndex CreateFlatVectorIndexProto(
    int dimensions, data_model::DistanceMetric distance_metric, int initial_cap,
    uint32_t block_size);

data_model::NumericIndex CreateNumericIndexProto();

data_model::TagIndex CreateTagIndexProto(const std::string &separator = ",",
                                         bool case_sensitive = false);

data_model::TextIndex CreateTextIndexProto(bool with_suffix_trie, bool no_stem,
                                           double weight);

class MockIndexSchema : public IndexSchema {
 public:
  static absl::StatusOr<std::shared_ptr<MockIndexSchema>> Create(
      ValkeyModuleCtx *ctx, absl::string_view key,
      const std::vector<absl::string_view> &subscribed_key_prefixes,
      std::unique_ptr<AttributeDataType> attribute_data_type,
      vmsdk::ThreadPool *mutations_thread_pool = nullptr,
      data_model::Language language = data_model::Language::LANGUAGE_ENGLISH,
      std::string punctuation = ".", bool with_offsets = true,
      const std::vector<std::string> &stop_words = {}, float score = 1.0,
      const std::string &score_field = "", int db_num = 0) {
    if (mutations_thread_pool == nullptr) {
      mutations_thread_pool = ValkeySearch::Instance().GetWriterThreadPool();
    }
    data_model::IndexSchema index_schema_proto;
    index_schema_proto.set_name(std::string(key));
    index_schema_proto.set_db_num(db_num);
    index_schema_proto.mutable_subscribed_key_prefixes()->Add(
        subscribed_key_prefixes.begin(), subscribed_key_prefixes.end());
    index_schema_proto.set_language(language);
    index_schema_proto.set_punctuation(punctuation);
    index_schema_proto.set_with_offsets(with_offsets);
    index_schema_proto.mutable_stop_words()->Add(stop_words.begin(),
                                                 stop_words.end());
    index_schema_proto.set_score(score);
    if (!score_field.empty()) {
      index_schema_proto.set_score_field(score_field);
    }
    // NOLINTNEXTLINE
    auto res = std::shared_ptr<MockIndexSchema>(
        new MockIndexSchema(ctx, index_schema_proto,
                            std::move(attribute_data_type),
                            mutations_thread_pool),
        vmsdk::DestructByMainThread<MockIndexSchema>{});
    VMSDK_RETURN_IF_ERROR(res->Init(ctx));
    return res;
  }
  MockIndexSchema(ValkeyModuleCtx *ctx,
                  const data_model::IndexSchema &index_schema_proto,
                  std::unique_ptr<AttributeDataType> attribute_data_type,
                  vmsdk::ThreadPool *mutations_thread_pool = nullptr,
                  bool reload = false)
      : IndexSchema(ctx, index_schema_proto, std::move(attribute_data_type),
                    mutations_thread_pool
                        ? mutations_thread_pool
                        : ValkeySearch::Instance().GetWriterThreadPool(),
                    reload) {
    ON_CALL(*this, OnLoadingEnded(testing::_))
        .WillByDefault([this](ValkeyModuleCtx *ctx) {
          IndexSchema::OnLoadingEnded(ctx);
          return;
        });
    ON_CALL(*this, OnSwapDB(testing::_))
        .WillByDefault([this](ValkeyModuleSwapDbInfo *swap_db_info) {
          IndexSchema::OnSwapDB(swap_db_info);
          return;
        });
    ON_CALL(*this, RDBSave(testing::_)).WillByDefault([this](SafeRDB *rdb) {
      return IndexSchema::RDBSave(rdb);
    });
    ON_CALL(*this, GetIdentifier(testing::_))
        .WillByDefault([](absl::string_view attribute_name) {
          return std::string(attribute_name);
        });
  }
  MOCK_METHOD(void, OnLoadingEnded, (ValkeyModuleCtx * ctx), (override));
  MOCK_METHOD(void, OnSwapDB, (ValkeyModuleSwapDbInfo * swap_db_info),
              (override));
  MOCK_METHOD(absl::Status, RDBSave, (SafeRDB * rdb), (const, override));
  MOCK_METHOD(absl::StatusOr<std::string>, GetIdentifier,
              (absl::string_view attribute_name), (const, override));
};

// TestableValkeySearch subclasses ValkeySearch and makes it creatable for
// testing purposes.
class TestableValkeySearch : public ValkeySearch {
 public:
  void InitThreadPools(std::optional<size_t> readers,
                       std::optional<size_t> writers,
                       std::optional<size_t> utility);
};

class TestableSchemaManager : public SchemaManager {
 public:
  TestableSchemaManager(
      ValkeyModuleCtx *ctx,
      absl::AnyInvocable<void()> server_events_callback = []() {},
      vmsdk::ThreadPool *writer_thread_pool = nullptr,
      bool coordinator_enabled = false)
      : SchemaManager(ctx, std::move(server_events_callback),
                      writer_thread_pool, coordinator_enabled) {}
};

class TestableMetadataManager : public coordinator::MetadataManager {
 public:
  TestableMetadataManager(ValkeyModuleCtx *ctx,
                          coordinator::ClientPool &client_pool)
      : coordinator::MetadataManager(ctx, client_pool) {}
};

inline void InitThreadPools(std::optional<size_t> readers,
                            std::optional<size_t> writers,
                            std::optional<size_t> utility) {
  ((TestableValkeySearch *)&ValkeySearch::Instance())
      ->InitThreadPools(readers, writers, utility);
}

absl::StatusOr<std::shared_ptr<MockIndexSchema>> CreateIndexSchema(
    std::string index_schema_key, ValkeyModuleCtx *fake_ctx = nullptr,
    vmsdk::ThreadPool *writer_thread_pool = nullptr,
    const std::vector<absl::string_view> *key_prefixes = nullptr,
    int32_t index_schema_db_num = 0);
absl::StatusOr<std::shared_ptr<MockIndexSchema>> CreateVectorHNSWSchema(
    std::string index_schema_key, ValkeyModuleCtx *fake_ctx = nullptr,
    vmsdk::ThreadPool *writer_thread_pool = nullptr,
    const std::vector<absl::string_view> *key_prefixes = nullptr,
    int32_t index_schema_db_num = 0);

// TestableKeyspaceEventManager subclasses KeyspaceEventManager and makes it
// creatable for testing purposes.
class TestableKeyspaceEventManager : public KeyspaceEventManager {
 public:
  TestableKeyspaceEventManager() = default;
};

class MockThreadPool : public vmsdk::ThreadPool {
 public:
  MockThreadPool(const std::string &name, size_t num_threads)
      : vmsdk::ThreadPool(name, num_threads) {
    ON_CALL(*this, Schedule(testing::_, testing::_))
        .WillByDefault(
            [this](absl::AnyInvocable<void()> task, Priority priority) {
              return ThreadPool::Schedule(std::move(task), priority);
            });
  }
  MOCK_METHOD(bool, Schedule,
              (absl::AnyInvocable<void()> task, Priority priority), (override));
};

void WaitWorkerTasksAreCompleted(vmsdk::ThreadPool &mutations_thread_pool);

class ValkeySearchTest : public vmsdk::ValkeyTest {
 protected:
  ValkeyModuleCtx fake_ctx_;
  ValkeyModuleCtx registry_ctx_;

  void SetUp() override {
    auto &enable_sharing =
        const_cast<vmsdk::config::Boolean &>(options::GetEnableVectorSharing());
    VMSDK_EXPECT_OK(enable_sharing.SetValue(false));
    ValkeyTest::SetUp();
    ValkeySearch::InitInstance(std::make_unique<TestableValkeySearch>());
    InitThreadPools(/*readers=*/std::nullopt, /*writers=*/1,
                    /*utility=*/std::nullopt);
    KeyspaceEventManager::InitInstance(
        std::make_unique<TestableKeyspaceEventManager>());
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() { server_events::SubscribeToServerEvents(); },
        ValkeySearch::Instance().GetWriterThreadPool(), false));
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault([&](ValkeyModuleCtx *ctx) {
          return ctx == &registry_ctx_ ? ctx : nullptr;
        });
    VectorRegistry::Construct(&registry_ctx_);
  }
  void TearDown() override {
    if (ValkeySearch::HasInstance()) {
      if (auto *pool = ValkeySearch::Instance().GetWriterThreadPool()) {
        if (pool->IsSuspended()) {
          (void)pool->ResumeWorkers();
        }
        WaitWorkerTasksAreCompleted(*pool);
      }
    }
    kMockValkeyModule->RunPendingOneShots();
    SchemaManager::InitInstance(nullptr);
    ValkeySearch::InitInstance(nullptr);
    KeyspaceEventManager::InitInstance(nullptr);
    VectorRegistry::Destruct();
    auto &enable_sharing =
        const_cast<vmsdk::config::Boolean &>(options::GetEnableVectorSharing());
    VMSDK_EXPECT_OK(enable_sharing.SetValue(true));
    ValkeyTest::TearDown();
  }
};

struct TestReturnAttribute {
  std::string identifier;
  std::string alias;
};

query::ReturnAttribute ToReturnAttribute(
    const TestReturnAttribute &test_return_attribute);

std::unordered_map<std::string, std::string> ToStringMap(const RecordsMap &map);

struct NeighborTest {
  std::string external_id;
  float score;
  std::optional<std::unordered_map<std::string, std::string>>
      attribute_contents;
  bool operator==(const NeighborTest &other) const {
    return external_id == other.external_id && score == other.score &&
           attribute_contents == other.attribute_contents;
  }
};

indexes::Neighbor ToIndexesNeighbor(const NeighborTest &neighbor_test);
NeighborTest ToNeighborTest(const indexes::Neighbor &neighbor_test);

template <typename T>
std::vector<NeighborTest> ToVectorNeighborTest(const T &neighbors) {
  std::vector<NeighborTest> neighbors_test(neighbors.size());
  for (const auto &neighbor : neighbors) {
    neighbors_test.push_back(ToNeighborTest(neighbor));
  }
  return neighbors_test;
}

template <typename T>
class ValkeySearchTestWithParam : public vmsdk::ValkeyTestWithParam<T> {
 protected:
  ValkeyModuleCtx fake_ctx_;
  ValkeyModuleCtx registry_ctx_;

  void SetUp() override {
    vmsdk::ValkeyTestWithParam<T>::SetUp();
    ValkeySearch::InitInstance(std::make_unique<TestableValkeySearch>());
    InitThreadPools(/*readers=*/std::nullopt, /*writers=*/1,
                    /*utility=*/std::nullopt);
    KeyspaceEventManager::InitInstance(
        std::make_unique<TestableKeyspaceEventManager>());
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() { server_events::SubscribeToServerEvents(); },
        ValkeySearch::Instance().GetWriterThreadPool(), false));
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault([&](ValkeyModuleCtx *ctx) {
          return ctx == &registry_ctx_ ? ctx : nullptr;
        });
    VectorRegistry::Construct(&registry_ctx_);
  }
  void TearDown() override {
    if (ValkeySearch::HasInstance()) {
      if (auto *pool = ValkeySearch::Instance().GetWriterThreadPool()) {
        if (pool->IsSuspended()) {
          (void)pool->ResumeWorkers();
        }
        WaitWorkerTasksAreCompleted(*pool);
      }
    }
    kMockValkeyModule->RunPendingOneShots();
    SchemaManager::InitInstance(nullptr);
    ValkeySearch::InitInstance(nullptr);
    KeyspaceEventManager::InitInstance(nullptr);
    VectorRegistry::Destruct();
    vmsdk::ValkeyTestWithParam<T>::TearDown();
  }
};

std::vector<std::vector<float>> DeterministicallyGenerateVectors(
    int size, int dimensions, float max_value);

struct RespReply {
  using RespArray = std::vector<RespReply>;
  std::variant<std::string, int64_t, RespArray> value;

  static bool CompareArrays(const RespArray &array1, const RespArray &array2);

  // Equality operator to compare two RespReply objects
  bool operator==(const RespReply &other) const;

  // Inequality operator
  bool operator!=(const RespReply &other) const { return !(*this == other); }
};

RespReply ParseRespReply(absl::string_view input);

inline auto VectorToStr = [](const std::vector<float> &v) {
  return absl::string_view((char *)v.data(), v.size() * sizeof(float));
};

class UnitTestSearchParameters : public query::SearchParameters {
 public:
  UnitTestSearchParameters() {
    timeout_ms = 10000;
    db_num = 0;
    cancellation_token = cancel::Make(timeout_ms, nullptr);
  }
  void QueryCompleteBackground(
      std::unique_ptr<SearchParameters> self) override {
    CHECK(false);
  }
  void QueryCompleteMainThread(
      std::unique_ptr<SearchParameters> self) override {
    CHECK(false);
  }
};

namespace testing_infra {

inline std::shared_ptr<indexes::VectorRecord> MakeVectorRecord(
    absl::string_view raw_vector_bytes) {
  float reciprocal_mag = indexes::CalcReciprocalMagnitude(
      reinterpret_cast<const float *>(raw_vector_bytes.data()),
      raw_vector_bytes.size() / sizeof(float));
  return indexes::VectorRecord::Construct(raw_vector_bytes, reciprocal_mag);
}

inline AttributeData MakeStringAttributeData(absl::string_view str) {
  return AttributeData(vmsdk::MakeUniqueValkeyString(str));
}

inline AttributeData MakeVectorAttributeData(
    const InternedStringPtr &key, const InternedStringPtr &attribute_identifier,
    absl::string_view raw_vector_bytes, int db_num = 0) {
  return AttributeData(indexes::VectorRecordWithSize{
      MakeVectorRecord(raw_vector_bytes), raw_vector_bytes.size()});
}

inline AttributeData MakeDeletionAttributeData(
    indexes::DeletionType del = indexes::DeletionType::kRecord) {
  return AttributeData(del);
}

}  // namespace testing_infra

}  // namespace valkey_search

#endif  // VALKEYSEARCH_TESTING_COMMON_H_
