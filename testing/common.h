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
#include "src/vector_externalizer.h"
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
  absl::StatusOr<bool> AddRecord(absl::string_view key,
                                 absl::string_view data) {
    auto interned_key = StringInternStore::Intern(key);
    return T::AddRecord(interned_key, data);
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
    return T::ModifyRecord(interned_key, data);
  }
  bool IsTracked(absl::string_view key) const {
    auto interned_key = StringInternStore::Intern(key);
    return T::IsTracked(interned_key);
  }
};

class MockIndex : public indexes::IndexBase {
 public:
  MockIndex() : indexes::IndexBase(indexes::IndexerType::kNone) {}
  MockIndex(indexes::IndexerType type) : indexes::IndexBase(type) {}
  MOCK_METHOD(absl::StatusOr<bool>, AddRecord,
              (const InternedStringPtr& key, absl::string_view data),
              (override));
  MOCK_METHOD(absl::StatusOr<bool>, RemoveRecord,
              (const InternedStringPtr& key,
               indexes::DeletionType deletion_type),
              (override));
  MOCK_METHOD(absl::StatusOr<bool>, ModifyRecord,
              (const InternedStringPtr& key, absl::string_view data),
              (override));
  MOCK_METHOD(std::unique_ptr<data_model::Index>, ToProto, (),
              (const, override));
  MOCK_METHOD(int, RespondWithInfo, (ValkeyModuleCtx * ctx), (const, override));
  MOCK_METHOD(absl::Status, SaveIndex, (RDBChunkOutputStream chunked_out),
              (const, override));
  MOCK_METHOD((size_t), GetTrackedKeyCount, (), (const, override));
  MOCK_METHOD((size_t), GetUnTrackedKeyCount, (), (const, override));
  MOCK_METHOD(bool, IsTracked, (const InternedStringPtr& key),
              (const, override));
  MOCK_METHOD(bool, IsUnTracked, (const InternedStringPtr& key),
              (const, override));
  MOCK_METHOD(
      (absl::Status), ForEachTrackedKey,
      (absl::AnyInvocable<absl::Status(const InternedStringPtr& key)> fn),
      (const, override));
  MOCK_METHOD(
      (absl::Status), ForEachUnTrackedKey,
      (absl::AnyInvocable<absl::Status(const InternedStringPtr& key)> fn),
      (const, override));
};

class MockKeyspaceEventSubscription : public KeyspaceEventSubscription {
 public:
  MOCK_METHOD(AttributeDataType&, GetAttributeDataType, (), (override, const));
  MOCK_METHOD(const std::vector<std::string>&, GetKeyPrefixes, (),
              (override, const));
  MOCK_METHOD(void, OnKeyspaceNotification,
              (ValkeyModuleCtx * ctx, int type, const char* event,
               ValkeyModuleString* key),
              (override));
};

class MockAttributeDataType : public AttributeDataType {
 public:
  MOCK_METHOD(absl::StatusOr<vmsdk::UniqueValkeyString>, GetRecord,
              (ValkeyModuleCtx * ctx, ValkeyModuleKey* open_key,
               absl::string_view key, absl::string_view identifier),
              (override, const));
  MOCK_METHOD(int, GetValkeyEventTypes, (), (override, const));
  MOCK_METHOD((absl::StatusOr<RecordsMap>), FetchAllRecords,
              (ValkeyModuleCtx * ctx,
               const std::optional<std::string>& query_attribute_name,
               ValkeyModuleKey* open_key, absl::string_view key,
               const absl::flat_hash_set<absl::string_view>& identifiers),
              (override, const));
  MOCK_METHOD((data_model::AttributeDataType), ToProto, (), (override, const));
  MOCK_METHOD((std::string), ToString, (), (override, const));
  MOCK_METHOD((bool), IsProperType, (ValkeyModuleKey * key), (override, const));
  MOCK_METHOD(bool, RecordsProvidedAsString, (), (override, const));
};

class FakeSafeRDB : public SafeRDB {
 public:
  FakeSafeRDB() = default;
  FakeSafeRDB(unsigned char dump_rdb[], size_t len) {
    buffer_.write((const char*)dump_rdb, len);
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
    buffer_.read((char*)&val, sizeof(T));
    EXPECT_TRUE(buffer_);
    return val;
  }

  template <typename T>
  void SavePOD(const T val) {
    buffer_.write((char*)&val, sizeof(T));
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

data_model::TagIndex CreateTagIndexProto(const std::string& separator = ",",
                                         bool case_sensitive = false);

data_model::TextIndex CreateTextIndexProto(bool with_suffix_trie, bool no_stem,
                                           double weight);

class MockIndexSchema : public IndexSchema {
 public:
  static absl::StatusOr<std::shared_ptr<MockIndexSchema>> Create(
      ValkeyModuleCtx* ctx, absl::string_view key,
      const std::vector<absl::string_view>& subscribed_key_prefixes,
      std::unique_ptr<AttributeDataType> attribute_data_type,
      vmsdk::ThreadPool* mutations_thread_pool,
      data_model::Language language = data_model::Language::LANGUAGE_ENGLISH,
      std::string punctuation = ".", bool with_offsets = true,
      const std::vector<std::string>& stop_words = {}) {
    data_model::IndexSchema index_schema_proto;
    index_schema_proto.set_name(std::string(key));
    index_schema_proto.mutable_subscribed_key_prefixes()->Add(
        subscribed_key_prefixes.begin(), subscribed_key_prefixes.end());
    index_schema_proto.set_language(language);
    index_schema_proto.set_punctuation(punctuation);
    index_schema_proto.set_with_offsets(with_offsets);
    index_schema_proto.mutable_stop_words()->Add(stop_words.begin(),
                                                 stop_words.end());
    // NOLINTNEXTLINE
    auto res = std::shared_ptr<MockIndexSchema>(new MockIndexSchema(
        ctx, index_schema_proto, std::move(attribute_data_type),
        mutations_thread_pool));
    VMSDK_RETURN_IF_ERROR(res->Init(ctx));
    return res;
  }
  MockIndexSchema(ValkeyModuleCtx* ctx,
                  const data_model::IndexSchema& index_schema_proto,
                  std::unique_ptr<AttributeDataType> attribute_data_type,
                  vmsdk::ThreadPool* mutations_thread_pool, bool reload = false)
      : IndexSchema(ctx, index_schema_proto, std::move(attribute_data_type),
                    mutations_thread_pool, reload) {
    ON_CALL(*this, OnLoadingEnded(testing::_))
        .WillByDefault([this](ValkeyModuleCtx* ctx) {
          return IndexSchema::OnLoadingEnded(ctx);
        });
    ON_CALL(*this, OnSwapDB(testing::_))
        .WillByDefault([this](ValkeyModuleSwapDbInfo* swap_db_info) {
          return IndexSchema::OnSwapDB(swap_db_info);
        });
    ON_CALL(*this, RDBSave(testing::_)).WillByDefault([this](SafeRDB* rdb) {
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

  vmsdk::ThreadPool* GetWriterThreadPool() const {
    return writer_thread_pool_.get();
  }
  vmsdk::ThreadPool* GetReaderThreadPool() const {
    return reader_thread_pool_.get();
  }
};

class TestableSchemaManager : public SchemaManager {
 public:
  TestableSchemaManager(
      ValkeyModuleCtx* ctx,
      absl::AnyInvocable<void()> server_events_callback = []() {},
      vmsdk::ThreadPool* writer_thread_pool = nullptr,
      bool coordinator_enabled = false)
      : SchemaManager(ctx, std::move(server_events_callback),
                      writer_thread_pool, coordinator_enabled) {}
};

class TestableMetadataManager : public coordinator::MetadataManager {
 public:
  TestableMetadataManager(ValkeyModuleCtx* ctx,
                          coordinator::ClientPool& client_pool)
      : coordinator::MetadataManager(ctx, client_pool) {}
};

inline void InitThreadPools(std::optional<size_t> readers,
                            std::optional<size_t> writers,
                            std::optional<size_t> utility) {
  ((TestableValkeySearch*)&ValkeySearch::Instance())
      ->InitThreadPools(readers, writers, utility);
}

absl::StatusOr<std::shared_ptr<MockIndexSchema>> CreateIndexSchema(
    std::string index_schema_key, ValkeyModuleCtx* fake_ctx = nullptr,
    vmsdk::ThreadPool* writer_thread_pool = nullptr,
    const std::vector<absl::string_view>* key_prefixes = nullptr,
    int32_t index_schema_db_num = 0);
absl::StatusOr<std::shared_ptr<MockIndexSchema>> CreateVectorHNSWSchema(
    std::string index_schema_key, ValkeyModuleCtx* fake_ctx = nullptr,
    vmsdk::ThreadPool* writer_thread_pool = nullptr,
    const std::vector<absl::string_view>* key_prefixes = nullptr,
    int32_t index_schema_db_num = 0);

// TestableKeyspaceEventManager subclasses KeyspaceEventManager and makes it
// creatable for testing purposes.
class TestableKeyspaceEventManager : public KeyspaceEventManager {
 public:
  TestableKeyspaceEventManager() = default;
};

class MockThreadPool : public vmsdk::ThreadPool {
 public:
  MockThreadPool(const std::string& name, size_t num_threads)
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

class ValkeySearchTest : public vmsdk::ValkeyTest {
 protected:
  ValkeyModuleCtx fake_ctx_;
  ValkeyModuleCtx registry_ctx_;

  void SetUp() override {
    ValkeyTest::SetUp();
    ValkeySearch::InitInstance(std::make_unique<TestableValkeySearch>());
    KeyspaceEventManager::InitInstance(
        std::make_unique<TestableKeyspaceEventManager>());
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() { server_events::SubscribeToServerEvents(); }, nullptr,
        false));
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault([&](ValkeyModuleCtx* ctx) {
          return ctx == &registry_ctx_ ? ctx : nullptr;
        });
    VectorExternalizer::Instance().Init(&registry_ctx_);
  }
  void TearDown() override {
    SchemaManager::InitInstance(nullptr);
    ValkeySearch::InitInstance(nullptr);
    KeyspaceEventManager::InitInstance(nullptr);
    VectorExternalizer::Instance().Reset();
    ValkeyTest::TearDown();
  }
};

struct TestReturnAttribute {
  std::string identifier;
  std::string alias;
};

query::ReturnAttribute ToReturnAttribute(
    const TestReturnAttribute& test_return_attribute);

std::unordered_map<std::string, std::string> ToStringMap(const RecordsMap& map);

struct NeighborTest {
  std::string external_id;
  float distance;
  std::optional<std::unordered_map<std::string, std::string>>
      attribute_contents;
  inline bool operator==(const NeighborTest& other) const {
    return external_id == other.external_id && distance == other.distance &&
           attribute_contents == other.attribute_contents;
  }
};

indexes::Neighbor ToIndexesNeighbor(const NeighborTest& neighbor_test);
NeighborTest ToNeighborTest(const indexes::Neighbor& neighbor_test);

template <typename T>
std::vector<NeighborTest> ToVectorNeighborTest(const T& neighbors) {
  std::vector<NeighborTest> neighbors_test(neighbors.size());
  for (const auto& neighbor : neighbors) {
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
    KeyspaceEventManager::InitInstance(
        std::make_unique<TestableKeyspaceEventManager>());
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() { server_events::SubscribeToServerEvents(); }, nullptr,
        false));
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault([&](ValkeyModuleCtx* ctx) {
          return ctx == &registry_ctx_ ? ctx : nullptr;
        });
    VectorExternalizer::Instance().Init(&registry_ctx_);
  }
  void TearDown() override {
    SchemaManager::InitInstance(nullptr);
    ValkeySearch::InitInstance(nullptr);
    KeyspaceEventManager::InitInstance(nullptr);
    VectorExternalizer::Instance().Reset();
    vmsdk::ValkeyTestWithParam<T>::TearDown();
  }
};

std::vector<std::vector<float>> DeterministicallyGenerateVectors(
    int size, int dimensions, float max_value);

struct RespReply {
  using RespArray = std::vector<RespReply>;
  std::variant<std::string, int64_t, RespArray> value;

  static bool CompareArrays(const RespArray& array1, const RespArray& array2);

  // Equality operator to compare two RespReply objects
  bool operator==(const RespReply& other) const;

  // Inequality operator
  bool operator!=(const RespReply& other) const { return !(*this == other); }
};

RespReply ParseRespReply(absl::string_view input);
void WaitWorkerTasksAreCompleted(vmsdk::ThreadPool& mutations_thread_pool);

inline auto VectorToStr = [](const std::vector<float>& v) {
  return absl::string_view((char*)v.data(), v.size() * sizeof(float));
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_TESTING_COMMON_H_
