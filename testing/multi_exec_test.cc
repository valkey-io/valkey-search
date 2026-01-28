/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/commands/commands.h"
#include "src/metrics.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search.h"
#include "testing/common.h"
#include "vmsdk/src/blocked_client.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
namespace valkey_search {

namespace {

class MultiExecTest : public ValkeySearchTest {
 public:
  void SetUp() override {
    ValkeySearchTest::SetUp();
    InitThreadPools(2, 2, 1);
    mutations_thread_pool = ValkeySearch::Instance().GetWriterThreadPool();

    std::vector<absl::string_view> key_prefixes;
    index_schema = CreateVectorHNSWSchema(index_schema_name_str, &fake_ctx_,
                                          mutations_thread_pool)
                       .value();
    mock_index = std::make_shared<MockIndex>();
    const char *identifier = "test_identifier";
    VMSDK_EXPECT_OK(
        index_schema->AddIndex("attribute_name", identifier, mock_index));
    EXPECT_CALL(*mock_index, IsTracked(testing::_))
        .WillRepeatedly(testing::Return(false));
    EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
        .WillRepeatedly(testing::Return(VALKEYMODULE_KEYTYPE_HASH));

    EXPECT_CALL(*kMockValkeyModule,
                HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_,
                        testing::An<ValkeyModuleString **>(),
                        testing::TypedEq<void *>(nullptr)))
        .WillRepeatedly([this, identifier](ValkeyModuleKey *key, int flags,
                                           const char *field,
                                           ValkeyModuleString **value_out,
                                           void *terminating_null) {
          ValkeyModuleString *value_valkey_str =
              TestValkeyModule_CreateStringPrintf(nullptr, "%s%d",
                                                  record_value_, record_index);
          std::string field_str(field);
          std::string identifier_str(identifier);
          if (identifier_str == field_str) {
            record_index++;
          }
          *value_out = value_valkey_str;
          return VALKEYMODULE_OK;
        });
    EXPECT_CALL(*mock_index, AddRecordImpl(testing::_, testing::_))
        .WillRepeatedly(
            [this](const InternedStringPtr &key, absl::string_view record) {
              absl::MutexLock lock(&mutex);
              added_keys.push_back(std::string(*key));
              added_records.push_back(std::string(record));
              return true;
            });
  }
  void TearDown() override {
    ValkeySearchTest::TearDown();
    if (cb_data) {
      absl::AnyInvocable<void()> *fn = (absl::AnyInvocable<void()> *)cb_data;
      delete fn;
    }
  }
  const char *record_value_ = "value";
  vmsdk::ThreadPool *mutations_thread_pool;
  std::shared_ptr<MockIndexSchema> index_schema;
  std::shared_ptr<MockIndex> mock_index;
  void *cb_data{nullptr};
  std::vector<std::string> added_keys ABSL_GUARDED_BY(mutex);
  std::vector<std::string> added_records ABSL_GUARDED_BY(mutex);
  const std::string key_prefix = "key";
  const int max_keys = 3;
  int record_index = 0;
  const std::string index_schema_name_str{"index_schema_name"};
  mutable absl::Mutex mutex;
};

TEST_F(MultiExecTest, Basic) {
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(testing::_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_CTX_FLAGS_MULTI));
  EXPECT_CALL(*kMockValkeyModule, EventLoopAddOneShot(testing::_, testing::_))
      .WillOnce([this](ValkeyModuleEventLoopOneShotFunc func, void *data) {
        cb_data = data;
        return VALKEYMODULE_OK;
      });
  std::vector<std::string> expected_keys;
  expected_keys.reserve(max_keys + 1);
  for (int i = 0; i < max_keys; ++i) {
    expected_keys.push_back(key_prefix + std::to_string(i));
  }
  EXPECT_CALL(
      *kMockValkeyModule,
      BlockClient(testing::_, testing::_, testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_CALL(*kMockValkeyModule,
              UnblockClient((ValkeyModuleBlockedClient *)1, testing::_))
      .Times(0);
  {
    absl::MutexLock lock(&mutex);
    EXPECT_TRUE(added_keys.empty());
  }
  for (const auto &expected_key : expected_keys) {
    auto key_valkey_str = vmsdk::MakeUniqueValkeyString(expected_key);
    index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                         "event", key_valkey_str.get());
  }
  {
    absl::MutexLock lock(&mutex);
    EXPECT_TRUE(added_keys.empty());
  }
  EXPECT_EQ(mutations_thread_pool->QueueSize(), 0);
  {
    absl::MutexLock lock(&mutex);
    EXPECT_TRUE(added_keys.empty());
  }
  WaitWorkerTasksAreCompleted(*mutations_thread_pool);
  absl::AnyInvocable<void()> *fn = (absl::AnyInvocable<void()> *)cb_data;
  (*fn)();
  delete fn;
  cb_data = nullptr;
  WaitWorkerTasksAreCompleted(*mutations_thread_pool);
  {
    absl::MutexLock lock(&mutex);
    EXPECT_THAT(expected_keys, testing::UnorderedElementsAreArray(added_keys));
    added_keys.clear();

    EXPECT_CALL(*kMockValkeyModule, GetContextFlags(testing::_))
        .WillRepeatedly(testing::Return(0));
    auto key_valkey_str = vmsdk::MakeUniqueValkeyString("key3");
    EXPECT_CALL(*kMockValkeyModule, GetClientId(&fake_ctx_))
        .WillRepeatedly(testing::Return(1));
    EXPECT_CALL(
        *kMockValkeyModule,
        BlockClient(testing::_, testing::_, testing::_, testing::_, testing::_))
        .WillOnce(testing::Return((ValkeyModuleBlockedClient *)1));
    EXPECT_CALL(*kMockValkeyModule,
                UnblockClient((ValkeyModuleBlockedClient *)1, testing::_))
        .WillOnce(testing::Return(VALKEYMODULE_OK));
    index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                         "event", key_valkey_str.get());
  }
  WaitWorkerTasksAreCompleted(*mutations_thread_pool);
  {
    absl::MutexLock lock(&mutex);
    expected_keys = {"key3"};
    EXPECT_THAT(expected_keys, testing::UnorderedElementsAreArray(added_keys));
    index_schema = nullptr;
  }
}

TEST_F(MultiExecTest, TrackMutationOverride) {
  // Get initial metrics values to compare after operations
  auto &metrics = Metrics::GetStats();
  uint64_t initial_batches = metrics.ingest_total_batches;

  VMSDK_EXPECT_OK(mutations_thread_pool->SuspendWorkers());
  EXPECT_CALL(*kMockValkeyModule, EventLoopAddOneShot(testing::_, testing::_))
      .WillOnce([this](ValkeyModuleEventLoopOneShotFunc func, void *data) {
        cb_data = data;
        return VALKEYMODULE_OK;
      });
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(testing::_))
      .WillRepeatedly(testing::Return(0));
  EXPECT_CALL(*kMockValkeyModule, GetClientId(&fake_ctx_))
      .WillRepeatedly(testing::Return(1));
  EXPECT_CALL(
      *kMockValkeyModule,
      BlockClient(testing::_, testing::_, testing::_, testing::_, testing::_))
      .Times(1)
      .WillRepeatedly(testing::Return((ValkeyModuleBlockedClient *)1));

  EXPECT_CALL(*kMockValkeyModule,
              UnblockClient((ValkeyModuleBlockedClient *)1, testing::_))
      .Times(1)
      .WillRepeatedly(testing::Return(VALKEYMODULE_OK));
  auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key_prefix + "0");
  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                       "event", key_valkey_str.get());
  EXPECT_EQ(mutations_thread_pool->QueueSize(), 1);

  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(testing::_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_CTX_FLAGS_MULTI));

  std::vector<std::string> expected_keys;
  expected_keys.reserve(max_keys + 1);
  for (int i = 0; i < max_keys; ++i) {
    expected_keys.push_back(key_prefix + std::to_string(i));
  }
  for (const auto &key : expected_keys) {
    auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key);
    index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                         "event", key_valkey_str.get());
  }

  EXPECT_EQ(mutations_thread_pool->QueueSize(), 1);
  {
    absl::MutexLock lock(&mutex);
    EXPECT_TRUE(added_keys.empty());
  }
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(testing::_))
      .WillRepeatedly(testing::Return(0));
  EXPECT_CALL(*kMockValkeyModule, GetClientId(&fake_ctx_))
      .WillRepeatedly(testing::Return(1));
  key_valkey_str = vmsdk::MakeUniqueValkeyString(key_prefix + "1");
  VMSDK_EXPECT_OK(mutations_thread_pool->ResumeWorkers());
  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                       "event", key_valkey_str.get());
  absl::AnyInvocable<void()> *fn = (absl::AnyInvocable<void()> *)cb_data;
  (*fn)();
  delete fn;
  cb_data = nullptr;
  WaitWorkerTasksAreCompleted(*mutations_thread_pool);
  {
    absl::MutexLock lock(&mutex);
    std::vector<std::string> expected_records = {
        std::string(record_value_) + "1", std::string(record_value_) + "4",
        std::string(record_value_) + "3"};
    EXPECT_THAT(expected_keys, testing::UnorderedElementsAreArray(added_keys));

    // Check that the batch metrics were updated
    EXPECT_GT(metrics.ingest_total_batches, initial_batches);
    EXPECT_EQ(metrics.ingest_last_batch_size, expected_keys.size());
  }
  EXPECT_EQ(vmsdk::BlockedClientTracker::GetInstance().GetClientCount(
                vmsdk::BlockedClientCategory::kHash),
            0);
  EXPECT_EQ(vmsdk::BlockedClientTracker::GetInstance().GetClientCount(
                vmsdk::BlockedClientCategory::kJson),
            0);
  EXPECT_EQ(vmsdk::BlockedClientTracker::GetInstance().GetClientCount(
                vmsdk::BlockedClientCategory::kOther),
            0);
  index_schema = nullptr;
}

TEST_F(MultiExecTest, FtSearchMulti) {
  EXPECT_CALL(*kMockValkeyModule, EventLoopAddOneShot(testing::_, testing::_))
      .Times(0);
  VMSDK_EXPECT_OK(
      ValkeySearch::Instance().GetReaderThreadPool()->SuspendWorkers());
  EXPECT_CALL(
      *kMockValkeyModule,
      OpenKey(&fake_ctx_, testing::An<ValkeyModuleString *>(), testing::_))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);

  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(testing::_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_CTX_FLAGS_MULTI));
  std::vector<std::string> expected_keys;
  expected_keys.reserve(max_keys);
  for (size_t i = 0; i < mutations_thread_pool->Size() - 1; ++i) {
    expected_keys.push_back(key_prefix + std::to_string(i));
  }
  EXPECT_CALL(
      *kMockValkeyModule,
      BlockClient(testing::_, testing::_, testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_CALL(*kMockValkeyModule,
              UnblockClient((ValkeyModuleBlockedClient *)1, testing::_))
      .Times(0);
  for (const auto &key : expected_keys) {
    auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key);
    index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                         "event", key_valkey_str.get());
  }

  std::vector<std::string> argv = {
      "FT.SEARCH",
      "index_schema_name",
      "*=>[KNN 1 @vector $query_vector "
      "EF_RUNTIME 100 AS score]",
      "params",
      "2",
      "query_vector",
      "$embedding",
      "DIALECT",
      "2",
  };
  auto vectors = DeterministicallyGenerateVectors(1, 100, 10.0);
  std::vector<ValkeyModuleString *> cmd_argv;
  std::transform(
      argv.begin(), argv.end(), std::back_inserter(cmd_argv),
      [&](std::string val) {
        if (val == "$embedding") {
          return ValkeyModule_CreateString(&fake_ctx_,
                                           (char *)vectors[0].data(),
                                           vectors[0].size() * sizeof(float));
        }
        return ValkeyModule_CreateString(&fake_ctx_, val.data(), val.size());
      });
  EXPECT_FALSE(cb_data);
  VMSDK_EXPECT_OK(FTSearchCmd(&fake_ctx_, cmd_argv.data(), cmd_argv.size()));
  {
    absl::MutexLock lock(&mutex);
    EXPECT_THAT(expected_keys, testing::UnorderedElementsAreArray(added_keys));
  }
  for (auto cmd_arg : cmd_argv) {
    TestValkeyModule_FreeString(&fake_ctx_, cmd_arg);
  }
  index_schema = nullptr;
}

}  // namespace
}  // namespace valkey_search
