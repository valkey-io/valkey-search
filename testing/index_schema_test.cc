/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "src/index_schema.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/attribute_data_type.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_flat.h"
#include "src/indexes/vector_hnsw.h"
#include "src/keyspace_event_manager.h"
#include "src/schema_manager.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"
#include "third_party/hnswlib/hnswlib.h"  // IWYU pragma: keep
#include "third_party/hnswlib/space_ip.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
#include "vmsdk/src/memory_tracker.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/memory_allocation_overrides.h"

namespace valkey_search {

using testing::An;
using testing::Bool;
using testing::Combine;
using testing::Return;
using testing::StrEq;
using testing::TestParamInfo;
using testing::TypedEq;
using testing::UnorderedElementsAreArray;
using testing::ValuesIn;

struct IndexSchemaSubscriptionTestCase {
  std::string test_name;
  std::string hash_field;
  bool open_key_fail;
  int open_key_type;
  bool expect_wrong_type;
  // Set to nullopt to have Redis return does not exist
  absl::optional<std::pair<std::string, std::string>> redis_hash_data;
  bool is_tracked;
  // Set to nullopt to not expect a call to the given index method.
  absl::optional<absl::StatusOr<bool>> expect_index_add_w_result;
  absl::optional<absl::StatusOr<bool>> expect_index_modify_w_result;
  absl::optional<absl::StatusOr<bool>> expect_index_remove_w_result;
  // Validated against the input to add/modify if they are not nullopt.
  std::string expected_vector_buffer;
  IndexSchema::Stats::ResultCnt<uint64_t> expected_add_cnt_delta;
  IndexSchema::Stats::ResultCnt<uint64_t> expected_remove_cnt_delta;
  IndexSchema::Stats::ResultCnt<uint64_t> expected_modify_cnt_delta;
  indexes::DeletionType expected_deletion_type = indexes::DeletionType::kNone;
  int expected_document_cnt_delta;
};

class IndexSchemaSubscriptionTest
    : public ValkeySearchTestWithParam<IndexSchemaSubscriptionTestCase> {};

TEST_P(IndexSchemaSubscriptionTest, OnKeyspaceNotificationTest) {
  const IndexSchemaSubscriptionTestCase &test_case = GetParam();
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  for (bool use_thread_pool : {true, false}) {
    RedisModuleCtx fake_ctx;
    std::vector<absl::string_view> key_prefixes = {"prefix:"};
    std::string index_schema_name_str("index_schema_name");
    auto index_schema = MockIndexSchema::Create(
                            &fake_ctx, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(),
                            use_thread_pool ? &mutations_thread_pool : nullptr)
                            .value();
    EXPECT_TRUE(
        KeyspaceEventManager::Instance().HasSubscription(index_schema.get()));
    auto mock_index = std::make_shared<MockIndex>();
    VMSDK_EXPECT_OK(index_schema->AddIndex("attribute_name",
                                           test_case.hash_field, mock_index));

    auto key = StringInternStore::Intern("key");
    auto key_redis_str = vmsdk::MakeUniqueRedisString(key->Str().data());
    EXPECT_CALL(*mock_index, IsTracked(key))
        .WillRepeatedly(Return(test_case.is_tracked));
    if (test_case.expect_index_add_w_result.has_value()) {
      EXPECT_CALL(
          *mock_index,
          AddRecord(key, absl::string_view(test_case.expected_vector_buffer)))
          .WillOnce(Return(test_case.expect_index_add_w_result.value()));
    } else if (test_case.expect_index_modify_w_result.has_value()) {
      EXPECT_CALL(*mock_index,
                  ModifyRecord(
                      key, absl::string_view(test_case.expected_vector_buffer)))
          .WillOnce(Return(test_case.expect_index_modify_w_result.value()));
    } else if (test_case.expect_index_remove_w_result.has_value()) {
      EXPECT_CALL(*mock_index,
                  RemoveRecord(key, test_case.expected_deletion_type))
          .WillOnce(Return(test_case.expect_index_remove_w_result.value()));
    }
    if (test_case.open_key_fail) {
      // Keep the default behavior still for other keys (e.g. IndexSchema key).
      EXPECT_CALL(*kMockRedisModule, OpenKey(&fake_ctx, testing::_, testing::_))
          .WillRepeatedly(TestRedisModule_OpenKeyDefaultImpl);
      EXPECT_CALL(*kMockRedisModule,
                  OpenKey(&fake_ctx, key_redis_str.get(),
                          REDISMODULE_OPEN_KEY_NOEFFECTS | REDISMODULE_READ))
          .WillOnce(Return(nullptr));
    } else {
      EXPECT_CALL(*kMockRedisModule, KeyType(testing::_))
          .WillRepeatedly(TestRedisModule_KeyTypeDefaultImpl);
      EXPECT_CALL(*kMockRedisModule,
                  KeyType(vmsdk::RedisModuleKeyIsForString(key->Str())))
          .WillRepeatedly(Return(test_case.open_key_type));
    }

    if (test_case.redis_hash_data.has_value()) {
      const char *field = test_case.redis_hash_data.value().first.c_str();
      const char *value = test_case.redis_hash_data.value().second.c_str();
      RedisModuleString *value_redis_str =
          TestRedisModule_CreateStringPrintf(nullptr, "%s", value);

      EXPECT_CALL(*kMockRedisModule,
                  HashGet(vmsdk::RedisModuleKeyIsForString(key->Str()),
                          REDISMODULE_HASH_CFIELDS, StrEq(field),
                          An<RedisModuleString **>(), TypedEq<void *>(nullptr)))
          .WillOnce([value_redis_str](
                        RedisModuleKey *key, int flags, const char *field,
                        RedisModuleString **value_out, void *terminating_null) {
            *value_out = value_redis_str;
            return REDISMODULE_OK;
          });
    } else if (!test_case.open_key_fail && !test_case.expect_wrong_type) {
      EXPECT_CALL(*kMockRedisModule,
                  HashGet(vmsdk::RedisModuleKeyIsForString(key->Str()),
                          REDISMODULE_HASH_CFIELDS, StrEq(test_case.hash_field),
                          An<RedisModuleString **>(), TypedEq<void *>(nullptr)))
          .WillOnce([](RedisModuleKey *key, int flags, const char *field,
                       RedisModuleString **value_out, void *terminating_null) {
            *value_out = nullptr;
            return REDISMODULE_OK;
          });
    }

    IndexSchema::Stats::ResultCnt<uint64_t> add_cnt = {
        .failure_cnt = index_schema->GetStats().subscription_add.failure_cnt,
        .success_cnt = index_schema->GetStats().subscription_add.success_cnt,
        .skipped_cnt = index_schema->GetStats().subscription_add.skipped_cnt};
    IndexSchema::Stats::ResultCnt<uint64_t> remove_cnt = {
        .failure_cnt = index_schema->GetStats().subscription_remove.failure_cnt,
        .success_cnt = index_schema->GetStats().subscription_remove.success_cnt,
        .skipped_cnt =
            index_schema->GetStats().subscription_remove.skipped_cnt};
    IndexSchema::Stats::ResultCnt<uint64_t> modify_cnt = {
        .failure_cnt = index_schema->GetStats().subscription_modify.failure_cnt,
        .success_cnt = index_schema->GetStats().subscription_modify.success_cnt,
        .skipped_cnt =
            index_schema->GetStats().subscription_modify.skipped_cnt};
    uint32_t document_cnt = index_schema->GetStats().document_cnt;
    index_schema->OnKeyspaceNotification(&fake_ctx, REDISMODULE_NOTIFY_HASH,
                                         "event", key_redis_str.get());
    if (use_thread_pool) {
      WaitWorkerTasksAreCompleted(mutations_thread_pool);
    }
    for (const auto &tuple :
         {std::make_tuple(add_cnt, &index_schema->GetStats().subscription_add,
                          &test_case.expected_add_cnt_delta),
          std::make_tuple(remove_cnt,
                          &index_schema->GetStats().subscription_remove,
                          &test_case.expected_remove_cnt_delta),
          std::make_tuple(modify_cnt,
                          &index_schema->GetStats().subscription_modify,
                          &test_case.expected_modify_cnt_delta)}) {
      EXPECT_EQ(
          std::get<1>(tuple)->success_cnt - std::get<0>(tuple).success_cnt,
          std::get<2>(tuple)->success_cnt);
      EXPECT_EQ(
          std::get<1>(tuple)->skipped_cnt - std::get<0>(tuple).skipped_cnt,
          std::get<2>(tuple)->skipped_cnt);
      EXPECT_EQ(
          std::get<1>(tuple)->failure_cnt - std::get<0>(tuple).failure_cnt,
          std::get<2>(tuple)->failure_cnt);
    }
    EXPECT_EQ(index_schema->GetStats().document_cnt - document_cnt,
              test_case.expected_document_cnt_delta);
  }
}

INSTANTIATE_TEST_SUITE_P(
    IndexSchemaSubscriptionTests, IndexSchemaSubscriptionTest,
    ValuesIn<IndexSchemaSubscriptionTestCase>({
        {
            .test_name = "happy_path_add",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = REDISMODULE_KEYTYPE_HASH,
            .redis_hash_data = std::make_pair("vector", "vector_buffer"),
            .is_tracked = false,
            .expect_index_add_w_result = true,
            .expected_vector_buffer = "vector_buffer",
            .expected_add_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .success_cnt = 1,
                },
            .expected_document_cnt_delta = 1,
        },
        {
            .test_name = "happy_path_remove_key",
            .hash_field = "vector",
            .open_key_fail = true,
            .open_key_type = REDISMODULE_KEYTYPE_HASH,
            .redis_hash_data = std::nullopt,
            .is_tracked = true,
            .expect_index_remove_w_result = true,
            .expected_vector_buffer = "vector_buffer",
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .success_cnt = 1,
                },
            .expected_deletion_type = indexes::DeletionType::kRecord,
        },
        {
            .test_name = "happy_path_remove_record",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = REDISMODULE_KEYTYPE_HASH,
            .redis_hash_data = std::nullopt,
            .is_tracked = true,
            .expect_index_remove_w_result = true,
            .expected_vector_buffer = "vector_buffer",
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .success_cnt = 1,
                },
        },
        {
            .test_name = "happy_path_modify",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = REDISMODULE_KEYTYPE_HASH,
            .redis_hash_data = std::make_pair("vector", "vector_buffer"),
            .is_tracked = true,
            .expect_index_modify_w_result = true,
            .expected_vector_buffer = "vector_buffer",
            .expected_modify_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .success_cnt = 1,
                },
        },
        {
            .test_name = "untracked_and_record_does_not_exist",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = REDISMODULE_KEYTYPE_HASH,
            .redis_hash_data = std::nullopt,
            .is_tracked = false,
            .expect_index_remove_w_result = false,
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .skipped_cnt = 1,
                },
        },
        {
            .test_name = "untracked_and_key_does_not_exist",
            .hash_field = "vector",
            .open_key_fail = true,
            .open_key_type = REDISMODULE_KEYTYPE_HASH,
            .redis_hash_data = std::nullopt,
            .is_tracked = false,
            .expect_index_remove_w_result = false,
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .skipped_cnt = 1,
                },
            .expected_deletion_type = indexes::DeletionType::kRecord,
        },
        {
            .test_name = "add_failure",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = REDISMODULE_KEYTYPE_HASH,
            .redis_hash_data = std::make_pair("vector", "vector_buffer"),
            .is_tracked = false,
            .expect_index_add_w_result = absl::InternalError("error"),
            .expected_vector_buffer = "vector_buffer",
            .expected_add_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .failure_cnt = 1,
                },
        },
        {
            .test_name = "modify_failure",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = REDISMODULE_KEYTYPE_HASH,
            .redis_hash_data = std::make_pair("vector", "vector_buffer"),
            .is_tracked = true,
            .expect_index_modify_w_result = absl::InternalError("error"),
            .expected_vector_buffer = "vector_buffer",
            .expected_modify_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .failure_cnt = 1,
                },
        },
        {
            .test_name = "remove_failure",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = REDISMODULE_KEYTYPE_HASH,
            .redis_hash_data = std::nullopt,
            .is_tracked = true,
            .expect_index_remove_w_result = absl::InternalError("error"),
            .expected_vector_buffer = "vector_buffer",
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .failure_cnt = 1,
                },
        },
        {
            .test_name = "add_skipped",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = REDISMODULE_KEYTYPE_HASH,
            .redis_hash_data = std::make_pair("vector", "vector_buffer"),
            .is_tracked = false,
            .expect_index_add_w_result = false,
            .expected_vector_buffer = "vector_buffer",
            .expected_add_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .skipped_cnt = 1,
                },
        },
        {
            .test_name = "add_wrong_type",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = REDISMODULE_KEYTYPE_STRING,
            .expect_wrong_type = true,
        },
        {
            .test_name = "modify_skipped",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = REDISMODULE_KEYTYPE_HASH,
            .redis_hash_data = std::make_pair("vector", "vector_buffer"),
            .is_tracked = true,
            .expect_index_modify_w_result = false,
            .expected_vector_buffer = "vector_buffer",
            .expected_modify_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .skipped_cnt = 1,
                },
        },
        {
            .test_name = "remove_skipped",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = REDISMODULE_KEYTYPE_HASH,
            .redis_hash_data = std::nullopt,
            .is_tracked = true,
            .expect_index_remove_w_result = false,
            .expected_vector_buffer = "vector_buffer",
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .skipped_cnt = 1,
                },
        },
    }),
    [](const TestParamInfo<IndexSchemaSubscriptionTestCase> &info) {
      return info.param.test_name;
    });

class IndexSchemaSubscriptionSimpleTest
    : public ValkeySearchTestWithParam<bool> {};

TEST_P(IndexSchemaSubscriptionSimpleTest, DropIndexPrematurely) {
  // This test covers verifies that Unblockclient is called when an index schema
  // is dropped prematurely while there are pending mutations in the worker
  // thread pool
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  VMSDK_EXPECT_OK(mutations_thread_pool.SuspendWorkers());
  std::vector<absl::string_view> key_prefixes = {"prefix:"};
  std::string index_schema_name_str("index_schema_name");
  {
    auto index_schema =
        MockIndexSchema::Create(&fake_ctx_, index_schema_name_str, key_prefixes,
                                std::make_unique<HashAttributeDataType>(),
                                &mutations_thread_pool)
            .value();
    EXPECT_TRUE(
        KeyspaceEventManager::Instance().HasSubscription(index_schema.get()));
    auto mock_index = std::make_shared<MockIndex>();
    VMSDK_EXPECT_OK(
        index_schema->AddIndex("attribute_name", "vector", mock_index));

    auto key = StringInternStore::Intern("key");
    auto key_redis_str = vmsdk::MakeUniqueRedisString(key->Str().data());
    EXPECT_CALL(*mock_index, IsTracked(key)).WillRepeatedly(Return(false));

    EXPECT_CALL(*mock_index, AddRecord(key, testing::_)).Times(0);

    EXPECT_CALL(*kMockRedisModule, KeyType(testing::_))
        .WillRepeatedly(TestRedisModule_KeyTypeDefaultImpl);
    EXPECT_CALL(*kMockRedisModule,
                KeyType(vmsdk::RedisModuleKeyIsForString(key->Str())))
        .WillRepeatedly(Return(REDISMODULE_KEYTYPE_HASH));
    EXPECT_CALL(*kMockRedisModule, GetClientId(testing::_))
        .WillRepeatedly(testing::Return(1));
    EXPECT_CALL(
        *kMockRedisModule,
        BlockClient(testing::_, testing::_, testing::_, testing::_, testing::_))
        .WillOnce(Return((RedisModuleBlockedClient *)1));
    const char *field = "vector";
    const char *value = "vector_buffer";
    RedisModuleString *value_redis_str =
        TestRedisModule_CreateStringPrintf(nullptr, "%s", value);

    EXPECT_CALL(*kMockRedisModule,
                HashGet(vmsdk::RedisModuleKeyIsForString(key->Str()),
                        REDISMODULE_HASH_CFIELDS, StrEq(field),
                        An<RedisModuleString **>(), TypedEq<void *>(nullptr)))
        .WillOnce([value_redis_str](
                      RedisModuleKey *key, int flags, const char *field,
                      RedisModuleString **value_out, void *terminating_null) {
          *value_out = value_redis_str;
          return REDISMODULE_OK;
        });

    index_schema->OnKeyspaceNotification(&fake_ctx_, REDISMODULE_NOTIFY_HASH,
                                         "event", key_redis_str.get());
    EXPECT_CALL(*kMockRedisModule,
                UnblockClient((RedisModuleBlockedClient *)1, nullptr))
        .WillOnce(Return(1));
  }
  EXPECT_EQ(mutations_thread_pool.QueueSize(), 1);
  VMSDK_EXPECT_OK(mutations_thread_pool.ResumeWorkers());
  WaitWorkerTasksAreCompleted(mutations_thread_pool);
  EXPECT_TRUE(vmsdk::TrackedBlockedClients().empty());
}

TEST_P(IndexSchemaSubscriptionSimpleTest, EmptyKeyPrefixesTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  auto use_thread_pool = GetParam();

  mutations_thread_pool.StartWorkers();
  std::vector<absl::string_view> key_prefixes = {};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(),
                          use_thread_pool ? &mutations_thread_pool : nullptr)
                          .value();

  EXPECT_THAT(index_schema->GetKeyPrefixes(), UnorderedElementsAreArray({""}));
}

TEST_P(IndexSchemaSubscriptionSimpleTest, DuplicateKeyPrefixesTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  auto use_thread_pool = GetParam();

  std::vector<absl::string_view> key_prefixes = {"pre", "pre"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(),
                          use_thread_pool ? &mutations_thread_pool : nullptr)
                          .value();

  EXPECT_THAT(index_schema->GetKeyPrefixes(),
              UnorderedElementsAreArray({"pre"}));
}

TEST_P(IndexSchemaSubscriptionSimpleTest, PrefixIsPrefixedByAnotherTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  auto use_thread_pool = GetParam();
  std::vector<absl::string_view> key_prefixes = {"pre", "prefix"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(),
                          use_thread_pool ? &mutations_thread_pool : nullptr)
                          .value();

  EXPECT_THAT(index_schema->GetKeyPrefixes(),
              UnorderedElementsAreArray({"pre"}));
}

TEST_P(IndexSchemaSubscriptionSimpleTest, IndexSchemaInDifferentDBTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  auto use_thread_pool = GetParam();
  std::vector<absl::string_view> key_prefixes = {};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(),
                          use_thread_pool ? &mutations_thread_pool : nullptr)
                          .value();
  auto mock_index = std::make_shared<MockIndex>();
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("attribute_name", "test_identifier", mock_index));

  EXPECT_CALL(*mock_index, AddRecord(testing::_, testing::_)).Times(0);
  std::string key = "key";
  auto key_redis_str = vmsdk::MakeUniqueRedisString(key.c_str());
  RedisModuleCtx different_db_ctx;
  index_schema->OnKeyspaceNotification(
      &different_db_ctx, REDISMODULE_NOTIFY_HASH, "event", key_redis_str.get());
  if (use_thread_pool) {
    WaitWorkerTasksAreCompleted(mutations_thread_pool);
  }
}

TEST_P(IndexSchemaSubscriptionSimpleTest,
       DBHasMatchingKeyWithWrongModuleTypeTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  auto use_thread_pool = GetParam();
  std::vector<absl::string_view> key_prefixes = {};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(),
                          use_thread_pool ? &mutations_thread_pool : nullptr)
                          .value();
  auto mock_index = std::make_shared<MockIndex>();
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("attribute_name", "test_identifier", mock_index));

  EXPECT_CALL(*mock_index, AddRecord(testing::_, testing::_)).Times(0);
  std::string key = "key";
  auto key_redis_str = vmsdk::MakeUniqueRedisString(key.c_str());
  RedisModuleCtx different_db_ctx;
  auto match_key =
      vmsdk::MakeUniqueRedisOpenKey(&different_db_ctx, key_redis_str.get(), 0);
  TestRedisModule_ModuleTypeSetValueDefaultImpl(
      match_key.get(), (RedisModuleType *)0x1, nullptr);
  index_schema->OnKeyspaceNotification(
      &different_db_ctx, REDISMODULE_NOTIFY_HASH, "event", key_redis_str.get());
  if (use_thread_pool) {
    WaitWorkerTasksAreCompleted(mutations_thread_pool);
  }
}

TEST_P(IndexSchemaSubscriptionSimpleTest, KeyspaceNotificationWithNullptrTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  auto use_thread_pool = GetParam();
  std::vector<absl::string_view> key_prefixes = {};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(),
                          use_thread_pool ? &mutations_thread_pool : nullptr)
                          .value();
  auto mock_index = std::make_shared<MockIndex>();
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("attribute_name", "test_identifier", mock_index));
  EXPECT_CALL(*kMockRedisModule, OpenKey(&fake_ctx_, testing::_, testing::_))
      .Times(0);
  index_schema->OnKeyspaceNotification(&fake_ctx_, REDISMODULE_NOTIFY_HASH,
                                       "event", nullptr);
  if (use_thread_pool) {
    WaitWorkerTasksAreCompleted(mutations_thread_pool);
  }
}

TEST_P(IndexSchemaSubscriptionSimpleTest, GetKeyPrefixesTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  auto use_thread_pool = GetParam();
  std::vector<absl::string_view> key_prefixes = {
      "prefix:", "prefix1:", "prefix2:"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(),
                          use_thread_pool ? &mutations_thread_pool : nullptr)
                          .value();

  EXPECT_THAT(index_schema->GetKeyPrefixes(),
              UnorderedElementsAreArray(key_prefixes));
}

TEST_P(IndexSchemaSubscriptionSimpleTest, GetEventTypesTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  auto use_thread_pool = GetParam();
  std::vector<absl::string_view> key_prefixes = {"unused"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(),
                          use_thread_pool ? &mutations_thread_pool : nullptr)
                          .value();

  EXPECT_EQ(index_schema->GetAttributeDataType().GetRedisEventTypes(),
            REDISMODULE_NOTIFY_HASH | REDISMODULE_NOTIFY_GENERIC |
                REDISMODULE_NOTIFY_EXPIRED | REDISMODULE_NOTIFY_EVICTED);
}

INSTANTIATE_TEST_SUITE_P(IndexSchemaSubscriptionSimpleTests,
                         IndexSchemaSubscriptionSimpleTest,
                         ::testing::Values(false, true),
                         [](const testing::TestParamInfo<bool> &info) {
                           return std::to_string(info.param);
                         });

struct IndexSchemaBackfillTestCase {
  std::string test_name;
  uint32_t scan_batch_size;
  std::vector<std::string> key_prefixes;
  uint64_t db_size;
  std::vector<std::string> keys_to_return_in_scan;
  bool return_wrong_types;
  int context_flags = 0;
  uint32_t expected_keys_scanned;
  std::vector<std::string> expected_keys_processed;
  float expected_backfill_percent;
  std::string expected_state;
};

class IndexSchemaBackfillTest
    : public ValkeySearchTestWithParam<
          ::testing::tuple<bool, IndexSchemaBackfillTestCase>> {};

TEST_P(IndexSchemaBackfillTest, PerformBackfillTest) {
  auto &params = GetParam();
  bool use_thread_pool = std::get<0>(params);
  const IndexSchemaBackfillTestCase &test_case = std::get<1>(params);
  MockThreadPool thread_pool("writer-thread-pool-", 5);
  thread_pool.StartWorkers();
  std::vector<absl::string_view> key_prefixes;
  std::transform(test_case.key_prefixes.begin(), test_case.key_prefixes.end(),
                 std::back_inserter(key_prefixes),
                 [](const std::string &key_prefix) {
                   return absl::string_view(key_prefix);
                 });
  std::string index_schema_name_str("index_schema_name");
  EXPECT_CALL(*kMockRedisModule, DbSize(testing::_))
      .WillRepeatedly(Return(test_case.db_size));

  RedisModuleCtx parent_ctx;
  RedisModuleCtx scan_ctx;
  EXPECT_CALL(*kMockRedisModule, GetDetachedThreadSafeContext(&parent_ctx))
      .WillRepeatedly(Return(&scan_ctx));
  EXPECT_CALL(*kMockRedisModule, GetContextFlags(&parent_ctx))
      .WillRepeatedly(Return(test_case.context_flags));
  EXPECT_CALL(*kMockRedisModule, GetContextFlags(&scan_ctx))
      .WillRepeatedly(Return(0));
  auto index_schema =
      MockIndexSchema::Create(&parent_ctx, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              use_thread_pool ? &thread_pool : nullptr)
          .value();
  auto mock_index = std::make_shared<MockIndex>();
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("attribute_name", "test_identifier", mock_index));

  size_t i = 0;
  EXPECT_CALL(*kMockRedisModule,
              Scan(&scan_ctx, testing::An<RedisModuleScanCursor *>(),
                   testing::An<RedisModuleScanCB>(), testing::An<void *>()))
      .WillRepeatedly([&](RedisModuleCtx *ctx, RedisModuleScanCursor *cursor,
                          RedisModuleScanCB fn, void *privdata) -> int {
        if (i >= test_case.keys_to_return_in_scan.size()) {
          return 0;
        }
        bool expect_processed =
            std::find(test_case.expected_keys_processed.begin(),
                      test_case.expected_keys_processed.end(),
                      test_case.keys_to_return_in_scan[i]) !=
            test_case.expected_keys_processed.end();

        auto key_str = test_case.keys_to_return_in_scan[i];
        auto key_r_str = vmsdk::MakeUniqueRedisString(key_str);
        RedisModuleKey key = {.ctx = &scan_ctx, .key = key_str};
        if (expect_processed) {
          RedisModuleString *value_redis_str =
              TestRedisModule_CreateStringPrintf(nullptr, "arbitrary data");
          EXPECT_CALL(
              *kMockRedisModule,
              HashGet(vmsdk::RedisModuleKeyIsForString(key_str),
                      REDISMODULE_HASH_CFIELDS, testing::_,
                      An<RedisModuleString **>(), TypedEq<void *>(nullptr)))
              .WillOnce([value_redis_str](RedisModuleKey *key, int flags,
                                          const char *field,
                                          RedisModuleString **value_out,
                                          void *terminating_null) {
                *value_out = value_redis_str;
                return REDISMODULE_OK;
              });
          EXPECT_CALL(*mock_index,
                      IsTracked(testing::Pointee(testing::StrEq(key_str))))
              .WillRepeatedly(testing::Return(false));
          EXPECT_CALL(
              *mock_index,
              AddRecord(testing::Pointee(testing::StrEq(key_str)), testing::_))
              .WillOnce(testing::Return(true));
          if (use_thread_pool) {
            EXPECT_CALL(thread_pool,
                        Schedule(testing::_, vmsdk::ThreadPool::Priority::kLow))
                .Times(1);
            EXPECT_CALL(*kMockRedisModule,
                        BlockClient(testing::_, testing::_, testing::_,
                                    testing::_, testing::_))
                .Times(0);
            EXPECT_CALL(*kMockRedisModule,
                        UnblockClient(testing::_, testing::_))
                .Times(0);
          }
        }
        if (test_case.return_wrong_types) {
          EXPECT_CALL(*kMockRedisModule,
                      KeyType(vmsdk::RedisModuleKeyIsForString(key_str)))
              .WillRepeatedly(Return(REDISMODULE_KEYTYPE_STRING));
        } else {
          EXPECT_CALL(*kMockRedisModule,
                      KeyType(vmsdk::RedisModuleKeyIsForString(key_str)))
              .WillRepeatedly(Return(REDISMODULE_KEYTYPE_HASH));
        }
        fn(ctx, key_r_str.get(), &key, privdata);
        if (use_thread_pool) {
          EXPECT_CALL(thread_pool,
                      Schedule(testing::_, vmsdk::ThreadPool::Priority::kLow))
              .Times(thread_pool.Size());
          WaitWorkerTasksAreCompleted(thread_pool);
        }
        return (++i < test_case.keys_to_return_in_scan.size()) ? 1 : 0;
      });
  EXPECT_EQ(
      index_schema->PerformBackfill(&parent_ctx, test_case.scan_batch_size),
      test_case.expected_keys_scanned);
  if (!use_thread_pool) {
    EXPECT_EQ(index_schema->IsBackfillInProgress(),
              test_case.expected_backfill_percent != 1.0);
    EXPECT_EQ(index_schema->GetBackfillPercent(),
              test_case.expected_backfill_percent);
    EXPECT_EQ(index_schema->GetStateForInfo(), test_case.expected_state);
  } else {
    EXPECT_CALL(thread_pool,
                Schedule(testing::_, vmsdk::ThreadPool::Priority::kLow))
        .Times(thread_pool.Size());
    WaitWorkerTasksAreCompleted(thread_pool);
  }
}

TEST_F(IndexSchemaBackfillTest, PerformBackfill_NoOngoingBackfillTest) {
  std::vector<absl::string_view> key_prefixes = {"unused"};
  std::string index_schema_name_str("index_schema_name");
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  for (bool use_thread_pool : {true, false}) {
    RedisModuleCtx parent_ctx;
    RedisModuleCtx scan_ctx;
    EXPECT_CALL(*kMockRedisModule, GetDetachedThreadSafeContext(&parent_ctx))
        .WillRepeatedly(Return(&scan_ctx));
    auto index_schema = MockIndexSchema::Create(
                            &parent_ctx, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(),
                            use_thread_pool ? &mutations_thread_pool : nullptr)
                            .value();

    // We only expect it to do the scan the first iteration.
    EXPECT_CALL(*kMockRedisModule,
                Scan(&scan_ctx, testing::An<RedisModuleScanCursor *>(),
                     testing::An<RedisModuleScanCB>(), testing::An<void *>()))
        .WillOnce([&](RedisModuleCtx *ctx, RedisModuleScanCursor *cursor,
                      RedisModuleScanCB fn,
                      void *privdata) -> int { return 0; });

    for (size_t i = 0; i < 100; ++i) {
      EXPECT_EQ(index_schema->PerformBackfill(&parent_ctx, 1024), 0);
    }
  }
}

TEST_F(IndexSchemaBackfillTest, PerformBackfill_SwapDB) {
  std::vector<absl::string_view> key_prefixes = {"unused"};
  std::string index_schema_name_str("index_schema_name");
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  for (bool use_thread_pool : {true, false}) {
    int starting_db = 0;
    int db_to_swap = 1;
    RedisModuleCtx parent_ctx;
    RedisModuleCtx scan_ctx;
    EXPECT_CALL(*kMockRedisModule, GetDetachedThreadSafeContext(&parent_ctx))
        .WillRepeatedly(Return(&scan_ctx));
    EXPECT_CALL(*kMockRedisModule, GetSelectedDb(&parent_ctx))
        .WillRepeatedly(Return(starting_db));
    EXPECT_CALL(*kMockRedisModule, SelectDb(&scan_ctx, starting_db))
        .WillRepeatedly(Return(REDISMODULE_OK));
    auto index_schema = MockIndexSchema::Create(
                            &parent_ctx, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(),
                            use_thread_pool ? &mutations_thread_pool : nullptr)
                            .value();

    // Validate swapping changes the db in the context
    RedisModuleSwapDbInfo swap_db_info = {
        .dbnum_first = starting_db,
        .dbnum_second = db_to_swap,
    };
    EXPECT_CALL(*kMockRedisModule, SelectDb(&scan_ctx, db_to_swap))
        .WillOnce(Return(REDISMODULE_OK));
    index_schema->OnSwapDB(&swap_db_info);

    // Validate swapping again brings the db back to the original
    EXPECT_CALL(*kMockRedisModule, SelectDb(&scan_ctx, starting_db))
        .WillOnce(Return(REDISMODULE_OK));
    index_schema->OnSwapDB(&swap_db_info);
  }
}

TEST_F(IndexSchemaBackfillTest, PerformBackfill_MemoryTrackingScopeConstructor) {
  std::vector<absl::string_view> key_prefixes = {"prefix:"};
  std::string index_schema_name_str("test_index");

  RedisModuleCtx parent_ctx;
  RedisModuleCtx scan_ctx;

  EXPECT_CALL(*kMockRedisModule, GetDetachedThreadSafeContext(&parent_ctx))
      .WillRepeatedly(Return(&scan_ctx));
  EXPECT_CALL(*kMockRedisModule, DbSize(testing::_))
      .WillRepeatedly(Return(5));
  EXPECT_CALL(*kMockRedisModule, GetContextFlags(&parent_ctx))
      .WillRepeatedly(Return(0));
  EXPECT_CALL(*kMockRedisModule, GetContextFlags(&scan_ctx))
      .WillRepeatedly(Return(0));
  auto index_schema = MockIndexSchema::Create(
                          &parent_ctx, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(),
                          nullptr)  // No thread pool for simplicity
                          .value();

  auto mock_index = std::make_shared<MockIndex>();
  VMSDK_EXPECT_OK(index_schema->AddIndex("test_attribute", "test_identifier", mock_index));

  EXPECT_CALL(*kMockRedisModule,
              Scan(&scan_ctx, testing::An<RedisModuleScanCursor *>(),
                   testing::An<RedisModuleScanCB>(), testing::An<void *>()))
      .WillOnce([&](RedisModuleCtx *ctx, RedisModuleScanCursor *cursor,
                    RedisModuleScanCB fn, void *privdata) -> int {
        auto key_str = "prefix:test_key";
        auto key_redis_str = vmsdk::MakeUniqueRedisString(key_str);
        RedisModuleKey key = {.ctx = ctx, .key = key_str};

        EXPECT_CALL(*kMockRedisModule, KeyType(testing::_))
            .WillRepeatedly(Return(REDISMODULE_KEYTYPE_HASH));
        EXPECT_CALL(*mock_index, IsTracked(testing::_))
            .WillRepeatedly(Return(false));
        EXPECT_CALL(*mock_index, AddRecord(testing::_, testing::_))
            .WillOnce(Return(true));

        RedisModuleString *value_redis_str =
            TestRedisModule_CreateStringPrintf(nullptr, "test_data");
        EXPECT_CALL(*kMockRedisModule,
                    HashGet(testing::_, REDISMODULE_HASH_CFIELDS, testing::_,
                           testing::An<RedisModuleString **>(), testing::An<void *>()))
            .WillOnce([value_redis_str](RedisModuleKey *key, int flags,
                                      const char *field, RedisModuleString **value_out,
                                      void *terminating_null) {
              *value_out = value_redis_str;
              return REDISMODULE_OK;
            });

        fn(ctx, key_redis_str.get(), &key, privdata);

        // simulate the memory allocation
        vmsdk::ReportAllocMemorySize(1024 * 1024);
        return 0;
      });

  uint32_t scanned = index_schema->PerformBackfill(&parent_ctx, 10);

  EXPECT_EQ(index_schema->GetMemoryPool().load(), 1024 * 1024);
}

INSTANTIATE_TEST_SUITE_P(
    IndexSchemaBackfillTests, IndexSchemaBackfillTest,
    Combine(Bool(),
            ValuesIn<IndexSchemaBackfillTestCase>({
                {
                    .test_name = "batch_size_5",
                    .scan_batch_size = 5,
                    .key_prefixes = {"prefix1:"},
                    .db_size = 5,
                    .keys_to_return_in_scan = {"prefix1:key1", "prefix1:key2",
                                               "prefix1:key3", "prefix1:key4",
                                               "prefix1:key5"},
                    .expected_keys_scanned = 5,
                    .expected_keys_processed = {"prefix1:key1", "prefix1:key2",
                                                "prefix1:key3", "prefix1:key4",
                                                "prefix1:key5"},
                    .expected_backfill_percent = 1.0,
                    .expected_state = "ready",
                },
                {
                    .test_name = "not_all_match",
                    .scan_batch_size = 5,
                    .key_prefixes = {"prefix1:"},
                    .db_size = 5,
                    .keys_to_return_in_scan = {"prefix1:key1", "prefix2:key2",
                                               "prefix1:key3", "prefix2:key4",
                                               "prefix1:key5"},
                    .expected_keys_scanned = 5,
                    .expected_keys_processed = {"prefix1:key1", "prefix1:key3",
                                                "prefix1:key5"},
                    .expected_backfill_percent = 1.0,
                    .expected_state = "ready",
                },
                {
                    .test_name = "smaller_scan_batch_size_than_available",
                    .scan_batch_size = 3,
                    .key_prefixes = {"prefix1:"},
                    .db_size = 5,
                    .keys_to_return_in_scan = {"prefix1:key1", "prefix1:key2",
                                               "prefix1:key3", "prefix1:key4",
                                               "prefix1:key5"},
                    .expected_keys_scanned = 3,
                    .expected_keys_processed = {"prefix1:key1", "prefix1:key2",
                                                "prefix1:key3"},
                    .expected_backfill_percent = 0.6,
                    .expected_state = "backfill_in_progress",
                },
                {
                    .test_name = "bigger_scan_batch_size_than_available",
                    .scan_batch_size = 7,
                    .key_prefixes = {"prefix1:"},
                    .db_size = 5,
                    .keys_to_return_in_scan = {"prefix1:key1", "prefix1:key2",
                                               "prefix1:key3", "prefix1:key4",
                                               "prefix1:key5"},
                    .expected_keys_scanned = 5,
                    .expected_keys_processed = {"prefix1:key1", "prefix1:key2",
                                                "prefix1:key3", "prefix1:key4",
                                                "prefix1:key5"},
                    .expected_backfill_percent = 1.0,
                    .expected_state = "ready",
                },
                {
                    .test_name = "no_backfill",
                    .scan_batch_size = 5,
                    .key_prefixes = {"prefix1:"},
                    .db_size = 0,
                    .keys_to_return_in_scan = {},
                    .expected_keys_scanned = 0,
                    .expected_keys_processed = {},
                    .expected_backfill_percent = 1.0,
                    .expected_state = "ready",
                },
                {
                    .test_name = "wrong_types_not_added",
                    .scan_batch_size = 5,
                    .key_prefixes = {"prefix1:"},
                    .db_size = 1,
                    .keys_to_return_in_scan = {"prefix1:key1"},
                    .return_wrong_types = true,
                    .expected_keys_scanned = 1,
                    .expected_keys_processed = {},
                    .expected_backfill_percent = 1.0,
                    .expected_state = "ready",
                },
                {
                    .test_name = "dbsize_shrunk",
                    .scan_batch_size = 3,
                    .key_prefixes = {"prefix1:"},
                    .db_size = 1,
                    .keys_to_return_in_scan = {"prefix1:key1", "prefix1:key2",
                                               "prefix1:key3", "prefix1:key4",
                                               "prefix1:key5"},
                    .expected_keys_scanned = 3,
                    .expected_keys_processed = {"prefix1:key1", "prefix1:key2",
                                                "prefix1:key3"},
                    .expected_backfill_percent = 0.99,
                    .expected_state = "backfill_in_progress",
                },
                {
                    .test_name = "oom",
                    .scan_batch_size = 100,
                    .key_prefixes = {"prefix1:"},
                    .db_size = 100,
                    .keys_to_return_in_scan = {},
                    .context_flags = REDISMODULE_CTX_FLAGS_OOM,
                    .expected_keys_scanned = 0,
                    .expected_keys_processed = {},
                    .expected_backfill_percent = 0.0,
                    .expected_state = "backfill_paused_by_oom",
                },
            })),
    [](const TestParamInfo<::testing::tuple<bool, IndexSchemaBackfillTestCase>>
           &info) {
      return std::get<1>(info.param).test_name + "_" +
             (std::get<0>(info.param) ? "WithThreadPool" : "WithoutThreadPool");
    });

class IndexSchemaRDBTest : public ValkeySearchTest {};

TEST_F(IndexSchemaRDBTest, SaveAndLoad) ABSL_NO_THREAD_SAFETY_ANALYSIS {
  std::vector<absl::string_view> key_prefixes = {"prefix1", "prefix2"};
  std::string index_schema_name_str("index_schema_name");
  int dimensions = 100;
  auto distance_metric = data_model::DISTANCE_METRIC_COSINE;
  int initial_cap = 12;
  int m = 16;
  int ef_construction = 100;
  int ef_runtime = 5;
  int block_size = 250;

  FakeSafeRDB rdb_stream;

  // Construct and save index schema
  {
    auto index_schema = MockIndexSchema::Create(
                            &fake_ctx_, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(), nullptr)
                            .value();

    auto hnsw_index =
        indexes::VectorHNSW<float>::Create(
            CreateHNSWVectorIndexProto(dimensions, distance_metric, initial_cap,
                                       m, ef_construction, ef_runtime),
            "hnsw_attribute",
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
            .value();
    VMSDK_EXPECT_OK(index_schema->AddIndex("hnsw_attribute", "hnsw_identifier",
                                           hnsw_index));
    auto itr = index_schema->attributes_.find("hnsw_attribute");

    EXPECT_FALSE(itr == index_schema->attributes_.end());
    auto vectors = DeterministicallyGenerateVectors(10, dimensions, 2);
    for (size_t i = 0; i < vectors.size(); ++i) {
      vmsdk::UniqueRedisString data =
          vmsdk::MakeUniqueRedisString(absl::string_view(
              (char *)&vectors[i][0], dimensions * sizeof(float)));
      auto interned_key = StringInternStore::Intern("key" + std::to_string(i));
      index_schema->ProcessAttributeMutation(&fake_ctx_, itr->second,
                                             interned_key, std::move(data),
                                             indexes::DeletionType::kNone);
    }

    auto flat_index =
        indexes::VectorFlat<float>::Create(
            CreateFlatVectorIndexProto(dimensions, distance_metric, initial_cap,
                                       block_size),
            "flat_identifier",
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
            .value();
    VMSDK_EXPECT_OK(index_schema->AddIndex("flat_attribute", "flat_identifier",
                                           flat_index));

    VMSDK_EXPECT_OK(index_schema->RDBSave(&rdb_stream));
  }

  // Load the saved index schema and validate
  RedisModuleCtx parent_ctx;
  RedisModuleCtx scan_ctx;
  EXPECT_CALL(*kMockRedisModule, GetDetachedThreadSafeContext(&parent_ctx))
      .WillRepeatedly(Return(&scan_ctx));
  RDBSectionIter iter(&rdb_stream, 1);
  auto section = iter.Next();
  VMSDK_EXPECT_OK_STATUSOR(section);
  auto index_schema_or =
      IndexSchema::LoadFromRDB(&parent_ctx,
                               /*mutations_thread_pool=*/nullptr,
                               std::make_unique<data_model::IndexSchema>(
                                   (*section)->index_schema_contents()),
                               iter.IterateSupplementalContent());
  VMSDK_EXPECT_OK_STATUSOR(index_schema_or);
  auto index_schema = std::move(index_schema_or.value());

  EXPECT_THAT(index_schema->GetKeyPrefixes(),
              testing::UnorderedElementsAre("prefix1", "prefix2"));
  EXPECT_TRUE(dynamic_cast<const HashAttributeDataType *>(
      &index_schema->GetAttributeDataType()));
  VMSDK_EXPECT_OK(index_schema->GetIndex("hnsw_attribute"));
  auto hnsw_index = dynamic_cast<indexes::VectorHNSW<float> *>(
      index_schema->GetIndex("hnsw_attribute").value().get());
  EXPECT_TRUE(hnsw_index != nullptr);
  EXPECT_EQ(hnsw_index->GetDimensions(), dimensions);
  EXPECT_TRUE(dynamic_cast<const hnswlib::InnerProductSpace *>(
                  hnsw_index->GetSpace()) != nullptr);
  EXPECT_EQ(hnsw_index->GetCapacity(), initial_cap);
  EXPECT_EQ(hnsw_index->GetM(), m);
  EXPECT_EQ(hnsw_index->GetEfConstruction(), ef_construction);
  EXPECT_EQ(hnsw_index->GetEfRuntime(), ef_runtime);

  VMSDK_EXPECT_OK(index_schema->GetIndex("flat_attribute"));
  auto flat_index = dynamic_cast<indexes::VectorFlat<float> *>(
      index_schema->GetIndex("flat_attribute").value().get());
  EXPECT_TRUE(flat_index != nullptr);
  EXPECT_EQ(flat_index->GetDimensions(), dimensions);
  EXPECT_TRUE(dynamic_cast<const hnswlib::InnerProductSpace *>(
                  flat_index->GetSpace()) != nullptr);
  EXPECT_EQ(flat_index->GetCapacity(), initial_cap);
  EXPECT_EQ(flat_index->GetBlockSize(), block_size);

  EXPECT_TRUE(index_schema->IsBackfillInProgress());
  EXPECT_EQ(index_schema->GetStats().document_cnt, 10);
  EXPECT_EQ(index_schema->CountRecords(), 10);
}

TEST_F(IndexSchemaRDBTest, LoadEndedDeletesOrphanedKeys) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  for (bool use_thread_pool : {true, false}) {
    auto mock_index = std::make_shared<MockIndex>();
    absl::flat_hash_map<std::string, uint64_t> keys_in_index = {
        {"key1", 1}, {"key2", 2}, {"key3", 3}};
    EXPECT_CALL(*mock_index, ForEachTrackedKey(testing::_))
        .WillOnce([&keys_in_index](
                      absl::AnyInvocable<void(const InternedStringPtr &)> fn) {
          for (const auto &[key, internal_id] : keys_in_index) {
            InternedStringPtr interned_key = StringInternStore::Intern(key);
            fn(interned_key);
          }
        });

    std::vector<absl::string_view> key_prefixes = {"prefix1", "prefix2"};
    std::string index_schema_name_str("index_schema_name");

    auto index_schema = MockIndexSchema::Create(
                            &fake_ctx_, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(),
                            use_thread_pool ? &mutations_thread_pool : nullptr)
                            .value();

    VMSDK_EXPECT_OK(
        index_schema->AddIndex("attribute", "identifier", mock_index));
    EXPECT_CALL(*kMockRedisModule, SelectDb(testing::_, testing::_))
        .WillRepeatedly(Return(1));  // So backfill job can be created.
    EXPECT_CALL(*kMockRedisModule, SelectDb(&fake_ctx_, 0)).WillOnce(Return(1));
    EXPECT_CALL(*kMockRedisModule,
                KeyExists(&fake_ctx_, vmsdk::RedisModuleStringValueEq("key1")))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*kMockRedisModule,
                KeyExists(&fake_ctx_, vmsdk::RedisModuleStringValueEq("key2")))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*kMockRedisModule,
                KeyExists(&fake_ctx_, vmsdk::RedisModuleStringValueEq("key3")))
        .WillRepeatedly(Return(1));

    EXPECT_CALL(*mock_index,
                RemoveRecord(testing::Pointee(testing::StrEq("key1")),
                             indexes::DeletionType::kRecord))
        .WillOnce(Return(true));
    EXPECT_CALL(*mock_index,
                RemoveRecord(testing::Pointee(testing::StrEq("key2")),
                             indexes::DeletionType::kRecord))
        .WillOnce(Return(true));
    EXPECT_CALL(*mock_index,
                RemoveRecord(testing::Pointee(testing::StrEq("key3")),
                             indexes::DeletionType::kRecord))
        .Times(0);
    index_schema->OnLoadingEnded(&fake_ctx_);
    if (use_thread_pool) {
      WaitWorkerTasksAreCompleted(mutations_thread_pool);
    }
  }
}

TEST_F(IndexSchemaRDBTest, MemoryTrackingScopeConstructorInLoadFromRDB) {
  RedisModuleCtx fake_ctx;

  data_model::IndexSchema index_schema_proto;
  index_schema_proto.set_name("test_index");
  index_schema_proto.set_db_num(0);
  index_schema_proto.set_attribute_data_type(data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  index_schema_proto.add_subscribed_key_prefixes("prefix:");

  RedisModuleCtx scan_ctx;
  EXPECT_CALL(*kMockRedisModule, GetDetachedThreadSafeContext(&fake_ctx))
      .WillRepeatedly(Return(&scan_ctx));
  EXPECT_CALL(*kMockRedisModule, SelectDb(&scan_ctx, 0))
      .WillRepeatedly(Return(REDISMODULE_OK));

  FakeSafeRDB fake_rdb;

  data_model::SupplementalContentHeader supp_header;
  supp_header.set_type(data_model::SUPPLEMENTAL_CONTENT_INDEX_CONTENT);
  data_model::Attribute* attribute = supp_header.mutable_index_content_header()->mutable_attribute();
  attribute->set_alias("test_attr");
  attribute->set_identifier("test_attr_id");
  attribute->mutable_index()->mutable_tag_index();

  std::string serialized_supp = supp_header.SerializeAsString();
  VMSDK_EXPECT_OK(fake_rdb.SaveStringBuffer(serialized_supp));

  data_model::SupplementalContentChunk chunk_1;
  chunk_1.set_binary_content("test-content");
  std::string serialized_chunk_1 = chunk_1.SerializeAsString();
  VMSDK_EXPECT_OK(fake_rdb.SaveStringBuffer(serialized_chunk_1));

  data_model::SupplementalContentChunk chunk_2;
  std::string serialized_chunk_2 = chunk_2.SerializeAsString();
  VMSDK_EXPECT_OK(fake_rdb.SaveStringBuffer(serialized_chunk_2));

  EXPECT_CALL(*kMockRedisModule, CreateString(testing::_, testing::_, testing::_))
      .WillOnce(testing::Invoke([](RedisModuleCtx* ctx, const char* ptr, size_t len) -> RedisModuleString* {
        // simulate the memory allocation
        vmsdk::ReportAllocMemorySize(1024 * 1024);
        return new RedisModuleString{std::string(ptr, len)};
      }))
      .WillRepeatedly(testing::Invoke([](RedisModuleCtx* ctx, const char* ptr, size_t len) -> RedisModuleString* {
        return new RedisModuleString{std::string(ptr, len)};
      }));

  auto result = IndexSchema::LoadFromRDB(&fake_ctx, nullptr,
                                        std::make_unique<data_model::IndexSchema>(index_schema_proto),
                                        SupplementalContentIter(&fake_rdb, 1));

  VMSDK_EXPECT_OK_STATUSOR(result);
  EXPECT_EQ(result.value()->GetMemoryPool().load(), 1024 * 1024);
}

class IndexSchemaFriendTest : public ValkeySearchTest {
  void SetUp() override {
    ValkeySearchTest::SetUp();
    mutations_thread_pool.StartWorkers();
    index_schema =
        MockIndexSchema::Create(&fake_ctx, index_schema_name_str, key_prefixes,
                                std::make_unique<HashAttributeDataType>(),
                                &mutations_thread_pool)
            .value();
    hnsw_index =
        indexes::VectorHNSW<float>::Create(
            CreateHNSWVectorIndexProto(dimensions, distance_metric, initial_cap,
                                       m, ef_construction, ef_runtime),
            attribute_identifier,
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
            .value();
    VMSDK_EXPECT_OK(index_schema->AddIndex(attribute_identifier,
                                           "hnsw_identifier", hnsw_index));
    VMSDK_EXPECT_OK(SchemaManager::Instance().ImportIndexSchema(index_schema));
  }
  void TearDown() override {
    index_schema.reset();
    ValkeySearchTest::TearDown();
  }

 protected:
  std::string index_schema_name_str{"index_schema_name"};
  std::vector<absl::string_view> key_prefixes = {"prefix1", "prefix2"};

  int dimensions = 100;
  data_model::DistanceMetric distance_metric{data_model::DISTANCE_METRIC_L2};
  int initial_cap = 1000;
  int m = 16;
  int ef_construction = 100;
  int ef_runtime = 5;
  RedisModuleCtx fake_ctx;
  vmsdk::ThreadPool mutations_thread_pool{"writer-thread-pool-", 10};
  std::shared_ptr<IndexSchema> index_schema;
  std::shared_ptr<indexes::VectorHNSW<float>> hnsw_index;
  const std::string attribute_identifier{"hnsw_id"};
  InternedStringPtr key = StringInternStore::Intern("my_key_");
};

IndexSchema::MutatedAttributes CreateMutatedAttributes(
    const std::string &attribute_identifier, absl::string_view data_ptr) {
  IndexSchema::MutatedAttributes mutated_attributes;
  mutated_attributes[attribute_identifier].data =
      vmsdk::MakeUniqueRedisString(data_ptr);
  return mutated_attributes;
}

TEST_F(IndexSchemaFriendTest, MutatedAttributesSanity) {
  absl::string_view data_ptr;
  EXPECT_EQ(index_schema->attributes_.size(), 1);
  auto mutated_attributes_1 =
      CreateMutatedAttributes(attribute_identifier, data_ptr);
  EXPECT_TRUE(index_schema->TrackMutatedRecord(
      nullptr, key, std::move(mutated_attributes_1), true, false));
  // Verify that adding a track attribute with backfill off after on return true
  auto mutated_attributes_2 =
      CreateMutatedAttributes(attribute_identifier, data_ptr);
  EXPECT_TRUE(index_schema->TrackMutatedRecord(
      nullptr, key, std::move(mutated_attributes_2), false, false));
  auto mutated_attributes_3 =
      CreateMutatedAttributes(attribute_identifier, data_ptr);
  EXPECT_FALSE(index_schema->TrackMutatedRecord(
      nullptr, key, std::move(mutated_attributes_3), false, false));
  EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 1);
  auto consumed_data = index_schema->ConsumeTrackedMutatedAttribute(key, true);
  EXPECT_TRUE(consumed_data.has_value());
  auto mutated_attributes_4 =
      CreateMutatedAttributes(attribute_identifier, data_ptr);
  EXPECT_FALSE(index_schema->TrackMutatedRecord(
      nullptr, key, std::move(mutated_attributes_4), false, false));
  consumed_data = index_schema->ConsumeTrackedMutatedAttribute(key, true);
  EXPECT_FALSE(consumed_data.has_value());
  EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 1);
  consumed_data = index_schema->ConsumeTrackedMutatedAttribute(key, false);
  EXPECT_TRUE(consumed_data.has_value());
  EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 1);
  consumed_data = index_schema->ConsumeTrackedMutatedAttribute(key, false);
  EXPECT_FALSE(consumed_data.has_value());
  EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 0);
}

TEST_F(IndexSchemaFriendTest, MutatedAttributes) {
  auto tester = [this](absl::string_view data_ptr,
                       absl::string_view track_before_consumption_data_ptr,
                       absl::string_view track_after_consumption_data_ptr) {
    VLOG(1) << "Starting test";
    {
      auto mutated_attributes =
          CreateMutatedAttributes(attribute_identifier, data_ptr);
      EXPECT_EQ(index_schema->attributes_.size(), 1);
      EXPECT_TRUE(index_schema->TrackMutatedRecord(
          nullptr, key, std::move(mutated_attributes), false, false));
    }
    if (!track_before_consumption_data_ptr.empty()) {
      VLOG(1) << "track_before_consumption_data_ptr is not empty";
      EXPECT_EQ(index_schema->attributes_.size(), 1);
      auto mutated_attributes = CreateMutatedAttributes(
          attribute_identifier, track_before_consumption_data_ptr);
      EXPECT_FALSE(index_schema->TrackMutatedRecord(
          nullptr, key, std::move(mutated_attributes), false, false));
      data_ptr = track_before_consumption_data_ptr;
    }
    EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 1);
    auto consumed_data =
        index_schema->ConsumeTrackedMutatedAttribute(key, true);
    EXPECT_TRUE(consumed_data.has_value());
    auto data_view =
        vmsdk::ToStringView(consumed_data->begin()->second.data.get());
    vmsdk::UniqueRedisString expected_data =
        vmsdk::MakeUniqueRedisString(data_ptr);
    VLOG(1) << "consumed_data size: " << consumed_data->size();
    auto expected_data_view = vmsdk::ToStringView(expected_data.get());
    EXPECT_EQ(data_view, expected_data_view);
    EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 1);
    if (!track_before_consumption_data_ptr.empty()) {
      VLOG(1) << "before consumed_data due to "
                 "track_before_consumption_data_ptr not empty: "
              << consumed_data->size();
      auto consumed_data =
          index_schema->ConsumeTrackedMutatedAttribute(key, false);
      EXPECT_FALSE(consumed_data.has_value());
    }
    if (!track_after_consumption_data_ptr.empty()) {
      {
        VLOG(1) << "track_after_consumption_data_ptr is not empty";
        EXPECT_EQ(index_schema->attributes_.size(), 1);
        auto mutated_attributes = CreateMutatedAttributes(
            attribute_identifier, track_after_consumption_data_ptr);
        EXPECT_EQ(
            index_schema->TrackMutatedRecord(
                nullptr, key, std::move(mutated_attributes), false, false),
            !track_before_consumption_data_ptr.empty());
      }
      auto consumed_data =
          index_schema->ConsumeTrackedMutatedAttribute(key, false);
      EXPECT_TRUE(consumed_data.has_value());
      auto data_view =
          vmsdk::ToStringView(consumed_data->begin()->second.data.get());
      vmsdk::UniqueRedisString expected_data =
          vmsdk::MakeUniqueRedisString(track_after_consumption_data_ptr);

      auto expected_data_view = vmsdk::ToStringView(expected_data.get());
      EXPECT_EQ(data_view, expected_data_view);
    }
    consumed_data = index_schema->ConsumeTrackedMutatedAttribute(key, false);
    EXPECT_FALSE(consumed_data.has_value());
    EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 0);
  };

  auto vectors = DeterministicallyGenerateVectors(3, dimensions, 2);
  for (auto initial_track :
       {absl::string_view(), absl::string_view((char *)&vectors[0][0],
                                               dimensions * sizeof(float))}) {
    for (auto track_before_consumption_data_ptr :
         {absl::string_view(), absl::string_view((char *)&vectors[1][0],
                                                 dimensions * sizeof(float))}) {
      for (auto track_after_consumption_data_ptr :
           {absl::string_view(),
            absl::string_view((char *)&vectors[2][0],
                              dimensions * sizeof(float))}) {
        tester(initial_track, track_before_consumption_data_ptr,
               track_after_consumption_data_ptr);
      }
    }
  }
}

TEST_F(IndexSchemaFriendTest, ConsistencyTest) {
  auto vectors = DeterministicallyGenerateVectors(1000, dimensions, 2);
  auto itr = index_schema->attributes_.find(attribute_identifier);

  EXPECT_FALSE(itr == index_schema->attributes_.end());
  {
    // Verify that the mutations were processed asynchronous, by the writer
    // worker pool
    VMSDK_EXPECT_OK(mutations_thread_pool.SuspendWorkers());
    vmsdk::UniqueRedisString data = vmsdk::MakeUniqueRedisString(
        absl::string_view((char *)&vectors[0][0], dimensions * sizeof(float)));
    IndexSchema::MutatedAttributes mutated_attributes;
    mutated_attributes[itr->second.GetIdentifier()].data = std::move(data);
    auto key_interned = StringInternStore::Intern(std::string(*key) + "0");
    index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned,
                                  false);
    EXPECT_EQ(mutations_thread_pool.QueueSize(), 1);
    VMSDK_EXPECT_OK(mutations_thread_pool.ResumeWorkers());
  }
  const auto &stats = index_schema->GetStats();
  const size_t iterations = 100;
  // Test delete consistency
  for (size_t j = 0; j < iterations; ++j) {
    for (size_t i = 0; i < vectors.size(); ++i) {
      vmsdk::UniqueRedisString data =
          vmsdk::MakeUniqueRedisString(absl::string_view(
              (char *)&vectors[i][0], dimensions * sizeof(float)));
      IndexSchema::MutatedAttributes mutated_attributes;
      mutated_attributes[attribute_identifier].data = std::move(data);
      auto key_interned =
          StringInternStore::Intern(std::string(*key) + std::to_string(i));
      index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned,
                                    false);
    }
  }
  for (size_t i = 0; i < vectors.size(); ++i) {
    vmsdk::UniqueRedisString data;
    IndexSchema::MutatedAttributes mutated_attributes;
    mutated_attributes[attribute_identifier].data = std::move(data);
    auto key_interned =
        StringInternStore::Intern(std::string(*key) + std::to_string(i));
    index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned,
                                  false);
  }

  WaitWorkerTasksAreCompleted(mutations_thread_pool);
  EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 0);
  for (size_t i = 0; i < vectors.size(); ++i) {
    auto interned_key =
        StringInternStore::Intern(std::string(*key) + std::to_string(i));
    EXPECT_FALSE(hnsw_index->IsTracked(interned_key));
  }

  EXPECT_GE(stats.subscription_remove.success_cnt +
                stats.subscription_remove.skipped_cnt,
            vectors.size());
  EXPECT_EQ(stats.subscription_remove.success_cnt,
            stats.subscription_add.success_cnt);
  // Test update consistency
  for (size_t j = 0; j < iterations; ++j) {
    for (size_t i = 0; i < vectors.size(); ++i) {
      vmsdk::UniqueRedisString data =
          vmsdk::MakeUniqueRedisString(absl::string_view(
              (char *)&vectors[0][0], dimensions * sizeof(float)));
      IndexSchema::MutatedAttributes mutated_attributes;
      mutated_attributes[attribute_identifier].data = std::move(data);
      auto key_interned =
          StringInternStore::Intern(std::string(*key) + std::to_string(i));
      index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned,
                                    false);
    }
  }
  for (size_t i = 0; i < vectors.size(); ++i) {
    vmsdk::UniqueRedisString data = vmsdk::MakeUniqueRedisString(
        absl::string_view((char *)&vectors[i][0], dimensions * sizeof(float)));
    IndexSchema::MutatedAttributes mutated_attributes;
    mutated_attributes[attribute_identifier].data = std::move(data);
    auto key_interned =
        StringInternStore::Intern(std::string(*key) + std::to_string(i));
    index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned,
                                  false);
  }
  WaitWorkerTasksAreCompleted(mutations_thread_pool);
  EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 0);
  EXPECT_EQ(stats.subscription_remove.success_cnt + vectors.size(),
            stats.subscription_add.success_cnt);
  EXPECT_GT(stats.subscription_modify.success_cnt +
                stats.subscription_modify.skipped_cnt,
            vectors.size() * 0.1);
  for (size_t i = 0; i < vectors.size(); ++i) {
    auto interned_key =
        StringInternStore::Intern(std::string(*key) + std::to_string(i));
    EXPECT_TRUE(hnsw_index->IsTracked(interned_key));
  }
  EXPECT_EQ(stats.subscription_add.failure_cnt, 0);
  EXPECT_EQ(stats.subscription_remove.failure_cnt, 0);
  EXPECT_EQ(stats.subscription_modify.failure_cnt, 0);
}

TEST_F(IndexSchemaFriendTest, ProcessMutation_MemoryTrackingScopeConstructor) {
  data_model::IndexSchema index_schema_proto;
  index_schema_proto.set_name("test_index");
  index_schema_proto.set_db_num(0);
  index_schema_proto.set_attribute_data_type(data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  index_schema_proto.add_subscribed_key_prefixes("prefix:");

  RedisModuleCtx scan_ctx;
  EXPECT_CALL(*kMockRedisModule, GetDetachedThreadSafeContext(&fake_ctx))
      .WillRepeatedly(Return(&scan_ctx));
  EXPECT_CALL(*kMockRedisModule, SelectDb(&scan_ctx, 0))
      .WillRepeatedly(Return(REDISMODULE_OK));

  auto test_index_schema = IndexSchema::Create(
                              &fake_ctx,
                              index_schema_proto,
                              nullptr)  // No thread pool - ProcessMutation will call SyncProcessMutation directly
                              .value();

  auto mock_index = std::make_shared<MockIndex>();
  VMSDK_EXPECT_OK(test_index_schema->AddIndex("test_attribute", "test_identifier", mock_index));

  auto key_interned = StringInternStore::Intern("prefix:test_key");
  EXPECT_CALL(*mock_index, IsTracked(key_interned))
      .WillRepeatedly(Return(false));  // Called twice: once directly, once in IsTrackedByAnyIndex
  EXPECT_CALL(*mock_index, AddRecord(key_interned, testing::_))
      .WillOnce([](const auto&, const auto&) {
        // Simulate memory allocation by directly reporting memory usage
        // This will be tracked by the MemoryTrackingScope in SyncProcessMutation
        vmsdk::ReportAllocMemorySize(1024 * 1024);
        return true;
      });

      IndexSchema::MutatedAttributes mutated_attributes;
  mutated_attributes["test_attribute"].data = vmsdk::MakeUniqueRedisString("test_data");

  auto before_memory_pool = test_index_schema->GetMemoryPool().load();

  test_index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned, false);

  EXPECT_EQ(test_index_schema->GetMemoryPool().load(), before_memory_pool + 1024 * 1024);
}

TEST_F(IndexSchemaFriendTest, ProcessMutation_TrackNewKey) {
  data_model::IndexSchema index_schema_proto;
  index_schema_proto.set_name("test_index");
  index_schema_proto.set_db_num(0);
  index_schema_proto.set_attribute_data_type(data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  index_schema_proto.add_subscribed_key_prefixes("prefix:");

  RedisModuleCtx scan_ctx;
  EXPECT_CALL(*kMockRedisModule, GetDetachedThreadSafeContext(&fake_ctx))
      .WillRepeatedly(Return(&scan_ctx));
  EXPECT_CALL(*kMockRedisModule, SelectDb(&scan_ctx, 0))
      .WillRepeatedly(Return(REDISMODULE_OK));

  auto test_index_schema = IndexSchema::Create(
                              &fake_ctx,
                              index_schema_proto,
                              nullptr)  // No thread pool - ProcessMutation will call SyncProcessMutation directly
                              .value();

  auto mock_index = std::make_shared<MockIndex>();
  VMSDK_EXPECT_OK(test_index_schema->AddIndex("test_attribute", "test_identifier", mock_index));

  auto key_interned = StringInternStore::Intern("prefix:test_key");
  EXPECT_CALL(*mock_index, IsTracked(key_interned))
      .WillRepeatedly(Return(false));  // Called twice: once directly, once in IsTrackedByAnyIndex
  EXPECT_CALL(*mock_index, AddRecord(key_interned, testing::_))
      .WillOnce([](const auto&, const auto&) {
        return true;
      });

  IndexSchema::MutatedAttributes mutated_attributes;
  mutated_attributes["test_attribute"].data = vmsdk::MakeUniqueRedisString("test_data");

  test_index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned, false);

  EXPECT_TRUE(StringInternStore::IsUsedByIndex(test_index_schema.get(), "prefix:test_key"));
}

TEST_F(IndexSchemaFriendTest, ProcessMutation_UnTrackDeletedKey) {
  data_model::IndexSchema index_schema_proto;
  index_schema_proto.set_name("test_index");
  index_schema_proto.set_db_num(0);
  index_schema_proto.set_attribute_data_type(data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  index_schema_proto.add_subscribed_key_prefixes("prefix:");

  RedisModuleCtx scan_ctx;
  EXPECT_CALL(*kMockRedisModule, GetDetachedThreadSafeContext(&fake_ctx))
      .WillRepeatedly(Return(&scan_ctx));
  EXPECT_CALL(*kMockRedisModule, SelectDb(&scan_ctx, 0))
      .WillRepeatedly(Return(REDISMODULE_OK));

  auto test_index_schema = IndexSchema::Create(
                              &fake_ctx,
                              index_schema_proto,
                              nullptr)  // No thread pool - ProcessMutation will call SyncProcessMutation directly
                              .value();

  auto mock_index = std::make_shared<MockIndex>();
  VMSDK_EXPECT_OK(test_index_schema->AddIndex("test_attribute", "test_identifier", mock_index));

  auto key_interned = StringInternStore::Intern("prefix:test_key");
  EXPECT_CALL(*mock_index, RemoveRecord(key_interned, testing::_))
      .WillOnce([](const auto&, const auto&) {
        return true;
      });
  StringInternStore::RegisterIndexUsage(test_index_schema.get(), "prefix:test_key");
  IndexSchema::MutatedAttributes mutated_attributes;
  mutated_attributes["test_attribute"].data = nullptr;  // nullptr indicates deletion
  mutated_attributes["test_attribute"].deletion_type = indexes::DeletionType::kRecord;

  test_index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned, false);

  EXPECT_FALSE(StringInternStore::IsUsedByIndex(test_index_schema.get(), "prefix:test_key"));
}

class IndexSchemaTest : public vmsdk::RedisTest {};

TEST_F(IndexSchemaTest, ShouldBlockClient) {
  RedisModuleCtx fake_ctx;
  {
    EXPECT_CALL(*kMockRedisModule, GetClientId(&fake_ctx))
        .WillOnce(testing::Return(1));
    EXPECT_TRUE(ShouldBlockClient(&fake_ctx, false, false));
  }
  {
    EXPECT_CALL(*kMockRedisModule, GetClientId(&fake_ctx))
        .WillOnce(testing::Return(0));
    EXPECT_FALSE(ShouldBlockClient(&fake_ctx, false, false));
  }
  EXPECT_FALSE(ShouldBlockClient(&fake_ctx, true, false));
  EXPECT_FALSE(ShouldBlockClient(&fake_ctx, false, true));
  EXPECT_FALSE(ShouldBlockClient(&fake_ctx, true, true));
}
}  // namespace valkey_search
