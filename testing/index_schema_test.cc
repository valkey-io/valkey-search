/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/index_schema.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <string>
#include <thread>
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
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/indexes/text.h"
#include "src/indexes/text/text_index.h"
#include "src/indexes/vector_flat.h"
#include "src/indexes/vector_hnsw.h"
#include "src/keyspace_event_manager.h"
#include "src/metrics.h"
#include "src/schema_manager.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "testing/common.h"
#include "third_party/hnswlib/hnswlib.h"  // IWYU pragma: keep
#include "third_party/hnswlib/space_ip.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

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
  absl::optional<std::pair<std::string, std::string>> valkey_hash_data;
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
  indexes::IndexerType index_type = indexes::IndexerType::kNone;
};

class IndexSchemaSubscriptionTest
    : public ValkeySearchTestWithParam<IndexSchemaSubscriptionTestCase> {
 protected:
  // Helper functions to check operation success/failure patterns
  static bool IsOperationSuccessful(
      const absl::optional<absl::StatusOr<bool>> &result) {
    return result.has_value() && result.value().ok() && result.value().value();
  }

  static bool IsOperationFailed(
      const absl::optional<absl::StatusOr<bool>> &result) {
    return result.has_value() && !result.value().ok();
  }
};

TEST_P(IndexSchemaSubscriptionTest, OnKeyspaceNotificationTest) {
  const IndexSchemaSubscriptionTestCase &test_case = GetParam();
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();

  // Get initial metrics values to compare after operations
  auto &metrics = Metrics::GetStats();
  uint64_t initial_field_vector{metrics.ingest_field_vector};
  uint64_t initial_field_numeric{metrics.ingest_field_numeric};
  uint64_t initial_field_tag{metrics.ingest_field_tag};
  uint64_t initial_hash_keys{metrics.ingest_hash_keys};
  uint64_t initial_total_failures{metrics.ingest_total_failures};
  for (bool use_thread_pool : {true, false}) {
    ValkeyModuleCtx fake_ctx;
    std::vector<absl::string_view> key_prefixes = {"prefix:"};
    std::string index_schema_name_str("index_schema_name");
    auto index_schema = MockIndexSchema::Create(
                            &fake_ctx, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(),
                            use_thread_pool ? &mutations_thread_pool : nullptr)
                            .value();
    EXPECT_TRUE(
        KeyspaceEventManager::Instance().HasSubscription(index_schema.get()));
    auto mock_index = std::make_shared<MockIndex>(test_case.index_type);
    VMSDK_EXPECT_OK(index_schema->AddIndex("attribute_name",
                                           test_case.hash_field, mock_index));

    auto key = StringInternStore::Intern("key");
    auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key->Str().data());
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
      if (test_case.expect_index_remove_w_result.value().ok() &&
          test_case.expect_index_remove_w_result.value().value() == true) {
        EXPECT_CALL(*mock_index,
                    RemoveRecord(key, test_case.expected_deletion_type))
            .WillOnce(Return(test_case.expect_index_remove_w_result.value()));
      }
    }
    if (test_case.open_key_fail) {
      // Keep the default behavior still for other keys (e.g. IndexSchema key).
      EXPECT_CALL(*kMockValkeyModule,
                  OpenKey(&fake_ctx, testing::_, testing::_))
          .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
      EXPECT_CALL(*kMockValkeyModule,
                  OpenKey(&fake_ctx, key_valkey_str.get(),
                          VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ))
          .WillOnce(Return(nullptr));
    } else {
      EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
          .WillRepeatedly(TestValkeyModule_KeyTypeDefaultImpl);
      EXPECT_CALL(*kMockValkeyModule,
                  KeyType(vmsdk::ValkeyModuleKeyIsForString(key->Str())))
          .WillRepeatedly(Return(test_case.open_key_type));
    }

    if (test_case.valkey_hash_data.has_value()) {
      const char *field = test_case.valkey_hash_data.value().first.c_str();
      const char *value = test_case.valkey_hash_data.value().second.c_str();
      ValkeyModuleString *value_valkey_str =
          TestValkeyModule_CreateStringPrintf(nullptr, "%s", value);

      EXPECT_CALL(
          *kMockValkeyModule,
          HashGet(vmsdk::ValkeyModuleKeyIsForString(key->Str()),
                  VALKEYMODULE_HASH_CFIELDS, StrEq(field),
                  An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
          .WillOnce([value_valkey_str](ValkeyModuleKey *key, int flags,
                                       const char *field,
                                       ValkeyModuleString **value_out,
                                       void *terminating_null) {
            *value_out = value_valkey_str;
            return VALKEYMODULE_OK;
          });
    } else if (!test_case.open_key_fail && !test_case.expect_wrong_type) {
      EXPECT_CALL(
          *kMockValkeyModule,
          HashGet(vmsdk::ValkeyModuleKeyIsForString(key->Str()),
                  VALKEYMODULE_HASH_CFIELDS, StrEq(test_case.hash_field),
                  An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
          .WillOnce([](ValkeyModuleKey *key, int flags, const char *field,
                       ValkeyModuleString **value_out, void *terminating_null) {
            *value_out = nullptr;
            return VALKEYMODULE_OK;
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

    // Capture initial Time Slice Mutex metrics
    auto &global_stats = Metrics::GetStats();
    uint64_t initial_upserts = global_stats.time_slice_upserts;
    uint64_t initial_deletes = global_stats.time_slice_deletes;

    index_schema->OnKeyspaceNotification(&fake_ctx, VALKEYMODULE_NOTIFY_HASH,
                                         "event", key_valkey_str.get());
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
      if (!test_case.expect_index_remove_w_result.has_value() ||
          !test_case.expect_index_remove_w_result.value().ok() ||
          test_case.expect_index_remove_w_result.value().value() == true) {
        EXPECT_EQ(
            std::get<1>(tuple)->failure_cnt - std::get<0>(tuple).failure_cnt,
            std::get<2>(tuple)->failure_cnt);
      }
    }
    EXPECT_EQ(index_schema->GetStats().document_cnt - document_cnt,
              test_case.expected_document_cnt_delta);

    // Determine operation success/failure states using helper functions
    bool successful_add =
        IsOperationSuccessful(test_case.expect_index_add_w_result);
    bool successful_modify =
        IsOperationSuccessful(test_case.expect_index_modify_w_result);
    bool successful_remove =
        IsOperationSuccessful(test_case.expect_index_remove_w_result);

    bool failed_operation =
        IsOperationFailed(test_case.expect_index_add_w_result) ||
        IsOperationFailed(test_case.expect_index_modify_w_result) ||
        IsOperationFailed(test_case.expect_index_remove_w_result);

    bool successful_upsert = successful_add || successful_modify;
    bool is_hash_operation =
        !test_case.open_key_fail &&
        test_case.open_key_type == VALKEYMODULE_KEYTYPE_HASH &&
        test_case.valkey_hash_data.has_value();

    // Check field type metrics for successful operations with document count
    // increase
    if (successful_upsert && test_case.expected_document_cnt_delta > 0) {
      switch (test_case.index_type) {
        case indexes::IndexerType::kVector:
          EXPECT_GT(metrics.ingest_field_vector, initial_field_vector);
          break;
        case indexes::IndexerType::kNumeric:
          EXPECT_GT(metrics.ingest_field_numeric, initial_field_numeric);
          break;
        case indexes::IndexerType::kTag:
          EXPECT_GT(metrics.ingest_field_tag, initial_field_tag);
          break;
        default:
          break;
      }
    }

    // Check failure metrics
    if (failed_operation) {
      EXPECT_GT(metrics.ingest_total_failures, initial_total_failures);
    }

    // Check hash keys metrics
    if (is_hash_operation) {
      EXPECT_GT(metrics.ingest_hash_keys, initial_hash_keys);
    }

    // Verify Time Slice Mutex metrics
    if (successful_upsert) {
      EXPECT_EQ(global_stats.time_slice_upserts, initial_upserts + 1);
      EXPECT_EQ(global_stats.time_slice_deletes, initial_deletes);
    } else if (successful_remove) {
      EXPECT_EQ(global_stats.time_slice_deletes, initial_deletes + 1);
      EXPECT_EQ(global_stats.time_slice_upserts, initial_upserts);
    } else {
      // No successful operation expected
      EXPECT_EQ(global_stats.time_slice_upserts, initial_upserts);
      EXPECT_EQ(global_stats.time_slice_deletes, initial_deletes);
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
    IndexSchemaSubscriptionTests, IndexSchemaSubscriptionTest,
    ValuesIn<IndexSchemaSubscriptionTestCase>({
        {
            .test_name = "happy_path_add",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("vector", "vector_buffer"),
            .is_tracked = false,
            .expect_index_add_w_result = true,
            .expected_vector_buffer = "vector_buffer",
            .expected_add_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .success_cnt = 1,
                },
            .expected_document_cnt_delta = 1,
            .index_type = indexes::IndexerType::kVector,
        },
        {
            .test_name = "happy_path_remove_key",
            .hash_field = "vector",
            .open_key_fail = true,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::nullopt,
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
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::nullopt,
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
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("vector", "vector_buffer"),
            .is_tracked = true,
            .expect_index_modify_w_result = true,
            .expected_vector_buffer = "vector_buffer",
            .expected_modify_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .success_cnt = 1,
                },
            .index_type = indexes::IndexerType::kVector,
        },
        {
            .test_name = "untracked_and_record_does_not_exist",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::nullopt,
            .is_tracked = false,
            .expect_index_remove_w_result = false,
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .skipped_cnt = 0,
                },
        },
        {
            .test_name = "untracked_and_key_does_not_exist",
            .hash_field = "vector",
            .open_key_fail = true,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::nullopt,
            .is_tracked = false,
            .expect_index_remove_w_result = false,
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .skipped_cnt = 0,
                },
            .expected_deletion_type = indexes::DeletionType::kRecord,
        },
        {
            .test_name = "add_failure",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("vector", "vector_buffer"),
            .is_tracked = false,
            .expect_index_add_w_result = absl::InternalError("error"),
            .expected_vector_buffer = "vector_buffer",
            .expected_add_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .failure_cnt = 1,
                },
            .index_type = indexes::IndexerType::kVector,
        },
        {
            .test_name = "modify_failure",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("vector", "vector_buffer"),
            .is_tracked = true,
            .expect_index_modify_w_result = absl::InternalError("error"),
            .expected_vector_buffer = "vector_buffer",
            .expected_modify_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .failure_cnt = 1,
                },
            .index_type = indexes::IndexerType::kVector,
        },
        {
            .test_name = "happy_path_add_numeric",
            .hash_field = "numeric",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("numeric", "numeric_buffer"),
            .is_tracked = false,
            .expect_index_add_w_result = true,
            .expected_vector_buffer = "numeric_buffer",
            .expected_add_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .success_cnt = 1,
                },
            .expected_document_cnt_delta = 1,
            .index_type = indexes::IndexerType::kNumeric,
        },
        {
            .test_name = "happy_path_modify_numeric",
            .hash_field = "numeric",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("numeric", "numeric_buffer"),
            .is_tracked = true,
            .expect_index_modify_w_result = true,
            .expected_vector_buffer = "numeric_buffer",
            .expected_modify_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .success_cnt = 1,
                },
            .index_type = indexes::IndexerType::kNumeric,
        },
        {
            .test_name = "add_failure_numeric",
            .hash_field = "numeric",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("numeric", "numeric_buffer"),
            .is_tracked = false,
            .expect_index_add_w_result = absl::InternalError("error"),
            .expected_vector_buffer = "numeric_buffer",
            .expected_add_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .failure_cnt = 1,
                },
            .index_type = indexes::IndexerType::kNumeric,
        },
        {
            .test_name = "happy_path_add_tag",
            .hash_field = "tag",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("tag", "tag_buffer"),
            .is_tracked = false,
            .expect_index_add_w_result = true,
            .expected_vector_buffer = "tag_buffer",
            .expected_add_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .success_cnt = 1,
                },
            .expected_document_cnt_delta = 1,
            .index_type = indexes::IndexerType::kTag,
        },
        {
            .test_name = "happy_path_modify_tag",
            .hash_field = "tag",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("tag", "tag_buffer"),
            .is_tracked = true,
            .expect_index_modify_w_result = true,
            .expected_vector_buffer = "tag_buffer",
            .expected_modify_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .success_cnt = 1,
                },
            .index_type = indexes::IndexerType::kTag,
        },
        {
            .test_name = "add_failure_tag",
            .hash_field = "tag",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("tag", "tag_buffer"),
            .is_tracked = false,
            .expect_index_add_w_result = absl::InternalError("error"),
            .expected_vector_buffer = "tag_buffer",
            .expected_add_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .failure_cnt = 1,
                },
            .index_type = indexes::IndexerType::kTag,
        },
        {
            .test_name = "remove_failure",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::nullopt,
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
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("vector", "vector_buffer"),
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
            .open_key_type = VALKEYMODULE_KEYTYPE_STRING,
            .expect_wrong_type = true,
        },
        {
            .test_name = "modify_skipped",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("vector", "vector_buffer"),
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
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::nullopt,
            .is_tracked = true,
            .expect_index_remove_w_result = false,
            .expected_vector_buffer = "vector_buffer",
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .skipped_cnt = 0,
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
    auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key->Str().data());
    EXPECT_CALL(*mock_index, IsTracked(key)).WillRepeatedly(Return(false));

    EXPECT_CALL(*mock_index, AddRecord(key, testing::_)).Times(0);

    EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
        .WillRepeatedly(TestValkeyModule_KeyTypeDefaultImpl);
    EXPECT_CALL(*kMockValkeyModule,
                KeyType(vmsdk::ValkeyModuleKeyIsForString(key->Str())))
        .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
    EXPECT_CALL(*kMockValkeyModule, GetClientId(testing::_))
        .WillRepeatedly(testing::Return(1));
    EXPECT_CALL(
        *kMockValkeyModule,
        BlockClient(testing::_, testing::_, testing::_, testing::_, testing::_))
        .WillOnce(Return((ValkeyModuleBlockedClient *)1));
    const char *field = "vector";
    const char *value = "vector_buffer";
    ValkeyModuleString *value_valkey_str =
        TestValkeyModule_CreateStringPrintf(nullptr, "%s", value);

    EXPECT_CALL(*kMockValkeyModule,
                HashGet(vmsdk::ValkeyModuleKeyIsForString(key->Str()),
                        VALKEYMODULE_HASH_CFIELDS, StrEq(field),
                        An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
        .WillOnce([value_valkey_str](
                      ValkeyModuleKey *key, int flags, const char *field,
                      ValkeyModuleString **value_out, void *terminating_null) {
          *value_out = value_valkey_str;
          return VALKEYMODULE_OK;
        });

    index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                         "event", key_valkey_str.get());
    EXPECT_CALL(*kMockValkeyModule,
                UnblockClient((ValkeyModuleBlockedClient *)1, nullptr))
        .WillOnce(Return(1));
  }
  EXPECT_EQ(mutations_thread_pool.QueueSize(), 1);
  VMSDK_EXPECT_OK(mutations_thread_pool.ResumeWorkers());
  WaitWorkerTasksAreCompleted(mutations_thread_pool);
  EXPECT_EQ(vmsdk::BlockedClientTracker::GetInstance().GetClientCount(
                vmsdk::BlockedClientCategory::kHash),
            0);
  EXPECT_EQ(vmsdk::BlockedClientTracker::GetInstance().GetClientCount(
                vmsdk::BlockedClientCategory::kJson),
            0);
  EXPECT_EQ(vmsdk::BlockedClientTracker::GetInstance().GetClientCount(
                vmsdk::BlockedClientCategory::kOther),
            0);
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
  auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key.c_str());
  ValkeyModuleCtx different_db_ctx;
  index_schema->OnKeyspaceNotification(&different_db_ctx,
                                       VALKEYMODULE_NOTIFY_HASH, "event",
                                       key_valkey_str.get());
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
  auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key.c_str());
  ValkeyModuleCtx different_db_ctx;
  auto match_key = vmsdk::MakeUniqueValkeyOpenKey(&different_db_ctx,
                                                  key_valkey_str.get(), 0);
  TestValkeyModule_ModuleTypeSetValueDefaultImpl(
      match_key.get(), (ValkeyModuleType *)0x1, nullptr);
  index_schema->OnKeyspaceNotification(&different_db_ctx,
                                       VALKEYMODULE_NOTIFY_HASH, "event",
                                       key_valkey_str.get());
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
  EXPECT_CALL(*kMockValkeyModule, OpenKey(&fake_ctx_, testing::_, testing::_))
      .Times(0);
  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
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

  EXPECT_EQ(index_schema->GetAttributeDataType().GetValkeyEventTypes(),
            VALKEYMODULE_NOTIFY_HASH | VALKEYMODULE_NOTIFY_GENERIC |
                VALKEYMODULE_NOTIFY_EXPIRED | VALKEYMODULE_NOTIFY_EVICTED);
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
  EXPECT_CALL(*kMockValkeyModule, DbSize(testing::_))
      .WillRepeatedly(Return(test_case.db_size));

  ValkeyModuleCtx parent_ctx;
  ValkeyModuleCtx scan_ctx;
  EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(&parent_ctx))
      .WillRepeatedly(Return(&scan_ctx));
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&parent_ctx))
      .WillRepeatedly(Return(test_case.context_flags));
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&scan_ctx))
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
  EXPECT_CALL(*kMockValkeyModule,
              Scan(&scan_ctx, testing::An<ValkeyModuleScanCursor *>(),
                   testing::An<ValkeyModuleScanCB>(), testing::An<void *>()))
      .WillRepeatedly([&](ValkeyModuleCtx *ctx, ValkeyModuleScanCursor *cursor,
                          ValkeyModuleScanCB fn, void *privdata) -> int {
        if (i >= test_case.keys_to_return_in_scan.size()) {
          return 0;
        }
        bool expect_processed =
            std::find(test_case.expected_keys_processed.begin(),
                      test_case.expected_keys_processed.end(),
                      test_case.keys_to_return_in_scan[i]) !=
            test_case.expected_keys_processed.end();

        auto key_str = test_case.keys_to_return_in_scan[i];
        auto key_r_str = vmsdk::MakeUniqueValkeyString(key_str);
        ValkeyModuleKey key = {.ctx = &scan_ctx, .key = key_str};
        if (expect_processed) {
          ValkeyModuleString *value_valkey_str =
              TestValkeyModule_CreateStringPrintf(nullptr, "arbitrary data");
          EXPECT_CALL(
              *kMockValkeyModule,
              HashGet(vmsdk::ValkeyModuleKeyIsForString(key_str),
                      VALKEYMODULE_HASH_CFIELDS, testing::_,
                      An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
              .WillOnce([value_valkey_str](ValkeyModuleKey *key, int flags,
                                           const char *field,
                                           ValkeyModuleString **value_out,
                                           void *terminating_null) {
                *value_out = value_valkey_str;
                return VALKEYMODULE_OK;
              });
          EXPECT_CALL(*mock_index,
                      IsTracked(testing::Property(&InternedStringPtr::operator*,
                                                  testing::StrEq(key_str))))
              .WillRepeatedly(testing::Return(false));
          EXPECT_CALL(*mock_index,
                      AddRecord(testing::Property(&InternedStringPtr::operator*,
                                                  testing::StrEq(key_str)),
                                testing::_))
              .WillOnce(testing::Return(true));
          if (use_thread_pool) {
            EXPECT_CALL(thread_pool,
                        Schedule(testing::_, vmsdk::ThreadPool::Priority::kLow))
                .Times(1);
            EXPECT_CALL(*kMockValkeyModule,
                        BlockClient(testing::_, testing::_, testing::_,
                                    testing::_, testing::_))
                .Times(0);
            EXPECT_CALL(*kMockValkeyModule,
                        UnblockClient(testing::_, testing::_))
                .Times(0);
          }
        }
        if (test_case.return_wrong_types) {
          EXPECT_CALL(*kMockValkeyModule,
                      KeyType(vmsdk::ValkeyModuleKeyIsForString(key_str)))
              .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_STRING));
        } else {
          EXPECT_CALL(*kMockValkeyModule,
                      KeyType(vmsdk::ValkeyModuleKeyIsForString(key_str)))
              .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
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
    ValkeyModuleCtx parent_ctx;
    ValkeyModuleCtx scan_ctx;
    EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(&parent_ctx))
        .WillRepeatedly(Return(&scan_ctx));
    auto index_schema = MockIndexSchema::Create(
                            &parent_ctx, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(),
                            use_thread_pool ? &mutations_thread_pool : nullptr)
                            .value();

    // We only expect it to do the scan the first iteration.
    EXPECT_CALL(*kMockValkeyModule,
                Scan(&scan_ctx, testing::An<ValkeyModuleScanCursor *>(),
                     testing::An<ValkeyModuleScanCB>(), testing::An<void *>()))
        .WillOnce([&](ValkeyModuleCtx *ctx, ValkeyModuleScanCursor *cursor,
                      ValkeyModuleScanCB fn,
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
    ValkeyModuleCtx parent_ctx;
    ValkeyModuleCtx scan_ctx;
    EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(&parent_ctx))
        .WillRepeatedly(Return(&scan_ctx));
    EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&parent_ctx))
        .WillRepeatedly(Return(starting_db));
    EXPECT_CALL(*kMockValkeyModule, SelectDb(&scan_ctx, starting_db))
        .WillRepeatedly(Return(VALKEYMODULE_OK));
    auto index_schema = MockIndexSchema::Create(
                            &parent_ctx, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(),
                            use_thread_pool ? &mutations_thread_pool : nullptr)
                            .value();

    // Validate swapping changes the db in the context
    ValkeyModuleSwapDbInfo swap_db_info = {
        .dbnum_first = starting_db,
        .dbnum_second = db_to_swap,
    };
    EXPECT_CALL(*kMockValkeyModule, SelectDb(&scan_ctx, db_to_swap))
        .WillOnce(Return(VALKEYMODULE_OK));
    index_schema->OnSwapDB(&swap_db_info);

    // Validate swapping again brings the db back to the original
    EXPECT_CALL(*kMockValkeyModule, SelectDb(&scan_ctx, starting_db))
        .WillOnce(Return(VALKEYMODULE_OK));
    index_schema->OnSwapDB(&swap_db_info);
  }
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
                    .context_flags = VALKEYMODULE_CTX_FLAGS_OOM,
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
      vmsdk::UniqueValkeyString data =
          vmsdk::MakeUniqueValkeyString(absl::string_view(
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

    // Add numeric index
    auto numeric_index =
        std::make_shared<indexes::Numeric>(CreateNumericIndexProto());
    VMSDK_EXPECT_OK(index_schema->AddIndex(
        "numeric_attribute", "numeric_identifier", numeric_index));

    // Add tag index
    auto tag_index =
        std::make_shared<indexes::Tag>(CreateTagIndexProto(",", false));
    VMSDK_EXPECT_OK(
        index_schema->AddIndex("tag_attribute", "tag_identifier", tag_index));

    VMSDK_EXPECT_OK(index_schema->RDBSave(&rdb_stream));
  }

  // Load the saved index schema and validate
  ValkeyModuleCtx parent_ctx;
  ValkeyModuleCtx scan_ctx;
  EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(&parent_ctx))
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

  VMSDK_EXPECT_OK(index_schema->GetIndex("numeric_attribute"));
  auto num_index = dynamic_cast<indexes::Numeric *>(
      index_schema->GetIndex("numeric_attribute").value().get());
  EXPECT_TRUE(num_index != nullptr);

  VMSDK_EXPECT_OK(index_schema->GetIndex("tag_attribute"));
  auto tag_index = dynamic_cast<indexes::Tag *>(
      index_schema->GetIndex("tag_attribute").value().get());
  EXPECT_TRUE(tag_index != nullptr);
  EXPECT_EQ(tag_index->GetSeparator(), ',');
  EXPECT_EQ(tag_index->IsCaseSensitive(), false);

  EXPECT_TRUE(index_schema->IsBackfillInProgress());
  EXPECT_EQ(index_schema->GetStats().document_cnt, 10);
  EXPECT_EQ(index_schema->CountRecords(), 10);
}

TEST_F(IndexSchemaRDBTest, SaveAndLoadTextIndex)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  std::vector<absl::string_view> key_prefixes = {"doc:"};
  std::string index_schema_name_str("text_index_schema");
  bool with_suffix_trie = false;
  bool no_stem = false;
  uint32_t min_stem_size = 4;  // MockIndexSchema::Create uses default value

  FakeSafeRDB rdb_stream;

  // Construct and save index schema with text index
  {
    // Set index schema text properties to values different than the IndexSchema
    // defaults
    data_model::Language language =
        data_model::LANGUAGE_ENGLISH;  // Only one supported language right now
    std::string punctuation = ".";
    bool with_offsets = false;
    std::vector<std::string> stop_words = {"stop"};

    auto index_schema = MockIndexSchema::Create(
                            &fake_ctx_, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(), nullptr,
                            language, punctuation, with_offsets, stop_words)
                            .value();

    // Create text index with both proto and schema
    auto text_index_schema = std::make_shared<indexes::text::TextIndexSchema>(
        language, punctuation, with_offsets, stop_words, min_stem_size);
    auto text_index = std::make_shared<indexes::Text>(
        CreateTextIndexProto(with_suffix_trie, no_stem, 1.0),
        text_index_schema);
    VMSDK_EXPECT_OK(
        index_schema->AddIndex("description", "desc_id", text_index));

    // Add various text documents to exercise text index functionality
    std::vector<std::string> texts = {
        "hello world search engine", "running runs runner stemming test",
        "Hello World Test Case with punctuation!",
        "document with multiple words and symbols@#$"};

    for (size_t i = 0; i < texts.size(); ++i) {
      auto interned_key = StringInternStore::Intern("doc:" + std::to_string(i));
      vmsdk::UniqueValkeyString data = vmsdk::MakeUniqueValkeyString(texts[i]);

      // Add record and handle return value properly
      auto result =
          text_index->AddRecord(interned_key, vmsdk::ToStringView(data.get()));
      VMSDK_EXPECT_OK(result);
      EXPECT_TRUE(result.value());
    }

    VMSDK_EXPECT_OK(index_schema->RDBSave(&rdb_stream));
  }

  // Load the saved index schema and validate text index restoration
  {
    ValkeyModuleCtx parent_ctx;
    ValkeyModuleCtx scan_ctx;
    EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(&parent_ctx))
        .WillRepeatedly(Return(&scan_ctx));

    RDBSectionIter iter(&rdb_stream, 1);
    auto section = iter.Next();
    VMSDK_EXPECT_OK_STATUSOR(section);

    auto index_schema_or =
        IndexSchema::LoadFromRDB(&parent_ctx, nullptr,
                                 std::make_unique<data_model::IndexSchema>(
                                     (*section)->index_schema_contents()),
                                 iter.IterateSupplementalContent());

    VMSDK_EXPECT_OK_STATUSOR(index_schema_or);
    auto index_schema = std::move(index_schema_or.value());

    // Validate basic schema properties
    EXPECT_THAT(index_schema->GetKeyPrefixes(),
                testing::UnorderedElementsAre("doc:"));
    EXPECT_TRUE(dynamic_cast<const HashAttributeDataType *>(
        &index_schema->GetAttributeDataType()));

    // Validate text schema properties
    auto schema_proto = index_schema->ToProto();
    EXPECT_EQ(schema_proto->language(), data_model::LANGUAGE_ENGLISH);
    EXPECT_EQ(schema_proto->punctuation(), ".");
    EXPECT_EQ(schema_proto->with_offsets(), false);
    EXPECT_THAT(schema_proto->stop_words(),
                testing::UnorderedElementsAre("stop"));

    // Validate text index schema was recreated
    EXPECT_THAT(index_schema->GetTextIndexSchema(), testing::NotNull());

    // Validate text index was restored correctly
    VMSDK_EXPECT_OK(index_schema->GetIndex("description"));
    auto text_index = dynamic_cast<indexes::Text *>(
        index_schema->GetIndex("description").value().get());
    EXPECT_TRUE(text_index != nullptr);

    // Validate text index configuration was preserved
    auto text_proto = text_index->ToProto();
    EXPECT_TRUE(text_proto->has_text_index());
    EXPECT_EQ(text_proto->text_index().with_suffix_trie(), with_suffix_trie);
    EXPECT_EQ(text_proto->text_index().no_stem(), no_stem);

    // Validate schema-level min_stem_size
    EXPECT_EQ(index_schema->GetMinStemSize(), min_stem_size);

    // TODO: Text index key tracking is not yet implemented
    // Validate that GetRecordCount returns 0 (expected for unimplemented key
    // tracking)
    EXPECT_EQ(text_index->GetTrackedKeyCount(), 0);

    // TODO: Document count will be 0 until proper key tracking is implemented
    // This test verifies RDB save/load works without crashing, not document
    // counting
    EXPECT_EQ(index_schema->GetStats().document_cnt, 0);

    // Validate backfill is properly set up for restored schema
    EXPECT_TRUE(index_schema->IsBackfillInProgress());
  }
}

TEST_F(IndexSchemaRDBTest, LoadEndedDeletesOrphanedKeys) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  for (bool use_thread_pool : {true, false}) {
    auto mock_index = std::make_shared<MockIndex>();
    absl::flat_hash_map<std::string, uint64_t> keys_in_index = {
        {"key1", 1}, {"key2", 2}, {"key3", 3}};
    EXPECT_CALL(*mock_index, ForEachTrackedKey(testing::_))
        .WillOnce(
            [&keys_in_index](
                absl::AnyInvocable<absl::Status(const InternedStringPtr &)> fn)
                -> absl::Status {
              for (const auto &[key, internal_id] : keys_in_index) {
                InternedStringPtr interned_key = StringInternStore::Intern(key);
                VMSDK_RETURN_IF_ERROR(fn(interned_key));
              }
              return absl::OkStatus();
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
    EXPECT_CALL(*kMockValkeyModule, SelectDb(testing::_, testing::_))
        .WillRepeatedly(Return(1));  // So backfill job can be created.
    EXPECT_CALL(*kMockValkeyModule, SelectDb(&fake_ctx_, 0))
        .WillOnce(Return(1));
    EXPECT_CALL(*kMockValkeyModule,
                KeyExists(&fake_ctx_, vmsdk::ValkeyModuleStringValueEq("key1")))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*kMockValkeyModule,
                KeyExists(&fake_ctx_, vmsdk::ValkeyModuleStringValueEq("key2")))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*kMockValkeyModule,
                KeyExists(&fake_ctx_, vmsdk::ValkeyModuleStringValueEq("key3")))
        .WillRepeatedly(Return(1));

    EXPECT_CALL(*mock_index,
                RemoveRecord(testing::Property(&InternedStringPtr::operator*,
                                               testing::StrEq("key1")),
                             indexes::DeletionType::kRecord))
        .WillOnce(Return(true));
    EXPECT_CALL(*mock_index,
                RemoveRecord(testing::Property(&InternedStringPtr::operator*,
                                               testing::StrEq("key2")),
                             indexes::DeletionType::kRecord))
        .WillOnce(Return(true));
    EXPECT_CALL(*mock_index,
                RemoveRecord(testing::Property(&InternedStringPtr::operator*,
                                               testing::StrEq("key3")),
                             indexes::DeletionType::kRecord))
        .Times(0);
    index_schema->OnLoadingEnded(&fake_ctx_);
    if (use_thread_pool) {
      WaitWorkerTasksAreCompleted(mutations_thread_pool);
    }
  }
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
  ValkeyModuleCtx fake_ctx;
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
      vmsdk::MakeUniqueValkeyString(data_ptr);
  return mutated_attributes;
}

TEST_F(IndexSchemaFriendTest, MutatedAttributesSanity) {
  absl::string_view data_ptr;
  EXPECT_EQ(index_schema->attributes_.size(), 1);
  auto mutated_attributes_1 =
      CreateMutatedAttributes(attribute_identifier, data_ptr);
  EXPECT_TRUE(index_schema->TrackMutatedRecord(
      nullptr, key, std::move(mutated_attributes_1), 0, true, false, false));
  // Verify that adding a track attribute with backfill off after on return true
  auto mutated_attributes_2 =
      CreateMutatedAttributes(attribute_identifier, data_ptr);
  EXPECT_TRUE(index_schema->TrackMutatedRecord(
      nullptr, key, std::move(mutated_attributes_2), 0, false, false, false));
  auto mutated_attributes_3 =
      CreateMutatedAttributes(attribute_identifier, data_ptr);
  EXPECT_FALSE(index_schema->TrackMutatedRecord(
      nullptr, key, std::move(mutated_attributes_3), 0, false, false, false));
  EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 1);
  auto consumed_data = index_schema->ConsumeTrackedMutatedAttribute(key, true);
  EXPECT_TRUE(consumed_data.has_value());
  auto mutated_attributes_4 =
      CreateMutatedAttributes(attribute_identifier, data_ptr);
  EXPECT_FALSE(index_schema->TrackMutatedRecord(
      nullptr, key, std::move(mutated_attributes_4), 0, false, false, false));
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
          nullptr, key, std::move(mutated_attributes), 0, false, false, false));
    }
    if (!track_before_consumption_data_ptr.empty()) {
      VLOG(1) << "track_before_consumption_data_ptr is not empty";
      EXPECT_EQ(index_schema->attributes_.size(), 1);
      auto mutated_attributes = CreateMutatedAttributes(
          attribute_identifier, track_before_consumption_data_ptr);
      EXPECT_FALSE(index_schema->TrackMutatedRecord(
          nullptr, key, std::move(mutated_attributes), 0, false, false, false));
      data_ptr = track_before_consumption_data_ptr;
    }
    EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 1);
    auto consumed_data =
        index_schema->ConsumeTrackedMutatedAttribute(key, true);
    EXPECT_TRUE(consumed_data.has_value());
    auto data_view =
        vmsdk::ToStringView(consumed_data->begin()->second.data.get());
    vmsdk::UniqueValkeyString expected_data =
        vmsdk::MakeUniqueValkeyString(data_ptr);
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
        EXPECT_EQ(index_schema->TrackMutatedRecord(
                      nullptr, key, std::move(mutated_attributes), 0, false,
                      false, false),
                  !track_before_consumption_data_ptr.empty());
      }
      auto consumed_data =
          index_schema->ConsumeTrackedMutatedAttribute(key, false);
      EXPECT_TRUE(consumed_data.has_value());
      auto data_view =
          vmsdk::ToStringView(consumed_data->begin()->second.data.get());
      vmsdk::UniqueValkeyString expected_data =
          vmsdk::MakeUniqueValkeyString(track_after_consumption_data_ptr);

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
    vmsdk::UniqueValkeyString data = vmsdk::MakeUniqueValkeyString(
        absl::string_view((char *)&vectors[0][0], dimensions * sizeof(float)));
    IndexSchema::MutatedAttributes mutated_attributes;
    mutated_attributes[itr->second.GetIdentifier()].data = std::move(data);
    auto key_interned = StringInternStore::Intern(std::string(*key) + "0");
    index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned,
                                  false, false);
    EXPECT_EQ(mutations_thread_pool.QueueSize(), 1);
    VMSDK_EXPECT_OK(mutations_thread_pool.ResumeWorkers());
  }
  const auto &stats = index_schema->GetStats();
  const size_t iterations = 100;
  // Test delete consistency
  for (size_t j = 0; j < iterations; ++j) {
    for (size_t i = 0; i < vectors.size(); ++i) {
      vmsdk::UniqueValkeyString data =
          vmsdk::MakeUniqueValkeyString(absl::string_view(
              (char *)&vectors[i][0], dimensions * sizeof(float)));
      IndexSchema::MutatedAttributes mutated_attributes;
      mutated_attributes[attribute_identifier].data = std::move(data);
      auto key_interned =
          StringInternStore::Intern(std::string(*key) + std::to_string(i));
      index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned,
                                    false, false);
    }
  }
  for (size_t i = 0; i < vectors.size(); ++i) {
    vmsdk::UniqueValkeyString data;
    IndexSchema::MutatedAttributes mutated_attributes;
    mutated_attributes[attribute_identifier].data = std::move(data);
    auto key_interned =
        StringInternStore::Intern(std::string(*key) + std::to_string(i));
    index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned,
                                  false, true);
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
      vmsdk::UniqueValkeyString data =
          vmsdk::MakeUniqueValkeyString(absl::string_view(
              (char *)&vectors[0][0], dimensions * sizeof(float)));
      IndexSchema::MutatedAttributes mutated_attributes;
      mutated_attributes[attribute_identifier].data = std::move(data);
      auto key_interned =
          StringInternStore::Intern(std::string(*key) + std::to_string(i));
      index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned,
                                    false, false);
    }
  }
  for (size_t i = 0; i < vectors.size(); ++i) {
    vmsdk::UniqueValkeyString data = vmsdk::MakeUniqueValkeyString(
        absl::string_view((char *)&vectors[i][0], dimensions * sizeof(float)));
    IndexSchema::MutatedAttributes mutated_attributes;
    mutated_attributes[attribute_identifier].data = std::move(data);
    auto key_interned =
        StringInternStore::Intern(std::string(*key) + std::to_string(i));
    index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned,
                                  false, true);
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

class IndexSchemaTest : public vmsdk::ValkeyTest {};

TEST_F(IndexSchemaTest, ShouldBlockClient) {
  ValkeyModuleCtx fake_ctx;
  {
    EXPECT_CALL(*kMockValkeyModule, GetClientId(&fake_ctx))
        .WillOnce(testing::Return(1));
    EXPECT_TRUE(ShouldBlockClient(&fake_ctx, false, false));
  }
  {
    EXPECT_CALL(*kMockValkeyModule, GetClientId(&fake_ctx))
        .WillOnce(testing::Return(0));
    EXPECT_FALSE(ShouldBlockClient(&fake_ctx, false, false));
  }
  EXPECT_FALSE(ShouldBlockClient(&fake_ctx, true, false));
  EXPECT_FALSE(ShouldBlockClient(&fake_ctx, false, true));
  EXPECT_FALSE(ShouldBlockClient(&fake_ctx, true, true));
}

TEST_F(IndexSchemaRDBTest, DrainMutationQueueOnSaveEnabled) {
  // Enable drain-mutation-queue-on-save configuration
  auto &drain_config = const_cast<vmsdk::config::Boolean &>(
      options::GetDrainMutationQueueOnSave());
  auto drain_config_old_value = drain_config.GetValue();
  VMSDK_EXPECT_OK(drain_config.SetValue(true));

  vmsdk::ThreadPool mutations_thread_pool("test-mutations-", 1);
  mutations_thread_pool.StartWorkers();

  std::vector<absl::string_view> key_prefixes = {"test:"};
  std::string index_schema_name_str("drain_test_index");
  auto index_schema =
      MockIndexSchema::Create(&fake_ctx_, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool)
          .value();

  auto mock_index = std::make_shared<MockIndex>();
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("test_attribute", "test_identifier", mock_index));

  // Set up mock expectations for creating a queued mutation
  EXPECT_CALL(*mock_index, IsTracked(testing::_)).WillRepeatedly(Return(false));
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));

  ValkeyModuleString *test_data =
      TestValkeyModule_CreateStringPrintf(nullptr, "test_data");
  EXPECT_CALL(*kMockValkeyModule, HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS,
                                          testing::StrEq("test_identifier"),
                                          testing::An<ValkeyModuleString **>(),
                                          testing::TypedEq<void *>(nullptr)))
      .WillOnce([test_data](ValkeyModuleKey *, int, const char *,
                            ValkeyModuleString **value_out, void *) {
        *value_out = test_data;
        return VALKEYMODULE_OK;
      });

  // Temporarily suspend workers to create queued mutations
  VMSDK_EXPECT_OK(mutations_thread_pool.SuspendWorkers());

  // Trigger keyspace notification to create a queued mutation
  auto key_str = vmsdk::MakeUniqueValkeyString("test:key1");
  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                       "event", key_str.get());

  // Verify mutation is queued
  EXPECT_GT(mutations_thread_pool.QueueSize(), 0);

  // Save RDB
  FakeSafeRDB rdb_stream;
  std::atomic<bool> rdb_save_started{false};
  std::atomic<bool> rdb_save_completed{false};
  std::thread rdb_saver_thread(
      [index_schema, &rdb_stream, &rdb_save_started, &rdb_save_completed]() {
        rdb_save_started.store(true);
        auto save_result = index_schema->RDBSave(&rdb_stream);
        rdb_save_completed.store(save_result.ok());
      });

  // Save is expected to be blocked
  while (!rdb_save_started.load()) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  EXPECT_FALSE(rdb_save_completed.load());

  // Resume mutations_thread_pool workers to unblock save
  VMSDK_EXPECT_OK(mutations_thread_pool.ResumeWorkers());
  rdb_saver_thread.join();
  EXPECT_TRUE(rdb_save_completed.load());
  EXPECT_EQ(mutations_thread_pool.QueueSize(), 0);

  // Reset configuration
  VMSDK_EXPECT_OK(drain_config.SetValue(drain_config_old_value));
}

TEST_F(IndexSchemaRDBTest, ComprehensiveSkipLoadTest) {
  const int num_vectors = 1000;
  const int dimensions = 64;
  const int additional_index_vectors = 100;
  auto distance_metric = data_model::DISTANCE_METRIC_L2;
  int initial_cap = 2000;
  int m = 16;
  int ef_construction = 200;
  int ef_runtime = 10;
  int block_size = 128;

  LOG(INFO) << "=== Comprehensive Skip Load Test Started ===";

  // STEP 1: Create vector index + add 1000 vectors, save to RDB
  LOG(INFO) << "STEP 1: Creating vector index with " << num_vectors
            << " vectors";
  std::vector<absl::string_view> key_prefixes = {"item:"};
  std::string index_schema_name_str("comprehensive_test");
  FakeSafeRDB rdb_stream_step1;

  {
    auto index_schema = MockIndexSchema::Create(
                            &fake_ctx_, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(), nullptr)
                            .value();

    auto hnsw_index =
        indexes::VectorHNSW<float>::Create(
            CreateHNSWVectorIndexProto(dimensions, distance_metric, initial_cap,
                                       m, ef_construction, ef_runtime),
            "embedding",
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
            .value();
    VMSDK_EXPECT_OK(index_schema->AddIndex("embedding", "emb_id", hnsw_index));

    // Add 1000 vectors
    auto vectors =
        DeterministicallyGenerateVectors(num_vectors, dimensions, 1.0);
    auto itr = index_schema->attributes_.find("embedding");
    EXPECT_FALSE(itr == index_schema->attributes_.end());

    for (size_t i = 0; i < vectors.size(); ++i) {
      vmsdk::UniqueValkeyString data =
          vmsdk::MakeUniqueValkeyString(absl::string_view(
              (char *)&vectors[i][0], dimensions * sizeof(float)));
      auto interned_key =
          StringInternStore::Intern("item:" + std::to_string(i));
      index_schema->ProcessAttributeMutation(&fake_ctx_, itr->second,
                                             interned_key, std::move(data),
                                             indexes::DeletionType::kNone);
    }

    EXPECT_EQ(hnsw_index->GetTrackedKeyCount(), num_vectors);
    VMSDK_EXPECT_OK(index_schema->RDBSave(&rdb_stream_step1));
    LOG(INFO) << "✓ Step 1 completed - saved " << num_vectors
              << " vectors to RDB";
  }

  // STEP 2: Load from RDB normally, then load with skip and verify vectors are
  // loaded correctly
  LOG(INFO) << "STEP 2: Testing normal load vs skip load for vector index";

  // Normal load
  {
    VMSDK_EXPECT_OK(options::GetSkipIndexLoadMutable().SetValue(false));

    ValkeyModuleCtx parent_ctx;
    ValkeyModuleCtx scan_ctx;
    EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillRepeatedly(Return(&scan_ctx));

    RDBSectionIter iter(&rdb_stream_step1, 1);
    auto section = iter.Next();
    VMSDK_EXPECT_OK_STATUSOR(section);

    auto schema_or =
        IndexSchema::LoadFromRDB(&parent_ctx, nullptr,
                                 std::make_unique<data_model::IndexSchema>(
                                     (*section)->index_schema_contents()),
                                 iter.IterateSupplementalContent());

    VMSDK_EXPECT_OK_STATUSOR(schema_or);
    auto normal_schema = std::move(schema_or.value());

    EXPECT_EQ(normal_schema->GetStats().document_cnt, num_vectors);
    auto vec_index = normal_schema->GetIndex("embedding");
    VMSDK_EXPECT_OK_STATUSOR(vec_index);
    EXPECT_EQ(vec_index.value()->GetTrackedKeyCount(), num_vectors);
    LOG(INFO) << "✓ Normal load verified - " << num_vectors
              << " vectors loaded";
  }

  // Skip load
  {
    VMSDK_EXPECT_OK(options::GetSkipIndexLoadMutable().SetValue(true));

    // Reset stream for reading
    rdb_stream_step1.buffer_.clear();
    rdb_stream_step1.buffer_.seekg(0, std::ios::beg);

    ValkeyModuleCtx parent_ctx;
    ValkeyModuleCtx scan_ctx;
    EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillRepeatedly(Return(&scan_ctx));
    EXPECT_CALL(*kMockValkeyModule, DbSize(testing::_))
        .WillRepeatedly(Return(num_vectors));

    // Mock scan operations for backfill
    int scan_call_count = 0;
    EXPECT_CALL(*kMockValkeyModule,
                Scan(&scan_ctx, testing::An<ValkeyModuleScanCursor *>(),
                     testing::An<ValkeyModuleScanCB>(), testing::An<void *>()))
        .WillRepeatedly([&scan_call_count, num_vectors](
                            ValkeyModuleCtx *ctx,
                            ValkeyModuleScanCursor *cursor,
                            ValkeyModuleScanCB fn, void *privdata) -> int {
          if (scan_call_count < num_vectors) {
            std::string key = "item:" + std::to_string(scan_call_count);
            auto key_r_str = vmsdk::MakeUniqueValkeyString(key);
            ValkeyModuleKey vkey = {.ctx = ctx, .key = key};
            fn(ctx, key_r_str.get(), &vkey, privdata);
            scan_call_count++;
            return 1;
          }
          return 0;
        });

    EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
        .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
    EXPECT_CALL(
        *kMockValkeyModule,
        HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_,
                testing::An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
        .WillRepeatedly(Return(VALKEYMODULE_ERR));

    RDBSectionIter iter(&rdb_stream_step1, 1);
    auto section = iter.Next();
    VMSDK_EXPECT_OK_STATUSOR(section);

    auto schema_or =
        IndexSchema::LoadFromRDB(&parent_ctx, nullptr,
                                 std::make_unique<data_model::IndexSchema>(
                                     (*section)->index_schema_contents()),
                                 iter.IterateSupplementalContent());

    VMSDK_EXPECT_OK_STATUSOR(schema_or);
    auto skip_schema = std::move(schema_or.value());

    EXPECT_EQ(skip_schema->GetStats().document_cnt, num_vectors);
    auto vec_index = skip_schema->GetIndex("embedding");
    VMSDK_EXPECT_OK_STATUSOR(vec_index);
    EXPECT_EQ(vec_index.value()->GetTrackedKeyCount(), 0);
    EXPECT_TRUE(skip_schema->IsBackfillInProgress());
    LOG(INFO) << "✓ Skip load verified - index empty, backfill ready";
  }

  // STEP 3: Drop the schema (implicitly done when schema goes out of scope)
  LOG(INFO) << "STEP 3: Index dropped (implicit)";

  // STEP 4: Create vector index + numeric index + tag index + text index + add
  // 1000 vectors + save to RDB
  LOG(INFO) << "STEP 4: Creating mixed schema with vector + numeric + tag + "
               "text indexes";
  FakeSafeRDB rdb_stream_step4;

  {
    auto index_schema = MockIndexSchema::Create(
                            &fake_ctx_, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(), nullptr)
                            .value();

    // Add vector index
    auto hnsw_index =
        indexes::VectorHNSW<float>::Create(
            CreateHNSWVectorIndexProto(dimensions, distance_metric, initial_cap,
                                       m, ef_construction, ef_runtime),
            "embedding",
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
            .value();
    VMSDK_EXPECT_OK(index_schema->AddIndex("embedding", "emb_id", hnsw_index));

    // Add numeric index
    auto numeric_index =
        std::make_shared<indexes::Numeric>(CreateNumericIndexProto());
    VMSDK_EXPECT_OK(index_schema->AddIndex("price", "price_id", numeric_index));

    // Add tag index
    auto tag_index =
        std::make_shared<indexes::Tag>(CreateTagIndexProto(",", false));
    VMSDK_EXPECT_OK(index_schema->AddIndex("category", "cat_id", tag_index));

    // Add text index
    auto text_index = std::make_shared<indexes::Text>(
        CreateTextIndexProto(true, false, 1.0),
        std::make_shared<indexes::text::TextIndexSchema>(
            data_model::LANGUAGE_ENGLISH,
            " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~", true,
            std::vector<std::string>{}, 6));
    VMSDK_EXPECT_OK(
        index_schema->AddIndex("description", "desc_id", text_index));

    // Add test data for all indexes
    auto vectors =
        DeterministicallyGenerateVectors(num_vectors, dimensions, 1.0);
    auto vec_itr = index_schema->attributes_.find("embedding");
    auto num_itr = index_schema->attributes_.find("price");
    auto tag_itr = index_schema->attributes_.find("category");
    auto text_itr = index_schema->attributes_.find("description");

    EXPECT_FALSE(vec_itr == index_schema->attributes_.end());
    EXPECT_FALSE(num_itr == index_schema->attributes_.end());
    EXPECT_FALSE(tag_itr == index_schema->attributes_.end());
    EXPECT_FALSE(text_itr == index_schema->attributes_.end());

    for (size_t i = 0; i < vectors.size(); ++i) {
      auto interned_key =
          StringInternStore::Intern("item:" + std::to_string(i));

      // Add vector data
      vmsdk::UniqueValkeyString vec_data =
          vmsdk::MakeUniqueValkeyString(absl::string_view(
              (char *)&vectors[i][0], dimensions * sizeof(float)));
      index_schema->ProcessAttributeMutation(&fake_ctx_, vec_itr->second,
                                             interned_key, std::move(vec_data),
                                             indexes::DeletionType::kNone);

      // Add numeric data (price)
      std::string price_str =
          std::to_string(i * 10 + 100);  // prices 100, 110, 120, etc.
      vmsdk::UniqueValkeyString num_data =
          vmsdk::MakeUniqueValkeyString(price_str);
      index_schema->ProcessAttributeMutation(&fake_ctx_, num_itr->second,
                                             interned_key, std::move(num_data),
                                             indexes::DeletionType::kNone);

      // Add tag data (category)
      std::string category = (i % 3 == 0)   ? "electronics"
                             : (i % 3 == 1) ? "books"
                                            : "clothing";
      vmsdk::UniqueValkeyString tag_data =
          vmsdk::MakeUniqueValkeyString(category);
      index_schema->ProcessAttributeMutation(&fake_ctx_, tag_itr->second,
                                             interned_key, std::move(tag_data),
                                             indexes::DeletionType::kNone);

      // Add text data (description)
      std::string description = "description" + std::to_string(i);
      vmsdk::UniqueValkeyString text_data =
          vmsdk::MakeUniqueValkeyString(description);
      index_schema->ProcessAttributeMutation(&fake_ctx_, text_itr->second,
                                             interned_key, std::move(text_data),
                                             indexes::DeletionType::kNone);
    }

    EXPECT_EQ(hnsw_index->GetTrackedKeyCount(), num_vectors);
    VMSDK_EXPECT_OK(index_schema->RDBSave(&rdb_stream_step4));
    LOG(INFO) << "✓ Step 4 completed - saved mixed index with " << num_vectors
              << " records";
  }

  // STEP 5: Load from RDB normally, then load with skip and verify
  LOG(INFO) << "STEP 5: Testing mixed index normal vs skip load";

  // Normal load for mixed index
  {
    VMSDK_EXPECT_OK(options::GetSkipIndexLoadMutable().SetValue(false));

    ValkeyModuleCtx parent_ctx;
    ValkeyModuleCtx scan_ctx;
    EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillRepeatedly(Return(&scan_ctx));

    RDBSectionIter iter(&rdb_stream_step4, 1);
    auto section = iter.Next();
    VMSDK_EXPECT_OK_STATUSOR(section);

    auto schema_or =
        IndexSchema::LoadFromRDB(&parent_ctx, nullptr,
                                 std::make_unique<data_model::IndexSchema>(
                                     (*section)->index_schema_contents()),
                                 iter.IterateSupplementalContent());

    VMSDK_EXPECT_OK_STATUSOR(schema_or);
    auto mixed_schema = std::move(schema_or.value());

    EXPECT_EQ(mixed_schema->GetStats().document_cnt, num_vectors);

    // Verify all index types are loaded
    auto vec_index = mixed_schema->GetIndex("embedding");
    auto num_index = mixed_schema->GetIndex("price");
    auto tag_index = mixed_schema->GetIndex("category");
    auto text_index = mixed_schema->GetIndex("description");

    VMSDK_EXPECT_OK_STATUSOR(vec_index);
    VMSDK_EXPECT_OK_STATUSOR(num_index);
    VMSDK_EXPECT_OK_STATUSOR(tag_index);
    VMSDK_EXPECT_OK_STATUSOR(text_index);

    EXPECT_EQ(vec_index.value()->GetTrackedKeyCount(), num_vectors);
    LOG(INFO) << "✓ Mixed index normal load verified";
  }

  // Skip load for mixed index
  {
    VMSDK_EXPECT_OK(options::GetSkipIndexLoadMutable().SetValue(true));

    // Reset stream
    rdb_stream_step4.buffer_.clear();
    rdb_stream_step4.buffer_.seekg(0, std::ios::beg);

    ValkeyModuleCtx parent_ctx;
    ValkeyModuleCtx scan_ctx;
    EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillRepeatedly(Return(&scan_ctx));
    EXPECT_CALL(*kMockValkeyModule, DbSize(testing::_))
        .WillRepeatedly(Return(num_vectors));

    // Mock scan operations
    int scan_call_count = 0;
    EXPECT_CALL(*kMockValkeyModule,
                Scan(&scan_ctx, testing::An<ValkeyModuleScanCursor *>(),
                     testing::An<ValkeyModuleScanCB>(), testing::An<void *>()))
        .WillRepeatedly([&scan_call_count, num_vectors](
                            ValkeyModuleCtx *ctx,
                            ValkeyModuleScanCursor *cursor,
                            ValkeyModuleScanCB fn, void *privdata) -> int {
          if (scan_call_count < num_vectors) {
            std::string key = "item:" + std::to_string(scan_call_count);
            auto key_r_str = vmsdk::MakeUniqueValkeyString(key);
            ValkeyModuleKey vkey = {.ctx = ctx, .key = key};
            fn(ctx, key_r_str.get(), &vkey, privdata);
            scan_call_count++;
            return 1;
          }
          return 0;
        });

    EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
        .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
    EXPECT_CALL(
        *kMockValkeyModule,
        HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_,
                testing::An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
        .WillRepeatedly(Return(VALKEYMODULE_ERR));

    RDBSectionIter iter(&rdb_stream_step4, 1);
    auto section = iter.Next();
    VMSDK_EXPECT_OK_STATUSOR(section);

    auto schema_or =
        IndexSchema::LoadFromRDB(&parent_ctx, nullptr,
                                 std::make_unique<data_model::IndexSchema>(
                                     (*section)->index_schema_contents()),
                                 iter.IterateSupplementalContent());

    VMSDK_EXPECT_OK_STATUSOR(schema_or);
    auto mixed_skip_schema = std::move(schema_or.value());

    EXPECT_EQ(mixed_skip_schema->GetStats().document_cnt, num_vectors);

    // All indexes should be empty initially
    auto vec_index = mixed_skip_schema->GetIndex("embedding");
    auto num_index = mixed_skip_schema->GetIndex("price");
    auto tag_index = mixed_skip_schema->GetIndex("category");
    auto text_index = mixed_skip_schema->GetIndex("description");

    VMSDK_EXPECT_OK_STATUSOR(vec_index);
    VMSDK_EXPECT_OK_STATUSOR(num_index);
    VMSDK_EXPECT_OK_STATUSOR(tag_index);
    VMSDK_EXPECT_OK_STATUSOR(text_index);

    EXPECT_EQ(vec_index.value()->GetTrackedKeyCount(), 0);
    EXPECT_TRUE(mixed_skip_schema->IsBackfillInProgress());
    LOG(INFO) << "✓ Mixed index skip load verified";
  }

  // STEPS 6-7: Create additional 2 indexes each with 100 vectors and save
  LOG(INFO) << "STEPS 6-7: Creating additional indexes with "
            << additional_index_vectors << " vectors each";
  FakeSafeRDB rdb_stream_multi;

  {
    auto index_schema = MockIndexSchema::Create(
                            &fake_ctx_, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(), nullptr)
                            .value();

    // Create 3 vector indexes (original + 2 additional)
    auto hnsw_index1 =
        indexes::VectorHNSW<float>::Create(
            CreateHNSWVectorIndexProto(dimensions, distance_metric, initial_cap,
                                       m, ef_construction, ef_runtime),
            "embedding1",
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
            .value();
    VMSDK_EXPECT_OK(
        index_schema->AddIndex("embedding1", "emb1_id", hnsw_index1));

    auto hnsw_index2 =
        indexes::VectorHNSW<float>::Create(
            CreateHNSWVectorIndexProto(dimensions, distance_metric, initial_cap,
                                       m, ef_construction, ef_runtime),
            "embedding2",
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
            .value();
    VMSDK_EXPECT_OK(
        index_schema->AddIndex("embedding2", "emb2_id", hnsw_index2));

    auto flat_index =
        indexes::VectorFlat<float>::Create(
            CreateFlatVectorIndexProto(dimensions, distance_metric, initial_cap,
                                       block_size),
            "embedding3",
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
            .value();
    VMSDK_EXPECT_OK(
        index_schema->AddIndex("embedding3", "emb3_id", flat_index));

    // Add vectors to each index (100 vectors each, different ranges)
    auto vectors = DeterministicallyGenerateVectors(
        additional_index_vectors * 3, dimensions, 1.0);

    auto itr1 = index_schema->attributes_.find("embedding1");
    auto itr2 = index_schema->attributes_.find("embedding2");
    auto itr3 = index_schema->attributes_.find("embedding3");

    EXPECT_FALSE(itr1 == index_schema->attributes_.end());
    EXPECT_FALSE(itr2 == index_schema->attributes_.end());
    EXPECT_FALSE(itr3 == index_schema->attributes_.end());

    // Add to index1: vectors 0-99
    for (int i = 0; i < additional_index_vectors; ++i) {
      vmsdk::UniqueValkeyString data =
          vmsdk::MakeUniqueValkeyString(absl::string_view(
              (char *)&vectors[i][0], dimensions * sizeof(float)));
      auto interned_key =
          StringInternStore::Intern("item:" + std::to_string(i));
      index_schema->ProcessAttributeMutation(&fake_ctx_, itr1->second,
                                             interned_key, std::move(data),
                                             indexes::DeletionType::kNone);
    }

    // Add to index2: vectors 100-199
    for (int i = 0; i < additional_index_vectors; ++i) {
      vmsdk::UniqueValkeyString data = vmsdk::MakeUniqueValkeyString(
          absl::string_view((char *)&vectors[i + additional_index_vectors][0],
                            dimensions * sizeof(float)));
      auto interned_key =
          StringInternStore::Intern("item:" + std::to_string(i + 1000));
      index_schema->ProcessAttributeMutation(&fake_ctx_, itr2->second,
                                             interned_key, std::move(data),
                                             indexes::DeletionType::kNone);
    }

    // Add to index3: vectors 200-299
    for (int i = 0; i < additional_index_vectors; ++i) {
      vmsdk::UniqueValkeyString data =
          vmsdk::MakeUniqueValkeyString(absl::string_view(
              (char *)&vectors[i + additional_index_vectors * 2][0],
              dimensions * sizeof(float)));
      auto interned_key =
          StringInternStore::Intern("item:" + std::to_string(i + 2000));
      index_schema->ProcessAttributeMutation(&fake_ctx_, itr3->second,
                                             interned_key, std::move(data),
                                             indexes::DeletionType::kNone);
    }

    EXPECT_EQ(hnsw_index1->GetTrackedKeyCount(), additional_index_vectors);
    EXPECT_EQ(hnsw_index2->GetTrackedKeyCount(), additional_index_vectors);
    EXPECT_EQ(flat_index->GetTrackedKeyCount(), additional_index_vectors);

    VMSDK_EXPECT_OK(index_schema->RDBSave(&rdb_stream_multi));
    LOG(INFO) << "✓ Steps 6-7 completed - saved 3 indexes with "
              << additional_index_vectors << " vectors each";
  }

  // Cleanup
  VMSDK_EXPECT_OK(options::GetSkipIndexLoadMutable().SetValue(false));
  LOG(INFO) << "=== Comprehensive Skip Load Test Completed ===";
}

}  // namespace valkey_search
