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
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/synchronization/notification.h"
#include "absl/types/optional.h"
#include "gmock/gmock.h"
#include "google/protobuf/any.pb.h"
#include "gtest/gtest.h"
#include "src/attribute_data_type.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/indexes/text.h"
#include "src/indexes/text/language_registry.h"
#include "src/indexes/text/text_index.h"
#include "src/indexes/vector_flat.h"
#include "src/indexes/vector_hnsw.h"
#include "src/keyspace_event_manager.h"
#include "src/metrics.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "src/vector_registry.h"
#include "src/version.h"
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
  absl::optional<absl::StatusOr<indexes::RecordResult>>
      expect_index_add_w_result;
  absl::optional<absl::StatusOr<indexes::RecordResult>>
      expect_index_modify_w_result;
  absl::optional<absl::StatusOr<bool>> expect_index_remove_w_result;
  // Validated against the input to add/modify if they are not nullopt.
  std::string expected_vector_buffer;
  IndexSchema::Stats::ResultCnt<uint64_t> expected_add_cnt_delta;
  IndexSchema::Stats::ResultCnt<uint64_t> expected_remove_cnt_delta;
  IndexSchema::Stats::ResultCnt<uint64_t> expected_modify_cnt_delta;
  indexes::DeletionType expected_deletion_type = indexes::DeletionType::kNone;
  int expected_document_cnt_delta;
  indexes::IndexerType index_type = indexes::IndexerType::kVector;
};

class IndexSchemaSubscriptionTest
    : public ValkeySearchTestWithParam<IndexSchemaSubscriptionTestCase> {
 protected:
  // Helper functions to check operation success/failure patterns. Templated so
  // they work for both the bool-returning Remove and the RecordResult-returning
  // Add/Modify. A RecordResult operation is "successful" only when kAdded.
  template <typename V>
  static bool IsOperationSuccessful(
      const absl::optional<absl::StatusOr<V>> &result) {
    if (!result.has_value() || !result.value().ok()) {
      return false;
    }
    if constexpr (std::is_same_v<V, indexes::RecordResult>) {
      return result.value().value() == indexes::RecordResult::kAdded;
    } else {
      return result.value().value();
    }
  }

  template <typename V>
  static bool IsOperationFailed(
      const absl::optional<absl::StatusOr<V>> &result) {
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
  ValkeyModuleCtx fake_ctx;
  std::vector<absl::string_view> key_prefixes = {"prefix:"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema =
      MockIndexSchema::Create(&fake_ctx, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool)
          .value();
  EXPECT_TRUE(
      KeyspaceEventManager::Instance().HasSubscription(index_schema.get()));
  auto mock_index = std::make_shared<MockIndex>(test_case.index_type, 4);
  VMSDK_EXPECT_OK(index_schema->AddIndex("attribute_name", test_case.hash_field,
                                         mock_index));
  EXPECT_CALL(
      *kMockValkeyModule,
      HashGet(testing::An<ValkeyModuleKey *>(), testing::An<int>(),
              testing::An<const char *>(), testing::An<ValkeyModuleString **>(),
              testing::An<void *>()))
      .WillRepeatedly(TestValkeyModule_HashGetDefaultImpl);

  auto key = StringInternStore::Intern("key");
  auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key->Str());
  EXPECT_CALL(*mock_index, IsTracked(key))
      .WillRepeatedly(Return(test_case.is_tracked));
  if (test_case.expect_index_add_w_result.has_value()) {
    EXPECT_CALL(*mock_index, AddRecord(key, testing::_))
        .WillOnce(Return(test_case.expect_index_add_w_result.value()));
  } else if (test_case.expect_index_modify_w_result.has_value()) {
    EXPECT_CALL(*mock_index, ModifyRecord(key, testing::_))
        .WillOnce(Return(test_case.expect_index_modify_w_result.value()));
  } else if ((test_case.expect_index_remove_w_result.has_value()) &&
             (test_case.expect_index_remove_w_result.value().ok() &&
              test_case.expect_index_remove_w_result.value().value())) {
    EXPECT_CALL(*mock_index,
                RemoveRecord(key, test_case.expected_deletion_type))
        .WillOnce(Return(test_case.expect_index_remove_w_result.value()));
  }

  if (test_case.open_key_fail) {
    // Keep the default behavior still for other keys (e.g. IndexSchema key).
    EXPECT_CALL(*kMockValkeyModule, OpenKey(&fake_ctx, testing::_, testing::_))
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
  } else if (!test_case.open_key_fail && !test_case.expect_wrong_type) {
    EXPECT_CALL(*kMockValkeyModule,
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
      .skipped_cnt = index_schema->GetStats().subscription_remove.skipped_cnt};
  IndexSchema::Stats::ResultCnt<uint64_t> modify_cnt = {
      .failure_cnt = index_schema->GetStats().subscription_modify.failure_cnt,
      .success_cnt = index_schema->GetStats().subscription_modify.success_cnt,
      .skipped_cnt = index_schema->GetStats().subscription_modify.skipped_cnt};

  // Capture initial Time Slice Mutex metrics
  auto &global_stats = Metrics::GetStats();
  uint64_t initial_upserts = global_stats.time_slice_upserts;
  uint64_t initial_deletes = global_stats.time_slice_deletes;

  index_schema->OnKeyspaceNotification(&fake_ctx, VALKEYMODULE_NOTIFY_HASH,
                                       "event", key_valkey_str.get());
  WaitWorkerTasksAreCompleted(mutations_thread_pool);
  for (const auto &tuple :
       {std::make_tuple(add_cnt, &index_schema->GetStats().subscription_add,
                        &test_case.expected_add_cnt_delta),
        std::make_tuple(remove_cnt,
                        &index_schema->GetStats().subscription_remove,
                        &test_case.expected_remove_cnt_delta),
        std::make_tuple(modify_cnt,
                        &index_schema->GetStats().subscription_modify,
                        &test_case.expected_modify_cnt_delta)}) {
    EXPECT_EQ(std::get<1>(tuple)->success_cnt - std::get<0>(tuple).success_cnt,
              std::get<2>(tuple)->success_cnt);
    EXPECT_EQ(std::get<1>(tuple)->skipped_cnt - std::get<0>(tuple).skipped_cnt,
              std::get<2>(tuple)->skipped_cnt);
    if (!test_case.expect_index_remove_w_result.has_value() ||
        !test_case.expect_index_remove_w_result.value().ok() ||
        test_case.expect_index_remove_w_result.value().value()) {
      EXPECT_EQ(
          std::get<1>(tuple)->failure_cnt - std::get<0>(tuple).failure_cnt,
          std::get<2>(tuple)->failure_cnt);
    }
  }

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

INSTANTIATE_TEST_SUITE_P(
    IndexSchemaSubscriptionTests, IndexSchemaSubscriptionTest,
    ValuesIn<IndexSchemaSubscriptionTestCase>({
        {
            .test_name = "happy_path_add",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("vector", "vector_buffer_16"),
            .is_tracked = false,
            .expect_index_add_w_result = indexes::RecordResult::kAdded,
            .expected_vector_buffer = "vector_buffer_16",
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
            .expected_vector_buffer = "vector_buffer_16",
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
            .expected_vector_buffer = "vector_buffer_16",
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .success_cnt = 1,
                },
            .expected_deletion_type = indexes::DeletionType::kIdentifier,
        },
        {
            .test_name = "happy_path_modify",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("vector", "vector_buffer_16"),
            .is_tracked = true,
            .expect_index_modify_w_result = indexes::RecordResult::kAdded,
            .expected_vector_buffer = "vector_buffer_16",
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
            .valkey_hash_data = std::make_pair("vector", "vector_buffer_16"),
            .is_tracked = false,
            .expect_index_add_w_result = absl::InternalError("error"),
            .expected_vector_buffer = "vector_buffer_16",
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
            .valkey_hash_data = std::make_pair("vector", "vector_buffer_16"),
            .is_tracked = true,
            .expect_index_modify_w_result = absl::InternalError("error"),
            .expected_vector_buffer = "vector_buffer_16",
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
            .expect_index_add_w_result = indexes::RecordResult::kAdded,
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
            .expect_index_modify_w_result = indexes::RecordResult::kAdded,
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
            .expect_index_add_w_result = indexes::RecordResult::kAdded,
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
            .expect_index_modify_w_result = indexes::RecordResult::kAdded,
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
            .expected_vector_buffer = "vector_buffer_16",
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
            .valkey_hash_data = std::make_pair("vector", "vector_buffer_16"),
            .is_tracked = false,
            .expect_index_add_w_result = indexes::RecordResult::kMissing,
            .expected_vector_buffer = "vector_buffer_16",
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
            .test_name = "replace_hash_with_json_wrong_type",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_MODULE,
            .expect_wrong_type = true,
            .is_tracked = true,
            .expect_index_remove_w_result = true,
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .success_cnt = 1,
                },
            .expected_deletion_type = indexes::DeletionType::kRecord,
            .expected_document_cnt_delta = -1,
        },
        {
            .test_name = "replace_hash_with_string_wrong_type",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_STRING,
            .expect_wrong_type = true,
            .is_tracked = true,
            .expect_index_remove_w_result = true,
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .success_cnt = 1,
                },
            .expected_deletion_type = indexes::DeletionType::kRecord,
            .expected_document_cnt_delta = -1,
        },
        {
            .test_name = "modify_skipped",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("vector", "vector_buffer_16"),
            .is_tracked = true,
            .expect_index_modify_w_result = indexes::RecordResult::kMissing,
            .expected_vector_buffer = "vector_buffer_16",
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
            .expected_vector_buffer = "vector_buffer_16",
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .skipped_cnt = 0,
                },
        },
    }),
    [](const TestParamInfo<IndexSchemaSubscriptionTestCase> &info) {
      return info.param.test_name;
    });

class IndexSchemaSubscriptionSimpleTest : public ValkeySearchTest {};

TEST_F(IndexSchemaSubscriptionSimpleTest, DropIndexPrematurely) {
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
    auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key->Str());
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

TEST_F(IndexSchemaSubscriptionSimpleTest, EmptyKeyPrefixesTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  std::vector<absl::string_view> key_prefixes = {};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema =
      MockIndexSchema::Create(&fake_ctx_, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool)
          .value();

  EXPECT_THAT(index_schema->GetKeyPrefixes(), UnorderedElementsAreArray({""}));
}

TEST_F(IndexSchemaSubscriptionSimpleTest, DuplicateKeyPrefixesTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();

  std::vector<absl::string_view> key_prefixes = {"pre", "pre"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema =
      MockIndexSchema::Create(&fake_ctx_, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool)
          .value();

  EXPECT_THAT(index_schema->GetKeyPrefixes(),
              UnorderedElementsAreArray({"pre"}));
}

TEST_F(IndexSchemaSubscriptionSimpleTest, PrefixIsPrefixedByAnotherTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  std::vector<absl::string_view> key_prefixes = {"pre", "prefix"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema =
      MockIndexSchema::Create(&fake_ctx_, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool)
          .value();

  EXPECT_THAT(index_schema->GetKeyPrefixes(),
              UnorderedElementsAreArray({"pre"}));
}

TEST_F(IndexSchemaSubscriptionSimpleTest, IndexSchemaInDifferentDBTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  std::vector<absl::string_view> key_prefixes = {};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema =
      MockIndexSchema::Create(&fake_ctx_, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool)
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
  WaitWorkerTasksAreCompleted(mutations_thread_pool);
}

TEST_F(IndexSchemaSubscriptionSimpleTest,
       DBHasMatchingKeyWithWrongModuleTypeTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  std::vector<absl::string_view> key_prefixes = {};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema =
      MockIndexSchema::Create(&fake_ctx_, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool)
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
  WaitWorkerTasksAreCompleted(mutations_thread_pool);
}

TEST_F(IndexSchemaSubscriptionSimpleTest, KeyspaceNotificationWithNullptrTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  std::vector<absl::string_view> key_prefixes = {};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema =
      MockIndexSchema::Create(&fake_ctx_, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool)
          .value();
  auto mock_index = std::make_shared<MockIndex>();
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("attribute_name", "test_identifier", mock_index));
  EXPECT_CALL(*kMockValkeyModule, OpenKey(&fake_ctx_, testing::_, testing::_))
      .Times(0);
  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                       "event", nullptr);
  WaitWorkerTasksAreCompleted(mutations_thread_pool);
}

TEST_F(IndexSchemaSubscriptionSimpleTest,
       ReplaceKeyTypeHashToJsonTriggersDeletion) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  std::vector<absl::string_view> key_prefixes = {"prefix:"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema =
      MockIndexSchema::Create(&fake_ctx_, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool)
          .value();
  auto mock_index = std::make_shared<MockIndex>();
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("attribute_name", "test_identifier", mock_index));

  auto key = StringInternStore::Intern("prefix:key");
  auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key->Str());

  // 1. When the key is tracked and replaced by JSON (MODULE type), it should be
  // removed.
  EXPECT_CALL(*mock_index, IsTracked(key)).WillRepeatedly(Return(true));
  EXPECT_CALL(*mock_index, RemoveRecord(key, indexes::DeletionType::kRecord))
      .WillOnce(Return(true));

  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(TestValkeyModule_KeyTypeDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule,
              KeyType(vmsdk::ValkeyModuleKeyIsForString(key->Str())))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_MODULE));

  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_MODULE,
                                       "event", key_valkey_str.get());
  WaitWorkerTasksAreCompleted(mutations_thread_pool);

  // 2. When the key is NOT tracked and of wrong type, it should early return
  // without removal.
  EXPECT_CALL(*mock_index, IsTracked(key)).WillRepeatedly(Return(false));
  EXPECT_CALL(*mock_index, RemoveRecord(key, testing::_)).Times(0);
  EXPECT_CALL(*mock_index, AddRecord(key, testing::_)).Times(0);

  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_MODULE,
                                       "event", key_valkey_str.get());
  WaitWorkerTasksAreCompleted(mutations_thread_pool);
}

TEST_F(IndexSchemaSubscriptionSimpleTest,
       ReplaceKeyTypeJsonToHashTriggersDeletion) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  std::vector<absl::string_view> key_prefixes = {"prefix:"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema =
      MockIndexSchema::Create(&fake_ctx_, index_schema_name_str, key_prefixes,
                              std::make_unique<JsonAttributeDataType>(),
                              &mutations_thread_pool)
          .value();
  auto mock_index = std::make_shared<MockIndex>();
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("attribute_name", "test_identifier", mock_index));

  auto key = StringInternStore::Intern("prefix:key");
  auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key->Str());

  // 1. When the key is tracked and replaced by HASH, it should be removed.
  EXPECT_CALL(*mock_index, IsTracked(key)).WillRepeatedly(Return(true));
  EXPECT_CALL(*mock_index, RemoveRecord(key, indexes::DeletionType::kRecord))
      .WillOnce(Return(true));

  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(TestValkeyModule_KeyTypeDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule,
              KeyType(vmsdk::ValkeyModuleKeyIsForString(key->Str())))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));

  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                       "event", key_valkey_str.get());
  WaitWorkerTasksAreCompleted(mutations_thread_pool);

  // 2. When the key is NOT tracked and of wrong type, it should early return
  // without removal.
  EXPECT_CALL(*mock_index, IsTracked(key)).WillRepeatedly(Return(false));
  EXPECT_CALL(*mock_index, RemoveRecord(key, testing::_)).Times(0);
  EXPECT_CALL(*mock_index, AddRecord(key, testing::_)).Times(0);

  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                       "event", key_valkey_str.get());
  WaitWorkerTasksAreCompleted(mutations_thread_pool);
}

TEST_F(IndexSchemaSubscriptionSimpleTest, GetKeyPrefixesTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  std::vector<absl::string_view> key_prefixes = {
      "prefix:", "prefix1:", "prefix2:"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema =
      MockIndexSchema::Create(&fake_ctx_, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool)
          .value();

  EXPECT_THAT(index_schema->GetKeyPrefixes(),
              UnorderedElementsAreArray(key_prefixes));
}

TEST_F(IndexSchemaSubscriptionSimpleTest, GetEventTypesTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  std::vector<absl::string_view> key_prefixes = {"unused"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema =
      MockIndexSchema::Create(&fake_ctx_, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool)
          .value();

  EXPECT_EQ(index_schema->GetAttributeDataType().GetValkeyEventTypes(),
            VALKEYMODULE_NOTIFY_HASH | VALKEYMODULE_NOTIFY_GENERIC |
                VALKEYMODULE_NOTIFY_EXPIRED | VALKEYMODULE_NOTIFY_EVICTED);
}

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
    : public ValkeySearchTestWithParam<IndexSchemaBackfillTestCase> {};

TEST_P(IndexSchemaBackfillTest, PerformBackfillTest) {
  const auto &test_case = GetParam();
  MockThreadPool thread_pool("writer-thread-pool-", 5);
  thread_pool.StartWorkers();
  std::vector<absl::string_view> key_prefixes;
  std::ranges::transform(test_case.key_prefixes,
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
  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  auto index_schema =
      MockIndexSchema::Create(&parent_ctx, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &thread_pool)
          .value();
  auto mock_index = std::make_shared<MockIndex>();
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("attribute_name", "test_identifier", mock_index));
  EXPECT_CALL(
      *kMockValkeyModule,
      HashGet(testing::An<ValkeyModuleKey *>(), testing::An<int>(),
              testing::An<const char *>(), testing::An<ValkeyModuleString **>(),
              testing::An<void *>()))
      .WillRepeatedly(TestValkeyModule_HashGetDefaultImpl);

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
              TestValkeyModule_CreateStringPrintf(nullptr, "arbitrary_data16");
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
          auto interned_key = StringInternStore::Intern(key_str);
          EXPECT_CALL(*mock_index, IsTracked(interned_key))
              .WillRepeatedly(testing::Return(false));
          EXPECT_CALL(*mock_index, AddRecord(interned_key, testing::_))
              .WillOnce(testing::Return(indexes::RecordResult::kAdded));
          EXPECT_CALL(thread_pool,
                      Schedule(testing::_, vmsdk::ThreadPool::Priority::kLow))
              .WillOnce([&thread_pool](absl::AnyInvocable<void()> task,
                                       vmsdk::ThreadPool::Priority priority) {
                return thread_pool.ThreadPool::Schedule(std::move(task),
                                                        priority);
              });
          EXPECT_CALL(*kMockValkeyModule,
                      BlockClient(testing::_, testing::_, testing::_,
                                  testing::_, testing::_))
              .Times(0);
          EXPECT_CALL(*kMockValkeyModule, UnblockClient(testing::_, testing::_))
              .Times(0);
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
        EXPECT_CALL(thread_pool,
                    Schedule(testing::_, vmsdk::ThreadPool::Priority::kLow))
            .Times(static_cast<int>(thread_pool.Size()))
            .WillRepeatedly(
                [&thread_pool](absl::AnyInvocable<void()> task,
                               vmsdk::ThreadPool::Priority priority) {
                  return thread_pool.ThreadPool::Schedule(std::move(task),
                                                          priority);
                });
        WaitWorkerTasksAreCompleted(thread_pool);
        return (++i < test_case.keys_to_return_in_scan.size()) ? 1 : 0;
      });
  EXPECT_EQ(
      index_schema->PerformBackfill(&parent_ctx, test_case.scan_batch_size),
      test_case.expected_keys_scanned);
  EXPECT_CALL(thread_pool,
              Schedule(testing::_, vmsdk::ThreadPool::Priority::kLow))
      .Times(static_cast<int>(thread_pool.Size()));
  WaitWorkerTasksAreCompleted(thread_pool);
  EXPECT_EQ(index_schema->GetStats().subscription_add.success_cnt,
            test_case.expected_keys_processed.size());
  EXPECT_EQ(index_schema->GetStats().subscription_add.failure_cnt, 0);
  EXPECT_EQ(index_schema->GetStats().document_cnt,
            test_case.expected_keys_processed.size());
  EXPECT_EQ(index_schema->IsBackfillInProgress(),
            test_case.expected_backfill_percent != 1.0);
  EXPECT_EQ(index_schema->GetBackfillPercent(),
            test_case.expected_backfill_percent);
  EXPECT_EQ(index_schema->GetStateForInfo(), test_case.expected_state);
}

TEST_F(IndexSchemaBackfillTest, PerformBackfill_NoOngoingBackfillTest) {
  std::vector<absl::string_view> key_prefixes = {"unused"};
  std::string index_schema_name_str("index_schema_name");
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  ValkeyModuleCtx parent_ctx;
  ValkeyModuleCtx scan_ctx;
  EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(&parent_ctx))
      .WillRepeatedly(Return(&scan_ctx));
  auto index_schema =
      MockIndexSchema::Create(&parent_ctx, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool)
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

TEST_F(IndexSchemaBackfillTest, PerformBackfill_SwapDB) {
  std::vector<absl::string_view> key_prefixes = {"unused"};
  std::string index_schema_name_str("index_schema_name");
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
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
  auto index_schema =
      MockIndexSchema::Create(&parent_ctx, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool)
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

INSTANTIATE_TEST_SUITE_P(
    IndexSchemaBackfillTests, IndexSchemaBackfillTest,
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
    }),
    [](const TestParamInfo<IndexSchemaBackfillTestCase> &info) {
      return info.param.test_name;
    });

class IndexSchemaRDBTest : public ValkeySearchTest {
 protected:
  // Intercepting the setup to change RDB version to 1
  // Currently these tests only work with RDB version 1
  // TODO: Will be fixed to work with RDB version 2
  void SetUp() override {
    auto &enable_sharing =
        const_cast<vmsdk::config::Boolean &>(options::GetEnableVectorSharing());
    VMSDK_EXPECT_OK(enable_sharing.SetValue(false));
    ValkeySearchTest::SetUp();
    SetDebugMode(true);
    auto &write_v2 =
        const_cast<vmsdk::config::Boolean &>(options::GetRdbWriteV2());
    auto &read_v2 =
        const_cast<vmsdk::config::Boolean &>(options::GetRdbReadV2());
    VMSDK_EXPECT_OK(write_v2.SetValue(false));
    VMSDK_EXPECT_OK(read_v2.SetValue(false));
  }

  void TearDown() override {
    auto &write_v2 =
        const_cast<vmsdk::config::Boolean &>(options::GetRdbWriteV2());
    auto &read_v2 =
        const_cast<vmsdk::config::Boolean &>(options::GetRdbReadV2());
    VMSDK_EXPECT_OK(write_v2.SetValue(true));
    VMSDK_EXPECT_OK(read_v2.SetValue(true));
    SetDebugMode(false);
    auto &enable_sharing =
        const_cast<vmsdk::config::Boolean &>(options::GetEnableVectorSharing());
    VMSDK_EXPECT_OK(enable_sharing.SetValue(true));
    ValkeySearchTest::TearDown();
  }
};

TEST_F(IndexSchemaRDBTest, SingleSlotNumberComputedOnCreate) {
  std::vector<absl::string_view> key_prefixes = {"doc:{slot1}:"};
  std::string single_slot_index_name("index_schema_name{slot1}");
  auto single_slot_schema =
      MockIndexSchema::Create(&fake_ctx_, single_slot_index_name, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              nullptr)
          .value();

  ASSERT_TRUE(single_slot_schema->GetSingleSlotNumber().has_value());

  std::string multi_slot_index_name("index_schema_name");
  auto multi_slot_schema =
      MockIndexSchema::Create(&fake_ctx_, multi_slot_index_name, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              nullptr)
          .value();
  EXPECT_FALSE(multi_slot_schema->GetSingleSlotNumber().has_value());
}

TEST_F(IndexSchemaRDBTest, SaveAndLoadSingleSlotNumber) {
  std::vector<absl::string_view> key_prefixes = {"doc:{slot2}:"};
  std::string index_schema_name_str("index_schema_name{slot2}");

  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(), nullptr)
                          .value();
  ASSERT_TRUE(index_schema->GetSingleSlotNumber().has_value());
  auto slot_number = index_schema->GetSingleSlotNumber();
  auto proto = index_schema->ToProto();

  ValkeyModuleCtx load_ctx;
  EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(&load_ctx))
      .WillRepeatedly(Return(&load_ctx));
  auto loaded_schema_or =
      IndexSchema::Create(&load_ctx, *proto, /*mutations_thread_pool=*/nullptr,
                          /*skip_attributes=*/false, /*reload=*/true);
  VMSDK_EXPECT_OK_STATUSOR(loaded_schema_or);
  auto loaded_schema = loaded_schema_or.value();

  ASSERT_TRUE(loaded_schema->GetSingleSlotNumber().has_value());
  EXPECT_EQ(*loaded_schema->GetSingleSlotNumber(), *slot_number);
}

TEST_F(IndexSchemaRDBTest, SaveAndLoad) ABSL_NO_THREAD_SAFETY_ANALYSIS {
  std::vector<absl::string_view> key_prefixes = {"prefix1", "prefix2"};
  std::string index_schema_name_str("index_schema_name");
  const int dimensions = 100;
  auto distance_metric = data_model::DISTANCE_METRIC_COSINE;
  int initial_cap = 12;
  int m = 16;
  int ef_construction = 100;
  int ef_runtime = 5;
  int block_size = 250;
  const int num_vectors = 10;

  FakeSafeRDB rdb_stream;
  auto vectors = DeterministicallyGenerateVectors(num_vectors, dimensions, 2);
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
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0)
            .value();
    VMSDK_EXPECT_OK(index_schema->AddIndex("hnsw_attribute", "hnsw_identifier",
                                           hnsw_index));
    auto itr = index_schema->attributes_.find("hnsw_attribute");

    EXPECT_FALSE(itr == index_schema->attributes_.end());
    for (size_t i = 0; i < vectors.size(); ++i) {
      auto interned_key = StringInternStore::Intern("key" + std::to_string(i));
      auto data = testing_infra::MakeAttributeData(
          *hnsw_index, interned_key,
          absl::string_view((char *)&vectors[i][0],
                            dimensions * sizeof(float)));
      index_schema->ProcessAttributeMutation(&fake_ctx_, itr->second,
                                             interned_key, std::move(data));
    }

    auto flat_index =
        indexes::VectorFlat<float>::Create(
            CreateFlatVectorIndexProto(dimensions, distance_metric, initial_cap,
                                       block_size),
            "flat_identifier",
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0)
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
  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  std::vector<ValkeyModuleString *> records(num_vectors);
  for (size_t i = 0; i < vectors.size(); ++i) {
    records[i] = new ValkeyModuleString{
        .data = std::string(reinterpret_cast<char *>(vectors[i].data()),
                            dimensions * sizeof(float)),
    };
  }
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_,
                      testing::An<ValkeyModuleString **>(),
                      testing::TypedEq<void *>(nullptr)))
      .WillRepeatedly([records](ValkeyModuleKey *key, int, const char *,
                                ValkeyModuleString **value_out, void *) {
        absl::string_view key_str = key->key;
        CHECK(absl::ConsumePrefix(&key_str, "key"));
        int index;
        CHECK(absl::SimpleAtoi(key_str, &index));
        *value_out = records[index];
        ValkeyModule_RetainString(nullptr, records[index]);
        return VALKEYMODULE_OK;
      });
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
  EXPECT_EQ(index_schema->CountRecords(), 10);
  for (size_t i = 0; i < vectors.size(); ++i) {
    delete records[i];
  }
}

TEST_F(IndexSchemaRDBTest, SaveAndLoadWithVectorSharing)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  ValkeyModuleCtx parent_ctx;
  ValkeyModuleCtx scan_ctx;
  EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
      .WillRepeatedly(Return(&scan_ctx));

  auto &enable_sharing =
      const_cast<vmsdk::config::Boolean &>(options::GetEnableVectorSharing());
  VMSDK_EXPECT_OK(enable_sharing.SetValue(true));
  VectorRegistry::Construct(&fake_ctx_);

  std::vector<absl::string_view> key_prefixes = {"prefix1:"};
  std::string index_schema_name_str("index_schema_sharing");
  const int dimensions = 4;
  auto distance_metric = data_model::DISTANCE_METRIC_L2;
  int initial_cap = 10;
  int m = 16;
  int ef_construction = 100;
  int ef_runtime = 5;
  const int num_vectors = 5;

  FakeSafeRDB rdb_stream;
  auto vectors = DeterministicallyGenerateVectors(num_vectors, dimensions, 2);
  {
    auto index_schema = MockIndexSchema::Create(
                            &fake_ctx_, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(), nullptr)
                            .value();

    auto hnsw_index =
        indexes::VectorHNSW<float>::Create(
            CreateHNSWVectorIndexProto(dimensions, distance_metric, initial_cap,
                                       m, ef_construction, ef_runtime),
            "vec", data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0)
            .value();
    VMSDK_EXPECT_OK(index_schema->AddIndex("vec", "vec", hnsw_index));
    auto itr = index_schema->attributes_.find("vec");
    ASSERT_FALSE(itr == index_schema->attributes_.end());

    for (size_t i = 0; i < vectors.size(); ++i) {
      auto interned_key =
          StringInternStore::Intern("prefix1:key" + std::to_string(i));
      auto data = testing_infra::MakeAttributeData(
          *hnsw_index, interned_key,
          absl::string_view((char *)&vectors[i][0],
                            dimensions * sizeof(float)));
      index_schema->ProcessAttributeMutation(&fake_ctx_, itr->second,
                                             interned_key, std::move(data));
    }

    VMSDK_EXPECT_OK(index_schema->RDBSave(&rdb_stream));
  }

  // Load the saved index schema and validate with vector sharing active
  RDBSectionIter iter(&rdb_stream, 1);
  auto section = iter.Next();
  VMSDK_EXPECT_OK_STATUSOR(section);
  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);

  std::vector<ValkeyModuleString *> records(num_vectors);
  for (size_t i = 0; i < vectors.size(); ++i) {
    records[i] = new ValkeyModuleString{
        .data = std::string(reinterpret_cast<char *>(vectors[i].data()),
                            dimensions * sizeof(float)),
    };
  }

  EXPECT_CALL(*kMockValkeyModule,
              HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_,
                      testing::An<ValkeyModuleString **>(),
                      testing::TypedEq<void *>(nullptr)))
      .WillRepeatedly([records](ValkeyModuleKey *key, int, const char *,
                                ValkeyModuleString **value_out, void *) {
        absl::string_view key_str = key->key;
        CHECK(absl::ConsumePrefix(&key_str, "prefix1:key"));
        int index;
        CHECK(absl::SimpleAtoi(key_str, &index));
        *value_out = records[index];
        ValkeyModule_RetainString(nullptr, records[index]);
        return VALKEYMODULE_OK;
      });

  auto index_schema_or =
      IndexSchema::LoadFromRDB(&parent_ctx,
                               /*mutations_thread_pool=*/nullptr,
                               std::make_unique<data_model::IndexSchema>(
                                   (*section)->index_schema_contents()),
                               iter.IterateSupplementalContent());
  VMSDK_EXPECT_OK_STATUSOR(index_schema_or);
  auto index_schema = std::move(index_schema_or.value());

  auto hnsw_index = dynamic_cast<indexes::VectorHNSW<float> *>(
      index_schema->GetIndex("vec").value().get());
  ASSERT_TRUE(hnsw_index != nullptr);
  EXPECT_EQ(index_schema->CountRecords(), num_vectors);
  EXPECT_EQ(VectorRegistry::Instance().GetStats().entry_cnt, num_vectors);

  for (size_t i = 0; i < vectors.size(); ++i) {
    delete records[i];
  }
  // Reset sharing setting
  VMSDK_EXPECT_OK(enable_sharing.SetValue(false));
  VectorRegistry::Construct(&fake_ctx_);
}

TEST_F(IndexSchemaRDBTest, SaveAndLoadTextIndex)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  std::vector<absl::string_view> key_prefixes = {"doc:"};
  std::string index_schema_name_str("text_index_schema");
  bool with_suffix_trie = false;
  bool no_stem = false;
  uint32_t min_stem_size = 4;  // MockIndexSchema::Create uses default value
  const int num_vectors = 10;
  FakeSafeRDB rdb_stream;
  const int dimensions = 100;
  auto vectors = DeterministicallyGenerateVectors(num_vectors, dimensions, 2);

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
        indexes::text::LanguageRegistry::Instance().Get(language), punctuation,
        stop_words, with_offsets, min_stem_size);
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
          text_index->AddRecord(interned_key, AttributeData(std::move(data)));
      VMSDK_EXPECT_OK(result);
      EXPECT_EQ(result.value(), indexes::RecordResult::kAdded);
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
    EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
        .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
    std::vector<ValkeyModuleString *> records(num_vectors);
    for (size_t i = 0; i < vectors.size(); ++i) {
      records[i] = new ValkeyModuleString{
          std::string((char *)&vectors[i][0], dimensions * sizeof(float))};
    }
    EXPECT_CALL(*kMockValkeyModule,
                HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_,
                        testing::An<ValkeyModuleString **>(),
                        testing::TypedEq<void *>(nullptr)))
        .WillRepeatedly([records](ValkeyModuleKey *key, int, const char *,
                                  ValkeyModuleString **value_out, void *) {
          absl::string_view key_str = key->key;
          CHECK(absl::ConsumePrefix(&key_str, "key"));
          int index;
          CHECK(absl::SimpleAtoi(key_str, &index));
          *value_out = records[index];
          ValkeyModule_RetainString(nullptr, records[index]);
          return VALKEYMODULE_OK;
        });
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
    for (size_t i = 0; i < vectors.size(); ++i) {
      delete records[i];
    }
  }
}

TEST_F(IndexSchemaRDBTest, LoadEndedDeletesOrphanedKeys) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
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

  auto index_schema =
      MockIndexSchema::Create(&fake_ctx_, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool)
          .value();

  VMSDK_EXPECT_OK(
      index_schema->AddIndex("attribute", "identifier", mock_index));
  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillRepeatedly(Return(1));
  EXPECT_CALL(*kMockValkeyModule, SelectDb(testing::_, testing::_))
      .WillRepeatedly(Return(1));  // So backfill job can be created.
  EXPECT_CALL(*kMockValkeyModule, SelectDb(&fake_ctx_, 0)).WillOnce(Return(1));
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
  WaitWorkerTasksAreCompleted(mutations_thread_pool);
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
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0)
            .value();
    VMSDK_EXPECT_OK(index_schema->AddIndex(attribute_identifier,
                                           "hnsw_identifier", hnsw_index));
    VMSDK_EXPECT_OK(SchemaManager::Instance().ImportIndexSchema(index_schema));
  }
  void TearDown() override {
    mutations_thread_pool.JoinWorkers();
    index_schema.reset();
    hnsw_index.reset();
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
  int block_size = 1024;
  ValkeyModuleCtx fake_ctx;
  vmsdk::ThreadPool mutations_thread_pool{"writer-thread-pool-", 10};
  std::shared_ptr<IndexSchema> index_schema;
  std::shared_ptr<indexes::VectorHNSW<float>> hnsw_index;
  const std::string attribute_identifier{"hnsw_id"};
  InternedStringPtr key = StringInternStore::Intern("my_key_");

  void MutateKey(const std::vector<float> &vector) {
    IndexSchema::MutatedAttributes mutated;
    mutated[attribute_identifier] = testing_infra::MakeAttributeData(
        *hnsw_index, key,
        absl::string_view(reinterpret_cast<const char *>(vector.data()),
                          dimensions * sizeof(float)));
    index_schema->ProcessMutation(&fake_ctx, mutated, key, false, false);
  }
  // Indexes `vector` under `key` and returns it as a neighbor of that
  // mutation, so a further MutateKey leaves the neighbor contended.
  std::vector<indexes::Neighbor> IndexedNeighbor(
      const std::vector<float> &vector) {
    MutateKey(vector);
    WaitWorkerTasksAreCompleted(mutations_thread_pool);
    std::vector<indexes::Neighbor> neighbors(1);
    neighbors[0].external_id = key;
    neighbors[0].sequence_number =
        index_schema->GetDbMutationSequenceNumber(key);
    return neighbors;
  }
  void ExpectWriterQueueDrained() {
    WaitWorkerTasksAreCompleted(mutations_thread_pool);
    EXPECT_EQ(mutations_thread_pool.QueueSize(), 0);
    EXPECT_EQ(index_schema->GetStats().GetStats().mutation_queue_size, 0u);
  }

  void VerifyVectorIndexConsistency(
      const std::shared_ptr<indexes::VectorBase> &vector_index,
      const std::string &attr_id);
};

IndexSchema::MutatedAttributes CreateMutatedAttributes(
    const std::string &attribute_identifier, absl::string_view data_ptr,
    std::optional<InternedStringPtr> k = std::nullopt,
    indexes::VectorBase *vector_index = nullptr) {
  IndexSchema::MutatedAttributes mutated_attributes;
  if (vector_index && !data_ptr.empty()) {
    auto interned_k = k.has_value() ? *k : StringInternStore::Intern("my_key_");
    mutated_attributes[attribute_identifier] =
        testing_infra::MakeAttributeData(*vector_index, interned_k, data_ptr);
  } else if (!data_ptr.empty()) {
    mutated_attributes[attribute_identifier] =
        AttributeData(vmsdk::MakeUniqueValkeyString(data_ptr));
  } else {
    mutated_attributes[attribute_identifier] =
        AttributeData(indexes::DeletionType::kRecord);
  }
  return mutated_attributes;
}

TEST_F(IndexSchemaFriendTest, WeightedBuffer) {
  // The IndexSchemaFriendTest fixture already has an HNSW index with alias
  // "hnsw_id". Add additional index types for testing.
  auto numeric_index =
      std::make_shared<indexes::Numeric>(CreateNumericIndexProto());
  VMSDK_EXPECT_OK(index_schema->AddIndex("numeric_id", "numeric_identifier",
                                         numeric_index));

  auto tag_index =
      std::make_shared<indexes::Tag>(CreateTagIndexProto(",", false));
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("tag_id", "tag_identifier", tag_index));

  index_schema->CreateTextIndexSchema();
  auto text_index =
      std::make_shared<indexes::Text>(CreateTextIndexProto(true, false, 1.0),
                                      index_schema->GetTextIndexSchema());
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("text_id", "text_identifier", text_index));

  // Default weights: vector=130, text=550, numeric=430, tag=330
  // Test 1: New entry with vector data (HNSW index)
  {
    std::string vector_data(400, 'x');  // 400 bytes
    auto key1 = StringInternStore::Intern("weighted_key_1");
    auto mutated_attrs = CreateMutatedAttributes(
        attribute_identifier, vector_data, key1, hnsw_index.get());
    EXPECT_TRUE(index_schema->TrackMutatedRecord(
        nullptr, key1, std::move(mutated_attrs), 0, false, false, false));
    absl::MutexLock lock(&index_schema->mutated_records_mutex_);
    auto itr = index_schema->tracked_mutated_records_.find(key1);
    ASSERT_NE(itr, index_schema->tracked_mutated_records_.end());
    // 400 * 130 / 100 = 520
    EXPECT_EQ(itr->second.weighted_buffer.size(), 520);
  }

  // Test 2: New entry with text data
  {
    std::string text_data(200, 'a');  // 200 bytes
    auto mutated_attrs = CreateMutatedAttributes("text_id", text_data);
    auto key2 = StringInternStore::Intern("weighted_key_2");
    EXPECT_TRUE(index_schema->TrackMutatedRecord(
        nullptr, key2, std::move(mutated_attrs), 0, false, false, false));
    absl::MutexLock lock(&index_schema->mutated_records_mutex_);
    auto itr = index_schema->tracked_mutated_records_.find(key2);
    ASSERT_NE(itr, index_schema->tracked_mutated_records_.end());
    // 200 * 550 / 100 = 1100
    EXPECT_EQ(itr->second.weighted_buffer.size(), 1100);
  }

  // Test 3: New entry with numeric data
  {
    std::string numeric_data(8, 'n');  // 8 bytes
    auto mutated_attrs = CreateMutatedAttributes("numeric_id", numeric_data);
    auto key3 = StringInternStore::Intern("weighted_key_3");
    EXPECT_TRUE(index_schema->TrackMutatedRecord(
        nullptr, key3, std::move(mutated_attrs), 0, false, false, false));
    absl::MutexLock lock(&index_schema->mutated_records_mutex_);
    auto itr = index_schema->tracked_mutated_records_.find(key3);
    ASSERT_NE(itr, index_schema->tracked_mutated_records_.end());
    // 8 * 430 / 100 = 34
    EXPECT_EQ(itr->second.weighted_buffer.size(), 34);
  }

  // Test 4: New entry with tag data
  {
    std::string tag_data(100, 't');  // 100 bytes
    auto mutated_attrs = CreateMutatedAttributes("tag_id", tag_data);
    auto key4 = StringInternStore::Intern("weighted_key_4");
    EXPECT_TRUE(index_schema->TrackMutatedRecord(
        nullptr, key4, std::move(mutated_attrs), 0, false, false, false));
    absl::MutexLock lock(&index_schema->mutated_records_mutex_);
    auto itr = index_schema->tracked_mutated_records_.find(key4);
    ASSERT_NE(itr, index_schema->tracked_mutated_records_.end());
    // 100 * 330 / 100 = 330
    EXPECT_EQ(itr->second.weighted_buffer.size(), 330);
  }

  // Test 5: Null data contributes 0 to size
  {
    IndexSchema::MutatedAttributes mutated_attrs;
    mutated_attrs[attribute_identifier] =
        AttributeData(indexes::DeletionType::kRecord);
    auto key5 = StringInternStore::Intern("weighted_key_5");
    EXPECT_TRUE(index_schema->TrackMutatedRecord(
        nullptr, key5, std::move(mutated_attrs), 0, false, false, false));
    absl::MutexLock lock(&index_schema->mutated_records_mutex_);
    auto itr = index_schema->tracked_mutated_records_.find(key5);
    ASSERT_NE(itr, index_schema->tracked_mutated_records_.end());
    EXPECT_EQ(itr->second.weighted_buffer.size(), 0);
  }

  // Test 6: Buffer resize on attribute merge (update path)
  {
    std::string initial_data(200, 'a');  // 200 bytes
    auto mutated_attrs = CreateMutatedAttributes("text_id", initial_data);
    auto key6 = StringInternStore::Intern("weighted_key_6");
    EXPECT_TRUE(index_schema->TrackMutatedRecord(
        nullptr, key6, std::move(mutated_attrs), 0, false, false, false));
    {
      absl::MutexLock lock(&index_schema->mutated_records_mutex_);
      auto itr = index_schema->tracked_mutated_records_.find(key6);
      ASSERT_NE(itr, index_schema->tracked_mutated_records_.end());
      // 200 * 550 / 100 = 1100
      EXPECT_EQ(itr->second.weighted_buffer.size(), 1100);
    }

    // Update with larger data — buffer should resize
    std::string larger_data(500, 'b');  // 500 bytes
    auto mutated_attrs2 = CreateMutatedAttributes("text_id", larger_data);
    EXPECT_FALSE(index_schema->TrackMutatedRecord(
        nullptr, key6, std::move(mutated_attrs2), 0, false, false, false));
    {
      absl::MutexLock lock(&index_schema->mutated_records_mutex_);
      auto itr = index_schema->tracked_mutated_records_.find(key6);
      ASSERT_NE(itr, index_schema->tracked_mutated_records_.end());
      // 500 * 550 / 100 = 2750
      EXPECT_EQ(itr->second.weighted_buffer.size(), 2750);
    }
  }

  // Test 7: Different weight config values
  {
    SetDebugMode(true);
    // Restore defaults even if an ASSERT below returns early.
    absl::Cleanup restore = [] {
      VMSDK_EXPECT_OK(options::GetMutationWeightVector().SetValue(130));
      SetDebugMode(false);
    };
    VMSDK_EXPECT_OK(options::GetMutationWeightVector().SetValue(200));
    std::string data(400, 'v');  // 400 bytes
    auto key7 = StringInternStore::Intern("weighted_key_7");
    auto mutated_attrs = CreateMutatedAttributes(attribute_identifier, data,
                                                 key7, hnsw_index.get());
    EXPECT_TRUE(index_schema->TrackMutatedRecord(
        nullptr, key7, std::move(mutated_attrs), 0, false, false, false));
    {
      absl::MutexLock lock(&index_schema->mutated_records_mutex_);
      auto itr = index_schema->tracked_mutated_records_.find(key7);
      ASSERT_NE(itr, index_schema->tracked_mutated_records_.end());
      // 400 * 200 / 100 = 800
      EXPECT_EQ(itr->second.weighted_buffer.size(), 800);
    }
  }
}

TEST_F(IndexSchemaFriendTest, MutatedAttributesSanity) {
  vmsdk::WriterMutexLock lock(&index_schema->GetTimeSlicedMutex());
  auto vectors = DeterministicallyGenerateVectors(1, dimensions, 2);
  absl::string_view data_ptr((char *)&vectors[0][0],
                             dimensions * sizeof(float));
  EXPECT_EQ(index_schema->attributes_.size(), 1);
  auto mutated_attributes_1 = CreateMutatedAttributes(
      attribute_identifier, data_ptr, key, hnsw_index.get());
  EXPECT_TRUE(index_schema->TrackMutatedRecord(
      nullptr, key, std::move(mutated_attributes_1), 0, true, false, false));
  // Verify that adding a track attribute with backfill off after on return true
  auto mutated_attributes_2 = CreateMutatedAttributes(
      attribute_identifier, data_ptr, key, hnsw_index.get());
  EXPECT_TRUE(index_schema->TrackMutatedRecord(
      nullptr, key, std::move(mutated_attributes_2), 0, false, false, false));
  auto mutated_attributes_3 = CreateMutatedAttributes(
      attribute_identifier, data_ptr, key, hnsw_index.get());
  EXPECT_FALSE(index_schema->TrackMutatedRecord(
      nullptr, key, std::move(mutated_attributes_3), 0, false, false, false));
  EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 1);
  auto consumed_data = index_schema->ConsumeTrackedMutatedAttribute(key, true);
  EXPECT_TRUE(consumed_data.has_value());
  auto mutated_attributes_4 = CreateMutatedAttributes(
      attribute_identifier, data_ptr, key, hnsw_index.get());
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

// Regression test for a pre-existing crash in InTrackedMutationRecords.
//
// DocumentMutation::attributes is std::optional<flat_hash_map<...>>.
// ConsumeTrackedMutatedAttribute, after extracting the attribute map, sets
// `attributes = std::nullopt` but leaves the entry in tracked_mutated_records_
// (it only erases the entry when there was nothing to consume). A subsequent
// call to InTrackedMutationRecords(key, identifier) on that same key would
// then dereference the disengaged optional via `attributes->find(...)`, which
// is UB. Under ASAN the bad dereference trips absl's SwissTable iterator
// generation guard and crashes the server (observed in CI as a SIGSEGV from
// the backfill scan path during a fulltext test).
//
// Repro: track a mutation, consume it (disengaging `attributes` while leaving
// the entry in the map), then call InTrackedMutationRecords. With the fix it
// returns false cleanly; without the fix it crashes under ASAN.
TEST_F(IndexSchemaFriendTest, InTrackedMutationRecordsAfterConsumeNoCrash) {
  vmsdk::WriterMutexLock lock(&index_schema->GetTimeSlicedMutex());
  auto vectors = DeterministicallyGenerateVectors(1, dimensions, 2);
  absl::string_view data_ptr((char *)&vectors[0][0],
                             dimensions * sizeof(float));
  auto mutated_attributes = CreateMutatedAttributes(
      attribute_identifier, data_ptr, key, hnsw_index.get());
  EXPECT_TRUE(index_schema->TrackMutatedRecord(
      nullptr, key, std::move(mutated_attributes), 0, false, false, false));
  EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 1u);

  // Consume the mutation. The entry stays in the map with attributes empty.
  auto consumed = index_schema->ConsumeTrackedMutatedAttribute(key, true);
  EXPECT_TRUE(consumed.has_value());
  EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 1u);

  // Verify our reading of the state: entry present, attributes disengaged.
  {
    absl::MutexLock lock(&index_schema->mutated_records_mutex_);
    auto itr = index_schema->tracked_mutated_records_.find(key);
    ASSERT_NE(itr, index_schema->tracked_mutated_records_.end());
    EXPECT_FALSE(itr->second.attributes.has_value());
  }

  // The crash: without the fix this dereferences a disengaged optional.
  // Expected behavior with the fix: returns false (no pending mutation for
  // this (key, identifier) pair after the consume drained it).
  EXPECT_FALSE(
      index_schema->InTrackedMutationRecords(key, attribute_identifier));
}

TEST_F(IndexSchemaFriendTest, MutatedAttributes)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto tester = [this](absl::string_view data_ptr,
                       absl::string_view track_before_consumption_data_ptr,
                       absl::string_view track_after_consumption_data_ptr) {
    VLOG(1) << "Starting test";
    vmsdk::WriterMutexLock lock(&index_schema->GetTimeSlicedMutex());
    {
      auto mutated_attributes = CreateMutatedAttributes(
          attribute_identifier, data_ptr, key, hnsw_index.get());
      EXPECT_EQ(index_schema->attributes_.size(), 1);
      EXPECT_TRUE(index_schema->TrackMutatedRecord(
          nullptr, key, std::move(mutated_attributes), 0, false, false, false));
    }
    if (!track_before_consumption_data_ptr.empty()) {
      VLOG(1) << "track_before_consumption_data_ptr is not empty";
      EXPECT_EQ(index_schema->attributes_.size(), 1);
      auto mutated_attributes = CreateMutatedAttributes(
          attribute_identifier, track_before_consumption_data_ptr, key,
          hnsw_index.get());
      EXPECT_FALSE(index_schema->TrackMutatedRecord(
          nullptr, key, std::move(mutated_attributes), 0, false, false, false));
      data_ptr = track_before_consumption_data_ptr;
    }
    EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 1);
    auto consumed_data =
        index_schema->ConsumeTrackedMutatedAttribute(key, true);
    ASSERT_TRUE(consumed_data.has_value());
    ASSERT_FALSE(consumed_data->empty());
    indexes::VectorRecord consumed_vector;
    absl::string_view data_view;
    if (!data_ptr.empty()) {
      EXPECT_TRUE(consumed_data->begin()->second.IsVector());
      consumed_vector = consumed_data->begin()->second.ConsumeVector();
      ASSERT_NE(consumed_vector, nullptr);
      data_view = absl::string_view(consumed_vector.GetRawVector(),
                                    dimensions * sizeof(float));
    } else {
      EXPECT_TRUE(consumed_data->begin()->second.IsNull());
    }
    VLOG(1) << "consumed_data size: " << consumed_data->size();
    EXPECT_EQ(data_view, data_ptr);
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
            attribute_identifier, track_after_consumption_data_ptr, key,
            hnsw_index.get());
        EXPECT_EQ(index_schema->TrackMutatedRecord(
                      nullptr, key, std::move(mutated_attributes), 0, false,
                      false, false),
                  !track_before_consumption_data_ptr.empty());
      }
      auto consumed_data =
          index_schema->ConsumeTrackedMutatedAttribute(key, false);
      ASSERT_TRUE(consumed_data.has_value());
      ASSERT_FALSE(consumed_data->empty());
      EXPECT_TRUE(consumed_data->begin()->second.IsVector());
      auto consumed_vector2 = consumed_data->begin()->second.ConsumeVector();
      ASSERT_NE(consumed_vector2, nullptr);
      absl::string_view data_view2(consumed_vector2.GetRawVector(),
                                   dimensions * sizeof(float));
      EXPECT_EQ(data_view2, track_after_consumption_data_ptr);
    }
    consumed_data = index_schema->ConsumeTrackedMutatedAttribute(key, false);
    EXPECT_FALSE(consumed_data.has_value());
    EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 0);
    WaitWorkerTasksAreCompleted(mutations_thread_pool);
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

// Exercises the data-type-agnostic machinery that orchestrates dropping a key
// when any indexed field contains invalid data, and verifies both the new
// (Redisearch-compatible) and the legacy behavior based on
// search.emulate-release. See COMPATIBILITY.md.
TEST_F(IndexSchemaFriendTest, InvalidDataDropsKey)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  // The fixture already has an HNSW index "hnsw_id"; add a numeric and a tag
  // index so a single key can carry an invalid numeric field alongside a valid
  // tag field.
  auto numeric_index =
      std::make_shared<indexes::Numeric>(CreateNumericIndexProto());
  VMSDK_EXPECT_OK(index_schema->AddIndex("numeric_id", "numeric_identifier",
                                         numeric_index));
  auto tag_index =
      std::make_shared<indexes::Tag>(CreateTagIndexProto(",", false));
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("tag_id", "tag_identifier", tag_index));

  // Builds a fresh mutation carrying an invalid numeric value ("not_a_number")
  // and a valid tag value ("electronics"), keyed by attribute alias.
  auto make_mixed_mutation = []() {
    IndexSchema::MutatedAttributes mutated_attributes;
    mutated_attributes["numeric_id"] =
        AttributeData(vmsdk::MakeUniqueValkeyString("not_a_number"));
    mutated_attributes["tag_id"] =
        AttributeData(vmsdk::MakeUniqueValkeyString("electronics"));
    return mutated_attributes;
  };

  const auto saved_emulate_release = options::GetEmulateRelease().GetValue();

  // --- Legacy behavior: emulate-release below the fix (1.3.0). The invalid
  // numeric field is treated as missing, but the rest of the key (the valid tag
  // field) is retained. This confirms the current/incompatible path still
  // works. ---
  VMSDK_EXPECT_OK(
      options::GetEmulateRelease().SetValue(vmsdk::ValkeyVersion(1, 0, 0)));
  auto legacy_key = StringInternStore::Intern("legacy_key");
  {
    auto mutated = make_mixed_mutation();
    index_schema->SyncProcessMutation(&fake_ctx, mutated, legacy_key);
  }
  EXPECT_FALSE(numeric_index->IsTracked(legacy_key));  // invalid -> missing
  EXPECT_TRUE(tag_index->IsTracked(legacy_key));       // key retained

  // --- Compatible behavior: emulate-release at/above the fix (1.3.0). The
  // entire key is dropped from every index, including the valid tag field. ---
  VMSDK_EXPECT_OK(
      options::GetEmulateRelease().SetValue(vmsdk::ValkeyVersion(1, 3, 0)));
  auto compat_key = StringInternStore::Intern("compat_key");
  {
    auto mutated = make_mixed_mutation();
    index_schema->SyncProcessMutation(&fake_ctx, mutated, compat_key);
  }
  EXPECT_FALSE(numeric_index->IsTracked(compat_key));
  EXPECT_FALSE(tag_index->IsTracked(compat_key));  // dropped despite valid tag

  // --- Sanity: with the compatible behavior enabled, a key whose fields are
  // all valid is indexed normally (no spurious drop). ---
  auto good_key = StringInternStore::Intern("good_key");
  {
    IndexSchema::MutatedAttributes mutated;
    mutated["numeric_id"] = AttributeData(vmsdk::MakeUniqueValkeyString("123"));
    mutated["tag_id"] =
        AttributeData(vmsdk::MakeUniqueValkeyString("electronics"));
    index_schema->SyncProcessMutation(&fake_ctx, mutated, good_key);
  }
  EXPECT_TRUE(numeric_index->IsTracked(good_key));
  EXPECT_TRUE(tag_index->IsTracked(good_key));

  VMSDK_EXPECT_OK(options::GetEmulateRelease().SetValue(saved_emulate_release));
}

TEST_F(IndexSchemaFriendTest, WaitForContendedKeysWhileSuspended)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto vectors = DeterministicallyGenerateVectors(2, dimensions, 2);
  auto neighbors = IndexedNeighbor(vectors[0]);

  VMSDK_EXPECT_OK(mutations_thread_pool.SuspendWorkers());
  MutateKey(vectors[1]);
  EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 1u);

  // Expired deadline: nothing is applied, the record stays for the workers.
  EXPECT_FALSE(
      index_schema->WaitForContendedKeys(neighbors, absl::InfinitePast()));
  EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 1u);

  EXPECT_TRUE(index_schema->WaitForContendedKeys(
      neighbors, absl::Now() + absl::Seconds(10)));
  EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 0u);

  VMSDK_EXPECT_OK(mutations_thread_pool.ResumeWorkers());
  ExpectWriterQueueDrained();
}

TEST_F(IndexSchemaFriendTest, WaitForContendedKeysUntilApplied)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto vectors = DeterministicallyGenerateVectors(2, dimensions, 2);
  auto neighbors = IndexedNeighbor(vectors[0]);

  // The writer cannot apply while a reader slice is held; it is let through
  // only once this thread has entered the await path (blocked count bumped).
  // Both waits are bounded so a regression fails instead of hanging.
  const auto blocked_before = Metrics::GetStats().text_query_blocked_cnt.load();
  const absl::Time give_up = absl::Now() + absl::Seconds(10);
  absl::Notification reader_held;
  bool observed_blocked = false;
  std::thread release([&] {
    vmsdk::ReaderMutexLock lock(&index_schema->GetTimeSlicedMutex());
    reader_held.Notify();
    while (Metrics::GetStats().text_query_blocked_cnt.load() ==
               blocked_before &&
           absl::Now() < give_up) {
      std::this_thread::yield();
    }
    observed_blocked =
        Metrics::GetStats().text_query_blocked_cnt.load() != blocked_before;
  });
  reader_held.WaitForNotification();
  MutateKey(vectors[1]);
  EXPECT_TRUE(index_schema->WaitForContendedKeys(neighbors, give_up));
  release.join();
  EXPECT_TRUE(observed_blocked);
  EXPECT_EQ(Metrics::GetStats().text_query_blocked_cnt.load(),
            blocked_before + 1);
  EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 0u);
  // The kMax re-queue and the original task both ran; accounting balances.
  ExpectWriterQueueDrained();
}

TEST_F(IndexSchemaFriendTest, WaitForContendedKeysDeadlineWhileBlocked)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto vectors = DeterministicallyGenerateVectors(2, dimensions, 2);
  auto neighbors = IndexedNeighbor(vectors[0]);

  // The reader slice is held until after the wait has given up, so the
  // deadline expires inside the await.
  const auto blocked_before = Metrics::GetStats().text_query_blocked_cnt.load();
  absl::Notification reader_held, wait_returned;
  std::thread release([&] {
    vmsdk::ReaderMutexLock lock(&index_schema->GetTimeSlicedMutex());
    reader_held.Notify();
    wait_returned.WaitForNotification();
  });
  reader_held.WaitForNotification();
  MutateKey(vectors[1]);
  EXPECT_FALSE(index_schema->WaitForContendedKeys(
      neighbors, absl::Now() + absl::Milliseconds(50)));
  EXPECT_EQ(Metrics::GetStats().text_query_blocked_cnt.load(),
            blocked_before + 1);
  // The record stays for the workers, which apply it once let through.
  EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 1u);
  wait_returned.Notify();
  release.join();
  ExpectWriterQueueDrained();
  EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 0u);
}

void IndexSchemaFriendTest::VerifyVectorIndexConsistency(
    const std::shared_ptr<indexes::VectorBase> &vector_index,
    const std::string &attr_id) {
  auto vectors = DeterministicallyGenerateVectors(100, dimensions, 2);
  auto itr = index_schema->attributes_.find(attr_id);

  EXPECT_FALSE(itr == index_schema->attributes_.end());
  {
    // Verify that the mutations were processed asynchronous, by the writer
    // worker pool
    VMSDK_EXPECT_OK(mutations_thread_pool.SuspendWorkers());
    auto key_interned = StringInternStore::Intern(std::string(*key) + "0");
    auto data = testing_infra::MakeAttributeData(
        *vector_index, key_interned,
        absl::string_view(reinterpret_cast<const char *>(vectors[0].data()),
                          dimensions * sizeof(float)));
    IndexSchema::MutatedAttributes mutated_attributes;
    mutated_attributes[attr_id] = std::move(data);
    index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned,
                                  false, false);
    EXPECT_EQ(mutations_thread_pool.QueueSize(), 1);
    VMSDK_EXPECT_OK(mutations_thread_pool.ResumeWorkers());
  }
  WaitWorkerTasksAreCompleted(mutations_thread_pool);
  EXPECT_EQ(index_schema->stats_.document_cnt, 1);
  const auto &stats = index_schema->GetStats();
  const size_t iterations = 10;
  // Test delete consistency
  for (size_t j = 0; j < iterations; ++j) {
    for (size_t i = 0; i < vectors.size(); ++i) {
      auto key_interned =
          StringInternStore::Intern(std::string(*key) + std::to_string(i));
      auto data = testing_infra::MakeAttributeData(
          *vector_index, key_interned,
          absl::string_view(reinterpret_cast<const char *>(vectors[i].data()),
                            dimensions * sizeof(float)));
      IndexSchema::MutatedAttributes mutated_attributes;
      mutated_attributes[attr_id] = std::move(data);
      index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned,
                                    false, false);
    }
  }
  EXPECT_EQ(index_schema->stats_.document_cnt, vectors.size());
  for (size_t i = 0; i < vectors.size(); ++i) {
    IndexSchema::MutatedAttributes mutated_attributes;
    mutated_attributes[attr_id] = AttributeData(indexes::DeletionType::kRecord);
    auto key_interned =
        StringInternStore::Intern(std::string(*key) + std::to_string(i));
    index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned,
                                  false, true);
  }
  EXPECT_EQ(index_schema->stats_.document_cnt, 0);

  WaitWorkerTasksAreCompleted(mutations_thread_pool);
  EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 0);
  for (size_t i = 0; i < vectors.size(); ++i) {
    auto interned_key =
        StringInternStore::Intern(std::string(*key) + std::to_string(i));
    EXPECT_FALSE(vector_index->IsTracked(interned_key));
  }

  EXPECT_GE(stats.subscription_remove.success_cnt +
                stats.subscription_remove.skipped_cnt,
            vectors.size());
  EXPECT_EQ(stats.subscription_remove.success_cnt,
            stats.subscription_add.success_cnt);
  // Test update consistency. Wave 1 adds every key; wave 2 races updates
  // against already indexed keys, so every count below is exact or bounded.
  const uint64_t adds_before_wave1 = stats.subscription_add.success_cnt;
  const uint64_t modifies_before_wave1 = stats.subscription_modify.success_cnt +
                                         stats.subscription_modify.skipped_cnt;
  for (size_t i = 0; i < vectors.size(); ++i) {
    auto key_interned =
        StringInternStore::Intern(std::string(*key) + std::to_string(i));
    auto data = testing_infra::MakeAttributeData(
        *vector_index, key_interned,
        absl::string_view(reinterpret_cast<const char *>(vectors[0].data()),
                          dimensions * sizeof(float)));
    IndexSchema::MutatedAttributes mutated_attributes;
    mutated_attributes[attr_id] = std::move(data);
    index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned,
                                  false, false);
  }
  WaitWorkerTasksAreCompleted(mutations_thread_pool);
  EXPECT_EQ(stats.subscription_add.success_cnt - adds_before_wave1,
            vectors.size());
  EXPECT_EQ(stats.subscription_modify.success_cnt +
                stats.subscription_modify.skipped_cnt - modifies_before_wave1,
            0u);
  for (size_t i = 0; i < vectors.size(); ++i) {
    auto interned_key =
        StringInternStore::Intern(std::string(*key) + std::to_string(i));
    EXPECT_TRUE(vector_index->IsTracked(interned_key));
  }

  const uint64_t adds_before_wave2 = stats.subscription_add.success_cnt;
  const uint64_t modifies_before_wave2 = stats.subscription_modify.success_cnt +
                                         stats.subscription_modify.skipped_cnt;
  for (size_t j = 1; j < iterations; ++j) {
    for (size_t i = 0; i < vectors.size(); ++i) {
      auto key_interned =
          StringInternStore::Intern(std::string(*key) + std::to_string(i));
      auto data = testing_infra::MakeAttributeData(
          *vector_index, key_interned,
          absl::string_view(reinterpret_cast<const char *>(vectors[0].data()),
                            dimensions * sizeof(float)));
      IndexSchema::MutatedAttributes mutated_attributes;
      mutated_attributes[attr_id] = std::move(data);
      index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned,
                                    false, false);
    }
  }
  EXPECT_EQ(index_schema->stats_.document_cnt, vectors.size());
  for (size_t i = 0; i < vectors.size(); ++i) {
    auto key_interned =
        StringInternStore::Intern(std::string(*key) + std::to_string(i));
    auto data = testing_infra::MakeAttributeData(
        *vector_index, key_interned,
        absl::string_view(reinterpret_cast<const char *>(vectors[i].data()),
                          dimensions * sizeof(float)));
    IndexSchema::MutatedAttributes mutated_attributes;
    mutated_attributes[attr_id] = std::move(data);
    index_schema->ProcessMutation(&fake_ctx, mutated_attributes, key_interned,
                                  false, true);
  }
  EXPECT_EQ(index_schema->stats_.document_cnt, 0);
  WaitWorkerTasksAreCompleted(mutations_thread_pool);
  EXPECT_EQ(index_schema->GetMutatedRecordsSize(), 0);
  EXPECT_EQ(stats.subscription_remove.success_cnt + vectors.size(),
            stats.subscription_add.success_cnt);
  const uint64_t wave2_modifies = stats.subscription_modify.success_cnt +
                                  stats.subscription_modify.skipped_cnt -
                                  modifies_before_wave2;
  EXPECT_EQ(stats.subscription_add.success_cnt, adds_before_wave2);
  EXPECT_GE(wave2_modifies, vectors.size());
  EXPECT_LE(wave2_modifies, vectors.size() * iterations);
  for (size_t i = 0; i < vectors.size(); ++i) {
    auto interned_key =
        StringInternStore::Intern(std::string(*key) + std::to_string(i));
    EXPECT_TRUE(vector_index->IsTracked(interned_key));
  }
  EXPECT_EQ(stats.subscription_add.failure_cnt, 0);
  EXPECT_EQ(stats.subscription_remove.failure_cnt, 0);
  EXPECT_EQ(stats.subscription_modify.failure_cnt, 0);
}

TEST_F(IndexSchemaFriendTest, ConsistencyTest) {
  VerifyVectorIndexConsistency(hnsw_index, attribute_identifier);
}

TEST_F(IndexSchemaFriendTest, FlatConsistencyTest) {
  auto flat_index =
      indexes::VectorFlat<float>::Create(
          CreateFlatVectorIndexProto(dimensions, distance_metric, initial_cap,
                                     block_size),
          "flat_identifier",
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0)
          .value();
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("flat_id", "flat_identifier", flat_index));
  VerifyVectorIndexConsistency(flat_index, "flat_id");
}

class IndexSchemaTest : public ValkeySearchTest {};

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
      TestValkeyModule_CreateStringPrintf(nullptr, "test_data_16_byte");
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

TEST_F(IndexSchemaRDBTest, ComprehensiveSkipLoadTest)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  const int num_vectors = 100;
  const int dimensions = 64;
  const int additional_index_vectors = 100;
  auto distance_metric = data_model::DISTANCE_METRIC_L2;
  int initial_cap = 200;
  int m = 16;
  int ef_construction = 200;
  int ef_runtime = 10;
  int block_size = 128;

  LOG(INFO) << "=== Comprehensive Skip Load Test Started ===";

  // STEP 1: Create vector index + add 1000 vectors, save to RDB
  LOG(INFO) << "STEP 1: Creating vector index with " << num_vectors
            << " vectors";
  std::vector<absl::string_view> key_prefixes = {"key"};
  std::string index_schema_name_str("comprehensive_test");
  FakeSafeRDB rdb_stream_step1;
  // Add 1000 vectors
  auto vectors = DeterministicallyGenerateVectors(num_vectors, dimensions, 1.0);
  std::vector<ValkeyModuleString *> records(num_vectors);
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
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0)
            .value();
    VMSDK_EXPECT_OK(index_schema->AddIndex("embedding", "emb_id", hnsw_index));

    auto itr = index_schema->attributes_.find("embedding");
    EXPECT_FALSE(itr == index_schema->attributes_.end());

    for (size_t i = 0; i < vectors.size(); ++i) {
      auto interned_key = StringInternStore::Intern("key" + std::to_string(i));
      auto data = testing_infra::MakeAttributeData(
          *hnsw_index, interned_key,
          absl::string_view((char *)&vectors[i][0],
                            dimensions * sizeof(float)));
      index_schema->ProcessAttributeMutation(&fake_ctx_, itr->second,
                                             interned_key, std::move(data));
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

    for (size_t i = 0; i < vectors.size(); ++i) {
      records[i] = new ValkeyModuleString{
          .data = std::string(reinterpret_cast<char *>(vectors[i].data()),
                              dimensions * sizeof(float)),
      };
    }
    EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
        .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
    EXPECT_CALL(*kMockValkeyModule,
                HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_,
                        testing::An<ValkeyModuleString **>(),
                        testing::TypedEq<void *>(nullptr)))
        .WillRepeatedly([records](ValkeyModuleKey *key, int, const char *,
                                  ValkeyModuleString **value_out, void *) {
          absl::string_view key_str = key->key;
          CHECK(absl::ConsumePrefix(&key_str, "key"));
          int index;
          CHECK(absl::SimpleAtoi(key_str, &index));
          *value_out = records[index];
          ValkeyModule_RetainString(nullptr, records[index]);
          return VALKEYMODULE_OK;
        });

    auto schema =
        IndexSchema::LoadFromRDB(&parent_ctx, nullptr,
                                 std::make_unique<data_model::IndexSchema>(
                                     (*section)->index_schema_contents()),
                                 iter.IterateSupplementalContent());

    VMSDK_EXPECT_OK_STATUSOR(schema);
    auto normal_schema = std::move(schema.value());

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
            std::string key = "key" + std::to_string(scan_call_count);
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

    auto schema =
        IndexSchema::LoadFromRDB(&parent_ctx, nullptr,
                                 std::make_unique<data_model::IndexSchema>(
                                     (*section)->index_schema_contents()),
                                 iter.IterateSupplementalContent());

    VMSDK_EXPECT_OK_STATUSOR(schema);
    auto skip_schema = std::move(schema.value());

    auto vec_index = skip_schema->GetIndex("embedding");
    VMSDK_EXPECT_OK_STATUSOR(vec_index);
    EXPECT_EQ(vec_index.value()->GetTrackedKeyCount(), 0);
    EXPECT_TRUE(skip_schema->IsBackfillInProgress());
    LOG(INFO) << "✓ Skip load verified - index empty, backfill ready";
    for (size_t i = 0; i < vectors.size(); ++i) {
      delete records[i];
    }
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
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0)
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
            indexes::text::LanguageRegistry::Instance().Get(
                data_model::LANGUAGE_ENGLISH),
            " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
            std::vector<std::string>{}, true, 6));
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
      auto interned_key = StringInternStore::Intern("key" + std::to_string(i));

      // Add vector data
      auto vec_data = testing_infra::MakeAttributeData(
          *hnsw_index, interned_key,
          absl::string_view(reinterpret_cast<char *>(vectors[i].data()),
                            dimensions * sizeof(float)));
      index_schema->ProcessAttributeMutation(&fake_ctx_, vec_itr->second,
                                             interned_key, std::move(vec_data));

      // Add numeric data (price)
      std::string price_str =
          std::to_string(i * 10 + 100);  // prices 100, 110, 120, etc.
      vmsdk::UniqueValkeyString num_data =
          vmsdk::MakeUniqueValkeyString(price_str);
      index_schema->ProcessAttributeMutation(
          &fake_ctx_, num_itr->second, interned_key,
          AttributeData(std::move(num_data)));

      // Add tag data (category)
      std::string category = (i % 3 == 0)   ? "electronics"
                             : (i % 3 == 1) ? "books"
                                            : "clothing";
      vmsdk::UniqueValkeyString tag_data =
          vmsdk::MakeUniqueValkeyString(category);
      index_schema->ProcessAttributeMutation(
          &fake_ctx_, tag_itr->second, interned_key,
          AttributeData(std::move(tag_data)));

      // Add text data (description)
      std::string description = "description" + std::to_string(i);
      vmsdk::UniqueValkeyString text_data =
          vmsdk::MakeUniqueValkeyString(description);
      index_schema->ProcessAttributeMutation(
          &fake_ctx_, text_itr->second, interned_key,
          AttributeData(std::move(text_data)));
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
    std::vector<ValkeyModuleString *> records_step5(num_vectors);
    for (size_t i = 0; i < vectors.size(); ++i) {
      records_step5[i] = new ValkeyModuleString{
          .data = std::string(reinterpret_cast<char *>(vectors[i].data()),
                              dimensions * sizeof(float)),
      };
    }
    EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
        .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
    EXPECT_CALL(*kMockValkeyModule,
                HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_,
                        testing::An<ValkeyModuleString **>(),
                        testing::TypedEq<void *>(nullptr)))
        .WillRepeatedly([records_step5](ValkeyModuleKey *key, int, const char *,
                                        ValkeyModuleString **value_out,
                                        void *) {
          absl::string_view key_str = key->key;
          CHECK(absl::ConsumePrefix(&key_str, "key"));
          int index;
          CHECK(absl::SimpleAtoi(key_str, &index));
          *value_out = records_step5[index];
          ValkeyModule_RetainString(nullptr, records_step5[index]);
          return VALKEYMODULE_OK;
        });
    auto schema =
        IndexSchema::LoadFromRDB(&parent_ctx, nullptr,
                                 std::make_unique<data_model::IndexSchema>(
                                     (*section)->index_schema_contents()),
                                 iter.IterateSupplementalContent());

    VMSDK_EXPECT_OK_STATUSOR(schema);
    auto mixed_schema = std::move(schema.value());

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
    for (size_t i = 0; i < vectors.size(); ++i) {
      delete records_step5[i];
    }
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
            std::string key = "key" + std::to_string(scan_call_count);
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
    ValkeyModuleString *records[num_vectors];
    for (size_t i = 0; i < vectors.size(); ++i) {
      records[i] = new ValkeyModuleString{
          std::string((char *)&vectors[i][0], dimensions * sizeof(float))};
    }
    std::vector<size_t> keys;
    keys.reserve(num_vectors);
    EXPECT_CALL(*kMockValkeyModule,
                OpenKey(testing::_, testing::_,
                        VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ))
        .WillRepeatedly(
            [&keys](ValkeyModuleCtx *ctx, ValkeyModuleString *key, int flags) {
              auto key_str = vmsdk::ToStringView(key);
              CHECK(absl::ConsumePrefix(&key_str, "key"));
              int index;
              CHECK(absl::SimpleAtoi(key_str, &index));
              keys.push_back(index);
              return TestValkeyModule_OpenKeyDefaultImpl(ctx, key, flags);
            });
    EXPECT_CALL(*kMockValkeyModule,
                HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_,
                        testing::An<ValkeyModuleString **>(),
                        testing::TypedEq<void *>(nullptr)))
        .WillRepeatedly([&records, &keys](ValkeyModuleKey *, int, const char *,
                                          ValkeyModuleString **value_out,
                                          void *) {
          static size_t key_i = 0;
          CHECK(key_i < keys.size());
          auto vector_i = keys[key_i];
          *value_out = records[vector_i];
          ValkeyModule_RetainString(nullptr, records[vector_i]);
          ++key_i;
          return VALKEYMODULE_OK;
        });
    auto schema =
        IndexSchema::LoadFromRDB(&parent_ctx, nullptr,
                                 std::make_unique<data_model::IndexSchema>(
                                     (*section)->index_schema_contents()),
                                 iter.IterateSupplementalContent());

    VMSDK_EXPECT_OK_STATUSOR(schema);
    auto mixed_skip_schema = std::move(schema.value());

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
    for (size_t i = 0; i < vectors.size(); ++i) {
      delete records[i];
    }
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
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0)
            .value();
    VMSDK_EXPECT_OK(
        index_schema->AddIndex("embedding1", "emb1_id", hnsw_index1));

    auto hnsw_index2 =
        indexes::VectorHNSW<float>::Create(
            CreateHNSWVectorIndexProto(dimensions, distance_metric, initial_cap,
                                       m, ef_construction, ef_runtime),
            "embedding2",
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0)
            .value();
    VMSDK_EXPECT_OK(
        index_schema->AddIndex("embedding2", "emb2_id", hnsw_index2));

    auto flat_index =
        indexes::VectorFlat<float>::Create(
            CreateFlatVectorIndexProto(dimensions, distance_metric, initial_cap,
                                       block_size),
            "embedding3",
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0)
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
      auto interned_key = StringInternStore::Intern("key" + std::to_string(i));
      auto data = testing_infra::MakeAttributeData(
          *hnsw_index1, interned_key,
          absl::string_view((char *)&vectors[i][0],
                            dimensions * sizeof(float)));
      index_schema->ProcessAttributeMutation(&fake_ctx_, itr1->second,
                                             interned_key, std::move(data));
    }

    // Add to index2: vectors 100-199
    for (int i = 0; i < additional_index_vectors; ++i) {
      auto interned_key =
          StringInternStore::Intern("key" + std::to_string(i + 1000));
      auto data = testing_infra::MakeAttributeData(
          *hnsw_index2, interned_key,
          absl::string_view((char *)&vectors[i + additional_index_vectors][0],
                            dimensions * sizeof(float)));
      index_schema->ProcessAttributeMutation(&fake_ctx_, itr2->second,
                                             interned_key, std::move(data));
    }

    // Add to index3: vectors 200-299
    for (int i = 0; i < additional_index_vectors; ++i) {
      auto interned_key =
          StringInternStore::Intern("key" + std::to_string(i + 2000));
      auto data = testing_infra::MakeAttributeData(
          *flat_index, interned_key,
          absl::string_view(
              (char *)&vectors[i + additional_index_vectors * 2][0],
              dimensions * sizeof(float)));
      index_schema->ProcessAttributeMutation(&fake_ctx_, itr3->second,
                                             interned_key, std::move(data));
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

class IndexSchemaScoreFieldTest : public ValkeySearchTest {
 protected:
  void SetUp() override {
    ValkeySearchTest::SetUp();
    auto &enable_sharing =
        const_cast<vmsdk::config::Boolean &>(options::GetEnableVectorSharing());
    VMSDK_EXPECT_OK(enable_sharing.SetValue(true));
    VectorRegistry::Construct(&registry_ctx_);
  }
  void TearDown() override {
    auto &enable_sharing =
        const_cast<vmsdk::config::Boolean &>(options::GetEnableVectorSharing());
    VMSDK_EXPECT_OK(enable_sharing.SetValue(false));
    VectorRegistry::Construct(&registry_ctx_);
    ValkeySearchTest::TearDown();
  }
};

TEST_F(IndexSchemaScoreFieldTest, IngestsDocumentScoreFromScoreField) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();

  std::vector<absl::string_view> key_prefixes = {"product:"};
  // Create schema with SCORE_FIELD "priority" and default score 0.5
  auto index_schema =
      MockIndexSchema::Create(&fake_ctx_, "test_index", key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool,
                              data_model::Language::LANGUAGE_ENGLISH, ".", true,
                              {}, 0.5, "priority")
          .value();
  EXPECT_TRUE(
      KeyspaceEventManager::Instance().HasSubscription(index_schema.get()));
  auto mock_index = std::make_shared<MockIndex>(indexes::IndexerType::kTag);
  VMSDK_EXPECT_OK(index_schema->AddIndex("name", "name", mock_index));

  auto key = StringInternStore::Intern("product:1");
  auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key->Str());

  EXPECT_CALL(*mock_index, IsTracked(key)).WillRepeatedly(Return(false));
  EXPECT_CALL(*mock_index, AddRecord(key, testing::_))
      .WillOnce(Return(indexes::RecordResult::kAdded));

  // Mock the key type
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(TestValkeyModule_KeyTypeDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule,
              KeyType(vmsdk::ValkeyModuleKeyIsForString(key->Str())))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));

  // Mock HashGet for the "name" attribute field
  ValkeyModuleString *name_value =
      TestValkeyModule_CreateStringPrintf(nullptr, "%s", "Widget");
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(vmsdk::ValkeyModuleKeyIsForString(key->Str()),
                      VALKEYMODULE_HASH_CFIELDS, StrEq("name"),
                      An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
      .WillOnce([name_value](ValkeyModuleKey *key, int flags, const char *field,
                             ValkeyModuleString **value_out,
                             void *terminating_null) {
        *value_out = name_value;
        return VALKEYMODULE_OK;
      });

  // Mock HashGet for the "priority" score field
  ValkeyModuleString *score_value =
      TestValkeyModule_CreateStringPrintf(nullptr, "%s", "0.8");
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(vmsdk::ValkeyModuleKeyIsForString(key->Str()),
                      VALKEYMODULE_HASH_CFIELDS, StrEq("priority"),
                      An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
      .WillOnce([score_value](ValkeyModuleKey *key, int flags,
                              const char *field, ValkeyModuleString **value_out,
                              void *terminating_null) {
        *value_out = score_value;
        return VALKEYMODULE_OK;
      });

  // Trigger ingestion
  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                       "hset", key_valkey_str.get());
  WaitWorkerTasksAreCompleted(mutations_thread_pool);

  // Verify the document score was stored
  vmsdk::ReaderMutexLock lock(&index_schema->GetTimeSlicedMutex());
  EXPECT_FLOAT_EQ(
      index_schema->GetDocumentScore(BorrowedInternedStringPtr(key)), 0.8f);
}

TEST_F(IndexSchemaScoreFieldTest, FallsBackToDefaultScoreWhenFieldMissing) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();

  std::vector<absl::string_view> key_prefixes = {"product:"};
  auto index_schema =
      MockIndexSchema::Create(&fake_ctx_, "test_index", key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool,
                              data_model::Language::LANGUAGE_ENGLISH, ".", true,
                              {}, 0.5, "priority")
          .value();

  auto mock_index = std::make_shared<MockIndex>(indexes::IndexerType::kTag);
  VMSDK_EXPECT_OK(index_schema->AddIndex("name", "name", mock_index));

  auto key = StringInternStore::Intern("product:2");
  auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key->Str());

  EXPECT_CALL(*mock_index, IsTracked(key)).WillRepeatedly(Return(false));
  EXPECT_CALL(*mock_index, AddRecord(key, testing::_))
      .WillOnce(Return(indexes::RecordResult::kAdded));

  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(TestValkeyModule_KeyTypeDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule,
              KeyType(vmsdk::ValkeyModuleKeyIsForString(key->Str())))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));

  // Mock HashGet for "name" — returns data
  ValkeyModuleString *name_value =
      TestValkeyModule_CreateStringPrintf(nullptr, "%s", "Gadget");
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(vmsdk::ValkeyModuleKeyIsForString(key->Str()),
                      VALKEYMODULE_HASH_CFIELDS, StrEq("name"),
                      An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
      .WillOnce([name_value](ValkeyModuleKey *key, int flags, const char *field,
                             ValkeyModuleString **value_out,
                             void *terminating_null) {
        *value_out = name_value;
        return VALKEYMODULE_OK;
      });

  // Mock HashGet for "priority" — field doesn't exist (returns nullptr)
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(vmsdk::ValkeyModuleKeyIsForString(key->Str()),
                      VALKEYMODULE_HASH_CFIELDS, StrEq("priority"),
                      An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
      .WillOnce([](ValkeyModuleKey *key, int flags, const char *field,
                   ValkeyModuleString **value_out, void *terminating_null) {
        *value_out = nullptr;
        return VALKEYMODULE_OK;
      });

  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                       "hset", key_valkey_str.get());
  WaitWorkerTasksAreCompleted(mutations_thread_pool);

  // Should fall back to default score (0.5)
  vmsdk::ReaderMutexLock lock(&index_schema->GetTimeSlicedMutex());
  EXPECT_FLOAT_EQ(
      index_schema->GetDocumentScore(BorrowedInternedStringPtr(key)), 0.5f);
}

enum class IndexAttributeType { kText, kVector };

struct GetMinVersionTestCase {
  std::string test_name;
  std::string index_name;
  data_model::Language language;
  IndexAttributeType attribute_type;
  int db_num;
  vmsdk::ValkeyVersion expected_version;
};

class GetMinVersionTest
    : public ValkeySearchTestWithParam<GetMinVersionTestCase> {};

TEST_P(GetMinVersionTest, ReturnsExpectedVersion) {
  const auto &tc = GetParam();

  data_model::IndexSchema index_schema_proto;
  index_schema_proto.set_name(tc.index_name);
  index_schema_proto.set_language(tc.language);
  if (tc.db_num != 0) {
    index_schema_proto.set_db_num(tc.db_num);
  }

  auto *attr = index_schema_proto.add_attributes();
  switch (tc.attribute_type) {
    case IndexAttributeType::kText:
      attr->mutable_index()->mutable_text_index();
      break;
    case IndexAttributeType::kVector: {
      auto *vector_index = attr->mutable_index()->mutable_vector_index();
      vector_index->set_dimension_count(3);
      vector_index->set_initial_cap(100);
      vector_index->mutable_hnsw_algorithm();
      break;
    }
  }

  google::protobuf::Any any_proto;
  any_proto.PackFrom(index_schema_proto);

  auto result = IndexSchema::GetMinVersion(any_proto);
  VMSDK_EXPECT_OK(result);
  EXPECT_EQ(result.value(), tc.expected_version);
}

INSTANTIATE_TEST_SUITE_P(GetMinVersionTests, GetMinVersionTest,
                         ValuesIn<GetMinVersionTestCase>({
                             {
                                 .test_name = "EnglishTextIndex",
                                 .index_name = "english_text_idx",
                                 .language = data_model::LANGUAGE_ENGLISH,
                                 .attribute_type = IndexAttributeType::kText,
                                 .db_num = 0,
                                 .expected_version = kRelease12,
                             },
                             {
                                 .test_name = "UnspecifiedLanguageTextIndex",
                                 .index_name = "unspecified_text_idx",
                                 .language = data_model::LANGUAGE_UNSPECIFIED,
                                 .attribute_type = IndexAttributeType::kText,
                                 .db_num = 0,
                                 .expected_version = kRelease12,
                             },
                             {
                                 .test_name = "NonEnglishTextIndex",
                                 .index_name = "french_text_idx",
                                 .language = data_model::LANGUAGE_FRENCH,
                                 .attribute_type = IndexAttributeType::kText,
                                 .db_num = 0,
                                 .expected_version = kRelease13,
                             },
                             {
                                 .test_name = "VectorOnlyIndex",
                                 .index_name = "vector_idx",
                                 .language = data_model::LANGUAGE_ENGLISH,
                                 .attribute_type = IndexAttributeType::kVector,
                                 .db_num = 0,
                                 .expected_version = kRelease10,
                             },
                             {
                                 .test_name = "NonZeroDbNum",
                                 .index_name = "dbnum_idx",
                                 .language = data_model::LANGUAGE_UNSPECIFIED,
                                 .attribute_type = IndexAttributeType::kVector,
                                 .db_num = 2,
                                 .expected_version = kRelease11,
                             },
                             {
                                 .test_name = "EnglishText_DbNumNonZero",
                                 .index_name = "english_text_dbnum_idx",
                                 .language = data_model::LANGUAGE_ENGLISH,
                                 .attribute_type = IndexAttributeType::kText,
                                 .db_num = 3,
                                 .expected_version = kRelease12,
                             },
                             {
                                 .test_name = "NonEnglishText_DbNumNonZero",
                                 .index_name = "french_text_dbnum_idx",
                                 .language = data_model::LANGUAGE_FRENCH,
                                 .attribute_type = IndexAttributeType::kText,
                                 .db_num = 3,
                                 .expected_version = kRelease13,
                             },
                         }),
                         [](const TestParamInfo<GetMinVersionTestCase> &info) {
                           return info.param.test_name;
                         });
TEST_F(IndexSchemaScoreFieldTest, KeyspaceNotificationDeletesRegistryEntry) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  std::vector<absl::string_view> key_prefixes = {"prefix:"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema =
      MockIndexSchema::Create(&fake_ctx_, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool)
          .value();

  int dimensions = 8;
  auto hnsw_index =
      indexes::VectorHNSW<float>::Create(
          CreateHNSWVectorIndexProto(
              dimensions, data_model::DistanceMetric::DISTANCE_METRIC_L2, 100,
              16, 200, 50),
          "emb_id", data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0)
          .value();
  VMSDK_EXPECT_OK(index_schema->AddIndex("embedding", "emb_id", hnsw_index));

  auto key = StringInternStore::Intern("prefix:key");
  auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key->Str());

  // 1. Ingest vector into hnsw_index
  std::string vec_data(dimensions * sizeof(float), 'a');
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(vec_data);
  auto vec = VectorRegistry::Instance().DedupOrConstruct(
      key, valkey_vec.get(),
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0,
      hnsw_index.get());
  EXPECT_NE(vec, nullptr);
  VMSDK_EXPECT_OK(hnsw_index->AddRecord(key, AttributeData(std::move(vec))));
  EXPECT_TRUE(hnsw_index->IsTracked(key));
  EXPECT_EQ(VectorRegistry::Instance().GetStats().entry_cnt, 1);

  // 2. Mock OpenKey to return nullptr (simulating deleted key)
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(testing::_, key_valkey_str.get(), testing::_))
      .Times(1)
      .WillOnce(Return(nullptr));

  // 3. Process the deletion notification
  KeyspaceEventManager::Instance().NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_HASH, "del", key_valkey_str.get());
  WaitWorkerTasksAreCompleted(mutations_thread_pool);
  kMockValkeyModule->RunPendingOneShots();

  // 4. Verify that the key is removed from index and vector registry
  EXPECT_FALSE(hnsw_index->IsTracked(key));
  EXPECT_EQ(VectorRegistry::Instance().GetStats().entry_cnt, 0);
}

TEST_F(IndexSchemaScoreFieldTest,
       KeyspaceNotificationInvalidVectorPayloadRemovesFromIndex) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();

  std::vector<absl::string_view> key_prefixes = {"prefix:"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema =
      MockIndexSchema::Create(&fake_ctx_, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool)
          .value();

  int dimensions = 4;
  auto hnsw_index =
      indexes::VectorHNSW<float>::Create(
          CreateHNSWVectorIndexProto(
              dimensions, data_model::DistanceMetric::DISTANCE_METRIC_L2, 100,
              16, 200, 50),
          "vec", data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0)
          .value();
  VMSDK_EXPECT_OK(index_schema->AddIndex("vec", "vec", hnsw_index));

  auto key = StringInternStore::Intern("prefix:key1");
  auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key->Str());

  // 1. Initial valid vector ingestion
  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  ValkeyModuleString *valid_vec_val =
      TestValkeyModule_CreateString(nullptr, vec_str.data(), vec_str.size());

  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(vmsdk::ValkeyModuleKeyIsForString(key->Str()),
                      VALKEYMODULE_HASH_CFIELDS, StrEq("vec"),
                      An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
      .WillOnce([valid_vec_val](
                    ValkeyModuleKey *key, int flags, const char *field,
                    ValkeyModuleString **value_out, void *terminating_null) {
        *value_out = valid_vec_val;
        return VALKEYMODULE_OK;
      });

  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                       "hset", key_valkey_str.get());
  WaitWorkerTasksAreCompleted(mutations_thread_pool);

  EXPECT_TRUE(hnsw_index->IsTracked(key));

  // 2. Ingest invalid vector payload (too short)
  std::string invalid_vec_str = "short";
  ValkeyModuleString *invalid_vec_val = TestValkeyModule_CreateString(
      nullptr, invalid_vec_str.data(), invalid_vec_str.size());
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(vmsdk::ValkeyModuleKeyIsForString(key->Str()),
                      VALKEYMODULE_HASH_CFIELDS, StrEq("vec"),
                      An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
      .WillOnce([invalid_vec_val](
                    ValkeyModuleKey *key, int flags, const char *field,
                    ValkeyModuleString **value_out, void *terminating_null) {
        *value_out = invalid_vec_val;
        return VALKEYMODULE_OK;
      });

  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                       "hset", key_valkey_str.get());
  WaitWorkerTasksAreCompleted(mutations_thread_pool);
  kMockValkeyModule->RunPendingOneShots();

  // 3. Verify vector removed from index
  EXPECT_FALSE(hnsw_index->IsTracked(key));
}

TEST_F(IndexSchemaScoreFieldTest,
       KeyspaceNotificationMissingVectorFieldRetainsRegistryEntry) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();

  std::vector<absl::string_view> key_prefixes = {"prefix:"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema =
      MockIndexSchema::Create(&fake_ctx_, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              &mutations_thread_pool)
          .value();

  int dimensions = 4;
  auto hnsw_index =
      indexes::VectorHNSW<float>::Create(
          CreateHNSWVectorIndexProto(
              dimensions, data_model::DistanceMetric::DISTANCE_METRIC_L2, 100,
              16, 200, 50),
          "vec", data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0)
          .value();
  VMSDK_EXPECT_OK(index_schema->AddIndex("vec", "vec", hnsw_index));

  auto key = StringInternStore::Intern("prefix:key1");
  auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key->Str());

  // 1. Initial valid vector ingestion
  std::vector<float> vec_data = {1.0f, 2.0f, 3.0f, 4.0f};
  std::string vec_str(reinterpret_cast<const char *>(vec_data.data()),
                      vec_data.size() * sizeof(float));
  ValkeyModuleString *valid_vec_val =
      TestValkeyModule_CreateString(nullptr, vec_str.data(), vec_str.size());

  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(vmsdk::ValkeyModuleKeyIsForString(key->Str()),
                      VALKEYMODULE_HASH_CFIELDS, StrEq("vec"),
                      An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
      .WillOnce([valid_vec_val](
                    ValkeyModuleKey *key, int flags, const char *field,
                    ValkeyModuleString **value_out, void *terminating_null) {
        *value_out = valid_vec_val;
        return VALKEYMODULE_OK;
      });

  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                       "hset", key_valkey_str.get());
  WaitWorkerTasksAreCompleted(mutations_thread_pool);

  EXPECT_TRUE(hnsw_index->IsTracked(key));
  EXPECT_EQ(VectorRegistry::Instance().GetStats().entry_cnt, 1);

  // 2. Ingest notification where hash field is missing (HashGet returns
  // nullptr)
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(vmsdk::ValkeyModuleKeyIsForString(key->Str()),
                      VALKEYMODULE_HASH_CFIELDS, StrEq("vec"),
                      An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
      .WillOnce([](ValkeyModuleKey *key, int flags, const char *field,
                   ValkeyModuleString **value_out, void *terminating_null) {
        *value_out = nullptr;
        return VALKEYMODULE_OK;
      });

  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                       "hdel", key_valkey_str.get());
  WaitWorkerTasksAreCompleted(mutations_thread_pool);
  kMockValkeyModule->RunPendingOneShots();

  // 3. The vector leaves the index. The registry keeps it while the key is a
  // hash, since a field that is not visible may be logically expired and still
  // hold the shared bytes; it goes when the key is deleted.
  EXPECT_FALSE(hnsw_index->IsTracked(key));
  EXPECT_EQ(VectorRegistry::Instance().GetStats().entry_cnt, 1);
  VectorRegistry::Instance().OnKeyDeleted(0, key->Str());
  EXPECT_EQ(VectorRegistry::Instance().GetStats().entry_cnt, 0);
}

TEST_F(IndexSchemaScoreFieldTest,
       DestructingSchemaIgnoresKeyspaceNotification) {
  std::vector<absl::string_view> key_prefixes = {"prefix:"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(), nullptr)
                          .value();

  int dimensions = 4;
  auto hnsw_index =
      indexes::VectorHNSW<float>::Create(
          CreateHNSWVectorIndexProto(
              dimensions, data_model::DistanceMetric::DISTANCE_METRIC_L2, 100,
              16, 200, 50),
          "vec", data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0)
          .value();
  VMSDK_EXPECT_OK(index_schema->AddIndex("embedding", "vec", hnsw_index));

  auto key = StringInternStore::Intern("prefix:key_destructing");
  auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key->Str());

  // Mark schema as destructing
  index_schema->MarkAsDestructing();

  // Notification should be ignored immediately
  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                       "hset", key_valkey_str.get());

  EXPECT_FALSE(hnsw_index->IsTracked(key));
  EXPECT_EQ(VectorRegistry::Instance().GetStats().entry_cnt, 0);
}

TEST_F(IndexSchemaTest, GetVectorIndexesFiltersNonVectorAttributes) {
  std::vector<absl::string_view> key_prefixes = {"prefix:"};
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, "schema_mixed", key_prefixes,
                          std::make_unique<HashAttributeDataType>(), nullptr)
                          .value();
  auto hnsw_index =
      indexes::VectorHNSW<float>::Create(
          CreateHNSWVectorIndexProto(
              4, data_model::DistanceMetric::DISTANCE_METRIC_L2, 100, 16, 200,
              50),
          "vec1", data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0)
          .value();
  auto flat_index =
      indexes::VectorFlat<float>::Create(
          CreateFlatVectorIndexProto(
              8, data_model::DistanceMetric::DISTANCE_METRIC_COSINE, 100, 50),
          "vec2", data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0)
          .value();
  auto tag_index =
      std::make_shared<indexes::Tag>(CreateTagIndexProto(",", false));
  auto num_index =
      std::make_shared<indexes::Numeric>(CreateNumericIndexProto());

  VMSDK_EXPECT_OK(index_schema->AddIndex("embedding1", "vec1", hnsw_index));
  VMSDK_EXPECT_OK(index_schema->AddIndex("embedding2", "vec2", flat_index));
  VMSDK_EXPECT_OK(index_schema->AddIndex("category", "tag", tag_index));
  VMSDK_EXPECT_OK(index_schema->AddIndex("price", "num", num_index));

  auto vector_indexes = index_schema->GetVectorIndexes();
  EXPECT_EQ(vector_indexes.size(), 2);
  std::vector<std::pair<std::string, size_t>> vectors;
  vectors.reserve(vector_indexes.size());
  for (const auto *vec : vector_indexes) {
    vectors.emplace_back(vec->GetInternedAttributeIdentifier()->Str(),
                         vec->GetDimensions());
  }
  EXPECT_THAT(vectors,
              testing::UnorderedElementsAre(std::make_pair("vec1", 4),
                                            std::make_pair("vec2", 8)));
}

TEST_F(IndexSchemaTest, GetVectorIndexesSchemaWithNoVectorFieldsReturnsEmpty) {
  std::vector<absl::string_view> key_prefixes = {"prefix:"};
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, "schema_no_vectors", key_prefixes,
                          std::make_unique<HashAttributeDataType>(), nullptr)
                          .value();
  auto tag_index =
      std::make_shared<indexes::Tag>(CreateTagIndexProto(",", false));
  VMSDK_EXPECT_OK(index_schema->AddIndex("category", "tag", tag_index));

  auto vector_indexes = index_schema->GetVectorIndexes();
  EXPECT_TRUE(vector_indexes.empty());
}

TEST_F(IndexSchemaTest, IsInDBMatchesDbNum) {
  std::vector<absl::string_view> key_prefixes = {"prefix:"};
  auto index_schema =
      MockIndexSchema::Create(&fake_ctx_, "schema_db", key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              nullptr, data_model::Language::LANGUAGE_ENGLISH,
                              ".", true, {}, 1.0, "", 2)
          .value();
  EXPECT_TRUE(index_schema->IsInDB(2));
  EXPECT_FALSE(index_schema->IsInDB(0));
  EXPECT_FALSE(index_schema->IsInDB(1));
}

// GetMinVersion must move an index schema to the 1.3 floor if and ONLY IF it
// uses one of the new low-precision vector storage types. Nothing else about a
// schema may push it to 1.3, or older modules would needlessly refuse to load
// schemas they can in fact interpret.
namespace {

data_model::IndexSchema MakeSchemaWithVectorType(
    data_model::VectorDataType data_type) {
  data_model::IndexSchema schema;
  schema.set_name("idx");
  auto *attr = schema.add_attributes();
  attr->set_alias("v");
  attr->set_identifier("v");
  auto *vector_index = attr->mutable_index()->mutable_vector_index();
  vector_index->set_dimension_count(4);
  vector_index->set_vector_data_type(data_type);
  vector_index->set_distance_metric(
      data_model::DistanceMetric::DISTANCE_METRIC_L2);
  vector_index->mutable_flat_algorithm()->set_block_size(100);
  return schema;
}

vmsdk::ValkeyVersion MinVersionOf(const data_model::IndexSchema &schema) {
  google::protobuf::Any any;
  any.PackFrom(schema);
  auto version = IndexSchema::GetMinVersion(any);
  CHECK_OK(version);
  return *version;
}

TEST(IndexSchemaMinVersionTest, Float32VectorDoesNotRequire13) {
  EXPECT_LT(MinVersionOf(
                MakeSchemaWithVectorType(data_model::VECTOR_DATA_TYPE_FLOAT32)),
            kRelease13);
}

TEST(IndexSchemaMinVersionTest, Float16VectorRequires13) {
  EXPECT_EQ(MinVersionOf(
                MakeSchemaWithVectorType(data_model::VECTOR_DATA_TYPE_FLOAT16)),
            kRelease13);
}

TEST(IndexSchemaMinVersionTest, BFloat16VectorRequires13) {
  EXPECT_EQ(MinVersionOf(MakeSchemaWithVectorType(
                data_model::VECTOR_DATA_TYPE_BFLOAT16)),
            kRelease13);
}

// A text index moves the floor to 1.2, not 1.3 -- i.e. the 1.3 gate is not
// entangled with any other feature.
TEST(IndexSchemaMinVersionTest, TextIndexDoesNotRequire13) {
  data_model::IndexSchema schema;
  schema.set_name("idx");
  auto *attr = schema.add_attributes();
  attr->set_alias("t");
  attr->set_identifier("t");
  attr->mutable_index()->mutable_text_index();
  EXPECT_EQ(MinVersionOf(schema), kRelease12);
}

// A non-zero db_num moves the floor to 1.1, not 1.3.
TEST(IndexSchemaMinVersionTest, DbNumDoesNotRequire13) {
  auto schema = MakeSchemaWithVectorType(data_model::VECTOR_DATA_TYPE_FLOAT32);
  schema.set_db_num(3);
  EXPECT_EQ(MinVersionOf(schema), kRelease11);
}

// Low precision wins over every other contributor, since 1.3 is the highest
// floor any of them can demand.
TEST(IndexSchemaMinVersionTest, LowPrecisionDominatesOtherContributors) {
  auto schema = MakeSchemaWithVectorType(data_model::VECTOR_DATA_TYPE_BFLOAT16);
  schema.set_db_num(3);
  auto *attr = schema.add_attributes();
  attr->set_alias("t");
  attr->set_identifier("t");
  attr->mutable_index()->mutable_text_index();
  EXPECT_EQ(MinVersionOf(schema), kRelease13);
}

}  // namespace

// A multi/exec key whose mutation record has already been consumed (in the
// deque, absent from the map) is skipped by SaveIndexExtension, not fatal.
TEST_F(IndexSchemaFriendTest, OrphanMultiKeySkippedOnSave)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto k = StringInternStore::Intern("prefix1_orphan");
  index_schema->SetDbMutationSequenceNumber(k, 1);
  index_schema->EnqueueMultiMutation(k);  // deque=[K]
  auto vectors = DeterministicallyGenerateVectors(1, dimensions, 2);
  absl::string_view data_ptr((char *)&vectors[0][0],
                             dimensions * sizeof(float));
  {
    auto a = CreateMutatedAttributes(attribute_identifier, data_ptr, k,
                                     hnsw_index.get());
    index_schema->TrackMutatedRecord(nullptr, k, std::move(a), 1, false, false,
                                     /*from_multi=*/true, 0);  // map={K}
  }
  // Drive the real consumer (the writer-pool task body) to index and erase the
  // mutation record, leaving K referenced only by the deque.
  index_schema->ProcessSingleMutationAsync(&fake_ctx, /*from_backfill=*/false,
                                           k,
                                           /*delay_capturer=*/nullptr);

  // Confirm the state under test: K is in the deque but not the map.
  {
    absl::MutexLock lock(&index_schema->mutated_records_mutex_);
    ASSERT_EQ(index_schema->multi_mutations_keys_.Get().size(), 1u);
    EXPECT_EQ(index_schema->multi_mutations_keys_.Get().front()->Str(),
              "prefix1_orphan");
    EXPECT_EQ(index_schema->tracked_mutated_records_.find(k),
              index_schema->tracked_mutated_records_.end());
  }

  FakeSafeRDB rdb;
  EXPECT_TRUE(
      index_schema->SaveIndexExtension(RDBChunkOutputStream(&rdb)).ok());
}

TEST_F(IndexSchemaRDBTest, SwapDbDeferredV2LoadAndCronRecovery)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto &write_v2 =
      const_cast<vmsdk::config::Boolean &>(options::GetRdbWriteV2());
  auto &read_v2 = const_cast<vmsdk::config::Boolean &>(options::GetRdbReadV2());
  auto &enable_sharing =
      const_cast<vmsdk::config::Boolean &>(options::GetEnableVectorSharing());
  VMSDK_EXPECT_OK(write_v2.SetValue(true));
  VMSDK_EXPECT_OK(read_v2.SetValue(true));
  VMSDK_EXPECT_OK(enable_sharing.SetValue(true));
  VectorRegistry::Construct(&fake_ctx_);
  absl::Cleanup restore = [&]() {
    VMSDK_EXPECT_OK(write_v2.SetValue(false));
    VMSDK_EXPECT_OK(read_v2.SetValue(false));
    VMSDK_EXPECT_OK(enable_sharing.SetValue(false));
    VectorRegistry::Construct(&fake_ctx_);
    vmsdk::DrainPendingMainCallbacks();
  };

  ValkeyModuleCtx parent_ctx;
  ValkeyModuleCtx scan_ctx;
  ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
      .WillByDefault(Return(&scan_ctx));
  ON_CALL(*kMockValkeyModule, SelectDb(testing::_, testing::_))
      .WillByDefault(Return(VALKEYMODULE_OK));

  constexpr int kDims = 4;
  // key0..key2 survive; key3 is in extension_keys but absent from the swapped
  // DB (e.g. missing/expired on RDB load); key4 has a pending DEL in
  // mutation_entries. Both must be evicted on top of #1734.
  constexpr int kNumKeys = 5;
  auto vectors = DeterministicallyGenerateVectors(kNumKeys, kDims, 2);
  std::vector<vmsdk::UniqueValkeyString> vec_strs;
  std::vector<vmsdk::UniqueValkeyString> tag_strs;
  for (int i = 0; i < kNumKeys; ++i) {
    vec_strs.push_back(vmsdk::MakeUniqueValkeyString(
        absl::string_view(reinterpret_cast<const char *>(vectors[i].data()),
                          kDims * sizeof(float))));
    tag_strs.push_back(vmsdk::MakeUniqueValkeyString(absl::StrCat("tag", i)));
  }

  std::vector<absl::AnyInvocable<void()>> queued_tasks;
  bool run_inline = true;
  MockThreadPool pool("swapdb-v2-pool-", 1);
  ON_CALL(pool, Schedule(testing::_, testing::_))
      .WillByDefault(
          [&](absl::AnyInvocable<void()> task, vmsdk::ThreadPool::Priority) {
            if (run_inline) {
              task();
            } else {
              queued_tasks.push_back(std::move(task));
            }
            return true;
          });
  auto drain_tasks = [&]() {
    while (!queued_tasks.empty()) {
      auto batch = std::move(queued_tasks);
      queued_tasks.clear();
      for (auto &t : batch) {
        t();
      }
    }
    vmsdk::DrainPendingMainCallbacks();
  };

  bool key3_exists = true;
  bool key4_exists = true;
  auto setup_keyspace_mocks = [&]() {
    EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
        .WillRepeatedly([&](ValkeyModuleCtx *ctx, ValkeyModuleString *key,
                            int flags) -> ValkeyModuleKey * {
          if ((!key3_exists && vmsdk::ToStringView(key) == "prefix1:key3") ||
              (!key4_exists && vmsdk::ToStringView(key) == "prefix1:key4")) {
            return nullptr;
          }
          return TestValkeyModule_OpenKeyDefaultImpl(ctx, key, flags);
        });
    EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
        .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
    EXPECT_CALL(*kMockValkeyModule,
                HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_,
                        testing::An<ValkeyModuleString **>(),
                        testing::TypedEq<void *>(nullptr)))
        .WillRepeatedly([&](ValkeyModuleKey *key, int, const char *field,
                            ValkeyModuleString **value_out, void *) {
          absl::string_view key_str = key->key;
          CHECK(absl::ConsumePrefix(&key_str, "prefix1:key"));
          int idx = 0;
          CHECK(absl::SimpleAtoi(key_str, &idx));
          ValkeyModuleString *src = (absl::string_view(field) == "vec")
                                        ? vec_strs[idx].get()
                                        : tag_strs[idx].get();
          *value_out = src;
          ValkeyModule_RetainString(nullptr, src);
          return VALKEYMODULE_OK;
        });
  };

  FakeSafeRDB rdb_stream;
  {
    setup_keyspace_mocks();
    std::vector<absl::string_view> prefixes = {"prefix1:"};
    auto orig = MockIndexSchema::Create(
                    &fake_ctx_, "swapdb_v2_idx", prefixes,
                    std::make_unique<HashAttributeDataType>(), &pool)
                    .value();
    auto hnsw = indexes::VectorHNSW<float>::Create(
                    CreateHNSWVectorIndexProto(
                        kDims, data_model::DISTANCE_METRIC_L2, 16, 16, 100, 10),
                    "vec", data_model::ATTRIBUTE_DATA_TYPE_HASH, 0)
                    .value();
    auto tag = std::make_shared<indexes::Tag>(CreateTagIndexProto(",", false));
    VMSDK_EXPECT_OK(orig->AddIndex("vec", "vec", hnsw));
    VMSDK_EXPECT_OK(orig->AddIndex("tag", "tag", tag));
    EXPECT_CALL(*kMockValkeyModule,
                Scan(&scan_ctx, testing::An<ValkeyModuleScanCursor *>(),
                     testing::An<ValkeyModuleScanCB>(), testing::An<void *>()))
        .WillOnce(Return(0));
    EXPECT_EQ(orig->PerformBackfill(&fake_ctx_, 1024), 0u);
    ASSERT_FALSE(orig->IsBackfillInProgress());

    for (int i = 0; i < kNumKeys; ++i) {
      auto key_str =
          vmsdk::MakeUniqueValkeyString(absl::StrCat("prefix1:key", i));
      orig->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH, "hset",
                                   key_str.get());
    }
    // Queue a pending DEL on prefix1:key4 (unprocessed before RDBSave) so V2
    // mutation_entries is also exercised.
    run_inline = false;
    key4_exists = false;
    auto del_key = vmsdk::MakeUniqueValkeyString("prefix1:key4");
    orig->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "del",
                                 del_key.get());
    ASSERT_EQ(queued_tasks.size(), 1u);

    VMSDK_EXPECT_OK(orig->RDBSave(&rdb_stream));
    queued_tasks.clear();
  }

  VectorRegistry::Instance().OnFlushDB(nullptr);
  ASSERT_EQ(VectorRegistry::Instance().GetStats().entry_cnt, 0u);
  testing::Mock::VerifyAndClearExpectations(kMockValkeyModule);

  // During deferred LoadFromRDB AND OnLoadingEnded, zero OpenKey or KeyExists
  // calls may occur and VectorRegistry must remain untouched.
  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_CALL(*kMockValkeyModule, KeyExists(testing::_, testing::_)).Times(0);

  RDBSectionIter iter(&rdb_stream, 1);
  auto section = iter.Next();
  VMSDK_EXPECT_OK_STATUSOR(section);
  auto loaded_or = IndexSchema::LoadFromRDB(
      &parent_ctx, &pool,
      std::make_unique<data_model::IndexSchema>(
          (*section)->index_schema_contents()),
      iter.IterateSupplementalContent(), /*defer_keyspace_load=*/true);
  VMSDK_EXPECT_OK_STATUSOR(loaded_or);
  auto loaded = std::move(loaded_or.value());

  EXPECT_TRUE(loaded->IsSwapDbRecoveryInProgress());
  EXPECT_TRUE(loaded->IsBackfillInProgress());
  EXPECT_EQ(loaded->GetStateForInfo(), "backfill_in_progress");
  EXPECT_LT(loaded->GetBackfillPercent(), 1.0f);
  EXPECT_EQ(VectorRegistry::Instance().GetStats().entry_cnt, 0u);
  auto loaded_tag = loaded->GetIndex("tag").value();
  auto loaded_vec = std::dynamic_pointer_cast<indexes::VectorBase>(
      loaded->GetIndex("vec").value());
  ASSERT_NE(loaded_vec, nullptr);
  EXPECT_EQ(loaded_tag->GetTrackedKeyCount(), 0u);
  EXPECT_TRUE(loaded_vec->HasDeferredRdbVectors());
  EXPECT_FALSE(loaded_vec->IsRegisteredInVectorRegistry());

  // Local query::Search must fail closed during recovery.
  {
    UnitTestSearchParameters params;
    params.index_schema = loaded;
    auto status = query::Search(params, query::SearchMode::kLocal);
    EXPECT_EQ(status.code(), absl::StatusCode::kFailedPrecondition);
    EXPECT_EQ(status.message(), IndexSchema::kSwapDbRecoveryInProgressMsg);
  }

  // OnLoadingEnded must perform zero OpenKey / KeyExists calls.
  loaded->OnLoadingEnded(&parent_ctx);
  EXPECT_TRUE(loaded->IsSwapDbRecoveryInProgress());
  EXPECT_TRUE(loaded_vec->IsRegisteredInVectorRegistry());
  testing::Mock::VerifyAndClearExpectations(kMockValkeyModule);

  // Now simulate post-swap keyspace (with prefix1:key3 absent despite being in
  // extension_keys, and prefix1:key4 absent via mutation_entries) and drain
  // PerformBackfill in bounded batches of 2.
  key3_exists = false;
  setup_keyspace_mocks();
  run_inline = false;
  uint32_t first_batch = loaded->PerformBackfill(&parent_ctx, /*batch_size=*/2);
  EXPECT_EQ(first_batch, 2u);
  EXPECT_TRUE(loaded->IsSwapDbRecoveryInProgress());
  EXPECT_EQ(loaded->GetBackfillScannedKeyCount(), 2u);
  EXPECT_LT(loaded->GetBackfillPercent(), 1.0f);
  EXPECT_EQ(loaded->GetStateForInfo(), "backfill_in_progress");
  drain_tasks();

  int ticks = 0;
  while (loaded->IsSwapDbRecoveryInProgress() && ticks < 20) {
    loaded->PerformBackfill(&parent_ctx, /*batch_size=*/2);
    drain_tasks();
    ++ticks;
  }

  EXPECT_FALSE(loaded->IsSwapDbRecoveryInProgress());
  EXPECT_FALSE(loaded->IsBackfillInProgress());
  EXPECT_EQ(loaded->GetStateForInfo(), "ready");
  EXPECT_FLOAT_EQ(loaded->GetBackfillPercent(), 1.0f);
  EXPECT_FALSE(loaded_vec->HasDeferredRdbVectors());
  EXPECT_EQ(loaded_vec->GetTrackedKeyCount(), 3u);
  EXPECT_FALSE(
      loaded_vec->IsTracked(StringInternStore::Intern("prefix1:key3")));
  EXPECT_FALSE(
      loaded_vec->IsTracked(StringInternStore::Intern("prefix1:key4")));
  EXPECT_EQ(loaded_tag->GetTrackedKeyCount(), 3u);
  EXPECT_EQ(VectorRegistry::Instance().GetStats().entry_cnt, 3u);
}

TEST_F(IndexSchemaRDBTest, SwapDbDeferredEmptyIndexFinishesInOnLoadingEnded)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto &write_v2 =
      const_cast<vmsdk::config::Boolean &>(options::GetRdbWriteV2());
  auto &read_v2 = const_cast<vmsdk::config::Boolean &>(options::GetRdbReadV2());
  VMSDK_EXPECT_OK(write_v2.SetValue(true));
  VMSDK_EXPECT_OK(read_v2.SetValue(true));
  absl::Cleanup restore = [&]() {
    VMSDK_EXPECT_OK(write_v2.SetValue(false));
    VMSDK_EXPECT_OK(read_v2.SetValue(false));
  };

  ValkeyModuleCtx parent_ctx;
  ValkeyModuleCtx scan_ctx;
  ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
      .WillByDefault(Return(&scan_ctx));

  FakeSafeRDB rdb_stream;
  {
    std::vector<absl::string_view> prefixes = {"empty:"};
    auto orig = MockIndexSchema::Create(
                    &fake_ctx_, "swapdb_empty_idx", prefixes,
                    std::make_unique<HashAttributeDataType>(), nullptr)
                    .value();
    auto hnsw = indexes::VectorHNSW<float>::Create(
                    CreateHNSWVectorIndexProto(
                        4, data_model::DISTANCE_METRIC_L2, 8, 16, 100, 10),
                    "vec", data_model::ATTRIBUTE_DATA_TYPE_HASH, 0)
                    .value();
    VMSDK_EXPECT_OK(orig->AddIndex("vec", "vec", hnsw));
    EXPECT_CALL(*kMockValkeyModule,
                Scan(&scan_ctx, testing::An<ValkeyModuleScanCursor *>(),
                     testing::An<ValkeyModuleScanCB>(), testing::An<void *>()))
        .WillOnce(Return(0));
    EXPECT_EQ(orig->PerformBackfill(&fake_ctx_, 1024), 0u);
    ASSERT_FALSE(orig->IsBackfillInProgress());
    VMSDK_EXPECT_OK(orig->RDBSave(&rdb_stream));
  }

  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_CALL(*kMockValkeyModule, KeyExists(testing::_, testing::_)).Times(0);

  RDBSectionIter iter(&rdb_stream, 1);
  auto section = iter.Next();
  VMSDK_EXPECT_OK_STATUSOR(section);
  auto loaded_or = IndexSchema::LoadFromRDB(
      &parent_ctx, nullptr,
      std::make_unique<data_model::IndexSchema>(
          (*section)->index_schema_contents()),
      iter.IterateSupplementalContent(), /*defer_keyspace_load=*/true);
  VMSDK_EXPECT_OK_STATUSOR(loaded_or);
  auto loaded = std::move(loaded_or.value());

  EXPECT_TRUE(loaded->IsSwapDbRecoveryInProgress());
  loaded->OnLoadingEnded(&parent_ctx);
  EXPECT_FALSE(loaded->IsSwapDbRecoveryInProgress());
  EXPECT_FALSE(loaded->IsBackfillInProgress());
  EXPECT_EQ(loaded->GetStateForInfo(), "ready");
}

TEST_F(IndexSchemaRDBTest,
       SwapDbDeferredV1CleansUpStalePendingDelViaPerformBackfill)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  // IndexSchemaRDBTest::SetUp already sets rdb-write-v2=false,
  // rdb-read-v2=false.
  absl::Cleanup restore = [&]() { vmsdk::DrainPendingMainCallbacks(); };

  ValkeyModuleCtx parent_ctx;
  ValkeyModuleCtx scan_ctx;
  ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
      .WillByDefault(Return(&scan_ctx));
  ON_CALL(*kMockValkeyModule, SelectDb(testing::_, testing::_))
      .WillByDefault(Return(VALKEYMODULE_OK));
  ON_CALL(*kMockValkeyModule, DbSize(testing::_)).WillByDefault(Return(2));

  constexpr int kDims = 4;
  constexpr int kNumKeys = 3;
  auto vectors = DeterministicallyGenerateVectors(kNumKeys, kDims, 2);

  FakeSafeRDB rdb_stream;
  {
    std::vector<absl::string_view> prefixes = {"v1:"};
    auto orig = MockIndexSchema::Create(
                    &fake_ctx_, "swapdb_v1_idx", prefixes,
                    std::make_unique<HashAttributeDataType>(), nullptr)
                    .value();
    auto hnsw = indexes::VectorHNSW<float>::Create(
                    CreateHNSWVectorIndexProto(
                        kDims, data_model::DISTANCE_METRIC_L2, 16, 16, 100, 10),
                    "vec", data_model::ATTRIBUTE_DATA_TYPE_HASH, 0)
                    .value();
    VMSDK_EXPECT_OK(orig->AddIndex("vec", "vec", hnsw));
    auto itr = orig->attributes_.find("vec");
    for (int i = 0; i < kNumKeys; ++i) {
      auto key = StringInternStore::Intern(absl::StrCat("v1:key", i));
      auto data = testing_infra::MakeAttributeData(
          *hnsw, key,
          absl::string_view(reinterpret_cast<const char *>(vectors[i].data()),
                            kDims * sizeof(float)));
      orig->ProcessAttributeMutation(&fake_ctx_, itr->second, key,
                                     std::move(data));
    }
    VMSDK_EXPECT_OK(orig->RDBSave(&rdb_stream));
  }

  MockThreadPool pool("swapdb-v1-pool-", 1);
  ON_CALL(pool, Schedule(testing::_, testing::_))
      .WillByDefault(
          [](absl::AnyInvocable<void()> task, vmsdk::ThreadPool::Priority) {
            task();
            return true;
          });

  // Zero OpenKey / KeyExists during deferred LoadFromRDB AND OnLoadingEnded.
  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_CALL(*kMockValkeyModule, KeyExists(testing::_, testing::_)).Times(0);

  RDBSectionIter iter(&rdb_stream, 1);
  auto section = iter.Next();
  VMSDK_EXPECT_OK_STATUSOR(section);
  auto loaded_or = IndexSchema::LoadFromRDB(
      &parent_ctx, &pool,
      std::make_unique<data_model::IndexSchema>(
          (*section)->index_schema_contents()),
      iter.IterateSupplementalContent(), /*defer_keyspace_load=*/true);
  VMSDK_EXPECT_OK_STATUSOR(loaded_or);
  auto loaded = std::move(loaded_or.value());
  EXPECT_TRUE(loaded->IsSwapDbRecoveryInProgress());

  loaded->OnLoadingEnded(&parent_ctx);
  EXPECT_TRUE(loaded->IsSwapDbRecoveryInProgress());
  testing::Mock::VerifyAndClearExpectations(kMockValkeyModule);

  // Now simulate post-swap keyspace: v1:key2 is absent (pending DEL), v1:key0
  // and v1:key1 exist.
  EXPECT_CALL(*kMockValkeyModule, KeyExists(&parent_ctx, testing::_))
      .WillRepeatedly([](ValkeyModuleCtx *, ValkeyModuleString *key) {
        return vmsdk::ToStringView(key) == "v1:key2" ? 0 : 1;
      });
  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillRepeatedly([](ValkeyModuleCtx *, ValkeyModuleString *key,
                         int) -> ValkeyModuleKey * {
        EXPECT_EQ(vmsdk::ToStringView(key), "v1:key2");
        return nullptr;
      });
  EXPECT_CALL(*kMockValkeyModule,
              Scan(testing::_, testing::_, testing::_, testing::_))
      .WillRepeatedly(Return(0));

  loaded->PerformBackfill(&parent_ctx, /*batch_size=*/100);
  vmsdk::DrainPendingMainCallbacks();

  EXPECT_FALSE(loaded->IsSwapDbRecoveryInProgress());
  EXPECT_EQ(loaded->GetStateForInfo(), "ready");
  auto loaded_vec = loaded->GetIndex("vec").value();
  EXPECT_EQ(loaded_vec->GetTrackedKeyCount(), 2u);
  EXPECT_FALSE(loaded_vec->IsTracked(StringInternStore::Intern("v1:key2")));
}

TEST_F(IndexSchemaRDBTest, SwapDbMidRecoveryRdbSavePreservesFullParity)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto &write_v2 =
      const_cast<vmsdk::config::Boolean &>(options::GetRdbWriteV2());
  auto &read_v2 = const_cast<vmsdk::config::Boolean &>(options::GetRdbReadV2());
  VMSDK_EXPECT_OK(write_v2.SetValue(true));
  VMSDK_EXPECT_OK(read_v2.SetValue(true));
  absl::Cleanup restore = [&]() {
    VMSDK_EXPECT_OK(write_v2.SetValue(false));
    VMSDK_EXPECT_OK(read_v2.SetValue(false));
    vmsdk::DrainPendingMainCallbacks();
  };

  ValkeyModuleCtx parent_ctx;
  ValkeyModuleCtx scan_ctx;
  ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
      .WillByDefault(Return(&scan_ctx));
  ON_CALL(*kMockValkeyModule, SelectDb(testing::_, testing::_))
      .WillByDefault(Return(VALKEYMODULE_OK));

  constexpr int kDims = 4;
  constexpr int kNumKeys = 5;  // key0..key3 survive, key4 deleted
  auto vectors = DeterministicallyGenerateVectors(kNumKeys, kDims, 3);
  std::vector<vmsdk::UniqueValkeyString> vec_strs;
  std::vector<vmsdk::UniqueValkeyString> tag_strs;
  for (int i = 0; i < kNumKeys; ++i) {
    vec_strs.push_back(vmsdk::MakeUniqueValkeyString(
        absl::string_view(reinterpret_cast<const char *>(vectors[i].data()),
                          kDims * sizeof(float))));
    tag_strs.push_back(vmsdk::MakeUniqueValkeyString(absl::StrCat("t", i)));
  }

  MockThreadPool pool("swapdb-mid-save-pool-", 1);
  ON_CALL(pool, Schedule(testing::_, testing::_))
      .WillByDefault(
          [](absl::AnyInvocable<void()> task, vmsdk::ThreadPool::Priority) {
            task();
            return true;
          });

  bool key4_exists = true;
  auto setup_mocks = [&]() {
    EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
        .WillRepeatedly([&](ValkeyModuleCtx *ctx, ValkeyModuleString *key,
                            int flags) -> ValkeyModuleKey * {
          if (!key4_exists && vmsdk::ToStringView(key) == "mid:key4") {
            return nullptr;
          }
          return TestValkeyModule_OpenKeyDefaultImpl(ctx, key, flags);
        });
    EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
        .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
    EXPECT_CALL(*kMockValkeyModule,
                HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_,
                        testing::An<ValkeyModuleString **>(),
                        testing::TypedEq<void *>(nullptr)))
        .WillRepeatedly([&](ValkeyModuleKey *key, int, const char *field,
                            ValkeyModuleString **value_out, void *) {
          absl::string_view key_str = key->key;
          CHECK(absl::ConsumePrefix(&key_str, "mid:key"));
          int idx = 0;
          CHECK(absl::SimpleAtoi(key_str, &idx));
          ValkeyModuleString *src = (absl::string_view(field) == "vec")
                                        ? vec_strs[idx].get()
                                        : tag_strs[idx].get();
          *value_out = src;
          ValkeyModule_RetainString(nullptr, src);
          return VALKEYMODULE_OK;
        });
  };

  FakeSafeRDB initial_rdb;
  {
    setup_mocks();
    std::vector<absl::string_view> prefixes = {"mid:"};
    auto orig = MockIndexSchema::Create(
                    &fake_ctx_, "swapdb_mid_idx", prefixes,
                    std::make_unique<HashAttributeDataType>(), &pool)
                    .value();
    auto hnsw = indexes::VectorHNSW<float>::Create(
                    CreateHNSWVectorIndexProto(
                        kDims, data_model::DISTANCE_METRIC_L2, 16, 16, 100, 10),
                    "vec", data_model::ATTRIBUTE_DATA_TYPE_HASH, 0)
                    .value();
    auto tag = std::make_shared<indexes::Tag>(CreateTagIndexProto(",", false));
    VMSDK_EXPECT_OK(orig->AddIndex("vec", "vec", hnsw));
    VMSDK_EXPECT_OK(orig->AddIndex("tag", "tag", tag));
    EXPECT_CALL(*kMockValkeyModule,
                Scan(&scan_ctx, testing::An<ValkeyModuleScanCursor *>(),
                     testing::An<ValkeyModuleScanCB>(), testing::An<void *>()))
        .WillOnce(Return(0));
    EXPECT_EQ(orig->PerformBackfill(&fake_ctx_, 1024), 0u);
    ASSERT_FALSE(orig->IsBackfillInProgress());
    for (int i = 0; i < kNumKeys; ++i) {
      auto key_str = vmsdk::MakeUniqueValkeyString(absl::StrCat("mid:key", i));
      orig->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH, "hset",
                                   key_str.get());
    }
    VMSDK_EXPECT_OK(orig->RDBSave(&initial_rdb));
  }

  // Load with defer_keyspace_load=true, process only 2 of 5 keys, then RDBSave
  // mid-recovery (simulating a sub-replica full resync mid-recovery).
  key4_exists = false;
  RDBSectionIter iter1(&initial_rdb, 1);
  auto section1 = iter1.Next();
  VMSDK_EXPECT_OK_STATUSOR(section1);
  auto mid_schema =
      IndexSchema::LoadFromRDB(&parent_ctx, &pool,
                               std::make_unique<data_model::IndexSchema>(
                                   (*section1)->index_schema_contents()),
                               iter1.IterateSupplementalContent(),
                               /*defer_keyspace_load=*/true)
          .value();
  mid_schema->OnLoadingEnded(&parent_ctx);
  EXPECT_EQ(mid_schema->PerformBackfill(&parent_ctx, /*batch_size=*/2), 2u);
  ASSERT_TRUE(mid_schema->IsSwapDbRecoveryInProgress());

  FakeSafeRDB mid_rdb;
  VMSDK_EXPECT_OK(mid_schema->RDBSave(&mid_rdb));

  // Sub-replica loads the mid-recovery snapshot normally (non-deferred).
  RDBSectionIter iter2(&mid_rdb, 1);
  auto section2 = iter2.Next();
  VMSDK_EXPECT_OK_STATUSOR(section2);
  auto sub_replica_or = IndexSchema::LoadFromRDB(
      &parent_ctx, &pool,
      std::make_unique<data_model::IndexSchema>(
          (*section2)->index_schema_contents()),
      iter2.IterateSupplementalContent(), /*defer_keyspace_load=*/false);
  VMSDK_EXPECT_OK_STATUSOR(sub_replica_or);
  auto sub_replica = std::move(sub_replica_or.value());
  sub_replica->OnLoadingEnded(&parent_ctx);

  EXPECT_FALSE(sub_replica->IsSwapDbRecoveryInProgress());
  EXPECT_FALSE(sub_replica->IsBackfillInProgress());
  EXPECT_EQ(sub_replica->GetIndex("vec").value()->GetTrackedKeyCount(), 4u);
  EXPECT_FALSE(sub_replica->GetIndex("vec").value()->IsTracked(
      StringInternStore::Intern("mid:key4")));
  EXPECT_EQ(sub_replica->GetIndex("tag").value()->GetTrackedKeyCount(), 4u);
}

TEST_F(IndexSchemaRDBTest,
       SwapDbMarkAsDestructingPreservesRecoveryFlagForQueuedQueries)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto &write_v2 =
      const_cast<vmsdk::config::Boolean &>(options::GetRdbWriteV2());
  auto &read_v2 = const_cast<vmsdk::config::Boolean &>(options::GetRdbReadV2());
  VMSDK_EXPECT_OK(write_v2.SetValue(true));
  VMSDK_EXPECT_OK(read_v2.SetValue(true));
  absl::Cleanup restore = [&]() {
    VMSDK_EXPECT_OK(write_v2.SetValue(false));
    VMSDK_EXPECT_OK(read_v2.SetValue(false));
  };

  ValkeyModuleCtx parent_ctx;
  ValkeyModuleCtx scan_ctx;
  ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
      .WillByDefault(Return(&scan_ctx));

  FakeSafeRDB rdb_stream;
  {
    std::vector<absl::string_view> prefixes = {"destr:"};
    auto orig = MockIndexSchema::Create(
                    &fake_ctx_, "swapdb_destr_idx", prefixes,
                    std::make_unique<HashAttributeDataType>(), nullptr)
                    .value();
    auto hnsw = indexes::VectorHNSW<float>::Create(
                    CreateHNSWVectorIndexProto(
                        4, data_model::DISTANCE_METRIC_L2, 8, 16, 100, 10),
                    "vec", data_model::ATTRIBUTE_DATA_TYPE_HASH, 0)
                    .value();
    VMSDK_EXPECT_OK(orig->AddIndex("vec", "vec", hnsw));
    VMSDK_EXPECT_OK(orig->RDBSave(&rdb_stream));
  }

  RDBSectionIter iter(&rdb_stream, 1);
  auto section = iter.Next();
  VMSDK_EXPECT_OK_STATUSOR(section);
  auto loaded =
      IndexSchema::LoadFromRDB(&parent_ctx, nullptr,
                               std::make_unique<data_model::IndexSchema>(
                                   (*section)->index_schema_contents()),
                               iter.IterateSupplementalContent(),
                               /*defer_keyspace_load=*/true)
          .value();
  ASSERT_TRUE(loaded->IsSwapDbRecoveryInProgress());

  loaded->MarkAsDestructing();
  // Queued queries already holding shared_ptr<IndexSchema> must still see
  // IsSwapDbRecoveryInProgress() == true and fail closed.
  EXPECT_TRUE(loaded->IsSwapDbRecoveryInProgress());
  UnitTestSearchParameters params;
  params.index_schema = loaded;
  auto status = query::Search(params, query::SearchMode::kLocal);
  EXPECT_EQ(status.code(), absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(status.message(), IndexSchema::kSwapDbRecoveryInProgressMsg);
}

TEST_F(IndexSchemaFriendTest, SwapDbCoalescedLiveMutationDelaysRecoveryFinish)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto k = StringInternStore::Intern("prefix1_coalesced");
  auto vectors = DeterministicallyGenerateVectors(1, dimensions, 2);
  absl::string_view vec_view(reinterpret_cast<const char *>(vectors[0].data()),
                             dimensions * sizeof(float));

  // Put schema into active swapdb recovery with 1 key remaining.
  IndexSchema::SwapDbRecoveryState state;
  state.extension_keys.push_back(k);
  state.total_items = 1;
  index_schema->swapdb_recovery_.Get().emplace(std::move(state));
  index_schema->swapdb_recovery_in_progress_.store(true,
                                                   std::memory_order_release);

  // Simulate an in-flight live mutation on `k` currently being consumed by a
  // worker (consume_in_progress == true, attributes already consumed).
  {
    auto live_attrs = CreateMutatedAttributes(attribute_identifier, vec_view, k,
                                              hnsw_index.get());
    ASSERT_TRUE(index_schema->TrackMutatedRecord(
        nullptr, k, std::move(live_attrs), /*sequence_number=*/1,
        /*from_backfill=*/false, /*block_client=*/false, /*from_multi=*/false,
        0));
    auto consumed =
        index_schema->ConsumeTrackedMutatedAttribute(k, /*first_time=*/true);
    ASSERT_TRUE(consumed.has_value());
  }

  // Now backfill recovery processes `k`: it coalesces into the existing
  // tracked record (TrackMutatedRecord returns false), recording `{k, seq}` in
  // `coalesced_keys` with zero `backfill_inqueue_tasks`.
  auto recovery_attrs = CreateMutatedAttributes(attribute_identifier, vec_view,
                                                k, hnsw_index.get());
  index_schema->ProcessMutation(&fake_ctx, recovery_attrs, k,
                                /*from_backfill=*/true, /*is_delete=*/false, 0);
  index_schema->swapdb_recovery_.Get()->extension_keys.pop_front();
  index_schema->swapdb_recovery_enqueue_done_.store(true,
                                                    std::memory_order_release);

  // Even if the worker consumes the coalesced attributes (bumping
  // index_key_info_[k].mutation_sequence_number_ >= seq), `consume_in_progress`
  // is still true while SyncProcessMutation is running, so recovery MUST NOT
  // finish yet.
  auto consumed2 =
      index_schema->ConsumeTrackedMutatedAttribute(k, /*first_time=*/false);
  ASSERT_TRUE(consumed2.has_value());
  index_schema->TryFinishSwapDbRecovery();
  EXPECT_TRUE(index_schema->IsSwapDbRecoveryInProgress());

  // Once the worker finishes SyncProcessMutation and calls
  // ConsumeTrackedMutatedAttribute a final time (erasing tracked entry),
  // TryFinishSwapDbRecovery completes recovery.
  auto consumed3 =
      index_schema->ConsumeTrackedMutatedAttribute(k, /*first_time=*/false);
  EXPECT_FALSE(consumed3.has_value());
  index_schema->TryFinishSwapDbRecovery();
  EXPECT_FALSE(index_schema->IsSwapDbRecoveryInProgress());
}

TEST_F(IndexSchemaFriendTest,
       SwapDbDeferredCorruptExtensionCountReturnsStatusError)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  ASSERT_TRUE(options::GetRdbReadV2().GetValue());

  FakeSafeRDB rdb_stream;
  {
    RDBChunkOutputStream chunk_out(&rdb_stream);
    // Write a corrupt huge key_count with no string payloads so LoadString
    // fails cleanly instead of pre-allocating a contiguous vector upfront.
    VMSDK_EXPECT_OK(chunk_out.SaveObject<size_t>(size_t{1} << 40));
  }

  SupplementalContentChunkIter chunk_iter(&rdb_stream);
  auto status = index_schema->LoadIndexExtension(
      &fake_ctx, RDBChunkInputStream(std::move(chunk_iter)),
      /*defer_keyspace_load=*/true);
  EXPECT_FALSE(status.ok());
}

TEST_F(IndexSchemaRDBTest,
       SwapDbDeferredV2WithBackfillingTrueWaitsForScanBeforeClearingRecovery)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto &write_v2 =
      const_cast<vmsdk::config::Boolean &>(options::GetRdbWriteV2());
  auto &read_v2 = const_cast<vmsdk::config::Boolean &>(options::GetRdbReadV2());
  VMSDK_EXPECT_OK(write_v2.SetValue(true));
  VMSDK_EXPECT_OK(read_v2.SetValue(true));
  absl::Cleanup restore = [&]() {
    VMSDK_EXPECT_OK(write_v2.SetValue(false));
    VMSDK_EXPECT_OK(read_v2.SetValue(false));
    vmsdk::DrainPendingMainCallbacks();
  };

  ValkeyModuleCtx parent_ctx;
  ValkeyModuleCtx scan_ctx;
  ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
      .WillByDefault(Return(&scan_ctx));
  ON_CALL(*kMockValkeyModule, SelectDb(testing::_, testing::_))
      .WillByDefault(Return(VALKEYMODULE_OK));
  ON_CALL(*kMockValkeyModule, DbSize(testing::_)).WillByDefault(Return(2));

  MockThreadPool pool("swapdb-v2-scan-pool-", 1);
  ON_CALL(pool, Schedule(testing::_, testing::_))
      .WillByDefault(
          [](absl::AnyInvocable<void()> task, vmsdk::ThreadPool::Priority) {
            task();
            return true;
          });

  constexpr int kDims = 4;
  auto vectors = DeterministicallyGenerateVectors(2, kDims, 3);
  std::vector<vmsdk::UniqueValkeyString> vec_strs;
  vec_strs.reserve(2);
  for (int i = 0; i < 2; ++i) {
    vec_strs.push_back(vmsdk::MakeUniqueValkeyString(
        absl::string_view(reinterpret_cast<const char *>(vectors[i].data()),
                          kDims * sizeof(float))));
  }
  ON_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillByDefault(Return(VALKEYMODULE_KEYTYPE_HASH));
  ON_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillByDefault(TestValkeyModule_OpenKeyDefaultImpl);
  ON_CALL(*kMockValkeyModule,
          HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_,
                  testing::An<ValkeyModuleString **>(),
                  testing::TypedEq<void *>(nullptr)))
      .WillByDefault([&](ValkeyModuleKey *key, int, const char *,
                         ValkeyModuleString **value_out, void *) {
        int idx = (absl::string_view(key->key) == "v2scan:key1") ? 1 : 0;
        *value_out = vec_strs[idx].get();
        ValkeyModule_RetainString(nullptr, vec_strs[idx].get());
        return VALKEYMODULE_OK;
      });

  // Save a V2 snapshot while initial backfill is still unfinished
  // (backfilling=true, extension_keys empty, unvisited keys still in DB).
  FakeSafeRDB rdb_stream;
  {
    std::vector<absl::string_view> prefixes = {"v2scan:"};
    auto orig = MockIndexSchema::Create(
                    &fake_ctx_, "swapdb_v2_scan_idx", prefixes,
                    std::make_unique<HashAttributeDataType>(), &pool)
                    .value();
    auto hnsw = indexes::VectorHNSW<float>::Create(
                    CreateHNSWVectorIndexProto(
                        kDims, data_model::DISTANCE_METRIC_L2, 8, 16, 100, 10),
                    "vec", data_model::ATTRIBUTE_DATA_TYPE_HASH, 0)
                    .value();
    VMSDK_EXPECT_OK(orig->AddIndex("vec", "vec", hnsw));
    ASSERT_TRUE(orig->IsBackfillInProgress());
    VMSDK_EXPECT_OK(orig->RDBSave(&rdb_stream));
  }

  RDBSectionIter iter(&rdb_stream, 1);
  auto section = iter.Next();
  VMSDK_EXPECT_OK_STATUSOR(section);
  auto loaded =
      IndexSchema::LoadFromRDB(&parent_ctx, &pool,
                               std::make_unique<data_model::IndexSchema>(
                                   (*section)->index_schema_contents()),
                               iter.IterateSupplementalContent(),
                               /*defer_keyspace_load=*/true)
          .value();

  // Even though V2 extension_keys and mutation_entries are empty,
  // OnLoadingEnded must keep swapdb recovery active because backfilling=true
  // requires a scan.
  loaded->OnLoadingEnded(&parent_ctx);
  EXPECT_TRUE(loaded->IsSwapDbRecoveryInProgress());

  UnitTestSearchParameters blocked_params;
  blocked_params.index_schema = loaded;
  auto blocked_status =
      query::Search(blocked_params, query::SearchMode::kLocal);
  EXPECT_EQ(blocked_status.code(), absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(blocked_status.message(),
            IndexSchema::kSwapDbRecoveryInProgressMsg);

  // First PerformBackfill(batch_size=1) scans 1 of 2 keys; recovery stays on.
  int scan_step = 0;
  EXPECT_CALL(*kMockValkeyModule,
              Scan(&scan_ctx, testing::An<ValkeyModuleScanCursor *>(),
                   testing::An<ValkeyModuleScanCB>(), testing::An<void *>()))
      .WillRepeatedly([&](ValkeyModuleCtx *ctx, ValkeyModuleScanCursor *,
                          ValkeyModuleScanCB fn, void *privdata) -> int {
        if (scan_step < 2) {
          std::string k = absl::StrCat("v2scan:key", scan_step++);
          auto k_str = vmsdk::MakeUniqueValkeyString(k);
          ValkeyModuleKey vkey = {.ctx = ctx, .key = k};
          fn(ctx, k_str.get(), &vkey, privdata);
          return 1;
        }
        return 0;
      });

  EXPECT_EQ(loaded->PerformBackfill(&parent_ctx, /*batch_size=*/1), 1u);
  vmsdk::DrainPendingMainCallbacks();
  EXPECT_TRUE(loaded->IsSwapDbRecoveryInProgress());

  // Remaining scan completes and clears swapdb recovery.
  EXPECT_EQ(loaded->PerformBackfill(&parent_ctx, /*batch_size=*/10), 1u);
  vmsdk::DrainPendingMainCallbacks();
  EXPECT_FALSE(loaded->IsSwapDbRecoveryInProgress());
  EXPECT_FALSE(loaded->IsBackfillInProgress());
  EXPECT_EQ(loaded->GetIndex("vec").value()->GetTrackedKeyCount(), 2u);
}

TEST_F(IndexSchemaFriendTest,
       SwapDbRecoveryBackpressureDrainsQueuedMultiExecMutations)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto &max_restore_q = options::GetMaxMutationQueueSizeOnRestore();
  const auto prev_max = max_restore_q.GetValue();
  VMSDK_EXPECT_OK(max_restore_q.SetValue(1));
  absl::Cleanup restore_q = [&]() {
    VMSDK_EXPECT_OK(max_restore_q.SetValue(prev_max));
  };

  vmsdk::ThreadPool multi_pool("multi-writer-", 2);
  multi_pool.StartWorkers();
  absl::Cleanup join_pool = [&]() { multi_pool.JoinWorkers(); };

  ValkeyModuleCtx scan_ctx;
  ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
      .WillByDefault(Return(&scan_ctx));
  ON_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillByDefault(Return(VALKEYMODULE_KEYTYPE_HASH));
  ON_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillByDefault(TestValkeyModule_OpenKeyDefaultImpl);

  auto vectors = DeterministicallyGenerateVectors(2, dimensions, 2);
  auto vec0_str = vmsdk::MakeUniqueValkeyString(
      absl::string_view(reinterpret_cast<const char *>(vectors[0].data()),
                        dimensions * sizeof(float)));
  auto vec1_str = vmsdk::MakeUniqueValkeyString(
      absl::string_view(reinterpret_cast<const char *>(vectors[1].data()),
                        dimensions * sizeof(float)));
  ON_CALL(*kMockValkeyModule,
          HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_,
                  testing::An<ValkeyModuleString **>(),
                  testing::TypedEq<void *>(nullptr)))
      .WillByDefault([&](ValkeyModuleKey *key, int, const char *,
                         ValkeyModuleString **value_out, void *) {
        ValkeyModuleString *src =
            (absl::string_view(key->key) == "prefix1_multi") ? vec0_str.get()
                                                             : vec1_str.get();
        *value_out = src;
        ValkeyModule_RetainString(nullptr, src);
        return VALKEYMODULE_OK;
      });

  std::vector<absl::string_view> prefixes = {"prefix1_"};
  auto schema = MockIndexSchema::Create(
                    &fake_ctx, "swapdb_multi_bp_idx", prefixes,
                    std::make_unique<HashAttributeDataType>(), &multi_pool)
                    .value();
  auto hnsw =
      indexes::VectorHNSW<float>::Create(
          CreateHNSWVectorIndexProto(dimensions, data_model::DISTANCE_METRIC_L2,
                                     8, 16, 100, 10),
          attribute_identifier, data_model::ATTRIBUTE_DATA_TYPE_HASH, 0)
          .value();
  VMSDK_EXPECT_OK(
      schema->AddIndex(attribute_identifier, attribute_identifier, hnsw));
  schema->backfill_job_.Get()->MarkScanAsDone();

  // Put schema into post-swap recovery with 1 extension key waiting to replay.
  auto k_replay = StringInternStore::Intern("prefix1_replay");
  IndexSchema::SwapDbRecoveryState state;
  state.extension_keys.push_back(k_replay);
  state.total_items = 1;
  schema->swapdb_recovery_.Get().emplace(std::move(state));
  schema->swapdb_recovery_in_progress_.store(true, std::memory_order_release);

  // Enqueue a single post-swap MULTI/EXEC mutation (< pool size 2, so
  // EnqueueMultiMutation does not auto-drain, yet GetMutatedRecordsSize() == 1
  // reaches max-mutation-queue-size-on-restore=1).
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&fake_ctx))
      .WillOnce(Return(VALKEYMODULE_CTX_FLAGS_MULTI))
      .WillRepeatedly(Return(0));
  auto multi_key_str = vmsdk::MakeUniqueValkeyString("prefix1_multi");
  schema->OnKeyspaceNotification(&fake_ctx, VALKEYMODULE_NOTIFY_HASH, "hset",
                                 multi_key_str.get());
  ASSERT_EQ(schema->GetMutatedRecordsSize(), 1u);
  ASSERT_FALSE(schema->multi_mutations_keys_.Get().empty());

  // PerformBackfill must drain multi_mutations_keys_ rather than stalling.
  EXPECT_EQ(schema->PerformBackfill(&fake_ctx, /*batch_size=*/10), 1u);
  EXPECT_TRUE(schema->multi_mutations_keys_.Get().empty());

  WaitWorkerTasksAreCompleted(multi_pool);
  schema->TryFinishSwapDbRecovery();
  EXPECT_FALSE(schema->IsSwapDbRecoveryInProgress());
  EXPECT_EQ(hnsw->GetTrackedKeyCount(), 2u);
}

TEST_F(IndexSchemaFriendTest, SwapDbRecoveryScanAppliesRestoreQueueBackpressure)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto &max_restore_q = options::GetMaxMutationQueueSizeOnRestore();
  const auto prev_max = max_restore_q.GetValue();
  VMSDK_EXPECT_OK(max_restore_q.SetValue(1));
  absl::Cleanup restore_q = [&]() {
    VMSDK_EXPECT_OK(max_restore_q.SetValue(prev_max));
    vmsdk::DrainPendingMainCallbacks();
  };

  std::vector<absl::AnyInvocable<void()>> queued_tasks;
  MockThreadPool scan_bp_pool("scan-bp-pool-", 1);
  ON_CALL(scan_bp_pool, Schedule(testing::_, testing::_))
      .WillByDefault(
          [&](absl::AnyInvocable<void()> task, vmsdk::ThreadPool::Priority) {
            queued_tasks.push_back(std::move(task));
            return true;
          });
  auto drain_queued = [&]() {
    while (!queued_tasks.empty()) {
      auto batch = std::move(queued_tasks);
      queued_tasks.clear();
      for (auto &t : batch) {
        t();
      }
    }
    vmsdk::DrainPendingMainCallbacks();
  };

  ValkeyModuleCtx scan_ctx;
  ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
      .WillByDefault(Return(&scan_ctx));
  ON_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillByDefault(Return(VALKEYMODULE_KEYTYPE_HASH));
  ON_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillByDefault(TestValkeyModule_OpenKeyDefaultImpl);
  ON_CALL(*kMockValkeyModule, DbSize(testing::_)).WillByDefault(Return(2));

  auto vectors = DeterministicallyGenerateVectors(2, dimensions, 2);
  auto vec0_str = vmsdk::MakeUniqueValkeyString(
      absl::string_view(reinterpret_cast<const char *>(vectors[0].data()),
                        dimensions * sizeof(float)));
  auto vec1_str = vmsdk::MakeUniqueValkeyString(
      absl::string_view(reinterpret_cast<const char *>(vectors[1].data()),
                        dimensions * sizeof(float)));
  ON_CALL(*kMockValkeyModule,
          HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_,
                  testing::An<ValkeyModuleString **>(),
                  testing::TypedEq<void *>(nullptr)))
      .WillByDefault([&](ValkeyModuleKey *key, int, const char *,
                         ValkeyModuleString **value_out, void *) {
        ValkeyModuleString *src =
            (absl::string_view(key->key) == "prefix1_scan1") ? vec1_str.get()
                                                             : vec0_str.get();
        *value_out = src;
        ValkeyModule_RetainString(nullptr, src);
        return VALKEYMODULE_OK;
      });

  std::vector<absl::string_view> prefixes = {"prefix1_"};
  auto schema = MockIndexSchema::Create(
                    &fake_ctx, "swapdb_scan_bp_idx", prefixes,
                    std::make_unique<HashAttributeDataType>(), &scan_bp_pool)
                    .value();
  auto hnsw =
      indexes::VectorHNSW<float>::Create(
          CreateHNSWVectorIndexProto(dimensions, data_model::DISTANCE_METRIC_L2,
                                     8, 16, 100, 10),
          attribute_identifier, data_model::ATTRIBUTE_DATA_TYPE_HASH, 0)
          .value();
  VMSDK_EXPECT_OK(
      schema->AddIndex(attribute_identifier, attribute_identifier, hnsw));

  // Put schema into post-swap recovery with an empty extension queue and
  // backfilling=true (scan_required=true).
  IndexSchema::SwapDbRecoveryState state;
  state.scan_required = true;
  schema->swapdb_recovery_.Get().emplace(std::move(state));
  schema->swapdb_recovery_in_progress_.store(true, std::memory_order_release);

  int scan_step = 0;
  EXPECT_CALL(*kMockValkeyModule,
              Scan(&scan_ctx, testing::An<ValkeyModuleScanCursor *>(),
                   testing::An<ValkeyModuleScanCB>(), testing::An<void *>()))
      .WillRepeatedly([&](ValkeyModuleCtx *ctx, ValkeyModuleScanCursor *,
                          ValkeyModuleScanCB fn, void *privdata) -> int {
        if (scan_step < 2) {
          std::string k = absl::StrCat("prefix1_scan", scan_step++);
          auto k_str = vmsdk::MakeUniqueValkeyString(k);
          ValkeyModuleKey vkey = {.ctx = ctx, .key = k};
          fn(ctx, k_str.get(), &vkey, privdata);
          return 1;
        }
        return 0;
      });

  // First tick: batch_size=10, but max-mutation-queue-size-on-restore=1 stops
  // scanning after 1 key while the worker has not yet drained it.
  EXPECT_EQ(schema->PerformBackfill(&fake_ctx, /*batch_size=*/10), 1u);
  EXPECT_EQ(scan_step, 1);
  EXPECT_EQ(schema->GetMutatedRecordsSize(), 1u);
  EXPECT_TRUE(schema->IsSwapDbRecoveryInProgress());

  // Second tick while queue is still full scans 0 keys and keeps recovery gate.
  EXPECT_EQ(schema->PerformBackfill(&fake_ctx, /*batch_size=*/10), 0u);
  EXPECT_EQ(scan_step, 1);
  EXPECT_TRUE(schema->IsSwapDbRecoveryInProgress());

  // Drain first key, scan second key (again bounded at 1), then drain & finish.
  drain_queued();
  EXPECT_EQ(schema->PerformBackfill(&fake_ctx, /*batch_size=*/10), 1u);
  EXPECT_EQ(scan_step, 2);
  EXPECT_TRUE(schema->IsSwapDbRecoveryInProgress());

  drain_queued();
  EXPECT_EQ(schema->PerformBackfill(&fake_ctx, /*batch_size=*/10), 0u);
  drain_queued();
  EXPECT_FALSE(schema->IsSwapDbRecoveryInProgress());
  EXPECT_EQ(hnsw->GetTrackedKeyCount(), 2u);
}

}  // namespace valkey_search
