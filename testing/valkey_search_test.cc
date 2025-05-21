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

#include "src/valkey_search.h"

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/attribute_data_type.h"
#include "src/coordinator/metadata_manager.h"
#include "src/index_schema.h"
#include "src/metrics.h"
#include "src/schema_manager.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"
#include "testing/coordinator/common.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/module.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

struct LoadTestCase {
  std::string test_name;
  std::string args;
  std::optional<int> redis_port;
  bool cluster_mode;
  bool use_coordinator{false};
  size_t expected_reader_thread_pool_size{0};
  size_t expected_writer_thread_pool_size{0};
  bool expected_coordinator_started{false};
  int expected_coordinator_port{0};
  int expected_load_ret{0};
  bool expect_thread_pool_started{false};
};

class LoadTest : public ValkeySearchTestWithParam<LoadTestCase> {};

INSTANTIATE_TEST_SUITE_P(
    LoadTests, LoadTest,
    testing::ValuesIn<LoadTestCase>({
        {
            .test_name = "basic",
            .args = "--writer-threads 10 --reader-threads 20",
            .expected_reader_thread_pool_size = 20,
            .expected_writer_thread_pool_size = 10,
            .expect_thread_pool_started = true,
        },
        {
            .test_name = "just_threads",
            .args = "--threads 30",
            .expected_reader_thread_pool_size = 30,
            .expected_writer_thread_pool_size = 75,
            .expect_thread_pool_started = true,
        },
        {
            .test_name = "zero_threads",
            .args = "--writer-threads 10 --reader-threads 20 --threads 0 ",
            .expected_reader_thread_pool_size = 1,
            .expected_writer_thread_pool_size = 1,
            .expect_thread_pool_started = true,
        },
        {
            .test_name = "one_threads",
            .args = "--writer-threads 10 --reader-threads 20 --threads 1 ",
            .expected_reader_thread_pool_size = 1,
            .expected_writer_thread_pool_size = 2,
            .expect_thread_pool_started = true,
        },
        {
            .test_name = "zero_rw_threads",
            .args = "--writer-threads 0 --reader-threads 0 ",
            .expected_reader_thread_pool_size = 0,
            .expected_writer_thread_pool_size = 0,
            .expect_thread_pool_started = true,
        },
        {
            .test_name = "one_rw_threads",
            .args = "--writer-threads 1 --reader-threads 1 ",
            .expected_reader_thread_pool_size = 1,
            .expected_writer_thread_pool_size = 1,
            .expect_thread_pool_started = true,
        },
        {
            .test_name = "threads_wins",
            .args = "--threads 30 --reader-threads 20 --writer-threads 10",
            .expected_reader_thread_pool_size = 30,
            .expected_writer_thread_pool_size = 75,
            .expect_thread_pool_started = true,
        },
        {
            .test_name = "invalid_args",
            .args = "--threads1 30 ",
            .expected_load_ret = 1,
            .expect_thread_pool_started = false,
        },
        {
            .test_name = "use_coordinator",
            .args = "--use-coordinator --writer-threads 10 "
                    "--reader-threads 20",
            .redis_port = 1000,
            .cluster_mode = true,
            .use_coordinator = true,
            .expected_reader_thread_pool_size = 20,
            .expected_writer_thread_pool_size = 10,
            .expected_coordinator_started = true,
            .expected_coordinator_port = 21294,  // 20294 larger than redis_port
            .expect_thread_pool_started = true,
        },
        {
            .test_name = "use_coordinator_false",
            .args = " --writer-threads 10 --reader-threads 20",
            .expected_reader_thread_pool_size = 20,
            .expected_writer_thread_pool_size = 10,
            .expected_coordinator_started = false,
            .expect_thread_pool_started = true,
        },
        {
            .test_name = "use_coordinator_not_cluster",
            .args = "--use-coordinator --writer-threads 10 "
                    "--reader-threads 20",
            .redis_port = 1000,
            .cluster_mode = false,
            .use_coordinator = true,
            .expected_reader_thread_pool_size = 20,
            .expected_writer_thread_pool_size = 10,
            .expected_coordinator_started = true,
            .expected_coordinator_port = 21294,  // 20294 larger than redis_port
            .expect_thread_pool_started = true,
        },
        {
            .test_name = "use_coordinator_not_cluster_fail_to_get_port",
            .args = "--use-coordinator --writer-threads 10 "
                    "--reader-threads 20",
            .redis_port = std::nullopt,
            .cluster_mode = false,
            .use_coordinator = true,
            .expected_reader_thread_pool_size = 20,
            .expected_writer_thread_pool_size = 10,
            .expected_load_ret = 1,
            .expect_thread_pool_started = true,
        },
        {
            .test_name = "use_coordinator_fail_to_get_port",
            .args = "--use-coordinator --writer-threads 10 "
                    "--reader-threads 20",
            .redis_port = std::nullopt,
            .cluster_mode = true,
            .use_coordinator = true,
            .expected_reader_thread_pool_size = 20,
            .expected_writer_thread_pool_size = 10,
            .expected_load_ret = 1,
            .expect_thread_pool_started = true,
        },
        {
            .test_name = "only_read_zero",
            .args = "--reader-threads 0 --writer-threads 10 ",
            .expected_load_ret = 1,
        },
        {
            .test_name = "only_write_zero",
            .args = "--reader-threads 10 --writer-threads 0 ",
            .expected_load_ret = 1,
        },
    }),
    [](const testing::TestParamInfo<LoadTestCase> &info) {
      return info.param.test_name;
    });

TEST_P(LoadTest, load) {
  const LoadTestCase &test_case = GetParam();
  auto args = vmsdk::ToRedisStringVector(test_case.args);
  ON_CALL(*kMockRedisModule, GetDetachedThreadSafeContext(&fake_ctx_))
      .WillByDefault(testing::Return(&fake_ctx_));
  EXPECT_CALL(
      *kMockRedisModule,
      CreateDataType(&fake_ctx_, testing::StrEq(kValkeySearchModuleTypeName),
                     testing::_, testing::_))
      .WillOnce(testing::Return((RedisModuleType *)0xBADF00D));
  if (test_case.expected_load_ret == 0) {
    EXPECT_CALL(*kMockRedisModule,
                Call(testing::_, testing::StrEq("MODULE"), testing::StrEq("c"),
                     testing::StrEq("LIST")))
        .WillOnce(testing::Return(nullptr));
    EXPECT_CALL(
        *kMockRedisModule,
        SetModuleOptions(&fake_ctx_,
                         REDISMODULE_OPTIONS_HANDLE_IO_ERRORS |
                             REDISMODULE_OPTIONS_HANDLE_REPL_ASYNC_LOAD |
                             REDISMODULE_OPTION_NO_IMPLICIT_SIGNAL_MODIFIED))
        .Times(1);
  }
  std::string port_str;
  if (test_case.use_coordinator) {
    if (test_case.redis_port.has_value()) {
      RedisModuleCallReply array_reply;
      RedisModuleCallReply string_reply;
      port_str = std::to_string(test_case.redis_port.value());
      EXPECT_CALL(
          *kMockRedisModule,
          Call(testing::_, testing::StrEq("CONFIG"), testing::StrEq("cc"),
               testing::StrEq("GET"), testing::StrEq("port")))
          .WillOnce(testing::Return(&array_reply));
      EXPECT_CALL(*kMockRedisModule, CallReplyArrayElement(&array_reply, 1))
          .WillOnce(testing::Return(&string_reply));
      EXPECT_CALL(*kMockRedisModule,
                  CallReplyStringPtr(&string_reply, testing::_))
          .WillOnce(testing::Return(port_str.c_str()));
      ON_CALL(*kMockRedisModule, GetMyClusterID())
          .WillByDefault(
              testing::Return("a415b9df6ce0c3c757ad4270242ae432147cacbb"));
    } else {
      EXPECT_CALL(
          *kMockRedisModule,
          Call(testing::_, testing::StrEq("CONFIG"), testing::StrEq("cc"),
               testing::StrEq("GET"), testing::StrEq("port")))
          .WillOnce(testing::Return(nullptr));
    }
  }
  if (test_case.cluster_mode) {
    EXPECT_CALL(*kMockRedisModule, GetContextFlags(&fake_ctx_))
        .WillRepeatedly(testing::Return(REDISMODULE_CTX_FLAGS_CLUSTER));
    EXPECT_CALL(
        *kMockRedisModule,
        RegisterClusterMessageReceiver(
            &fake_ctx_, coordinator::kMetadataBroadcastClusterMessageReceiverId,
            testing::_))
        .Times(1);
  } else {
    EXPECT_CALL(*kMockRedisModule, GetContextFlags(&fake_ctx_))
        .WillRepeatedly(testing::Return(0));
  }
  vmsdk::module::Options options;
  auto load_res = vmsdk::module::OnLoadDone(
      ValkeySearch::Instance().OnLoad(&fake_ctx_, args.data(), args.size()),
      &fake_ctx_, options);
  vmsdk::ResetValkeyAlloc();
  EXPECT_EQ(load_res, test_case.expected_load_ret);
  auto writer_thread_pool = ValkeySearch::Instance().GetWriterThreadPool();
  auto reader_thread_pool = ValkeySearch::Instance().GetReaderThreadPool();
  if (test_case.expect_thread_pool_started) {
    EXPECT_EQ(writer_thread_pool->Size(),
              test_case.expected_writer_thread_pool_size);
    EXPECT_EQ(reader_thread_pool->Size(),
              test_case.expected_reader_thread_pool_size);
  } else {
    EXPECT_FALSE(writer_thread_pool);
    EXPECT_FALSE(reader_thread_pool);
  }
  if (test_case.expected_coordinator_started) {
    EXPECT_THAT(ValkeySearch::Instance().GetCoordinatorServer(),
                testing::NotNull());
    EXPECT_EQ(ValkeySearch::Instance().GetCoordinatorServer()->GetPort(),
              test_case.expected_coordinator_port);
  } else {
    EXPECT_THAT(ValkeySearch::Instance().GetCoordinatorServer(),
                testing::IsNull());
  }
  for (const auto &arg : args) {
    TestRedisModule_FreeString(nullptr, arg);
  }
}

TEST_F(ValkeySearchTest, FullSyncFork) {
  InitThreadPools(2, 2);
  auto writer_thread_pool = ValkeySearch::Instance().GetWriterThreadPool();
  auto reader_thread_pool = ValkeySearch::Instance().GetReaderThreadPool();
  ValkeySearch::Instance().AtForkPrepare();
  EXPECT_TRUE(writer_thread_pool->IsSuspended());
  EXPECT_TRUE(reader_thread_pool->IsSuspended());
  EXPECT_EQ(Metrics::GetStats().reader_worker_thread_pool_resumed_cnt, 0);
  ValkeySearch::Instance().AfterForkParent();
  EXPECT_EQ(Metrics::GetStats().reader_worker_thread_pool_resumed_cnt, 1);
  EXPECT_EQ(Metrics::GetStats().writer_worker_thread_pool_resumed_cnt, 0);
  EXPECT_EQ(
      Metrics::GetStats().writer_worker_thread_pool_suspension_expired_cnt, 0);
  EXPECT_TRUE(writer_thread_pool->IsSuspended());
  EXPECT_FALSE(reader_thread_pool->IsSuspended());
  absl::SleepFor(absl::Seconds(5));
  RedisModuleEvent eid;
  RedisModuleCtx fake_ctx;
  ValkeySearch::Instance().OnServerCronCallback(&fake_ctx, eid, 0, nullptr);
  EXPECT_EQ(Metrics::GetStats().writer_worker_thread_pool_resumed_cnt, 1);
  EXPECT_EQ(
      Metrics::GetStats().writer_worker_thread_pool_suspension_expired_cnt, 1);
  EXPECT_FALSE(writer_thread_pool->IsSuspended());
  EXPECT_FALSE(reader_thread_pool->IsSuspended());
}

TEST_F(ValkeySearchTest, Info) {
  InitThreadPools(10, 5);
  auto writer_thread_pool = ValkeySearch::Instance().GetWriterThreadPool();
  auto reader_thread_pool = ValkeySearch::Instance().GetReaderThreadPool();
  VMSDK_EXPECT_OK(writer_thread_pool->SuspendWorkers());
  VMSDK_EXPECT_OK(reader_thread_pool->SuspendWorkers());
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(writer_thread_pool->Schedule(
        []() {}, vmsdk::ThreadPool::Priority::kHigh));
  }
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(reader_thread_pool->Schedule(
        []() {}, vmsdk::ThreadPool::Priority::kHigh));
  }
  auto test_index_schema =
      CreateVectorHNSWSchema("index_schema_key", nullptr, writer_thread_pool)
          .value();
  auto &index_schema_stats = test_index_schema->stats_;
  index_schema_stats.subscription_remove.failure_cnt = 1;
  index_schema_stats.subscription_remove.success_cnt = 2;
  index_schema_stats.subscription_remove.skipped_cnt = 3;
  index_schema_stats.subscription_modify.failure_cnt = 1;
  index_schema_stats.subscription_modify.success_cnt = 2;
  index_schema_stats.subscription_modify.skipped_cnt = 3;
  index_schema_stats.subscription_add.failure_cnt = 1;
  index_schema_stats.subscription_add.success_cnt = 2;
  index_schema_stats.subscription_add.skipped_cnt = 3;
  index_schema_stats.document_cnt = 4;
  index_schema_stats.backfill_inqueue_tasks = 4;
  {
    absl::MutexLock lock(&index_schema_stats.mutex_);
    index_schema_stats.mutation_queue_size_ = 4;
    index_schema_stats.mutations_queue_delay_ = absl::Seconds(3);
  }

  auto &stats = Metrics::GetStats();
  stats.query_failed_requests_cnt = 1;
  stats.query_successful_requests_cnt = 2;
  stats.query_hybrid_requests_cnt = 1;
  stats.query_inline_filtering_requests_cnt = 2;
  stats.hnsw_add_exceptions_cnt = 3;
  stats.hnsw_remove_exceptions_cnt = 4;
  stats.hnsw_modify_exceptions_cnt = 5;
  stats.hnsw_search_exceptions_cnt = 6;
  stats.hnsw_create_exceptions_cnt = 7;
  stats.flat_add_exceptions_cnt = 8;
  stats.flat_remove_exceptions_cnt = 9;
  stats.flat_modify_exceptions_cnt = 10;
  stats.flat_search_exceptions_cnt = 11;
  stats.flat_create_exceptions_cnt = 12;
  stats.worker_thread_pool_suspend_cnt = 13;
  stats.writer_worker_thread_pool_resumed_cnt = 14;
  stats.reader_worker_thread_pool_resumed_cnt = 15;
  stats.writer_worker_thread_pool_suspension_expired_cnt = 16;
  stats.rdb_load_success_cnt = 17;
  stats.rdb_load_failure_cnt = 18;
  stats.rdb_save_success_cnt = 19;
  stats.rdb_save_failure_cnt = 20;
  stats.hnsw_vector_index_search_latency.SubmitSample(absl::Milliseconds(100));
  stats.hnsw_vector_index_search_latency.SubmitSample(absl::Milliseconds(200));

  auto mock_server = std::make_unique<coordinator::MockServer>();
  ValkeySearch::Instance().SetCoordinatorServer(std::move(mock_server));
  stats.coordinator_client_get_global_metadata_failure_cnt = 21;
  stats.coordinator_client_get_global_metadata_success_cnt = 22;
  stats.coordinator_client_search_index_partition_failure_cnt = 23;
  stats.coordinator_client_search_index_partition_success_cnt = 24;
  stats.coordinator_server_get_global_metadata_failure_cnt = 25;
  stats.coordinator_server_get_global_metadata_success_cnt = 26;
  stats.coordinator_server_search_index_partition_failure_cnt = 27;
  stats.coordinator_server_search_index_partition_success_cnt = 28;
  auto interned_key_1 = StringInternStore::Intern("key1");
  EXPECT_EQ(std::string(*interned_key_1), "key1");
  RedisModuleInfoCtx fake_info_ctx;
  ValkeySearch::Instance().Info(&fake_info_ctx, false);
#ifndef TESTING_TMP_DISABLED
  EXPECT_EQ(
      fake_info_ctx.info_capture.GetInfo(),
      "memory\nused_memory_bytes: 0\nused_memory_human: "
      "'0.00M'\nindex_stats\nnumber_of_indexes: 1\nnumber_of_attributes: "
      "1\ntotal_indexed_hash_documents: "
      "4\ningestion\nbackground_indexing_status: "
      "'IN_PROGRESS'\nthread-pool\nquery_queue_size: 10\nwriter_queue_size: "
      "5\nworker_pool_suspend_cnt: 13\nwriter_resumed_cnt: "
      "14\nreader_resumed_cnt: 15\nwriter_suspension_expired_cnt: "
      "16\nrdb\nrdb_load_success_cnt: 17\nrdb_load_failure_cnt: "
      "18\nrdb_save_success_cnt: 19\nrdb_save_failure_cnt: "
      "20\nquery\nsuccessful_requests_count: 2\nfailure_requests_count: "
      "1\nhybrid_requests_count: 1\ninline_filtering_requests_count: "
      "2\nsubscription\nadd_subscription_successful_count: "
      "2\nadd_subscription_failure_count: 1\nadd_subscription_skipped_count: "
      "3\nmodify_subscription_successful_count: "
      "2\nmodify_subscription_failure_count: "
      "1\nmodify_subscription_skipped_count: "
      "3\nremove_subscription_successful_count: "
      "2\nremove_subscription_failure_count: "
      "1\nremove_subscription_skipped_count: "
      "3\nhnswlib\nhnsw_add_exceptions_count: 3\nhnsw_remove_exceptions_count: "
      "4\nhnsw_modify_exceptions_count: 5\nhnsw_search_exceptions_count: "
      "6\nhnsw_create_exceptions_count: "
      "7\nlatency\nhnsw_vector_index_search_latency_usec: "
      "'p50=100139.007,p99=200278.015,p99.9=200278.015'"
      "\ncoordinator\ncoordinator_server_get_global_metadata_success_count: "
      "26\ncoordinator_server_get_global_metadata_failure_count: "
      "25\ncoordinator_server_search_index_partition_success_count: "
      "28\ncoordinator_server_search_index_partition_failure_count: "
      "27\ncoordinator_client_get_global_metadata_success_count: "
      "22\ncoordinator_client_get_global_metadata_failure_count: "
      "21\ncoordinator_client_search_index_partition_success_count: "
      "24\ncoordinator_client_search_index_partition_failure_count: "
      "23\nstring_interning\nstring_interning_store_size: "
      "1\nvector_externing\nvector_externing_entry_count: "
      "0\nvector_externing_hash_extern_errors: "
      "0\nvector_externing_generated_value_cnt: "
      "0\nvector_externing_num_lru_entries: "
      "0\nvector_externing_lru_promote_cnt: "
      "0\nvector_externing_deferred_entry_cnt: 0\n");
#endif
}

TEST_F(ValkeySearchTest, OnForkChildCallback) {
  InitThreadPools(std::nullopt, 5);
  auto writer_thread_pool = ValkeySearch::Instance().GetWriterThreadPool();
  VMSDK_EXPECT_OK(writer_thread_pool->SuspendWorkers());
  RedisModuleEvent eid;
  Metrics::GetStats().writer_worker_thread_pool_suspension_expired_cnt = 0;
  Metrics::GetStats().writer_worker_thread_pool_resumed_cnt = 0;
  ValkeySearch::Instance().OnForkChildCallback(&fake_ctx_, eid, 0, nullptr);
  EXPECT_TRUE(writer_thread_pool->IsSuspended());
  ValkeySearch::Instance().OnForkChildCallback(
      &fake_ctx_, eid, REDISMODULE_SUBEVENT_FORK_CHILD_DIED, nullptr);
  EXPECT_FALSE(writer_thread_pool->IsSuspended());
  EXPECT_EQ(
      Metrics::GetStats().writer_worker_thread_pool_suspension_expired_cnt, 0);
  EXPECT_EQ(Metrics::GetStats().writer_worker_thread_pool_resumed_cnt, 1);
}

class MockPthreadAtfork {
 public:
  MOCK_METHOD(int, pthread_atfork,
              (void (*prepare)(), void (*parent)(), void (*child)()), ());
};
MockPthreadAtfork mock_pthread_atfork;

int pthread_atfork(void (*prepare)(), void (*parent)(), void (*child)()) {
  return mock_pthread_atfork.pthread_atfork(prepare, parent, child);
}

}  // namespace valkey_search
