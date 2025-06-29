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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "src/attribute_data_type.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/grpc_suspender.h"
#include "src/coordinator/metadata_manager.h"
#include "src/coordinator/server.h"
#include "src/coordinator/util.h"
#include "src/index_schema.h"
#include "src/metrics.h"
#include "src/rdb_serialization.h"
#include "src/schema_manager.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "src/vector_externalizer.h"
#include "vmsdk/src/latency_sampler.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

using vmsdk::config::ModuleConfigManager;

static absl::NoDestructor<std::unique_ptr<ValkeySearch>> valkey_search_instance;

constexpr size_t kMaxWorkerThreadPoolSuspensionSec{60};

size_t ValkeySearch::GetMaxWorkerThreadPoolSuspensionSec() const {
  return kMaxWorkerThreadPoolSuspensionSec;
}

ValkeySearch &ValkeySearch::Instance() { return **valkey_search_instance; };

void ValkeySearch::InitInstance(std::unique_ptr<ValkeySearch> instance) {
  *valkey_search_instance = std::move(instance);
}

uint32_t ValkeySearch::GetHNSWBlockSize() const {
  return options::GetHNSWBlockSize().GetValue();
}

void ValkeySearch::SetHNSWBlockSize(uint32_t block_size) {
  options::GetHNSWBlockSize().SetValueOrLog(block_size, WARNING);
}

static std::string ConvertToMB(double bytes_value) {
  const double CONVERSION_VALUE = 1024 * 1024;
  double mb_value = bytes_value / CONVERSION_VALUE;
  auto converted_mb = absl::StrFormat("%.2f", mb_value);
  return absl::StrCat(converted_mb, "M");
}

void ModuleInfo(RedisModuleInfoCtx *ctx, int for_crash_report) {
  ValkeySearch::Instance().Info(ctx, for_crash_report);
}

void AddLatencyStat(RedisModuleInfoCtx *ctx, absl::string_view stat_name,
                    vmsdk::LatencySampler &sampler) {
  // Latency stats are excluded unless they have values, following Valkey engine
  // logic.
  if (sampler.HasSamples()) {
    RedisModule_InfoAddFieldCString(ctx, stat_name.data(),
                                    sampler.GetStatsString().c_str());
  }
}
/* Note: ValkeySearch::Info may be invoked during a crashdump by the engine.
 * In such cases, any section deemed unsafe is skipped.
 * A section is considered unsafe if it involves any of the following:
 *   1. Acquiring locks
 *   2. Performing heap allocations
 *   3. Requiring execution on the main thread
 */
void ValkeySearch::Info(RedisModuleInfoCtx *ctx, bool for_crash_report) const {
  RedisModule_InfoAddSection(ctx, "memory");
  RedisModule_InfoAddFieldLongLong(ctx, "used_memory_bytes",
                                   vmsdk::GetUsedMemoryCnt());
  RedisModule_InfoAddFieldCString(
      ctx, "used_memory_human", ConvertToMB(vmsdk::GetUsedMemoryCnt()).c_str());
  if (!for_crash_report) {
    RedisModule_InfoAddSection(ctx, "index_stats");
    RedisModule_InfoAddFieldLongLong(
        ctx, "number_of_indexes",
        SchemaManager::Instance().GetNumberOfIndexSchemas());
    RedisModule_InfoAddFieldLongLong(
        ctx, "number_of_attributes",
        SchemaManager::Instance().GetNumberOfAttributes());
    RedisModule_InfoAddFieldLongLong(
        ctx, "total_indexed_documents",
        SchemaManager::Instance().GetTotalIndexedDocuments());

    RedisModule_InfoAddSection(ctx, "ingestion");
    RedisModule_InfoAddFieldCString(
        ctx, "background_indexing_status",
        SchemaManager::Instance().IsIndexingInProgress() ? "IN_PROGRESS"
                                                         : "NO_ACTIVITY");
  }
  RedisModule_InfoAddSection(ctx, "thread-pool");
  RedisModule_InfoAddFieldLongLong(ctx, "query_queue_size",
                                   reader_thread_pool_->QueueSize());
  RedisModule_InfoAddFieldLongLong(ctx, "writer_queue_size",
                                   writer_thread_pool_->QueueSize());
  RedisModule_InfoAddFieldLongLong(
      ctx, "worker_pool_suspend_cnt",
      Metrics::GetStats().worker_thread_pool_suspend_cnt);
  RedisModule_InfoAddFieldLongLong(
      ctx, "writer_resumed_cnt",
      Metrics::GetStats().writer_worker_thread_pool_resumed_cnt);
  RedisModule_InfoAddFieldLongLong(
      ctx, "reader_resumed_cnt",
      Metrics::GetStats().reader_worker_thread_pool_resumed_cnt);
  RedisModule_InfoAddFieldLongLong(
      ctx, "writer_suspension_expired_cnt",
      Metrics::GetStats().writer_worker_thread_pool_suspension_expired_cnt);

  RedisModule_InfoAddSection(ctx, "rdb");
  RedisModule_InfoAddFieldLongLong(ctx, "rdb_load_success_cnt",
                                   Metrics::GetStats().rdb_load_success_cnt);
  RedisModule_InfoAddFieldLongLong(ctx, "rdb_load_failure_cnt",
                                   Metrics::GetStats().rdb_load_failure_cnt);
  RedisModule_InfoAddFieldLongLong(ctx, "rdb_save_success_cnt",
                                   Metrics::GetStats().rdb_save_success_cnt);
  RedisModule_InfoAddFieldLongLong(ctx, "rdb_save_failure_cnt",
                                   Metrics::GetStats().rdb_save_failure_cnt);

  RedisModule_InfoAddSection(ctx, "query");
  RedisModule_InfoAddFieldLongLong(
      ctx, "successful_requests_count",
      Metrics::GetStats().query_successful_requests_cnt);
  RedisModule_InfoAddFieldLongLong(
      ctx, "failure_requests_count",
      Metrics::GetStats().query_failed_requests_cnt);
  RedisModule_InfoAddFieldLongLong(
      ctx, "hybrid_requests_count",
      Metrics::GetStats().query_hybrid_requests_cnt);
  RedisModule_InfoAddFieldLongLong(
      ctx, "inline_filtering_requests_count",
      Metrics::GetStats().query_inline_filtering_requests_cnt);

  if (!for_crash_report) {
    auto InfoResultCnt = [ctx](IndexSchema::Stats::ResultCnt<uint64_t> stat,
                               std::string section_name) {
      std::string successful_count_str =
          section_name + "_" + std::string("successful_count");
      std::string failure_count_str =
          section_name + "_" + std::string("failure_count");
      std::string skipped_count_str =
          section_name + "_" + std::string("skipped_count");

      RedisModule_InfoAddFieldLongLong(ctx, successful_count_str.c_str(),
                                       stat.success_cnt);
      RedisModule_InfoAddFieldLongLong(ctx, failure_count_str.c_str(),
                                       stat.failure_cnt);
      RedisModule_InfoAddFieldLongLong(ctx, skipped_count_str.c_str(),
                                       stat.skipped_cnt);
    };
#ifdef DEBUG_INFO
    RedisModule_InfoAddSection(ctx, "subscription");
    InfoResultCnt(
        SchemaManager::Instance().AccumulateIndexSchemaResults(
            [](const IndexSchema::Stats &stats)
                -> const IndexSchema::Stats::ResultCnt<std::atomic<uint64_t>>
                    & { return stats.subscription_add; }),
        "add_subscription");
    InfoResultCnt(
        SchemaManager::Instance().AccumulateIndexSchemaResults(
            [](const IndexSchema::Stats &stats)
                -> const IndexSchema::Stats::ResultCnt<std::atomic<uint64_t>>
                    & { return stats.subscription_modify; }),
        "modify_subscription");
    InfoResultCnt(
        SchemaManager::Instance().AccumulateIndexSchemaResults(
            [](const IndexSchema::Stats &stats)
                -> const IndexSchema::Stats::ResultCnt<std::atomic<uint64_t>>
                    & { return stats.subscription_remove; }),
        "remove_subscription");
#endif
  }
  RedisModule_InfoAddSection(ctx, "hnswlib");
  RedisModule_InfoAddFieldLongLong(ctx, "hnsw_add_exceptions_count",
                                   Metrics::GetStats().hnsw_add_exceptions_cnt);
  RedisModule_InfoAddFieldLongLong(
      ctx, "hnsw_remove_exceptions_count",
      Metrics::GetStats().hnsw_remove_exceptions_cnt);
  RedisModule_InfoAddFieldLongLong(
      ctx, "hnsw_modify_exceptions_count",
      Metrics::GetStats().hnsw_modify_exceptions_cnt);
  RedisModule_InfoAddFieldLongLong(
      ctx, "hnsw_search_exceptions_count",
      Metrics::GetStats().hnsw_search_exceptions_cnt);
  RedisModule_InfoAddFieldLongLong(
      ctx, "hnsw_create_exceptions_count",
      Metrics::GetStats().hnsw_create_exceptions_cnt);

  RedisModule_InfoAddSection(ctx, "latency");
  AddLatencyStat(ctx, "hnsw_vector_index_search_latency_usec",
                 Metrics::GetStats().hnsw_vector_index_search_latency);
  AddLatencyStat(ctx, "flat_vector_index_search_latency_usec",
                 Metrics::GetStats().flat_vector_index_search_latency);

  if (UsingCoordinator()) {
    RedisModule_InfoAddSection(ctx, "coordinator");
    RedisModule_InfoAddFieldLongLong(
        ctx, "coordinator_server_listening_port",
        GetCoordinatorServer()->GetPort());
    RedisModule_InfoAddFieldLongLong(
        ctx, "coordinator_server_get_global_metadata_success_count",
        Metrics::GetStats().coordinator_server_get_global_metadata_success_cnt);
    RedisModule_InfoAddFieldLongLong(
        ctx, "coordinator_server_get_global_metadata_failure_count",
        Metrics::GetStats().coordinator_server_get_global_metadata_failure_cnt);
    RedisModule_InfoAddFieldLongLong(
        ctx, "coordinator_server_search_index_partition_success_count",
        Metrics::GetStats()
            .coordinator_server_search_index_partition_success_cnt);
    RedisModule_InfoAddFieldLongLong(
        ctx, "coordinator_server_search_index_partition_failure_count",
        Metrics::GetStats()
            .coordinator_server_search_index_partition_failure_cnt);
    RedisModule_InfoAddFieldLongLong(
        ctx, "coordinator_client_get_global_metadata_success_count",
        Metrics::GetStats().coordinator_client_get_global_metadata_success_cnt);
    RedisModule_InfoAddFieldLongLong(
        ctx, "coordinator_client_get_global_metadata_failure_count",
        Metrics::GetStats().coordinator_client_get_global_metadata_failure_cnt);
    RedisModule_InfoAddFieldLongLong(
        ctx, "coordinator_client_search_index_partition_success_count",
        Metrics::GetStats()
            .coordinator_client_search_index_partition_success_cnt);
    RedisModule_InfoAddFieldLongLong(
        ctx, "coordinator_client_search_index_partition_failure_count",
        Metrics::GetStats()
            .coordinator_client_search_index_partition_failure_cnt);
    AddLatencyStat(
        ctx, "coordinator_client_get_global_metadata_success_latency_usec",
        Metrics::GetStats()
            .coordinator_client_get_global_metadata_success_latency);
    AddLatencyStat(
        ctx, "coordinator_client_get_global_metadata_failure_latency_usec",
        Metrics::GetStats()
            .coordinator_client_get_global_metadata_failure_latency);
    AddLatencyStat(
        ctx, "coordinator_client_search_index_partition_success_latency_usec",
        Metrics::GetStats()
            .coordinator_client_search_index_partition_success_latency);
    AddLatencyStat(
        ctx, "coordinator_client_search_index_partition_failure_latency_usec",
        Metrics::GetStats()
            .coordinator_client_search_index_partition_failure_latency);
    AddLatencyStat(
        ctx, "coordinator_server_get_global_metadata_success_latency_usec",
        Metrics::GetStats()
            .coordinator_server_get_global_metadata_success_latency);
    AddLatencyStat(
        ctx, "coordinator_server_get_global_metadata_failure_latency_usec",
        Metrics::GetStats()
            .coordinator_server_get_global_metadata_failure_latency);
    AddLatencyStat(
        ctx, "coordinator_server_search_index_partition_success_latency_usec",
        Metrics::GetStats()
            .coordinator_server_search_index_partition_success_latency);
    AddLatencyStat(
        ctx, "coordinator_server_search_index_partition_failure_latency_usec",
        Metrics::GetStats()
            .coordinator_server_search_index_partition_failure_latency);
  }
  if (!for_crash_report) {
    RedisModule_InfoAddSection(ctx, "string_interning");
    RedisModule_InfoAddFieldLongLong(ctx, "string_interning_store_size",
                                     StringInternStore::Instance().Size());
    RedisModule_InfoAddFieldCString(
        ctx, "string_interning_memory_mb", 
        ConvertToMB(StringInternStore::GetMemoryUsage()).c_str());

    RedisModule_InfoAddSection(ctx, "vector_externing");
    auto vector_externing_stats = VectorExternalizer::Instance().GetStats();
    RedisModule_InfoAddFieldLongLong(ctx, "vector_externing_entry_count",
                                     vector_externing_stats.entry_cnt);
    RedisModule_InfoAddFieldLongLong(ctx, "vector_externing_hash_extern_errors",
                                     vector_externing_stats.hash_extern_errors);
    RedisModule_InfoAddFieldLongLong(
        ctx, "vector_externing_generated_value_cnt",
        vector_externing_stats.generated_value_cnt);
    RedisModule_InfoAddFieldLongLong(ctx, "vector_externing_num_lru_entries",
                                     vector_externing_stats.num_lru_entries);
    RedisModule_InfoAddFieldLongLong(ctx, "vector_externing_lru_promote_cnt",
                                     vector_externing_stats.lru_promote_cnt);
    RedisModule_InfoAddFieldLongLong(ctx, "vector_externing_deferred_entry_cnt",
                                     vector_externing_stats.deferred_entry_cnt);
  }
}

// Beside the thread which initiates the fork, no other threads are present
// in the forked child process. This could lead to full sync corruption as the
// fork system-call may occur in the middle of mutating the index. In addition,
// vector insertion may lead to high amount of dirty pages which increases the
// chances to OOM during full sync. Addressing these by temporary suspending the
// writer thread pool during full sync. The writer thread pool resumes once the
// child process dies or suspension time exceeds 60 seconds. Suspending the
// workers guarantees that no thread is mutating the index while the fork is
// happening. For more details see:
// https://pubs.opengroup.org/onlinepubs/009695399/functions/pthread_atfork.html
void ValkeySearch::AtForkPrepare() {
  // Sanity: fork can occur (by example: calling to "popen") before the thread
  // pool is initialized
  if (writer_thread_pool_ == nullptr || reader_thread_pool_ == nullptr) {
    return;
  }
  Metrics::GetStats().worker_thread_pool_suspend_cnt++;
  auto status = writer_thread_pool_->SuspendWorkers();
  VMSDK_LOG(WARNING, nullptr) << "At prepare fork callback, suspend writer "
                                 "worker thread pool returned message: "
                              << status.message();
  status = reader_thread_pool_->SuspendWorkers();
  VMSDK_LOG(WARNING, nullptr) << "At prepare fork callback, suspend reader "
                                 "worker thread pool returned message: "
                              << status.message();
  status = coordinator::GRPCSuspender::Instance().Suspend();
  VMSDK_LOG(WARNING, nullptr) << "At prepare fork callback, suspend gRPC "
                                 "returned message: "
                              << status.message();
}

void ValkeySearch::AfterForkParent() {
  // Sanity: fork can occur (by example: calling to "popen") before the thread
  // pool is initialized
  if (reader_thread_pool_ == nullptr) {
    return;
  }
  auto status = reader_thread_pool_->ResumeWorkers();
  Metrics::GetStats().reader_worker_thread_pool_resumed_cnt++;
  VMSDK_LOG(WARNING, nullptr) << "After fork parent callback, resume reader "
                                 "worker thread pool returned message: "
                              << status.message();
  writer_thread_pool_suspend_watch_ = vmsdk::StopWatch();
  status = coordinator::GRPCSuspender::Instance().Resume();
  VMSDK_LOG(WARNING, nullptr) << "After fork parent callback, resume gRPC "
                                 "returned message: "
                              << status.message();
}

void ValkeySearch::OnServerCronCallback(RedisModuleCtx *ctx,
                                        [[maybe_unused]] RedisModuleEvent eid,
                                        [[maybe_unused]] uint64_t subevent,
                                        [[maybe_unused]] void *data) {
  // Clean-up after threads that exited without being "joined"
  if (writer_thread_pool_) {
    writer_thread_pool_->JoinTerminatedWorkers();
  }
  if (reader_thread_pool_) {
    reader_thread_pool_->JoinTerminatedWorkers();
  }
  // Resume worker thread pool if suspension time exceeds the max allowed
  // duration
  if (writer_thread_pool_suspend_watch_.has_value() &&
      writer_thread_pool_suspend_watch_.value().Duration() >
          absl::Seconds(GetMaxWorkerThreadPoolSuspensionSec())) {
    ResumeWriterThreadPool(ctx, /*is_expired=*/true);
  }
}

void ValkeySearch::OnForkChildCallback(RedisModuleCtx *ctx,
                                       [[maybe_unused]] RedisModuleEvent eid,
                                       uint64_t subevent,
                                       [[maybe_unused]] void *data) {
  if (subevent & REDISMODULE_SUBEVENT_FORK_CHILD_DIED) {
    ResumeWriterThreadPool(ctx, /*is_expired=*/false);
  }
}

absl::StatusOr<std::string> GetConfigGetReply(RedisModuleCtx *ctx, const char *config) {
  auto reply = vmsdk::UniquePtrRedisCallReply(
    RedisModule_Call(ctx, "CONFIG", "cc", "GET", config));
  if (reply == nullptr) {
    return absl::InternalError(
      absl::StrFormat("Failed to get config: %s", config));
  }
  RedisModuleCallReply *config_reply =
    RedisModule_CallReplyArrayElement(reply.get(), 1);

  size_t reply_len;
  const char *reply_str = RedisModule_CallReplyStringPtr(config_reply, &reply_len);
  return std::string(reply_str, reply_len);
}


absl::StatusOr<int> GetRedisLocalPort(RedisModuleCtx *ctx) {
  int port = -1;
  VMSDK_ASSIGN_OR_RETURN(auto tls_port_str, GetConfigGetReply(ctx, "tls-port"));
  if (!absl::SimpleAtoi(tls_port_str, &port)) {
    return absl::InternalError(
      absl::StrFormat("Failed to parse port: %s", tls_port_str));
  }
  if (port == 0) {
    VMSDK_ASSIGN_OR_RETURN(auto port_str, GetConfigGetReply(ctx, "port"));
    if (!absl::SimpleAtoi(port_str, &port)) {
      return absl::InternalError(
        absl::StrFormat("Failed to parse port: %s", port_str));
    }
  }

  if (port < 0) {
    return absl::InternalError("Redis port is negative");
  }
  if (coordinator::GetCoordinatorPort(port) > 65535) {
    return absl::FailedPreconditionError(
        "Coordinator port is too large, Redis port must be less than or equal "
        "to 45241 (max port of 65535 minus coordinator offset of 20294).");
  }
  return port;
}

absl::Status ValkeySearch::Startup(RedisModuleCtx *ctx) {
  reader_thread_pool_ = std::make_unique<vmsdk::ThreadPool>(
      "read-worker-", options::GetReaderThreadCount().GetValue());
  reader_thread_pool_->StartWorkers();
  writer_thread_pool_ = std::make_unique<vmsdk::ThreadPool>(
      "write-worker-", options::GetWriterThreadCount().GetValue());
  writer_thread_pool_->StartWorkers();

  VMSDK_LOG(NOTICE, ctx) << "use_coordinator: "
                         << options::GetUseCoordinator().GetValue()
                         << ", IsCluster: " << IsCluster();

  VMSDK_LOG(NOTICE, ctx) << "Reader workers count: "
                         << reader_thread_pool_->Size();
  VMSDK_LOG(NOTICE, ctx) << "Writer workers count: "
                         << writer_thread_pool_->Size();

  if (options::GetUseCoordinator().GetValue() && IsCluster()) {
    client_pool_ = std::make_unique<coordinator::ClientPool>(
        vmsdk::MakeUniqueRedisDetachedThreadSafeContext(ctx));
    coordinator::MetadataManager::InitInstance(
        std::make_unique<coordinator::MetadataManager>(ctx, *client_pool_));
    coordinator::MetadataManager::Instance().RegisterForClusterMessages(ctx);
  }
  SchemaManager::InitInstance(std::make_unique<SchemaManager>(
      ctx, server_events::SubscribeToServerEvents, writer_thread_pool_.get(),
      options::GetUseCoordinator().GetValue() && IsCluster()));
  if (options::GetUseCoordinator().GetValue()) {
    VMSDK_ASSIGN_OR_RETURN(auto redis_port, GetRedisLocalPort(ctx));
    auto coordinator_port = coordinator::GetCoordinatorPort(redis_port);
    coordinator_ = coordinator::ServerImpl::Create(
        ctx, reader_thread_pool_.get(), coordinator_port);
    if (coordinator_ == nullptr) {
      return absl::InternalError("Failed to create coordinator server");
    }
  }
  return absl::OkStatus();
}

void ValkeySearch::ResumeWriterThreadPool(RedisModuleCtx *ctx,
                                          bool is_expired) {
  auto status = writer_thread_pool_->ResumeWorkers();
  auto msg =
      is_expired
          ? absl::StrFormat(
                "Worker thread pool suspension took more than %lu seconds",
                GetMaxWorkerThreadPoolSuspensionSec())
          : "Fork child died notification received";
  if (is_expired) {
    Metrics::GetStats().writer_worker_thread_pool_suspension_expired_cnt++;
  }
  Metrics::GetStats().writer_worker_thread_pool_resumed_cnt++;
  VMSDK_LOG(WARNING, ctx) << msg
                          << ". Resuming writer "
                             "worker thread pool returned message: "
                          << status.message() << " Suspend duration: "
                          << FormatDuration(writer_thread_pool_suspend_watch_
                                                .value_or(vmsdk::StopWatch())
                                                .Duration());
  writer_thread_pool_suspend_watch_ = std::nullopt;
}

absl::Status ValkeySearch::OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv,
                                  int argc) {
  ctx_ = RedisModule_GetDetachedThreadSafeContext(ctx);

  // Register a single module type for Aux load/save callbacks.
  VMSDK_RETURN_IF_ERROR(RegisterModuleType(ctx));

  // Register all global configuration variables
  VMSDK_RETURN_IF_ERROR(ModuleConfigManager::Instance().Init(ctx));

  // Load configurations to initialize registered configs
  if (RedisModule_LoadConfigs(ctx) != REDISMODULE_OK) {
    return absl::InternalError("Failed to load configurations");
  }

  // Apply command line arguments and initialize the module
  VMSDK_RETURN_IF_ERROR(LoadAndParseArgv(ctx, argv, argc));
  VMSDK_RETURN_IF_ERROR(Startup(ctx));

  RedisModule_SetModuleOptions(
      ctx, REDISMODULE_OPTIONS_HANDLE_IO_ERRORS |
               REDISMODULE_OPTIONS_HANDLE_REPL_ASYNC_LOAD |
               REDISMODULE_OPTION_NO_IMPLICIT_SIGNAL_MODIFIED);
  VMSDK_LOG(NOTICE, ctx) << "Json module is "
                         << (IsJsonModuleLoaded(ctx) ? "" : "not ")
                         << "loaded!";
  VectorExternalizer::Instance().Init(ctx_);
  VMSDK_LOG(DEBUG, ctx) << "Search module completed initialization!";
  return absl::OkStatus();
}

absl::Status ValkeySearch::LoadAndParseArgv(RedisModuleCtx *ctx,
                                            RedisModuleString **argv,
                                            int argc) {
  VMSDK_RETURN_IF_ERROR(
      vmsdk::config::ModuleConfigManager::Instance().ParseAndLoadArgv(ctx, argv,
                                                                      argc));
  // Sanity check
  if ((options::GetReaderThreadCount().GetValue() == 0 &&
       options::GetWriterThreadCount().GetValue() != 0) ||
      (options::GetWriterThreadCount().GetValue() == 0 &&
       options::GetReaderThreadCount().GetValue() != 0)) {
    return absl::InvalidArgumentError(
        "Maintaining query integrity is only supported when both the reader "
        "and writer thread pools are either enabled or disabled "
        "simultaneously");
  }
  return absl::OkStatus();
}

bool ValkeySearch::IsChildProcess() {
  const auto flags = RedisModule_GetContextFlags(nullptr);
  return flags & REDISMODULE_CTX_FLAGS_IS_CHILD;
}

void ValkeySearch::OnUnload(RedisModuleCtx *ctx) {
  RedisModule_FreeThreadSafeContext(ctx_);
  reader_thread_pool_ = nullptr;
}

}  // namespace valkey_search
