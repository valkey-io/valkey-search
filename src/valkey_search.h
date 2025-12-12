/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_VALKEY_SEARCH_H_
#define VALKEYSEARCH_SRC_VALKEY_SEARCH_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/server.h"
#include "src/index_schema.h"
#include "vmsdk/src/cluster_map.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace server_events {
extern void SubscribeToServerEvents();
}

class ValkeySearch {
 public:
  ValkeySearch() = default;
  virtual ~ValkeySearch() = default;

  bool SupportParallelQueries() const {
    return reader_thread_pool_ && reader_thread_pool_->Size() > 0;
  }

  vmsdk::ThreadPool *GetReaderThreadPool() const {
    return reader_thread_pool_.get();
  }
  vmsdk::ThreadPool *GetWriterThreadPool() const {
    return writer_thread_pool_.get();
  }
  vmsdk::ThreadPool *GetCleanupThreadPool() const {
    return cleanup_thread_pool_.get();
  }
  void ScheduleNeighborCleanup(std::deque<indexes::Neighbor> neighbors) {
    if (cleanup_thread_pool_) {
      cleanup_thread_pool_->Schedule(
          [neighbors = std::move(neighbors)]() mutable { neighbors.clear(); },
          vmsdk::ThreadPool::Priority::kLow);
    }
  }
  void Info(ValkeyModuleInfoCtx *ctx, bool for_crash_report) const;

  IndexSchema::Stats::ResultCnt<uint64_t> AccumulateIndexSchemaResults(
      absl::AnyInvocable<const IndexSchema::Stats::ResultCnt<
          std::atomic<uint64_t>> &(const IndexSchema::Stats &) const>
          get_result_cnt_func) const;
  void OnServerCronCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                            uint64_t subevent, void *data);
  void OnForkChildCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                           uint64_t subevent, void *data);
  void OnClusterMessageCallback(ValkeyModuleCtx *ctx, const char *sender_id,
                                uint8_t type, const unsigned char *payload,
                                uint32_t len);
  void SendMetadataBroadcast(ValkeyModuleCtx *ctx, void *data);
  void AtForkPrepare();
  void AfterForkParent();
  static ValkeySearch &Instance();
  static void InitInstance(std::unique_ptr<ValkeySearch> instance);

  uint32_t GetHNSWBlockSize() const;
  void SetHNSWBlockSize(uint32_t block_size);

  absl::Status OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                      int argc);
  void OnUnload(ValkeyModuleCtx *ctx);
  friend absl::NoDestructor<ValkeySearch>;

  bool UsingCoordinator() const { return coordinator_ != nullptr; }
  bool IsCluster() const {
    return ValkeyModule_GetContextFlags(GetBackgroundCtx()) &
           VALKEYMODULE_CTX_FLAGS_CLUSTER;
  }

  coordinator::ClientPool *GetCoordinatorClientPool() const {
    return client_pool_.get();
  }
  void SetCoordinatorClientPool(
      std::unique_ptr<coordinator::ClientPool> client_pool) {
    client_pool_ = std::move(client_pool);
  }
  coordinator::Server *GetCoordinatorServer() const {
    return coordinator_.get();
  }
  void SetCoordinatorServer(std::unique_ptr<coordinator::Server> server) {
    coordinator_ = std::move(server);
  }
  // GetBackgroundCtx returns a ValkeyModuleCtx that is valid for the scope of
  // the module lifetime. Getting this context should be safe for the duration
  // of the program.
  ValkeyModuleCtx *GetBackgroundCtx() const { return ctx_; }

  // Get or create a new cluster map
  std::shared_ptr<vmsdk::cluster_map::ClusterMap> GetOrRefreshClusterMap(
      ValkeyModuleCtx *ctx);

  // Get current cluster map without refresh (thread-safe)
  std::shared_ptr<vmsdk::cluster_map::ClusterMap> GetClusterMap() const {
    return std::atomic_load(&cluster_map_);
  }

 protected:
  std::unique_ptr<vmsdk::ThreadPool> reader_thread_pool_;
  std::unique_ptr<vmsdk::ThreadPool> writer_thread_pool_;
  std::unique_ptr<vmsdk::ThreadPool> cleanup_thread_pool_;

 private:
  absl::Status Startup(ValkeyModuleCtx *ctx);
  absl::Status LoadAndParseArgv(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                                int argc);

  static void *RDBLoad(ValkeyModuleIO *rdb, int encoding_version);
  static void FreeIndexSchema(void *value);
  static bool IsChildProcess();
  void ProcessIndexSchemaBackfill(ValkeyModuleCtx *ctx, uint32_t batch_size);
  void ResumeWriterThreadPool(ValkeyModuleCtx *ctx, bool is_expired);
  absl::StatusOr<std::string> GetConfigGetReply(ValkeyModuleCtx *ctx,
                                                const char *config);

  uint64_t inc_id_{0};
  ValkeyModuleCtx *ctx_{nullptr};
  std::optional<vmsdk::StopWatch> writer_thread_pool_suspend_watch_;

  std::unique_ptr<coordinator::Server> coordinator_;
  std::unique_ptr<coordinator::ClientPool> client_pool_;
  std::shared_ptr<vmsdk::cluster_map::ClusterMap> cluster_map_;
};
void ModuleInfo(ValkeyModuleInfoCtx *ctx, int for_crash_report);
}  // namespace valkey_search

int (*JSON_GetValue)(ValkeyModuleKey *key, const char *path,
                     ValkeyModuleString **result) VALKEYMODULE_ATTR;
#endif  // VALKEYSEARCH_SRC_VALKEY_SEARCH_H_
