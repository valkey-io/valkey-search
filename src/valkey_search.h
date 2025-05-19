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
  void Info(RedisModuleInfoCtx *ctx, bool for_crash_report) const;

  static long long BlockSizeGetConfig([[maybe_unused]] const char *config_name,
                                      [[maybe_unused]] void *priv_data);
  static int BlockSizeSetConfig([[maybe_unused]] const char *config_name,
                                long long value,
                                [[maybe_unused]] void *priv_data,
                                [[maybe_unused]] RedisModuleString **err);

  IndexSchema::Stats::ResultCnt<uint64_t> AccumulateIndexSchemaResults(
      absl::AnyInvocable<const IndexSchema::Stats::ResultCnt<
          std::atomic<uint64_t>> &(const IndexSchema::Stats &) const>
          get_result_cnt_func) const;
  void OnServerCronCallback(RedisModuleCtx *ctx, RedisModuleEvent eid,
                            uint64_t subevent, void *data);
  void OnForkChildCallback(RedisModuleCtx *ctx, RedisModuleEvent eid,
                           uint64_t subevent, void *data);
  void OnClusterMessageCallback(RedisModuleCtx *ctx, const char *sender_id,
                                uint8_t type, const unsigned char *payload,
                                uint32_t len);
  void SendMetadataBroadcast(RedisModuleCtx *ctx, void *data);
  void AtForkPrepare();
  void AfterForkParent();
  static ValkeySearch &Instance();
  static void InitInstance(std::unique_ptr<ValkeySearch> instance);
  uint32_t GetHNSWBlockSize() const;

  absl::Status OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
  void OnUnload(RedisModuleCtx *ctx);
  friend absl::NoDestructor<ValkeySearch>;

  bool UsingCoordinator() const { return coordinator_ != nullptr; }
  bool IsCluster() const {
    return RedisModule_GetContextFlags(GetBackgroundCtx()) &
           REDISMODULE_CTX_FLAGS_CLUSTER;
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
  // GetBackgroundCtx returns a RedisModuleCtx that is valid for the scope of
  // the module lifetime. Getting this context should be safe for the duration
  // of the program.
  RedisModuleCtx *GetBackgroundCtx() const { return ctx_; }

 protected:
  std::unique_ptr<vmsdk::ThreadPool> reader_thread_pool_;
  std::unique_ptr<vmsdk::ThreadPool> writer_thread_pool_;
  virtual size_t GetMaxWorkerThreadPoolSuspensionSec() const;

 private:
  absl::Status LoadOptions(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);
  static void *RDBLoad(RedisModuleIO *rdb, int encoding_version);
  static void FreeIndexSchema(void *value);
  static bool IsChildProcess();
  void ProcessIndexSchemaBackfill(RedisModuleCtx *ctx, uint32_t batch_size);
  void ResumeWriterThreadPool(RedisModuleCtx *ctx, bool is_expired);

  uint64_t inc_id_{0};
  RedisModuleCtx *ctx_{nullptr};
  std::optional<vmsdk::StopWatch> writer_thread_pool_suspend_watch_;

  std::unique_ptr<coordinator::Server> coordinator_;
  std::unique_ptr<coordinator::ClientPool> client_pool_;
};
void ModuleInfo(RedisModuleInfoCtx *ctx, int for_crash_report);
}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_VALKEY_SEARCH_H_
