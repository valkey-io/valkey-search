/*
 * Copyright (c) 2025, ValkeySearch contributors
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

#ifndef VALKEYSEARCH_SRC_SCHEMA_MANAGER_H_
#define VALKEYSEARCH_SRC_SCHEMA_MANAGER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/index_schema.h"
#include "src/index_schema.pb.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

constexpr absl::string_view kSchemaManagerMetadataTypeName{"vs_index_schema"};
constexpr uint32_t kMetadataEncodingVersion = 1;

class SchemaManager {
 public:
  SchemaManager(RedisModuleCtx *ctx,
                absl::AnyInvocable<void()> server_events_subscriber_callback,
                vmsdk::ThreadPool *mutations_thread_pool,
                bool coordinator_enabled);
  ~SchemaManager() = default;
  SchemaManager(const SchemaManager &) = delete;
  SchemaManager &operator=(const SchemaManager &) = delete;

  absl::Status CreateIndexSchema(
      RedisModuleCtx *ctx, const data_model::IndexSchema &index_schema_proto)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  absl::Status ImportIndexSchema(std::shared_ptr<IndexSchema> index_schema)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  absl::Status RemoveIndexSchema(uint32_t db_num, absl::string_view name)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  absl::StatusOr<std::shared_ptr<IndexSchema>> GetIndexSchema(
      uint32_t db_num, absl::string_view name) const
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  absl::flat_hash_set<std::string> GetIndexSchemasInDB(uint32_t db_num) const;
  // TODO(b/350755299) Investigate storing aggregated counters to optimize stats
  // generation.
  uint64_t GetNumberOfIndexSchemas() const;
  uint64_t GetNumberOfAttributes() const;
  uint64_t GetTotalIndexedHashKeys() const;
  bool IsIndexingInProgress() const;
  IndexSchema::Stats::ResultCnt<uint64_t> AccumulateIndexSchemaResults(
      absl::AnyInvocable<const IndexSchema::Stats::ResultCnt<
          std::atomic<uint64_t>> &(const IndexSchema::Stats &) const>
          get_result_cnt_func) const;

  void OnFlushDBEnded(RedisModuleCtx *ctx);
  void OnSwapDB(RedisModuleSwapDbInfo *swap_db_info);

  // These functions provide cross-compatibility with prior versions
  // TODO(b/349436336) Remove after rolled out.
  void OnSavingStarted(RedisModuleCtx *ctx)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  void OnSavingEnded(RedisModuleCtx *ctx)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  void OnLoadingEnded(RedisModuleCtx *ctx)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  void OnReplicationLoadStart(RedisModuleCtx *ctx);

  void PerformBackfill(RedisModuleCtx *ctx, uint32_t batch_size)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);

  void OnFlushDBCallback(RedisModuleCtx *ctx, RedisModuleEvent eid,
                         uint64_t subevent, void *data)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);

  void OnLoadingCallback(RedisModuleCtx *ctx, RedisModuleEvent eid,
                         uint64_t subevent, void *data);

  void OnServerCronCallback(RedisModuleCtx *ctx, RedisModuleEvent eid,
                            uint64_t subevent, void *data);

  static void InitInstance(std::unique_ptr<SchemaManager> instance);
  static SchemaManager &Instance();

  absl::Status LoadIndex(RedisModuleCtx *ctx,
                         std::unique_ptr<data_model::RDBSection> section,
                         SupplementalContentIter &&supplemental_iter);
  absl::Status SaveIndexes(RedisModuleCtx *ctx, SafeRDB *rdb, int when);

 private:
  absl::Status RemoveAll()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);
  absl::AnyInvocable<void()> server_events_subscriber_callback_;
  bool is_subscribed_to_server_events_ = false;
  vmsdk::ThreadPool *mutations_thread_pool_;
  vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx_;

  static absl::StatusOr<uint64_t> ComputeFingerprint(
      const google::protobuf::Any &metadata);
  absl::Status OnMetadataCallback(absl::string_view id,
                                  const google::protobuf::Any *metadata)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);

  absl::Status CreateIndexSchemaInternal(
      RedisModuleCtx *ctx, const data_model::IndexSchema &index_schema_proto)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);
  absl::StatusOr<std::shared_ptr<IndexSchema>> RemoveIndexSchemaInternal(
      uint32_t db_num, absl::string_view name)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);

  void SubscribeToServerEventsIfNeeded();

  // GetIndexSchemasInDBInternal returns a set of strings representing the
  // names of the index schemas in the given DB. Note that the set of strings
  // is created through copying out the state at the time of the call. Due to
  // this copy - this should not be used in performance critical paths like
  // FT.SEARCH.
  absl::flat_hash_set<std::string> GetIndexSchemasInDBInternal(uint32_t db_num)
      const ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);

  mutable absl::Mutex db_to_index_schemas_mutex_;
  absl::flat_hash_map<
      uint32_t, absl::flat_hash_map<std::string, std::shared_ptr<IndexSchema>>>
      db_to_index_schemas_ ABSL_GUARDED_BY(db_to_index_schemas_mutex_);

  // Staged changes to index schemas, to be applied on loading ended.
  vmsdk::MainThreadAccessGuard<absl::flat_hash_map<
      uint32_t, absl::flat_hash_map<std::string, std::shared_ptr<IndexSchema>>>>
      staged_db_to_index_schemas_;
  absl::StatusOr<std::shared_ptr<IndexSchema>> LookupInternal(
      uint32_t db_num, absl::string_view name) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);
  vmsdk::MainThreadAccessGuard<bool> staging_indices_due_to_repl_load_ = false;

  bool coordinator_enabled_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_SCHEMA_MANAGER_H_
