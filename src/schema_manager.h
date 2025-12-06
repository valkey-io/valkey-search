/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
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
#include "src/coordinator/metadata_manager.h"
#include "src/index_schema.h"
#include "src/index_schema.pb.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

constexpr absl::string_view kSchemaManagerMetadataTypeName{"vs_index_schema"};
constexpr uint32_t kMetadataEncodingVersion = 1;

namespace options {

/// Return the maximum number of indexes allowed to create.
vmsdk::config::Number &GetMaxIndexes();

}  // namespace options
class SchemaManager {
 public:
  SchemaManager(ValkeyModuleCtx *ctx,
                absl::AnyInvocable<void()> server_events_subscriber_callback,
                vmsdk::ThreadPool *mutations_thread_pool,
                bool coordinator_enabled);
  ~SchemaManager() = default;
  SchemaManager(const SchemaManager &) = delete;
  SchemaManager &operator=(const SchemaManager &) = delete;

  absl::StatusOr<valkey_search::coordinator::IndexFingerprintVersion>
  CreateIndexSchema(ValkeyModuleCtx *ctx,
                    const data_model::IndexSchema &index_schema_proto)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  absl::Status ImportIndexSchema(std::shared_ptr<IndexSchema> index_schema)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  absl::Status RemoveIndexSchema(uint32_t db_num, absl::string_view name)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  absl::StatusOr<std::shared_ptr<IndexSchema>> GetIndexSchema(
      uint32_t db_num, absl::string_view name) const
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  absl::flat_hash_set<std::string> GetIndexSchemasInDB(uint32_t db_num) const;
  // TODO Investigate storing aggregated counters to optimize stats
  // generation.
  uint64_t GetNumberOfIndexSchemas() const;
  uint64_t GetNumberOfAttributes() const;
  uint64_t GetTotalIndexedDocuments() const;
  bool IsIndexingInProgress() const;
  IndexSchema::Stats::ResultCnt<uint64_t> AccumulateIndexSchemaResults(
      absl::AnyInvocable<const IndexSchema::Stats::ResultCnt<
          std::atomic<uint64_t>> &(const IndexSchema::Stats &) const>
          get_result_cnt_func) const;

  void OnFlushDBEnded(ValkeyModuleCtx *ctx);
  void OnSwapDB(ValkeyModuleSwapDbInfo *swap_db_info);

  void OnLoadingEnded(ValkeyModuleCtx *ctx)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  void OnReplicationLoadStart(ValkeyModuleCtx *ctx);

  void PerformBackfill(ValkeyModuleCtx *ctx, uint32_t batch_size)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);

  void OnFlushDBCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                         uint64_t subevent, void *data)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);

  void OnLoadingCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                         uint64_t subevent, void *data);

  void OnServerCronCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                            uint64_t subevent, void *data);

  void PopulateFingerprintVersionFromMetadata(uint32_t db_num,
                                              absl::string_view name,
                                              uint64_t fingerprint,
                                              uint32_t version);
  void UnblockAllAsyncClients();
  void UnblockAllAsyncClientsForDisable();
  static bool IsInitialized();

  // Global aggregation functions for async client client throttling metrics
  uint64_t GetTotalAsyncMutationCount() const;
  uint64_t GetTotalSyncMutationCount() const;

  // Fast atomic operations for blocked client tracking
  void IncrementGlobalBlockedAsyncClients();
  void DecrementGlobalBlockedAsyncClients(size_t count = 1);
  size_t GetGlobalBlockedAsyncClientsCount() const;

  // Global async client duration tracking
  uint64_t GetGlobalAsyncClientLastBlockedDurationUs() const;
  uint64_t GetGlobalAsyncClientCurrentBlockedDurationUs() const;
  void StartGlobalAsyncClientBlocking();
  void StopGlobalAsyncClientBlocking();

  static void InitInstance(std::unique_ptr<SchemaManager> instance);
  static SchemaManager &Instance();

  absl::Status LoadIndex(ValkeyModuleCtx *ctx,
                         std::unique_ptr<data_model::RDBSection> section,
                         SupplementalContentIter &&supplemental_iter);
  absl::Status SaveIndexes(ValkeyModuleCtx *ctx, SafeRDB *rdb, int when);
  static absl::StatusOr<uint64_t> ComputeFingerprint(
      const google::protobuf::Any &metadata);

 private:
  absl::Status RemoveAll()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);
  absl::AnyInvocable<void()> server_events_subscriber_callback_;
  bool is_subscribed_to_server_events_ = false;
  vmsdk::ThreadPool *mutations_thread_pool_;
  vmsdk::UniqueValkeyDetachedThreadSafeContext detached_ctx_;

  absl::Status OnMetadataCallback(absl::string_view id,
                                  const google::protobuf::Any *metadata,
                                  uint64_t fingerprint, uint32_t version)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);

  absl::Status CreateIndexSchemaInternal(
      ValkeyModuleCtx *ctx, const data_model::IndexSchema &index_schema_proto)
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

  // Global async client blocking duration tracking
  std::atomic<uint64_t> global_sm_last_blocked_duration_us_{0};
  std::unique_ptr<vmsdk::StopWatch> global_sm_block_stopwatch_;
  mutable absl::Mutex global_sm_block_mutex_;
  // Global atomic counter for blocked clients
  std::atomic<size_t> global_blocked_sm_clients_count_{0};
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_SCHEMA_MANAGER_H_
