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

/*

The SchemaManager uses the MetadataManager to manage a single name space of
indexes, i.e., map<string, GlobalMetadataEntry>.

In Valkey 8/Search 1.0 the string was the textual name of the index.
Also, index names with hash tags were considered illegal and rejected, i.e., no
named index could have a valid hash tag as this was explicitly reserved to
prepare for per-slot indexes.

In valkey 9/Search 1.2, we need to extend the naming convention to properly
handle numbered databases. Rather than redesigning the metadata system, the fact
that a legacy index won't have a hash tag is used to provide a backward (and
sorta forward) compatible naming system for indexes. Essentially now, the
metadata name is no longer directly an index_schema name, but rather an
encoded name which decodes to contain three parts: db_num, index_name and
optionally hash tag. The encoding system relies on the 1.0 no-hash-tag
restriction to ensure that the encoded form of a 1.0 index_schema (name, dbnum
== 0, no hash-tag) is just the index name.

*/
struct IndexName {
  // Construct an Encoded Index from an Encoded name
  IndexName(absl::string_view encoded_name);
  // Construct an Encoded Index from a non-Encoded name(aka DecodedName)
  IndexName(uint32_t db_num, absl::string_view decoded_name);

  // return the non-encoded name
  const std::string &GetDecodedName() const { return decoded_name_; }
  const std::string &GetEncodedName() const { return encoded_name_; }
  uint32_t GetDbNum() const { return db_num_; }
  std::optional<absl::string_view> GetHashTag() const { return hash_tag_; }

 private:
  uint32_t db_num_;
  std::optional<absl::string_view> hash_tag_;
  std::string decoded_name_;
  std::string encoded_name_;
};
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
                                  const google::protobuf::Any *metadata)
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
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_SCHEMA_MANAGER_H_
