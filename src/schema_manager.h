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
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/any_invocable.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/index_schema.h"
#include "src/index_schema.pb.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace coordinator {
class ObjName;
}

constexpr absl::string_view kSchemaManagerMetadataTypeName{"vs_index_schema"};
// The claims on one alias, keyed by claiming index name, and the claimant
// that owns it: highest epoch, then greater index name. A losing claim is
// kept, so every node derives the same owner from the same index protos.
struct AliasClaims {
  absl::flat_hash_map<std::string, uint64_t> epochs;
  std::string owner;
};
using AliasMap = absl::flat_hash_map<std::string, AliasClaims>;
using LosingClaims =
    absl::flat_hash_map<std::string,
                        std::vector<std::pair<std::string, uint64_t>>>;

// Enum for attribute metrics
enum class AttributeType : std::uint8_t { ALL, TEXT, TAG, NUMERIC, VECTOR };

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
  absl::Status RemoveIndexSchema(int db_num, absl::string_view name)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  absl::StatusOr<std::shared_ptr<IndexSchema>> GetIndexSchema(
      int db_num, absl::string_view name) const
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  absl::flat_hash_set<std::string> GetIndexSchemasInDB(int db_num) const;
  // TODO Investigate storing aggregated counters to optimize stats
  // generation.
  uint64_t GetNumberOfIndexSchemas() const;
  uint64_t GetNumberOfAttributes() const;
  uint64_t GetNumberOfTextAttributes() const;
  uint64_t GetNumberOfTagAttributes() const;
  uint64_t GetNumberOfNumericAttributes() const;
  uint64_t GetNumberOfVectorAttributes() const;
  uint64_t GetAttributeCountByType(AttributeType type) const;
  uint64_t GetCorpusNumTextItems() const;

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

  void OnFlushEndDBCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                            uint64_t subevent, void *data)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);

  void OnLoadingCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                         uint64_t subevent, void *data);

  void OnServerCronCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                            uint64_t subevent, void *data);
  void OnShutdownCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                          uint64_t subevent, void *data)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);

  void PopulateFingerprintVersionFromMetadata(int db_num,
                                              absl::string_view name,
                                              uint64_t fingerprint,
                                              uint32_t version);

  static void InitInstance(std::unique_ptr<SchemaManager> instance);
  static SchemaManager &Instance();

  absl::Status LoadIndex(ValkeyModuleCtx *ctx,
                         std::unique_ptr<data_model::RDBSection> section,
                         SupplementalContentIter &&supplemental_iter);
  absl::Status SaveIndexes(ValkeyModuleCtx *ctx, SafeRDB *rdb, int when);
  static absl::StatusOr<uint64_t> ComputeFingerprint(
      const google::protobuf::Any &metadata);
  absl::StatusOr<vmsdk::ValkeyVersion> GetMinVersion() const;

  absl::Status ShowIndexSchemas(ValkeyModuleCtx *ctx,
                                vmsdk::ArgsIterator &itr) const;

  // Alias management.
  absl::Status AddAlias(uint32_t db_num, absl::string_view alias,
                        absl::string_view index_name)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  absl::Status RemoveAlias(uint32_t db_num, absl::string_view alias)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  absl::Status UpdateAlias(uint32_t db_num, absl::string_view alias,
                           absl::string_view index_name)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  std::vector<std::pair<std::string, std::string>> GetAllAliases(
      uint32_t db_num) const ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);

  // Returns the sorted alias names owned by `index_name` in `db_num`.
  std::vector<std::string> GetAliasesForIndex(
      uint32_t db_num, absl::string_view index_name) const
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);

 private:
  absl::Status RemoveAll()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);
  absl::AnyInvocable<void()> server_events_subscriber_callback_;
  bool is_subscribed_to_server_events_ = false;
  vmsdk::ThreadPool *mutations_thread_pool_;
  vmsdk::UniqueValkeyDetachedThreadSafeContext detached_ctx_;

  absl::Status OnMetadataCallback(const coordinator::ObjName &obj_name,
                                  const google::protobuf::Any *metadata,
                                  uint64_t fingerprint, uint32_t version)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);

  absl::Status CreateIndexSchemaInternal(
      ValkeyModuleCtx *ctx, const data_model::IndexSchema &index_schema_proto)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);

  // Normalizes proto fields to match the defaults applied by the IndexSchema
  // constructor. Ensures the stored proto matches what ToProto() produces,
  // preventing spurious MessageDifferencer mismatches.
  static void NormalizeIndexSchemaProtoDefaults(data_model::IndexSchema &proto);

  // Coordinator-mode helper: fetches the stored IndexSchema proto for
  // (db_num, index_name) from MetadataManager, normalizes its defaults, applies
  // `mutate` to it, and re-commits it via CreateEntry. Centralizing the
  // fetch/normalize/commit boilerplate keeps NormalizeIndexSchemaProtoDefaults
  // as the single choke point, so an alias-only edit is never misclassified as
  // a structural change by OnMetadataCallback's MessageDifferencer. The raw
  // MetadataManager status is surfaced so callers can apply their own
  // NotFound policy. Must not be called while holding
  // db_to_index_schemas_mutex_ (CreateEntry reenters via OnMetadataCallback).
  absl::Status MutateIndexProtoInMetadata(
      uint32_t db_num, absl::string_view index_name,
      absl::FunctionRef<void(data_model::IndexSchema &)> mutate)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);

  // Removes other indexes' claims on the aliases `index_name` owns, ahead of
  // dropping it, so those aliases are deleted rather than handed over.
  absl::Status DropLosingClaimsOfOwnedAliases(uint32_t db_num,
                                              absl::string_view index_name)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);

  // Maps each claimant to the (alias, owner epoch) pairs it loses on aliases
  // owned by `index_name`.
  LosingClaims CollectLosingClaimsOfOwnedAliases(
      uint32_t db_num, absl::string_view index_name) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);

  // Removes each listed claim whose epoch is at most the owner epoch.
  absl::Status StripLosingClaims(uint32_t db_num,
                                 const LosingClaims &losers_by_index)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);

  absl::StatusOr<std::shared_ptr<IndexSchema>> RemoveIndexSchemaInternal(
      int db_num, absl::string_view name)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);

  // Replaces the alias claims of `index_name` with those in `proto`.
  void RebuildAliasMapsForIndex(uint32_t db_num, absl::string_view index_name,
                                const data_model::IndexSchema &proto)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);

  // Drops every alias claim of `index_name` in db_num; an alias it owned
  // passes to the next claimant, if any. Used by tombstone handling and
  // RemoveIndexSchemaInternal.
  void EraseAliasesForIndex(uint32_t db_num, absl::string_view index_name)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);

  // Returns the sorted alias names owned by `index_name` in `db_num`, read
  // from the Forward_Alias_Map (the single source of truth for aliases).
  std::vector<std::string> GetAliasesForIndexInternal(
      uint32_t db_num, absl::string_view index_name) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);

  // Returns every alias claim of `index_name` in `db_num`, owned or not.
  std::vector<data_model::IndexSchema::Alias> GetAliasClaimsForIndexInternal(
      uint32_t db_num, absl::string_view index_name) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);

  void SubscribeToServerEventsIfNeeded();

  // GetIndexSchemasInDBInternal returns a set of strings representing the
  // names of the index schemas in the given DB. Note that the set of strings
  // is created through copying out the state at the time of the call. Due to
  // this copy - this should not be used in performance critical paths like
  // FT.SEARCH.
  absl::flat_hash_set<std::string> GetIndexSchemasInDBInternal(int db_num) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);

  mutable absl::Mutex db_to_index_schemas_mutex_;
  absl::flat_hash_map<
      uint32_t, absl::flat_hash_map<std::string, std::shared_ptr<IndexSchema>>>
      db_to_index_schemas_ ABSL_GUARDED_BY(db_to_index_schemas_mutex_);

  // Forward alias map: db_num → {alias → claims and owning index}
  absl::flat_hash_map<uint32_t, AliasMap> db_to_aliases_
      ABSL_GUARDED_BY(db_to_index_schemas_mutex_);

  // Staged changes to index schemas, to be applied on loading ended.
  vmsdk::MainThreadAccessGuard<absl::flat_hash_map<
      uint32_t, absl::flat_hash_map<std::string, std::shared_ptr<IndexSchema>>>>
      staged_db_to_index_schemas_;
  // Staged aliases captured from loaded index protos, swapped into
  // db_to_aliases_ atomically on loading ended. IndexSchema does not carry
  // aliases, so the load path preserves them here (single source of truth).
  vmsdk::MainThreadAccessGuard<absl::flat_hash_map<uint32_t, AliasMap>>
      staged_db_to_aliases_;
  absl::StatusOr<std::shared_ptr<IndexSchema>> LookupInternal(
      int db_num, absl::string_view name) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);
  vmsdk::MainThreadAccessGuard<bool> staging_indices_due_to_repl_load_ = false;

  bool coordinator_enabled_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_SCHEMA_MANAGER_H_
