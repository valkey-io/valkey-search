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

#include "src/schema_manager.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "highwayhash/arch_specific.h"
#include "highwayhash/hh_types.h"
#include "highwayhash/highwayhash.h"
#include "src/coordinator/metadata_manager.h"
#include "src/index_schema.h"
#include "src/index_schema.pb.h"
#include "src/rdb_section.pb.h"
#include "src/rdb_serialization.h"
#include "src/vector_externalizer.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

// Randomly generated 32 bit key for fingerprinting the metadata.
static constexpr highwayhash::HHKey kHashKey{
    0x9736bad976c904ea, 0x08f963a1a52eece9, 0x1ea3f3f773f3b510,
    0x9290a6b4e4db3d51};

static absl::NoDestructor<std::unique_ptr<SchemaManager>>
    schema_manager_instance;

SchemaManager &SchemaManager::Instance() { return **schema_manager_instance; }
void SchemaManager::InitInstance(std::unique_ptr<SchemaManager> instance) {
  *schema_manager_instance = std::move(instance);
}

SchemaManager::SchemaManager(
    RedisModuleCtx *ctx,
    absl::AnyInvocable<void()> server_events_subscriber_callback,
    vmsdk::ThreadPool *mutations_thread_pool, bool coordinator_enabled)
    : server_events_subscriber_callback_(
          std::move(server_events_subscriber_callback)),
      mutations_thread_pool_(mutations_thread_pool),
      detached_ctx_(vmsdk::MakeUniqueRedisDetachedThreadSafeContext(ctx)),
      coordinator_enabled_(coordinator_enabled) {
  RegisterRDBCallback(
      data_model::RDB_SECTION_INDEX_SCHEMA,
      RDBSectionCallbacks{
          .load = [this](RedisModuleCtx *ctx,
                         std::unique_ptr<data_model::RDBSection> section,
                         SupplementalContentIter &&iter) -> absl::Status {
            return LoadIndex(ctx, std::move(section), std::move(iter));
          },

          .save = [this](RedisModuleCtx *ctx, SafeRDB *rdb, int when)
              -> absl::Status { return SaveIndexes(ctx, rdb, when); },

          .section_count = [this](RedisModuleCtx *ctx, int when) -> int {
            return this->GetNumberOfIndexSchemas();
          },
          .minimum_semantic_version = [](RedisModuleCtx *ctx, int when) -> int {
            return 0x010000;  // Always use 1.0.0 for now
          }});
  if (coordinator_enabled) {
    coordinator::MetadataManager::Instance().RegisterType(
        kSchemaManagerMetadataTypeName, kMetadataEncodingVersion,
        ComputeFingerprint,
        [this](absl::string_view id, const google::protobuf::Any *metadata)
            -> absl::Status { return this->OnMetadataCallback(id, metadata); });
  }
}

constexpr uint32_t kIndexSchemaBackfillBatchSize{10240};

absl::Status GenerateIndexNotFoundError(absl::string_view name) {
  return absl::NotFoundError(
      absl::StrFormat("Index with name '%s' not found", name));
}

absl::Status GenerateIndexAlreadyExistsError(absl::string_view name) {
  return absl::AlreadyExistsError(
      absl::StrFormat("Index %s already exists.", name));
}

absl::StatusOr<std::shared_ptr<IndexSchema>> SchemaManager::LookupInternal(
    uint32_t db_num, absl::string_view name) const {
  auto db_itr = db_to_index_schemas_.find(db_num);
  if (db_itr == db_to_index_schemas_.end()) {
    return absl::NotFoundError(absl::StrCat(
        "Index schema not found: ", vmsdk::config::RedactIfNeeded(name)));
  }
  auto name_itr = db_itr->second.find(name);
  if (name_itr == db_itr->second.end()) {
    return absl::NotFoundError(absl::StrCat(
        "Index schema not found: ", vmsdk::config::RedactIfNeeded(name)));
  }
  return name_itr->second;
}

void SchemaManager::SubscribeToServerEventsIfNeeded() {
  if (!is_subscribed_to_server_events_) {
    server_events_subscriber_callback_();
    is_subscribed_to_server_events_ = true;
  }
}

absl::Status SchemaManager::ImportIndexSchema(
    std::shared_ptr<IndexSchema> index_schema) {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);

  uint32_t db_num = index_schema->GetDBNum();
  const std::string &name = index_schema->GetName();
  auto existing_entry = LookupInternal(db_num, name);
  if (existing_entry.ok()) {
    return GenerateIndexAlreadyExistsError(name);
  }

  db_to_index_schemas_[db_num][name] = std::move(index_schema);

  // We delay subscription to the server events until the first index schema
  // is added.
  SubscribeToServerEventsIfNeeded();
  return absl::OkStatus();
}

absl::Status SchemaManager::CreateIndexSchemaInternal(
    RedisModuleCtx *ctx, const data_model::IndexSchema &index_schema_proto) {
  uint32_t db_num = index_schema_proto.db_num();
  const std::string &name = index_schema_proto.name();
  auto existing_entry = LookupInternal(db_num, name);
  if (existing_entry.ok()) {
    return GenerateIndexAlreadyExistsError(index_schema_proto.name());
  }

  VMSDK_ASSIGN_OR_RETURN(
      auto index_schema,
      IndexSchema::Create(ctx, index_schema_proto, mutations_thread_pool_));

  db_to_index_schemas_[db_num][name] = std::move(index_schema);

  // We delay subscription to the server events until the first index schema
  // is added.
  SubscribeToServerEventsIfNeeded();

  return absl::OkStatus();
}

absl::Status SchemaManager::CreateIndexSchema(
    RedisModuleCtx *ctx, const data_model::IndexSchema &index_schema_proto) {
  if (coordinator_enabled_) {
    CHECK(index_schema_proto.db_num() == 0)
        << "In cluster mode, we only support DB 0";
    // In coordinated mode, use the metadata_manager as the source of truth.
    // It will callback into us with the update.
    if (coordinator::MetadataManager::Instance()
            .GetEntry(kSchemaManagerMetadataTypeName, index_schema_proto.name())
            .ok()) {
      return GenerateIndexAlreadyExistsError(index_schema_proto.name());
    }
    auto any_proto = std::make_unique<google::protobuf::Any>();
    any_proto->PackFrom(index_schema_proto);
    return coordinator::MetadataManager::Instance().CreateEntry(
        kSchemaManagerMetadataTypeName, index_schema_proto.name(),
        std::move(any_proto));
  }

  // In non-coordinated mode, apply the update inline.
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  return CreateIndexSchemaInternal(ctx, index_schema_proto);
}

absl::StatusOr<std::shared_ptr<IndexSchema>> SchemaManager::GetIndexSchema(
    uint32_t db_num, absl::string_view name) const {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  auto existing_entry = LookupInternal(db_num, name);
  if (!existing_entry.ok()) {
    return GenerateIndexNotFoundError(name);
  }
  return existing_entry.value();
}

absl::StatusOr<std::shared_ptr<IndexSchema>>
SchemaManager::RemoveIndexSchemaInternal(uint32_t db_num,
                                         absl::string_view name) {
  auto existing_entry = LookupInternal(db_num, name);
  if (!existing_entry.ok()) {
    return GenerateIndexNotFoundError(name);
  }
  auto result = std::move(db_to_index_schemas_[db_num][name]);
  db_to_index_schemas_[db_num].erase(name);
  if (db_to_index_schemas_[db_num].empty()) {
    db_to_index_schemas_.erase(db_num);
  }
  // Mark the index schema as lame duck. Otherwise, if there is a large
  // backlog of mutations, they can keep the index schema alive and cause
  // unnecessary CPU and memory usage.
  result->MarkAsDestructing();
  return result;
}

absl::Status SchemaManager::RemoveIndexSchema(uint32_t db_num,
                                              absl::string_view name) {
  if (coordinator_enabled_) {
    CHECK(db_num == 0) << "In cluster mode, we only support DB 0";
    // In coordinated mode, use the metadata_manager as the source of truth.
    // It will callback into us with the update.
    auto status = coordinator::MetadataManager::Instance().DeleteEntry(
        kSchemaManagerMetadataTypeName, name);
    if (status.ok()) {
      return status;
    } else if (absl::IsNotFound(status)) {
      return GenerateIndexNotFoundError(name);
    } else {
      return absl::InternalError(status.message());
    }
  }

  // In non-coordinated mode, apply the update inline.
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  return RemoveIndexSchemaInternal(db_num, name).status();
}

absl::flat_hash_set<std::string> SchemaManager::GetIndexSchemasInDBInternal(
    uint32_t db_num) const {
  // Copy out the state at the time of the call. Due to the copy - this
  // should not be used in performance critical paths like FT.SEARCH.
  absl::flat_hash_set<std::string> names;
  auto db_itr = db_to_index_schemas_.find(db_num);
  if (db_itr == db_to_index_schemas_.end()) {
    return names;
  }
  for (const auto &[name, entry] : db_itr->second) {
    names.insert(name);
  }
  return names;
}

absl::flat_hash_set<std::string> SchemaManager::GetIndexSchemasInDB(
    uint32_t db_num) const {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  return GetIndexSchemasInDBInternal(db_num);
}

absl::StatusOr<uint64_t> SchemaManager::ComputeFingerprint(
    const google::protobuf::Any &metadata) {
  auto unpacked = std::make_unique<data_model::IndexSchema>();
  if (!metadata.UnpackTo(unpacked.get())) {
    return absl::InternalError(
        "Unable to unpack metadata for index schema fingerprint "
        "calculation");
  }

  // Note that serialization is non-deterministic.
  // https://protobuf.dev/programming-guides/serialization-not-canonical/
  // However, it should be good enough for us assuming the same version of
  // the module is deployed fleet wide. When different versions are
  // deployed, metadata with the latest encoding version is guaranteed to be
  // prioritized by the metadata manager
  std::string serialized_entry;
  if (!unpacked->SerializeToString(&serialized_entry)) {
    return absl::InternalError(
        "Unable to serialize metadata for index schema fingerprint "
        "calculation");
  }
  uint64_t entry_fingerprint;
  highwayhash::HHStateT<HH_TARGET> state(kHashKey);
  highwayhash::HighwayHashT(&state, serialized_entry.data(),
                            serialized_entry.size(), &entry_fingerprint);
  return entry_fingerprint;
}

absl::Status SchemaManager::OnMetadataCallback(
    absl::string_view id, const google::protobuf::Any *metadata) {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  // Note that there is only DB 0 in cluster mode, so we can hardcode this.
  auto status = RemoveIndexSchemaInternal(0, id);
  if (!status.ok() && !absl::IsNotFound(status.status())) {
    return status.status();
  }

  if (metadata == nullptr) {
    return absl::OkStatus();
  }
  auto proposed_schema = std::make_unique<data_model::IndexSchema>();
  if (!metadata->UnpackTo(proposed_schema.get())) {
    return absl::InternalError(absl::StrFormat(
        "Unable to unpack metadata for index schema %s", id.data()));
  }

  auto result =
      CreateIndexSchemaInternal(detached_ctx_.get(), *proposed_schema);
  if (!result.ok()) {
    return result;
  }

  return absl::OkStatus();
}

uint64_t SchemaManager::GetNumberOfIndexSchemas() const {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  auto num_schemas = 0;
  for (const auto &[db_num, schema_map] : db_to_index_schemas_) {
    num_schemas += schema_map.size();
  }
  return num_schemas;
}
uint64_t SchemaManager::GetNumberOfAttributes() const {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  auto num_attributes = 0;
  for (const auto &[db_num, schema_map] : db_to_index_schemas_) {
    for (const auto &[name, schema] : schema_map) {
      num_attributes += schema->GetAttributeCount();
    }
  }
  return num_attributes;
}
uint64_t SchemaManager::GetTotalIndexedDocuments() const {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  auto num_hash_keys = 0;
  for (const auto &[db_num, schema_map] : db_to_index_schemas_) {
    for (const auto &[name, schema] : schema_map) {
      num_hash_keys += schema->GetStats().document_cnt;
    }
  }
  return num_hash_keys;
}
bool SchemaManager::IsIndexingInProgress() const {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  for (const auto &[db_num, schema_map] : db_to_index_schemas_) {
    for (const auto &[name, schema] : schema_map) {
      if (schema->IsBackfillInProgress()) {
        return true;
      }
    }
  }
  return false;
}
IndexSchema::Stats::ResultCnt<uint64_t>
SchemaManager::AccumulateIndexSchemaResults(
    absl::AnyInvocable<const IndexSchema::Stats::ResultCnt<
        std::atomic<uint64_t>> &(const IndexSchema::Stats &) const>
        get_result_cnt_func) const {
  IndexSchema::Stats::ResultCnt<uint64_t> total_cnt;
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  for (const auto &[db_num, schema_map] : db_to_index_schemas_) {
    for (const auto &[name, schema] : schema_map) {
      auto &result_cnt = get_result_cnt_func(schema->GetStats());
      total_cnt.failure_cnt += result_cnt.failure_cnt;
      total_cnt.success_cnt += result_cnt.success_cnt;
      total_cnt.skipped_cnt += result_cnt.skipped_cnt;
    }
  }
  return total_cnt;
}

void SchemaManager::OnFlushDBEnded(RedisModuleCtx *ctx) {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  int selected_db = RedisModule_GetSelectedDb(ctx);
  if (!db_to_index_schemas_.contains(selected_db)) {
    return;
  }

  CHECK(!coordinator_enabled_ || selected_db == 0)
      << "In cluster mode, we only support DB 0";
  auto to_delete = GetIndexSchemasInDBInternal(selected_db);
  VMSDK_LOG(NOTICE, ctx) << "Deleting index schema on FLUSHDB of DB "
                         << selected_db;
  if (coordinator_enabled_) {
    VMSDK_LOG(NOTICE, ctx) << "Recreating index schema on FLUSHDB of DB "
                           << selected_db;
  }
  for (const auto &name : to_delete) {
    VMSDK_LOG(DEBUG, ctx) << "Deleting index schema " << name
                          << " on FLUSHDB of DB " << selected_db;
    auto old_schema = RemoveIndexSchemaInternal(selected_db, name);
    if (!old_schema.ok()) {
      VMSDK_LOG(WARNING, ctx)
          << "Unable to delete index schema "
          << vmsdk::config::RedactIfNeeded(name) << " on FLUSHDB of DB "
          << selected_db << ": " << old_schema.status().message();
      continue;
    }
    if (coordinator_enabled_) {
      // In coordinated mode - we recreate the indices, since they are a
      // cluster-level construct, not a node-level construct. To delete,
      // FT.DROPINDEX must be done explicitly.
      auto to_add = old_schema.value()->ToProto();
      VMSDK_LOG(DEBUG, ctx) << "Recreating index schema " << name
                            << " on FLUSHDB of DB " << selected_db;
      auto add_status = CreateIndexSchemaInternal(ctx, *to_add);
      if (!add_status.ok()) {
        VMSDK_LOG(WARNING, ctx)
            << "Unable to recreate index schema "
            << vmsdk::config::RedactIfNeeded(name) << " on FLUSHDB of DB "
            << selected_db << ": " << add_status.message();
        continue;
      }
    }
  }
}

void SchemaManager::OnSwapDB(RedisModuleSwapDbInfo *swap_db_info) {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  if (swap_db_info->dbnum_first == swap_db_info->dbnum_second) {
    for (auto &schema : db_to_index_schemas_[swap_db_info->dbnum_first]) {
      schema.second->OnSwapDB(swap_db_info);
    }
    return;
  }
  db_to_index_schemas_.insert(
      {swap_db_info->dbnum_first,
       absl::flat_hash_map<std::string, std::shared_ptr<IndexSchema>>()});
  db_to_index_schemas_.insert(
      {swap_db_info->dbnum_second,
       absl::flat_hash_map<std::string, std::shared_ptr<IndexSchema>>()});
  std::swap(db_to_index_schemas_[swap_db_info->dbnum_first],
            db_to_index_schemas_[swap_db_info->dbnum_second]);
  for (auto &schema : db_to_index_schemas_[swap_db_info->dbnum_first]) {
    schema.second->OnSwapDB(swap_db_info);
  }
  for (auto &schema : db_to_index_schemas_[swap_db_info->dbnum_second]) {
    schema.second->OnSwapDB(swap_db_info);
  }
}

void SchemaManager::OnReplicationLoadStart(RedisModuleCtx *ctx) {
  // Only in replication do we stage the changes first, before applying
  // them.
  //
  // Note that we do staging for all replication - even if it isn't diskless. It
  // is effectively the same performance since for disk-based sync, we will
  // first have flushed the DB, so there should be no additional memory
  // pressure, and the final swap from the staging schema set to the live schema
  // set is very cheap.
  VMSDK_LOG(NOTICE, ctx) << "Staging indices during RDB load due to "
                            "replication, will apply on loading finished";
  staging_indices_due_to_repl_load_ = true;
}

void SchemaManager::OnLoadingEnded(RedisModuleCtx *ctx) {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  if (staging_indices_due_to_repl_load_.Get()) {
    // Perform swap of staged schemas to main schemas. Note that no merge
    // occurs here, since for RDB load we are guaranteed that the new state
    // is not applied incrementally.
    VMSDK_LOG(NOTICE, ctx)
        << "Applying staged indices at the end of RDB loading";
    auto status = RemoveAll();
    if (!status.ok()) {
      VMSDK_LOG(WARNING, ctx) << "Failed to remove contents of existing "
                                 "schemas on loading end: "
                              << status.message();
    }
    db_to_index_schemas_ = staged_db_to_index_schemas_.Get();
    staged_db_to_index_schemas_ = absl::flat_hash_map<
        uint32_t,
        absl::flat_hash_map<std::string, std::shared_ptr<IndexSchema>>>();
    staging_indices_due_to_repl_load_ = false;
  }

  for (const auto &[db_num, inner_map] : db_to_index_schemas_) {
    for (const auto &[name, schema] : inner_map) {
      schema->OnLoadingEnded(ctx);
    }
  }
  VectorExternalizer::Instance().ProcessEngineUpdateQueue();
}

void SchemaManager::PerformBackfill(RedisModuleCtx *ctx, uint32_t batch_size) {
  // TODO: Address fairness of index backfill/mutation
  // processing.
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  uint32_t remaining_count = batch_size;
  for (const auto &[db_num, inner_map] : db_to_index_schemas_) {
    for (const auto &[name, schema] : inner_map) {
      remaining_count -= schema->PerformBackfill(ctx, remaining_count);
    }
  }
}

absl::Status SchemaManager::SaveIndexes(RedisModuleCtx *ctx, SafeRDB *rdb,
                                        int when) {
  if (when == REDISMODULE_AUX_BEFORE_RDB) {
    return absl::OkStatus();
  }
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  if (db_to_index_schemas_.empty()) {
    // Auxsave2 will ensure nothing is written to the aux section if we
    // write nothing.
    RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_NOTICE,
                    "Skipping aux metadata for SchemaManager since there "
                    "is no content");
    return absl::OkStatus();
  }

  RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_NOTICE,
                  "Saving aux metadata for SchemaManager to aux RDB");
  for (const auto &[db_num, inner_map] : db_to_index_schemas_) {
    for (const auto &[name, schema] : inner_map) {
      VMSDK_RETURN_IF_ERROR(schema->RDBSave(rdb));
    }
  }
  return absl::OkStatus();
}

absl::Status SchemaManager::RemoveAll() {
  std::vector<std::pair<int, std::string>> to_delete;
  for (const auto &[db_num, inner_map] : db_to_index_schemas_) {
    for (const auto &[name, _] : inner_map) {
      to_delete.push_back(std::make_pair(db_num, name));
    }
  }
  for (const auto &[db_num, name] : to_delete) {
    auto status = RemoveIndexSchemaInternal(db_num, name);
    if (!status.ok()) {
      return status.status();
    }
  }
  return absl::OkStatus();
}

absl::Status SchemaManager::LoadIndex(
    RedisModuleCtx *ctx, std::unique_ptr<data_model::RDBSection> section,
    SupplementalContentIter &&supplemental_iter) {
  // If not subscribed, we need to subscribe now so that we can get the loading
  // ended callback.
  SubscribeToServerEventsIfNeeded();

  if (section->type() != data_model::RDB_SECTION_INDEX_SCHEMA) {
    return absl::InternalError(
        "Unexpected RDB section type passed to SchemaManager");
  }

  // Load the index schema into memory
  auto index_schema_pb = std::unique_ptr<data_model::IndexSchema>(
      section->release_index_schema_contents());
  VMSDK_ASSIGN_OR_RETURN(auto index_schema,
                         IndexSchema::LoadFromRDB(ctx, mutations_thread_pool_,
                                                  std::move(index_schema_pb),
                                                  std::move(supplemental_iter)),
                         _ << "Failed to load index schema from RDB!");
  uint32_t db_num = index_schema->GetDBNum();
  const std::string &name = index_schema->GetName();

  // Select the DB number in the context for subsequent usage.
  if (RedisModule_SelectDb(ctx, db_num) != REDISMODULE_OK) {
    return absl::InternalError(
        absl::StrFormat("Unable to select DB %d for loading of index schema %s",
                        db_num, vmsdk::config::RedactIfNeeded(name).data()));
  }

  // In diskless load scenarios, we stage the index to allow serving from
  // the existing index schemas. The loading ended callback will swap these
  // atomically.
  if (staging_indices_due_to_repl_load_.Get()) {
    VMSDK_LOG(NOTICE, ctx) << "Staging index from RDB: "
                           << vmsdk::config::RedactIfNeeded(name) << " (in db "
                           << db_num << ")";
    staged_db_to_index_schemas_.Get()[db_num][name] = std::move(index_schema);
    return absl::OkStatus();
  }

  // If not staging, we first attempt to remove any existing indices, in
  // case we are loading on top of an existing index schema set. This
  // happens for example when a module triggers RDB load on a running
  // server. In this case, we may have existing indices when we load the DB.
  VMSDK_LOG(NOTICE, detached_ctx_.get())
      << "Loading index from RDB: " << vmsdk::config::RedactIfNeeded(name)
      << " (in db " << db_num << ")";
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  auto remove_existing_status = RemoveIndexSchemaInternal(db_num, name);
  if (remove_existing_status.ok()) {
    RedisModule_Log(detached_ctx_.get(), REDISMODULE_LOGLEVEL_NOTICE,
                    "Deleted existing index from RDB for: %s (in db %d)",
                    vmsdk::config::RedactIfNeeded(name).data(), db_num);
  } else if (!absl::IsNotFound(remove_existing_status.status())) {
    RedisModule_Log(detached_ctx_.get(), REDISMODULE_LOGLEVEL_WARNING,
                    "Failed to delete existing index from RDB for: %s (in db "
                    "%d): %s",
                    vmsdk::config::RedactIfNeeded(name).data(), db_num,
                    remove_existing_status.status().message().data());
  }

  db_to_index_schemas_[db_num][name] = std::move(index_schema);
  return absl::OkStatus();
}

void SchemaManager::OnFlushDBCallback(RedisModuleCtx *ctx, RedisModuleEvent eid,
                                      uint64_t subevent, void *data) {
  if (subevent & REDISMODULE_SUBEVENT_FLUSHDB_END) {
    SchemaManager::Instance().OnFlushDBEnded(ctx);
  }
}

void SchemaManager::OnLoadingCallback(RedisModuleCtx *ctx,
                                      [[maybe_unused]] RedisModuleEvent eid,
                                      uint64_t subevent,
                                      [[maybe_unused]] void *data) {
  if (subevent == REDISMODULE_SUBEVENT_LOADING_ENDED) {
    SchemaManager::Instance().OnLoadingEnded(ctx);
  }
  if (subevent == REDISMODULE_SUBEVENT_LOADING_REPL_START) {
    SchemaManager::Instance().OnReplicationLoadStart(ctx);
  }
}

void SchemaManager::OnServerCronCallback(RedisModuleCtx *ctx,
                                         [[maybe_unused]] RedisModuleEvent eid,
                                         [[maybe_unused]] uint64_t subevent,
                                         [[maybe_unused]] void *data) {
  SchemaManager::Instance().PerformBackfill(ctx, kIndexSchemaBackfillBatchSize);
}

}  // namespace valkey_search
