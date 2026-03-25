/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
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
#include "src/valkey_search.h"
#include "src/vector_externalizer.h"
#include "vmsdk/src/info.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

constexpr absl::string_view kMaxIndexesConfig{"max-indexes"};
constexpr uint32_t kMaxIndexes{10000000};
constexpr uint32_t kMaxIndexesDefault{1000};

constexpr absl::string_view kIndexSchemaBackfillBatchSizeConfig(
    "backfill-batch-size");
constexpr uint32_t kIndexSchemaBackfillBatchSize{10240};

namespace options {

/// Register the "--max-indexes" flag. Controls the max number of indexes we can
/// have.
static auto max_indexes =
    vmsdk::config::NumberBuilder(kMaxIndexesConfig,   // name
                                 kMaxIndexesDefault,  // default size
                                 1,                   // min size
                                 kMaxIndexes)         // max size
        .WithValidationCallback(CHECK_RANGE(1, kMaxIndexes, kMaxIndexesConfig))
        .Build();

vmsdk::config::Number &GetMaxIndexes() {
  return dynamic_cast<vmsdk::config::Number &>(*max_indexes);
}

/// Register the "--backfill-batch-size" flag. Controls the max number of
/// indexes we can have.
static auto backfill_batch_size =
    vmsdk::config::NumberBuilder(kIndexSchemaBackfillBatchSizeConfig,
                                 kIndexSchemaBackfillBatchSize, 1,
                                 std::numeric_limits<int32_t>::max())
        .WithValidationCallback(
            CHECK_RANGE(1, std::numeric_limits<int32_t>::max(),
                        kIndexSchemaBackfillBatchSizeConfig))
        .Build();

vmsdk::config::Number &GetBackfillBatchSize() {
  return dynamic_cast<vmsdk::config::Number &>(*backfill_batch_size);
}

}  // namespace options

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
    ValkeyModuleCtx *ctx,
    absl::AnyInvocable<void()> server_events_subscriber_callback,
    vmsdk::ThreadPool *mutations_thread_pool, bool coordinator_enabled)
    : server_events_subscriber_callback_(
          std::move(server_events_subscriber_callback)),
      mutations_thread_pool_(mutations_thread_pool),
      detached_ctx_(vmsdk::MakeUniqueValkeyDetachedThreadSafeContext(ctx)),
      coordinator_enabled_(coordinator_enabled) {
  RegisterRDBCallback(
      data_model::RDB_SECTION_INDEX_SCHEMA,
      RDBSectionCallbacks{
          .load = [this](ValkeyModuleCtx *ctx,
                         std::unique_ptr<data_model::RDBSection> section,
                         SupplementalContentIter &&iter) -> absl::Status {
            return LoadIndex(ctx, std::move(section), std::move(iter));
          },

          .save = [this](ValkeyModuleCtx *ctx, SafeRDB *rdb, int when)
              -> absl::Status { return SaveIndexes(ctx, rdb, when); },

          .section_count = [this](ValkeyModuleCtx *ctx, int when) -> int {
            return this->GetNumberOfIndexSchemas();
          },
          .minimum_semantic_version =
              [this](ValkeyModuleCtx *ctx, int when) {
                return this->GetMinVersion();
              }});
  // RDB_SECTION_ALIAS_MAP must be registered after RDB_SECTION_INDEX_SCHEMA.
  // LoadAliasMap validates alias targets exist, so indexes must be loaded
  // first. The RDB framework calls load callbacks in registration order.
  RegisterRDBCallback(
      data_model::RDB_SECTION_ALIAS_MAP,
      RDBSectionCallbacks{
          .load = [this](ValkeyModuleCtx *ctx,
                         std::unique_ptr<data_model::RDBSection> section,
                         SupplementalContentIter &&iter) -> absl::Status {
            return LoadAliasMap(ctx, std::move(section), std::move(iter));
          },
          .save = [this](ValkeyModuleCtx *ctx, SafeRDB *rdb, int when)
              -> absl::Status { return SaveAliases(ctx, rdb, when); },
          // Returns 1 if any DB has aliases to persist (all aliases are packed
          // into a single RDB section), 0 otherwise.
          .section_count = [this](ValkeyModuleCtx *ctx, int when) -> int {
            // Only report sections for the AFTER phase, matching SaveAliases.
            if (when == VALKEYMODULE_AUX_BEFORE_RDB) {
              return 0;
            }
            absl::MutexLock lock(&db_to_index_schemas_mutex_);
            for (const auto &kv : db_to_aliases_) {
              if (!kv.second.empty()) return 1;
            }
            return 0;
          },
          .minimum_semantic_version =
              [this](ValkeyModuleCtx *ctx, int when) {
                return this->GetMinVersion();
              }});
  if (coordinator_enabled) {
    coordinator::MetadataManager::Instance().RegisterType(
        kSchemaManagerMetadataTypeName, ComputeFingerprint,
        [this](const coordinator::ObjName &obj_name,
               const google::protobuf::Any *metadata, uint64_t fingerprint,
               uint32_t version) -> absl::Status {
          return this->OnMetadataCallback(obj_name, metadata, fingerprint,
                                          version);
        },
        [this](auto) { return this->GetMinVersion(); });
    coordinator::MetadataManager::Instance().RegisterType(
        kAliasMetadataTypeName, ComputeAliasFingerprint,
        [this](const coordinator::ObjName &obj_name,
               const google::protobuf::Any *metadata, uint64_t fingerprint,
               uint32_t version) -> absl::Status {
          return this->OnAliasMetadataCallback(obj_name, metadata, fingerprint,
                                               version);
        },
        [](const google::protobuf::Any &)
            -> absl::StatusOr<vmsdk::ValkeyVersion> {
          return vmsdk::ValkeyVersion(0);
        });
  }
}

absl::Status GenerateIndexNotFoundError(uint32_t db_num,
                                        absl::string_view name) {
  return absl::NotFoundError(absl::StrFormat(
      "Index with name '%s' not found in database %d", name, db_num));
}

absl::Status GenerateIndexAlreadyExistsError(uint32_t db_num,
                                             absl::string_view name) {
  return absl::AlreadyExistsError(
      absl::StrFormat("Index %s in database %d already exists.", name, db_num));
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
    return GenerateIndexAlreadyExistsError(db_num, name);
  }

  db_to_index_schemas_[db_num][name] = std::move(index_schema);

  // We delay subscription to the server events until the first index schema
  // is added.
  SubscribeToServerEventsIfNeeded();
  return absl::OkStatus();
}

absl::Status SchemaManager::CreateIndexSchemaInternal(
    ValkeyModuleCtx *ctx, const data_model::IndexSchema &index_schema_proto) {
  uint32_t db_num = index_schema_proto.db_num();
  const std::string &name = index_schema_proto.name();
  auto existing_entry = LookupInternal(db_num, name);
  if (existing_entry.ok()) {
    return GenerateIndexAlreadyExistsError(db_num, index_schema_proto.name());
  }

  VMSDK_ASSIGN_OR_RETURN(
      auto index_schema,
      IndexSchema::Create(ctx, index_schema_proto, mutations_thread_pool_,
                          false, false));

  db_to_index_schemas_[db_num][name] = std::move(index_schema);

  // We delay subscription to the server events until the first index schema
  // is added.
  SubscribeToServerEventsIfNeeded();

  return absl::OkStatus();
}

absl::StatusOr<coordinator::IndexFingerprintVersion>
SchemaManager::CreateIndexSchema(
    ValkeyModuleCtx *ctx, const data_model::IndexSchema &index_schema_proto) {
  const auto max_indexes = options::GetMaxIndexes().GetValue();

  VMSDK_RETURN_IF_ERROR(vmsdk::VerifyRange(
      SchemaManager::Instance().GetNumberOfIndexSchemas() + 1, std::nullopt,
      max_indexes))
      << "Maximum number of indexes reached (" << max_indexes
      << "). Cannot create additional indexes.";

  if (coordinator_enabled_) {
    // In coordinated mode, use the metadata_manager as the source of truth.
    // It will callback into us with the update.
    if (coordinator::MetadataManager::Instance()
            .GetEntryContent(kSchemaManagerMetadataTypeName,
                             coordinator::ObjName(index_schema_proto.db_num(),
                                                  index_schema_proto.name()))
            .ok()) {
      return GenerateIndexAlreadyExistsError(index_schema_proto.db_num(),
                                             index_schema_proto.name());
    }
    auto any_proto = std::make_unique<google::protobuf::Any>();
    any_proto->PackFrom(index_schema_proto);
    return coordinator::MetadataManager::Instance().CreateEntry(
        kSchemaManagerMetadataTypeName,
        coordinator::ObjName(index_schema_proto.db_num(),
                             index_schema_proto.name()),
        std::move(any_proto));
  }

  // In non-coordinated mode, apply the update inline.
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  VMSDK_RETURN_IF_ERROR(CreateIndexSchemaInternal(ctx, index_schema_proto));
  // return dummy value in non-cluster mode
  coordinator::IndexFingerprintVersion index_fingerprint_version;
  index_fingerprint_version.set_fingerprint(0);
  index_fingerprint_version.set_version(0);
  return index_fingerprint_version;
}

absl::StatusOr<std::shared_ptr<IndexSchema>> SchemaManager::GetIndexSchema(
    uint32_t db_num, absl::string_view name) const {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  auto existing_entry = LookupInternal(db_num, name);
  if (!existing_entry.ok()) {
    // Try alias resolution
    auto db_alias_it = db_to_aliases_.find(db_num);
    if (db_alias_it != db_to_aliases_.end()) {
      auto alias_it = db_alias_it->second.find(name);
      if (alias_it != db_alias_it->second.end()) {
        existing_entry = LookupInternal(db_num, alias_it->second);
      }
    }
  }
  if (!existing_entry.ok()) {
    return GenerateIndexNotFoundError(db_num, name);
  }
  return existing_entry.value();
}

absl::Status SchemaManager::AddAliasInternal(uint32_t db_num,
                                             absl::string_view alias,
                                             absl::string_view index_name) {
  // find() avoids default-inserting an empty map for db_num.
  auto db_alias_it = db_to_aliases_.find(db_num);
  if (db_alias_it != db_to_aliases_.end()) {
    if (db_alias_it->second.contains(index_name)) {
      return absl::InvalidArgumentError(
          "Unknown index name or name is an alias");
    }
    if (db_alias_it->second.contains(alias)) {
      return absl::AlreadyExistsError("Alias already exists");
    }
  }
  if (!LookupInternal(db_num, index_name).ok()) {
    return GenerateIndexNotFoundError(db_num, index_name);
  }
  db_to_aliases_[db_num][std::string(alias)] = std::string(index_name);
  return absl::OkStatus();
}

absl::Status SchemaManager::AddAlias(uint32_t db_num, absl::string_view alias,
                                     absl::string_view index_name) {
  if (coordinator_enabled_) {
    if (coordinator::MetadataManager::Instance()
            .GetEntryContent(kAliasMetadataTypeName,
                             coordinator::ObjName(db_num, alias))
            .ok()) {
      return absl::AlreadyExistsError("Alias already exists");
    }
    if (coordinator::MetadataManager::Instance()
            .GetEntryContent(kAliasMetadataTypeName,
                             coordinator::ObjName(db_num, index_name))
            .ok()) {
      return absl::InvalidArgumentError(
          "Unknown index name or name is an alias");
    }
    {
      absl::MutexLock lock(&db_to_index_schemas_mutex_);
      if (!LookupInternal(db_num, index_name).ok()) {
        return GenerateIndexNotFoundError(db_num, index_name);
      }
    }
    data_model::AliasEntry entry;
    entry.set_index_name(std::string(index_name));
    auto any_proto = std::make_unique<google::protobuf::Any>();
    any_proto->PackFrom(entry);
    return coordinator::MetadataManager::Instance()
        .CreateEntry(kAliasMetadataTypeName,
                     coordinator::ObjName(db_num, alias), std::move(any_proto))
        .status();
  }
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  return AddAliasInternal(db_num, alias, index_name);
}

absl::Status SchemaManager::RemoveAliasInternal(uint32_t db_num,
                                                absl::string_view alias) {
  // Use find() to avoid default-inserting an empty map for db_num.
  auto db_alias_it = db_to_aliases_.find(db_num);
  if (db_alias_it == db_to_aliases_.end() ||
      !db_alias_it->second.contains(alias)) {
    // Matches RediSearch error message for unknown alias.
    return absl::NotFoundError("Alias does not exist");
  }
  db_alias_it->second.erase(alias);
  return absl::OkStatus();
}

absl::Status SchemaManager::RemoveAlias(uint32_t db_num,
                                        absl::string_view alias) {
  if (coordinator_enabled_) {
    auto status = coordinator::MetadataManager::Instance().DeleteEntry(
        kAliasMetadataTypeName, coordinator::ObjName(db_num, alias));
    if (absl::IsNotFound(status)) {
      return absl::NotFoundError("Alias does not exist");
    }
    return status;
  }
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  return RemoveAliasInternal(db_num, alias);
}

absl::Status SchemaManager::UpdateAliasInternal(uint32_t db_num,
                                                absl::string_view alias,
                                                absl::string_view index_name) {
  // find() avoids default-inserting an empty map for db_num when db_num has
  // no aliases yet (alias-to-alias check would be a false negative otherwise).
  auto db_alias_it = db_to_aliases_.find(db_num);
  if (db_alias_it != db_to_aliases_.end()) {
    if (db_alias_it->second.contains(index_name)) {
      return absl::InvalidArgumentError(
          "Unknown index name or name is an alias");
    }
  }
  if (!LookupInternal(db_num, index_name).ok()) {
    return GenerateIndexNotFoundError(db_num, index_name);
  }
  // ALIASUPDATE is an upsert: creates the alias if absent, reassigns if
  // present.
  db_to_aliases_[db_num][std::string(alias)] = std::string(index_name);
  return absl::OkStatus();
}

absl::Status SchemaManager::UpdateAlias(uint32_t db_num,
                                        absl::string_view alias,
                                        absl::string_view index_name) {
  if (coordinator_enabled_) {
    if (coordinator::MetadataManager::Instance()
            .GetEntryContent(kAliasMetadataTypeName,
                             coordinator::ObjName(db_num, index_name))
            .ok()) {
      return absl::InvalidArgumentError(
          "Unknown index name or name is an alias");
    }
    // Verify the target index exists.
    {
      absl::MutexLock lock(&db_to_index_schemas_mutex_);
      if (!LookupInternal(db_num, index_name).ok()) {
        return GenerateIndexNotFoundError(db_num, index_name);
      }
    }
    data_model::AliasEntry entry;
    entry.set_index_name(std::string(index_name));
    auto any_proto = std::make_unique<google::protobuf::Any>();
    any_proto->PackFrom(entry);
    return coordinator::MetadataManager::Instance()
        .CreateEntry(kAliasMetadataTypeName,
                     coordinator::ObjName(db_num, alias), std::move(any_proto))
        .status();
  }
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  return UpdateAliasInternal(db_num, alias, index_name);
}

absl::StatusOr<std::shared_ptr<IndexSchema>>
SchemaManager::RemoveIndexSchemaInternal(uint32_t db_num,
                                         absl::string_view name) {
  auto existing_entry = LookupInternal(db_num, name);
  if (!existing_entry.ok()) {
    return GenerateIndexNotFoundError(db_num, name);
  }
  auto result = std::move(db_to_index_schemas_[db_num][name]);
  db_to_index_schemas_[db_num].erase(name);
  if (db_to_index_schemas_[db_num].empty()) {
    db_to_index_schemas_.erase(db_num);
  }
  // Purge any aliases that pointed to the dropped index so they don't dangle.
  auto db_alias_it = db_to_aliases_.find(db_num);
  if (db_alias_it != db_to_aliases_.end()) {
    auto &alias_map = db_alias_it->second;
    for (auto it = alias_map.begin(); it != alias_map.end();) {
      if (it->second == name) {
        // Advance before erasing: erasing invalidates the current iterator.
        auto to_erase = it++;
        alias_map.erase(to_erase);
      } else {
        ++it;
      }
    }
    // Remove the empty inner map to avoid accumulating empty entries,
    // consistent with how db_to_index_schemas_ is cleaned up above.
    if (alias_map.empty()) {
      db_to_aliases_.erase(db_alias_it);
    }
  }
  // Mark the index schema as lame duck. Otherwise, if there is a large
  // backlog of mutations, they can keep the index schema alive and cause
  // unnecessary CPU and memory usage.
  result->MarkAsDestructing();
  return result;
}

absl::Status SchemaManager::RemoveIndexSchema(uint32_t db_num,
                                              const absl::string_view name) {
  if (coordinator_enabled_) {
    // Snapshot aliases pointing to this index to tombstone them in
    // MetadataManager before dropping the index schema itself.
    std::vector<std::string> aliases_to_remove;
    {
      absl::MutexLock lock(&db_to_index_schemas_mutex_);
      auto db_alias_it = db_to_aliases_.find(db_num);
      if (db_alias_it != db_to_aliases_.end()) {
        for (const auto &[alias, target] : db_alias_it->second) {
          if (target == name) {
            aliases_to_remove.push_back(alias);
          }
        }
      }
    }
    for (const auto &alias : aliases_to_remove) {
      auto status = coordinator::MetadataManager::Instance().DeleteEntry(
          kAliasMetadataTypeName, coordinator::ObjName(db_num, alias));
      if (!status.ok() && !absl::IsNotFound(status)) {
        VMSDK_LOG(WARNING, detached_ctx_.get())
            << "Failed to delete alias MetadataManager entry for '"
            << vmsdk::config::RedactIfNeeded(alias)
            << "' while dropping index '" << vmsdk::config::RedactIfNeeded(name)
            << "': " << status.message();
      }
    }
    // MetadataManager is the source of truth in coordinator mode; it will
    // callback into us with the update.
    auto status = coordinator::MetadataManager::Instance().DeleteEntry(
        kSchemaManagerMetadataTypeName, coordinator::ObjName(db_num, name));
    if (status.ok()) {
      return status;
    } else if (absl::IsNotFound(status)) {
      return GenerateIndexNotFoundError(db_num, name);
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

std::vector<std::string> SchemaManager::GetAliasesForIndex(
    uint32_t db_num, absl::string_view index_name) const {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  std::vector<std::string> result;
  auto db_alias_it = db_to_aliases_.find(db_num);
  if (db_alias_it != db_to_aliases_.end()) {
    for (const auto &[alias, target] : db_alias_it->second) {
      if (target == index_name) {
        result.push_back(alias);
      }
    }
  }
  std::sort(result.begin(), result.end());
  return result;
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

absl::StatusOr<uint64_t> SchemaManager::ComputeAliasFingerprint(
    const google::protobuf::Any &metadata) {
  data_model::AliasEntry unpacked;
  if (!metadata.UnpackTo(&unpacked)) {
    return absl::InternalError(
        "Unable to unpack metadata for alias fingerprint calculation");
  }
  std::string serialized_entry;
  if (!unpacked.SerializeToString(&serialized_entry)) {
    return absl::InternalError(
        "Unable to serialize metadata for alias fingerprint calculation");
  }
  uint64_t entry_fingerprint;
  highwayhash::HHStateT<HH_TARGET> state(kHashKey);
  highwayhash::HighwayHashT(&state, serialized_entry.data(),
                            serialized_entry.size(), &entry_fingerprint);
  return entry_fingerprint;
}

absl::Status SchemaManager::OnMetadataCallback(
    const coordinator::ObjName &obj_name, const google::protobuf::Any *metadata,
    uint64_t fingerprint, uint32_t version) {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  auto status =
      RemoveIndexSchemaInternal(obj_name.GetDbNum(), obj_name.GetName());
  if (!status.ok() && !absl::IsNotFound(status.status())) {
    return status.status();
  }

  if (metadata == nullptr) {
    return absl::OkStatus();
  }
  auto proposed_schema = std::make_unique<data_model::IndexSchema>();
  if (!metadata->UnpackTo(proposed_schema.get())) {
    return absl::InternalError(
        absl::StrCat("Unable to unpack metadata for index schema ", obj_name));
  }

  VMSDK_RETURN_IF_ERROR(
      CreateIndexSchemaInternal(detached_ctx_.get(), *proposed_schema));

  auto created_schema =
      LookupInternal(obj_name.GetDbNum(), obj_name.GetName()).value();
  CHECK(created_schema != nullptr);
  created_schema->SetFingerprint(fingerprint);
  created_schema->SetVersion(version);

  return absl::OkStatus();
}

absl::Status SchemaManager::OnAliasMetadataCallback(
    const coordinator::ObjName &obj_name, const google::protobuf::Any *metadata,
    uint64_t /*fingerprint*/, uint32_t /*version*/) {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  // Idempotent remove first; ignore NotFound, propagate other errors.
  auto remove_status =
      RemoveAliasInternal(obj_name.GetDbNum(), obj_name.GetName());
  if (!remove_status.ok() && !absl::IsNotFound(remove_status)) {
    return remove_status;
  }
  if (metadata == nullptr) {
    // Tombstone/deletion: alias already removed above.
    return absl::OkStatus();
  }
  data_model::AliasEntry entry;
  if (!metadata->UnpackTo(&entry)) {
    return absl::InternalError(
        absl::StrCat("Unable to unpack alias metadata for ", obj_name));
  }
  uint32_t db_num = obj_name.GetDbNum();
  const std::string &alias = obj_name.GetName();
  const std::string &index_name = entry.index_name();
  if (!LookupInternal(db_num, index_name).ok()) {
    // Target index not yet present; transient during reconciliation.
    VMSDK_LOG(WARNING, detached_ctx_.get())
        << "Alias callback: target index '"
        << vmsdk::config::RedactIfNeeded(index_name)
        << "' not found for alias '" << vmsdk::config::RedactIfNeeded(alias)
        << "' in db " << db_num << "; will retry on next reconciliation";
    return absl::OkStatus();
  }
  db_to_aliases_[db_num][alias] = index_name;
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
  return GetAttributeCountByType(AttributeType::ALL);
}

uint64_t SchemaManager::GetAttributeCountByType(AttributeType type) const {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  uint64_t count = 0;
  for (const auto &[db_num, schema_map] : db_to_index_schemas_) {
    for (const auto &[name, schema] : schema_map) {
      switch (type) {
        case AttributeType::ALL:
          count += schema->GetAttributeCount();
          break;
        case AttributeType::TEXT:
          count += schema->GetTextAttributeCount();
          break;
        case AttributeType::TAG:
          count += schema->GetTagAttributeCount();
          break;
        case AttributeType::NUMERIC:
          count += schema->GetNumericAttributeCount();
          break;
        case AttributeType::VECTOR:
          count += schema->GetVectorAttributeCount();
          break;
      }
    }
  }
  return count;
}

uint64_t SchemaManager::GetNumberOfTextAttributes() const {
  return GetAttributeCountByType(AttributeType::TEXT);
}

uint64_t SchemaManager::GetNumberOfTagAttributes() const {
  return GetAttributeCountByType(AttributeType::TAG);
}

uint64_t SchemaManager::GetNumberOfNumericAttributes() const {
  return GetAttributeCountByType(AttributeType::NUMERIC);
}

uint64_t SchemaManager::GetNumberOfVectorAttributes() const {
  return GetAttributeCountByType(AttributeType::VECTOR);
}

uint64_t SchemaManager::GetCorpusNumTextItems() const {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  uint64_t count = 0;
  for (const auto &[db_num, schema_map] : db_to_index_schemas_) {
    for (const auto &[name, schema] : schema_map) {
      count += schema->GetTextItemCount();
    }
  }
  return count;
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

void SchemaManager::OnFlushDBEnded(ValkeyModuleCtx *ctx) {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  int selected_db = ValkeyModule_GetSelectedDb(ctx);
  if (!db_to_index_schemas_.contains(selected_db)) {
    return;
  }

  auto to_delete = GetIndexSchemasInDBInternal(selected_db);
  VMSDK_LOG(NOTICE, ctx) << "Deleting index schema on FLUSHDB of DB "
                         << selected_db;
  absl::once_flag log_recreate_once;
  for (const auto &name : to_delete) {
    VMSDK_LOG(DEBUG, ctx) << "Deleting index schema "
                          << vmsdk::config::RedactIfNeeded(name)
                          << " on FLUSHDB of DB " << selected_db;

    auto old_schema = RemoveIndexSchemaInternal(selected_db, name);
    if (!old_schema.ok()) {
      VMSDK_LOG(WARNING, ctx) << "Unable to delete index schema "
                              << vmsdk::config::RedactIfNeeded(name)
                              << " on FLUSHDB of DB " << selected_db;
      continue;
    }
    if (coordinator_enabled_) {
      // In coordinator mode, indexes are cluster-level constructs; recreate
      // them after flush. To permanently delete, FT.DROPINDEX must be used.
      absl::call_once(log_recreate_once, [&]() {
        VMSDK_LOG(NOTICE, ctx)
            << "Recreating index schema on FLUSHDB of DB " << selected_db;
      });
      auto to_add = old_schema.value()->ToProto();
      VMSDK_LOG(DEBUG, ctx)
          << "Recreating index schema " << vmsdk::config::RedactIfNeeded(name)
          << " on FLUSHDB of DB " << selected_db;
      auto add_status = CreateIndexSchemaInternal(ctx, *to_add);
      if (!add_status.ok()) {
        VMSDK_LOG(WARNING, ctx) << "Unable to recreate index schema "
                                << vmsdk::config::RedactIfNeeded(name)
                                << " on FLUSHDB of DB " << selected_db;
        continue;
      }
      // Aliases are managed by MetadataManager and reconciled cluster-wide.
    }
  }
}

void SchemaManager::OnSwapDB(ValkeyModuleSwapDbInfo *swap_db_info) {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  if (swap_db_info->dbnum_first == swap_db_info->dbnum_second) {
    // Swapping a DB with itself is a no-op, indexes stay in place and aliases
    // need no changes
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
  // Swap alias maps in lockstep with index schemas.
  // Use find() + conditional insert to avoid polluting db_to_aliases_ with
  // empty maps for DBs that have no aliases.
  auto swap_aliases = [this](uint32_t a, uint32_t b) {
    auto it_a = db_to_aliases_.find(a);
    auto it_b = db_to_aliases_.find(b);
    bool has_a = it_a != db_to_aliases_.end();
    bool has_b = it_b != db_to_aliases_.end();
    if (!has_a && !has_b) return;
    if (has_a && !has_b) {
      // Extract the inner map into a local before calling emplace: emplace may
      // trigger a rehash of db_to_aliases_, which would move all existing
      // elements to a new backing array and invalidate it_a. Extracting first
      // ensures the value lives on the stack and is safe across the rehash.
      auto inner = std::move(it_a->second);
      db_to_aliases_.emplace(b, std::move(inner));
      db_to_aliases_.erase(a);
    } else if (!has_a && has_b) {
      auto inner = std::move(it_b->second);
      db_to_aliases_.emplace(a, std::move(inner));
      db_to_aliases_.erase(b);
    } else {
      std::swap(it_a->second, it_b->second);
    }
  };
  swap_aliases(swap_db_info->dbnum_first, swap_db_info->dbnum_second);
}

void SchemaManager::OnReplicationLoadStart(ValkeyModuleCtx *ctx) {
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

void SchemaManager::OnLoadingEnded(ValkeyModuleCtx *ctx) {
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
                                 "schemas on loading end.";
    }
    db_to_index_schemas_ = staged_db_to_index_schemas_.Get();
    staged_db_to_index_schemas_ = absl::flat_hash_map<
        uint32_t,
        absl::flat_hash_map<std::string, std::shared_ptr<IndexSchema>>>();
    // Aliases are re-loaded from RDB after replication, clear any stale
    // in-memory state so LoadAliasMap starts from a clean slate.
    db_to_aliases_.clear();
    staging_indices_due_to_repl_load_ = false;
  }

  for (const auto &[db_num, inner_map] : db_to_index_schemas_) {
    for (const auto &[name, schema] : inner_map) {
      schema->OnLoadingEnded(ctx);
    }
  }
  VectorExternalizer::Instance().ProcessEngineUpdateQueue();
}

void SchemaManager::PerformBackfill(ValkeyModuleCtx *ctx, uint32_t batch_size) {
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

absl::Status SchemaManager::SaveIndexes(ValkeyModuleCtx *ctx, SafeRDB *rdb,
                                        int when) {
  if (when == VALKEYMODULE_AUX_BEFORE_RDB) {
    return absl::OkStatus();
  }
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  if (db_to_index_schemas_.empty()) {
    // Auxsave2 will ensure nothing is written to the aux section if we
    // write nothing.
    ValkeyModule_Log(ctx, VALKEYMODULE_LOGLEVEL_NOTICE,
                     "Skipping aux metadata for SchemaManager since there "
                     "is no content");
    return absl::OkStatus();
  }

  ValkeyModule_Log(ctx, VALKEYMODULE_LOGLEVEL_NOTICE,
                   "Saving aux metadata for SchemaManager to aux RDB");
  for (const auto &[db_num, inner_map] : db_to_index_schemas_) {
    for (const auto &[name, schema] : inner_map) {
      VMSDK_RETURN_IF_ERROR(schema->RDBSave(rdb));
    }
  }
  return absl::OkStatus();
}

absl::Status SchemaManager::SaveAliases(ValkeyModuleCtx *ctx, SafeRDB *rdb,
                                        int when) {
  if (when == VALKEYMODULE_AUX_BEFORE_RDB) {
    return absl::OkStatus();
  }
  // All aliases across all DBs are packed into a single RDB section proto.
  // This is intentional: aliases are lightweight (two strings per entry) and
  // their total count is implicitly bounded by the max-indexes config.
  absl::MutexLock lock(&db_to_index_schemas_mutex_);

  data_model::AliasMap alias_map_proto;
  for (const auto &[db_num, alias_map] : db_to_aliases_) {
    for (const auto &[alias, index_name] : alias_map) {
      auto *entry = alias_map_proto.add_entries();
      entry->set_db_num(db_num);
      entry->set_alias(alias);
      entry->set_index_name(index_name);
    }
  }

  data_model::RDBSection section;
  section.set_type(data_model::RDB_SECTION_ALIAS_MAP);
  section.set_supplemental_count(0);
  section.mutable_alias_map_contents()->CopyFrom(alias_map_proto);
  std::string serialized = section.SerializeAsString();
  VMSDK_RETURN_IF_ERROR(rdb->SaveStringBuffer(serialized))
      << "IO error while saving alias map to RDB";
  return absl::OkStatus();
}

absl::Status SchemaManager::LoadAliasMap(
    ValkeyModuleCtx *ctx, std::unique_ptr<data_model::RDBSection> section,
    SupplementalContentIter &&supplemental_iter) {
  if (section->type() != data_model::RDB_SECTION_ALIAS_MAP) {
    return absl::InternalError(
        "Unexpected RDB section type passed to SchemaManager alias loader");
  }
  const auto &alias_map_proto = section->alias_map_contents();
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  // ORDERING REQUIREMENT: LookupInternal requires indexes to already be loaded,
  // so RDB_SECTION_INDEX_SCHEMA callbacks must run before this one.
  for (const auto &entry : alias_map_proto.entries()) {
    // Validate that the referenced index exists. If the index is missing the
    // RDB may be partially corrupt; skip the alias with a warning rather than
    // aborting the entire load.
    if (!LookupInternal(entry.db_num(), entry.index_name()).ok()) {
      VMSDK_LOG(WARNING, ctx) << absl::StrFormat(
          "Skipping RDB alias '%s' in db %d: referenced index '%s' not found; "
          "RDB may be corrupt or index registration order was violated",
          entry.alias(), entry.db_num(), entry.index_name());
      continue;
    }
    db_to_aliases_[entry.db_num()][entry.alias()] = entry.index_name();
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
  // Clear all aliases so none dangle after all indexes are removed.
  db_to_aliases_.clear();
  return absl::OkStatus();
}

absl::Status SchemaManager::LoadIndex(
    ValkeyModuleCtx *ctx, std::unique_ptr<data_model::RDBSection> section,
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
    ValkeyModule_Log(detached_ctx_.get(), VALKEYMODULE_LOGLEVEL_NOTICE,
                     "Deleted existing index from RDB for: %s (in db %d)",
                     vmsdk::config::RedactIfNeeded(name).data(), db_num);
  } else if (!absl::IsNotFound(remove_existing_status.status())) {
    ValkeyModule_Log(detached_ctx_.get(), VALKEYMODULE_LOGLEVEL_WARNING,
                     "Failed to delete existing index from RDB for: %s (in db "
                     "%d)",
                     vmsdk::config::RedactIfNeeded(name).data(), db_num);
  }

  db_to_index_schemas_[db_num][name] = std::move(index_schema);

  // Increment completed index counter for restore progress tracking
  Metrics::GetStats().rdb_restore_completed_indexes++;

  return absl::OkStatus();
}

void SchemaManager::OnFlushDBCallback(ValkeyModuleCtx *ctx,
                                      ValkeyModuleEvent eid, uint64_t subevent,
                                      void *data) {
  if (subevent & VALKEYMODULE_SUBEVENT_FLUSHDB_END) {
    SchemaManager::Instance().OnFlushDBEnded(ctx);
  }
}

void SchemaManager::OnLoadingCallback(ValkeyModuleCtx *ctx,
                                      [[maybe_unused]] ValkeyModuleEvent eid,
                                      uint64_t subevent,
                                      [[maybe_unused]] void *data) {
  if (subevent == VALKEYMODULE_SUBEVENT_LOADING_ENDED) {
    SchemaManager::Instance().OnLoadingEnded(ctx);
  }
  if (subevent == VALKEYMODULE_SUBEVENT_LOADING_REPL_START) {
    SchemaManager::Instance().OnReplicationLoadStart(ctx);
  }
}

void SchemaManager::OnServerCronCallback(ValkeyModuleCtx *ctx,
                                         [[maybe_unused]] ValkeyModuleEvent eid,
                                         [[maybe_unused]] uint64_t subevent,
                                         [[maybe_unused]] void *data) {
  SchemaManager::Instance().PerformBackfill(
      ctx, options::GetBackfillBatchSize().GetValue());
}

void SchemaManager::OnShutdownCallback(ValkeyModuleCtx *ctx,
                                       [[maybe_unused]] ValkeyModuleEvent eid,
                                       [[maybe_unused]] uint64_t subevent,
                                       [[maybe_unused]] void *data) {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  if (db_to_index_schemas_.empty()) {
    return;
  }
  VMSDK_LOG(NOTICE, ctx) << "Deleting all index schemas on SHUTDOWN event";
  auto status = RemoveAll();
  if (!status.ok()) {
    VMSDK_LOG(WARNING, ctx)
        << "Failed to delete all index schemas on SHUTDOWN event: "
        << status.message();
  }
}

void SchemaManager::PopulateFingerprintVersionFromMetadata(
    uint32_t db_num, absl::string_view name, uint64_t fingerprint,
    uint32_t version) {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  auto existing_entry = LookupInternal(db_num, name);
  if (existing_entry.ok()) {
    existing_entry.value()->SetFingerprint(fingerprint);
    existing_entry.value()->SetVersion(version);
  }
}

absl::StatusOr<vmsdk::ValkeyVersion> SchemaManager::GetMinVersion() const {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  vmsdk::ValkeyVersion min_version(0);
  for (const auto &[db_num, schema_map] : db_to_index_schemas_) {
    for (const auto &[name, schema] : schema_map) {
      auto any_proto = std::make_unique<google::protobuf::Any>();
      auto proto = schema->ToProto();
      any_proto->PackFrom(*proto);

      VMSDK_ASSIGN_OR_RETURN(auto this_version,
                             IndexSchema::GetMinVersion(*any_proto));
      min_version = std::max(min_version, this_version);
    }
  }
  return min_version;
}

absl::Status SchemaManager::ShowIndexSchemas(ValkeyModuleCtx *ctx,
                                             vmsdk::ArgsIterator &itr) const {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  ValkeyModule_ReplyWithArray(ctx, db_to_index_schemas_.size());
  for (const auto &[db_num, inner_map] : db_to_index_schemas_) {
    ValkeyModule_ReplyWithArray(ctx, 2);
    ValkeyModule_ReplyWithLongLong(ctx, db_num);
    ValkeyModule_ReplyWithArray(ctx, inner_map.size());
    for (const auto &[name, schema] : inner_map) {
      auto proto = schema->ToProto()->DebugString();
      VMSDK_LOG(DEBUG, ctx) << "Index Schema in DB " << db_num << ": "
                            << vmsdk::config::RedactIfNeeded(name) << " "
                            << vmsdk::config::RedactIfNeeded(proto);
      ValkeyModule_ReplyWithStringBuffer(ctx, proto.data(), proto.size());
    }
  }
  return absl::OkStatus();
}

static vmsdk::info_field::Integer number_of_indexes(
    "index_stats", "number_of_indexes",
    vmsdk::info_field::IntegerBuilder().App().Computed([] {
      return SchemaManager::Instance().GetNumberOfIndexSchemas();
    }));
static vmsdk::info_field::Integer number_of_attributes(
    "index_stats", "number_of_attributes",
    vmsdk::info_field::IntegerBuilder().App().Computed([] {
      return SchemaManager::Instance().GetNumberOfAttributes();
    }));
static vmsdk::info_field::Integer number_of_text_attributes(
    "index_stats", "number_of_text_attributes",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([] {
      return SchemaManager::Instance().GetNumberOfTextAttributes();
    }));
static vmsdk::info_field::Integer number_of_tag_attributes(
    "index_stats", "number_of_tag_attributes",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([] {
      return SchemaManager::Instance().GetNumberOfTagAttributes();
    }));
static vmsdk::info_field::Integer number_of_numeric_attributes(
    "index_stats", "number_of_numeric_attributes",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([] {
      return SchemaManager::Instance().GetNumberOfNumericAttributes();
    }));
static vmsdk::info_field::Integer number_of_vector_attributes(
    "index_stats", "number_of_vector_attributes",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([] {
      return SchemaManager::Instance().GetNumberOfVectorAttributes();
    }));
static vmsdk::info_field::Integer corpus_num_text_items(
    "index_stats", "corpus_num_text_items",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([] {
      return SchemaManager::Instance().GetCorpusNumTextItems();
    }));

static vmsdk::info_field::Integer total_indexed_documents(
    "index_stats", "total_indexed_documents",
    vmsdk::info_field::IntegerBuilder().App().Computed([] {
      return SchemaManager::Instance().GetTotalIndexedDocuments();
    }));
static vmsdk::info_field::Integer total_active_write_threads(
    "index_stats", "total_active_write_threads",
    vmsdk::info_field::IntegerBuilder().App().Computed([] {
      auto &valkey_search = valkey_search::ValkeySearch::Instance();
      auto writer_thread_pool = valkey_search.GetWriterThreadPool();
      if (writer_thread_pool) {
        return writer_thread_pool->IsSuspended() ? 0
                                                 : writer_thread_pool->Size();
      }
      return (unsigned long)0;
    }));

}  // namespace valkey_search
