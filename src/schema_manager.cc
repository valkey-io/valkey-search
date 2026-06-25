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
#include "google/protobuf/util/message_differencer.h"
#include "highwayhash/arch_specific.h"
#include "highwayhash/hh_types.h"
#include "highwayhash/highwayhash.h"
#include "src/coordinator/metadata_manager.h"
#include "src/index_schema.h"
#include "src/index_schema.pb.h"
#include "src/metrics.h"
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
  }
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

// static
void SchemaManager::NormalizeIndexSchemaProtoDefaults(
    data_model::IndexSchema &proto) {
  // Apply the same defaults that the IndexSchema constructor applies, so
  // that the stored proto matches what ToProto() will produce later. This
  // prevents MessageDifferencer from seeing spurious differences when
  // comparing a fetched proto against a live index's ToProto() output.
  if (proto.min_stem_size() == 0) {
    proto.set_min_stem_size(4);
  }
  if (!proto.has_score()) {
    proto.set_score(IndexSchema::kDefaultDocumentScore);
  }
  // Insert the default empty prefix when none are specified, matching the
  // IndexSchema constructor behavior.
  if (proto.subscribed_key_prefixes().empty()) {
    proto.add_subscribed_key_prefixes("");
  }
  // Sort attributes by alias to match ToProto() output order.
  std::sort(proto.mutable_attributes()->begin(),
            proto.mutable_attributes()->end(),
            [](const data_model::Attribute &a, const data_model::Attribute &b) {
              return a.alias() < b.alias();
            });
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

  // Populate forward alias map from the proto's aliases field.
  for (const auto &alias : index_schema_proto.aliases()) {
    auto existing_it = db_to_aliases_.find(db_num);
    if (existing_it != db_to_aliases_.end()) {
      auto conflict = existing_it->second.find(alias);
      if (conflict != existing_it->second.end() && conflict->second != name) {
        VMSDK_LOG(WARNING, detached_ctx_.get())
            << "Alias '" << alias << "' reassigned from index '"
            << conflict->second << "' to '" << name << "' in db " << db_num;
        // Remove the alias from the previous owner's aliases_ vector.
        auto prev_it = db_to_index_schemas_[db_num].find(conflict->second);
        if (prev_it != db_to_index_schemas_[db_num].end()) {
          auto prev_aliases = prev_it->second->GetAliases();
          prev_aliases.erase(
              std::remove(prev_aliases.begin(), prev_aliases.end(), alias),
              prev_aliases.end());
          prev_it->second->SetAliases(std::move(prev_aliases));
        }
      }
    }
    db_to_aliases_[db_num][alias] = std::string(name);
  }

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

    // Normalize defaults so the stored proto matches what ToProto() would
    // produce after the IndexSchema constructor applies its defaults. This
    // prevents OnMetadataCallback from treating alias-only changes as
    // structural changes due to proto round-trip mismatches.
    data_model::IndexSchema normalized_proto = index_schema_proto;
    NormalizeIndexSchemaProtoDefaults(normalized_proto);

    auto any_proto = std::make_unique<google::protobuf::Any>();
    any_proto->PackFrom(normalized_proto);
    return coordinator::MetadataManager::Instance().CreateEntry(
        kSchemaManagerMetadataTypeName,
        coordinator::ObjName(normalized_proto.db_num(),
                             normalized_proto.name()),
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
  if (existing_entry.ok()) {
    return existing_entry.value();
  }
  // Try alias resolution: check if name is an alias.
  auto db_alias_it = db_to_aliases_.find(db_num);
  if (db_alias_it != db_to_aliases_.end()) {
    auto alias_it = db_alias_it->second.find(name);
    if (alias_it != db_alias_it->second.end()) {
      auto resolved = LookupInternal(db_num, alias_it->second);
      if (resolved.ok()) {
        return resolved.value();
      }
    }
  }
  return GenerateIndexNotFoundError(db_num, name);
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
  // Clean up any aliases pointing to this index.
  EraseAliasesForIndex(db_num, name);
  // Mark the index schema as lame duck. Otherwise, if there is a large
  // backlog of mutations, they can keep the index schema alive and cause
  // unnecessary CPU and memory usage.
  result->MarkAsDestructing();
  return result;
}

absl::Status SchemaManager::RemoveIndexSchema(uint32_t db_num,
                                              const absl::string_view name) {
  if (coordinator_enabled_) {
    // In coordinated mode, use the metadata_manager as the source of truth.
    // It will callback into us with the update.
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

  // Exclude aliases from fingerprint so alias operations don't destabilize
  // search consistency checks.
  unpacked->clear_aliases();
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

// O(n) scan over aliases in this db. Acceptable: alias counts are single-digit
// per index, and this only runs during index removal.
void SchemaManager::EraseAliasesForIndex(uint32_t db_num,
                                         absl::string_view index_name) {
  auto db_alias_it = db_to_aliases_.find(db_num);
  if (db_alias_it == db_to_aliases_.end()) {
    return;
  }
  auto &alias_map = db_alias_it->second;
  for (auto it = alias_map.begin(); it != alias_map.end();) {
    if (it->second == index_name) {
      auto to_erase = it;
      ++it;
      alias_map.erase(to_erase);
    } else {
      ++it;
    }
  }
  if (alias_map.empty()) {
    db_to_aliases_.erase(db_alias_it);
  }
}

void SchemaManager::RebuildAliasMapsForIndex(
    uint32_t db_num, absl::string_view index_name,
    const data_model::IndexSchema &proto) {
  // Remove all existing forward map entries pointing to this index.
  EraseAliasesForIndex(db_num, index_name);

  // Repopulate from proto.
  for (const auto &alias : proto.aliases()) {
    auto existing_it = db_to_aliases_.find(db_num);
    if (existing_it != db_to_aliases_.end()) {
      auto conflict = existing_it->second.find(alias);
      if (conflict != existing_it->second.end() &&
          conflict->second != index_name) {
        VMSDK_LOG(WARNING, detached_ctx_.get())
            << "Alias '" << alias << "' reassigned from index '"
            << conflict->second << "' to '" << index_name << "' in db "
            << db_num;
      }
    }
    db_to_aliases_[db_num][alias] = std::string(index_name);
  }
}

absl::Status SchemaManager::OnMetadataCallback(
    const coordinator::ObjName &obj_name, const google::protobuf::Any *metadata,
    uint64_t fingerprint, uint32_t version) {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);

  // Tombstone: remove everything.
  if (metadata == nullptr) {
    auto status =
        RemoveIndexSchemaInternal(obj_name.GetDbNum(), obj_name.GetName());
    if (!status.ok() && !absl::IsNotFound(status.status())) {
      return status.status();
    }
    return absl::OkStatus();
  }
  auto proposed_schema = std::make_unique<data_model::IndexSchema>();
  if (!metadata->UnpackTo(proposed_schema.get())) {
    return absl::InternalError(
        absl::StrCat("Unable to unpack metadata for index schema ", obj_name));
  }

  auto existing = LookupInternal(obj_name.GetDbNum(), obj_name.GetName());
  if (existing.ok()) {
    // Compare ignoring aliases and stats fields.
    google::protobuf::util::MessageDifferencer differ;
    const auto *descriptor = data_model::IndexSchema::descriptor();
    differ.IgnoreField(descriptor->FindFieldByName("aliases"));
    differ.IgnoreField(descriptor->FindFieldByName("stats"));
    differ.TreatAsSet(descriptor->FindFieldByName("attributes"));

    auto existing_proto = existing.value()->ToProto();
    if (differ.Compare(*existing_proto, *proposed_schema)) {
      // Alias-only change: rebuild maps, update alias list and
      // fingerprint/version.
      RebuildAliasMapsForIndex(obj_name.GetDbNum(), obj_name.GetName(),
                               *proposed_schema);
      std::vector<std::string> new_aliases(proposed_schema->aliases().begin(),
                                           proposed_schema->aliases().end());
      existing.value()->SetAliases(std::move(new_aliases));
      existing.value()->SetFingerprint(fingerprint);
      existing.value()->SetVersion(version);
      return absl::OkStatus();
    }

    // Structural change: full teardown + rebuild.
    auto status =
        RemoveIndexSchemaInternal(obj_name.GetDbNum(), obj_name.GetName());
    if (!status.ok() && !absl::IsNotFound(status.status())) {
      return status.status();
    }
  }

  // First creation or structural change: create fresh.
  VMSDK_RETURN_IF_ERROR(
      CreateIndexSchemaInternal(detached_ctx_.get(), *proposed_schema));

  auto created_schema =
      LookupInternal(obj_name.GetDbNum(), obj_name.GetName()).value();
  CHECK(created_schema != nullptr);
  created_schema->SetFingerprint(fingerprint);
  created_schema->SetVersion(version);

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
      // In coordinated mode - we recreate the indices, since they are a
      // cluster-level construct, not a node-level construct. To delete,
      // FT.DROPINDEX must be done explicitly.
      absl::call_once(log_recreate_once, [&]() {
        VMSDK_LOG(NOTICE, ctx)
            << "Recreating index schema on FLUSHDB of DB " << selected_db;
      });
      VMSDK_LOG(DEBUG, ctx)
          << "Recreating index schema " << vmsdk::config::RedactIfNeeded(name)
          << " on FLUSHDB of DB " << selected_db;
      // Fetch the authoritative stored proto from MetadataManager (includes
      // aliases). We do NOT rely on ToProto() for alias data per requirement
      // 6.3.
      auto stored_proto_or =
          coordinator::MetadataManager::Instance().GetEntryContent(
              kSchemaManagerMetadataTypeName,
              coordinator::ObjName(selected_db, name));
      if (!stored_proto_or.ok()) {
        if (absl::IsNotFound(stored_proto_or.status())) {
          // Entry genuinely absent — fall back to ToProto().
          auto to_add = old_schema.value()->ToProto();
          auto add_status = CreateIndexSchemaInternal(ctx, *to_add);
          if (!add_status.ok()) {
            VMSDK_LOG(WARNING, ctx) << "Unable to recreate index schema "
                                    << vmsdk::config::RedactIfNeeded(name)
                                    << " on FLUSHDB of DB " << selected_db;
          }
        } else {
          VMSDK_LOG(WARNING, ctx)
              << "MetadataManager lookup failed for "
              << vmsdk::config::RedactIfNeeded(name)
              << " on FLUSHDB of DB " << selected_db << ": "
              << stored_proto_or.status().message();
        }
        continue;
      }
      data_model::IndexSchema stored_schema;
      if (!stored_proto_or.value().UnpackTo(&stored_schema)) {
        VMSDK_LOG(WARNING, ctx) << "Unable to unpack stored proto for "
                                << vmsdk::config::RedactIfNeeded(name)
                                << " on FLUSHDB of DB " << selected_db;
        continue;
      }
      auto add_status = CreateIndexSchemaInternal(ctx, stored_schema);
      if (!add_status.ok()) {
        VMSDK_LOG(WARNING, ctx) << "Unable to recreate index schema "
                                << vmsdk::config::RedactIfNeeded(name)
                                << " on FLUSHDB of DB " << selected_db;
        continue;
      }
    }
  }
}

void SchemaManager::OnSwapDB(ValkeyModuleSwapDbInfo *swap_db_info) {
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
  // Swap the forward alias map between the two databases.
  db_to_aliases_.insert({static_cast<uint32_t>(swap_db_info->dbnum_first), {}});
  db_to_aliases_.insert(
      {static_cast<uint32_t>(swap_db_info->dbnum_second), {}});
  std::swap(db_to_aliases_[swap_db_info->dbnum_first],
            db_to_aliases_[swap_db_info->dbnum_second]);
  for (auto &schema : db_to_index_schemas_[swap_db_info->dbnum_first]) {
    schema.second->OnSwapDB(swap_db_info);
  }
  for (auto &schema : db_to_index_schemas_[swap_db_info->dbnum_second]) {
    schema.second->OnSwapDB(swap_db_info);
  }
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
    staging_indices_due_to_repl_load_ = false;

    // Rebuild the forward alias map from the newly swapped-in schemas.
    db_to_aliases_.clear();
    for (const auto &[db_num, inner_map] : db_to_index_schemas_) {
      for (const auto &[name, schema] : inner_map) {
        for (const auto &alias : schema->GetAliases()) {
          db_to_aliases_[db_num][alias] = name;
        }
      }
    }
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

  // Populate forward alias map from the loaded index's aliases.
  auto &loaded_schema = db_to_index_schemas_[db_num][name];
  for (const auto &alias : loaded_schema->GetAliases()) {
    db_to_aliases_[db_num][alias] = std::string(name);
  }

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
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      // Consider indexes pending RDB load
      auto &stats = Metrics::GetStats();
      return SchemaManager::Instance().GetNumberOfIndexSchemas() +
             std::max(stats.rdb_restore_total_indexes.load() -
                          stats.rdb_restore_completed_indexes.load(),
                      uint64_t{0});
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

absl::Status SchemaManager::AddAlias(uint32_t db_num, absl::string_view alias,
                                     absl::string_view index_name) {
  // Validate: no null bytes in alias.
  if (alias.find('\0') != absl::string_view::npos) {
    return absl::InvalidArgumentError("Alias name must not contain null bytes");
  }

  if (coordinator_enabled_) {
    // Coordinator mode: validate under lock, then release before calling
    // MetadataManager (which invokes OnMetadataCallback, acquiring the lock).
    // NOTE: This creates a TOCTOU window where concurrent AddAlias calls for
    // the same alias targeting different indexes can both pass validation.
    // Resolution is last-writer-wins at the MetadataManager level, the final
    // CreateEntry determines the authoritative alias mapping.
    {
      absl::MutexLock lock(&db_to_index_schemas_mutex_);

      // Reject if alias collides with an existing real index name.
      if (alias != index_name) {
        auto collision = LookupInternal(db_num, alias);
        if (collision.ok()) {
          return absl::AlreadyExistsError(
              "Alias collides with existing index name");
        }
      }

      // Check if alias already exists in this db.
      auto db_alias_it = db_to_aliases_.find(db_num);
      if (db_alias_it != db_to_aliases_.end()) {
        // Reject if index_name is itself an alias.
        if (db_alias_it->second.contains(index_name)) {
          return absl::InvalidArgumentError(
              "Unknown index name or name is an alias");
        }
        if (db_alias_it->second.contains(alias)) {
          return absl::AlreadyExistsError("Alias already exists");
        }
      }
    }

    // Fetch the target index proto from MetadataManager.
    auto entry_or = coordinator::MetadataManager::Instance().GetEntryContent(
        kSchemaManagerMetadataTypeName,
        coordinator::ObjName(db_num, index_name));
    if (!entry_or.ok()) {
      if (absl::IsNotFound(entry_or.status())) {
        return GenerateIndexNotFoundError(db_num, index_name);
      }
      return entry_or.status();
    }

    // Unpack the IndexSchema proto.
    data_model::IndexSchema schema_proto;
    if (!entry_or.value().UnpackTo(&schema_proto)) {
      return absl::InternalError("Unable to unpack index schema proto");
    }

    // Normalize defaults to match what ToProto() produces, preventing
    // MessageDifferencer from treating this as a structural change.
    NormalizeIndexSchemaProtoDefaults(schema_proto);

    // Append alias and sort lexicographically.
    schema_proto.add_aliases(std::string(alias));
    std::sort(schema_proto.mutable_aliases()->begin(),
              schema_proto.mutable_aliases()->end());

    // Re-commit the modified proto.
    auto any_proto = std::make_unique<google::protobuf::Any>();
    any_proto->PackFrom(schema_proto);
    auto result = coordinator::MetadataManager::Instance().CreateEntry(
        kSchemaManagerMetadataTypeName,
        coordinator::ObjName(db_num, index_name), std::move(any_proto));
    if (!result.ok()) {
      return result.status();
    }
    return absl::OkStatus();
  }

  // Standalone (non-coordinator) mode: modify in-memory state directly.
  absl::MutexLock lock(&db_to_index_schemas_mutex_);

  // Reject if alias collides with an existing real index name.
  if (alias != index_name) {
    auto collision = LookupInternal(db_num, alias);
    if (collision.ok()) {
      return absl::AlreadyExistsError(
          "Alias collides with existing index name");
    }
  }

  // Check if alias already exists.
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

  // Validate: target index must exist.
  auto target = LookupInternal(db_num, index_name);
  if (!target.ok()) {
    return GenerateIndexNotFoundError(db_num, index_name);
  }

  // Insert into forward alias map.
  db_to_aliases_[db_num][std::string(alias)] = std::string(index_name);

  // Sync in-memory IndexSchema proto: add alias and sort.
  auto aliases = target.value()->GetAliases();
  aliases.emplace_back(alias);
  std::sort(aliases.begin(), aliases.end());
  target.value()->SetAliases(std::move(aliases));

  return absl::OkStatus();
}

absl::Status SchemaManager::RemoveAlias(uint32_t db_num,
                                        absl::string_view alias) {
  // Validate: no null bytes in alias.
  if (alias.find('\0') != absl::string_view::npos) {
    return absl::InvalidArgumentError("Alias name must not contain null bytes");
  }

  if (coordinator_enabled_) {
    // Coordinator mode: look up owning index under lock, then release before
    // calling MetadataManager (which invokes OnMetadataCallback, acquiring the
    // lock).
    std::string owning_index;
    {
      absl::MutexLock lock(&db_to_index_schemas_mutex_);
      auto db_alias_it = db_to_aliases_.find(db_num);
      if (db_alias_it == db_to_aliases_.end()) {
        return absl::NotFoundError("Alias does not exist");
      }
      auto alias_it = db_alias_it->second.find(alias);
      if (alias_it == db_alias_it->second.end()) {
        return absl::NotFoundError("Alias does not exist");
      }
      owning_index = alias_it->second;
    }

    // Fetch the owning index proto from MetadataManager.
    auto entry_or = coordinator::MetadataManager::Instance().GetEntryContent(
        kSchemaManagerMetadataTypeName,
        coordinator::ObjName(db_num, owning_index));
    if (!entry_or.ok()) {
      return entry_or.status();
    }

    // Unpack the IndexSchema proto.
    data_model::IndexSchema schema_proto;
    if (!entry_or.value().UnpackTo(&schema_proto)) {
      return absl::InternalError("Unable to unpack index schema proto");
    }

    // Normalize defaults to match what ToProto() produces, preventing
    // MessageDifferencer from treating this as a structural change.
    NormalizeIndexSchemaProtoDefaults(schema_proto);

    // Remove the alias from the repeated field.
    auto *aliases = schema_proto.mutable_aliases();
    aliases->erase(
        std::remove(aliases->begin(), aliases->end(), std::string(alias)),
        aliases->end());

    // Re-commit the modified proto.
    auto any_proto = std::make_unique<google::protobuf::Any>();
    any_proto->PackFrom(schema_proto);
    auto result = coordinator::MetadataManager::Instance().CreateEntry(
        kSchemaManagerMetadataTypeName,
        coordinator::ObjName(db_num, owning_index), std::move(any_proto));
    if (!result.ok()) {
      return result.status();
    }
    return absl::OkStatus();
  }

  // Standalone (non-coordinator) mode: modify in-memory state directly.
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  auto db_alias_it = db_to_aliases_.find(db_num);
  if (db_alias_it == db_to_aliases_.end()) {
    return absl::NotFoundError("Alias does not exist");
  }
  auto alias_it = db_alias_it->second.find(alias);
  if (alias_it == db_alias_it->second.end()) {
    return absl::NotFoundError("Alias does not exist");
  }

  // Capture owning index before erasing from forward map.
  std::string owning_index = alias_it->second;
  db_alias_it->second.erase(alias_it);

  // Sync in-memory IndexSchema proto: remove alias.
  auto target = LookupInternal(db_num, owning_index);
  if (target.ok()) {
    auto aliases = target.value()->GetAliases();
    aliases.erase(
        std::remove(aliases.begin(), aliases.end(), std::string(alias)),
        aliases.end());
    target.value()->SetAliases(std::move(aliases));
  }

  return absl::OkStatus();
}

absl::Status SchemaManager::UpdateAlias(uint32_t db_num,
                                        absl::string_view alias,
                                        absl::string_view index_name) {
  // Validate: no null bytes in alias.
  if (alias.find('\0') != absl::string_view::npos) {
    return absl::InvalidArgumentError("Alias name must not contain null bytes");
  }

  if (coordinator_enabled_) {
    // Coordinator mode: look up current alias owner under lock, then release
    // before calling MetadataManager (which invokes OnMetadataCallback,
    // acquiring the lock).
    std::string old_index;
    {
      absl::MutexLock lock(&db_to_index_schemas_mutex_);

      // Check if alias collides with an existing index name (non-self).
      if (alias != index_name) {
        auto collision = LookupInternal(db_num, alias);
        if (collision.ok()) {
          return absl::AlreadyExistsError(
              "Alias collides with existing index name");
        }
      }

      auto db_alias_it = db_to_aliases_.find(db_num);
      if (db_alias_it != db_to_aliases_.end()) {
        auto alias_it = db_alias_it->second.find(alias);
        if (alias_it != db_alias_it->second.end()) {
          // Idempotent: alias already points to the target → OK.
          if (alias_it->second == index_name) {
            return absl::OkStatus();
          }
          old_index = alias_it->second;
        }
        // Reject if index_name is itself an alias.
        if (db_alias_it->second.contains(index_name)) {
          return absl::InvalidArgumentError(
              "Unknown index name or name is an alias");
        }
      }
    }

    // Verify target index B exists by fetching its proto.
    auto entry_or = coordinator::MetadataManager::Instance().GetEntryContent(
        kSchemaManagerMetadataTypeName,
        coordinator::ObjName(db_num, index_name));
    if (!entry_or.ok()) {
      if (absl::IsNotFound(entry_or.status())) {
        return GenerateIndexNotFoundError(db_num, index_name);
      }
      return entry_or.status();
    }

    // Step 1: Add alias to index B's proto (add-before-remove).
    {
      data_model::IndexSchema schema_proto;
      if (!entry_or.value().UnpackTo(&schema_proto)) {
        return absl::InternalError("Unable to unpack index schema proto");
      }

      // Normalize defaults to match what ToProto() produces, preventing
      // MessageDifferencer from treating this as a structural change.
      NormalizeIndexSchemaProtoDefaults(schema_proto);

      // Check if alias is already present in B's proto (idempotent at proto
      // level).
      bool already_in_target = false;
      for (const auto &existing_alias : schema_proto.aliases()) {
        if (existing_alias == alias) {
          already_in_target = true;
          break;
        }
      }

      if (!already_in_target) {
        schema_proto.add_aliases(std::string(alias));
        std::sort(schema_proto.mutable_aliases()->begin(),
                  schema_proto.mutable_aliases()->end());

        auto any_proto = std::make_unique<google::protobuf::Any>();
        any_proto->PackFrom(schema_proto);
        auto result = coordinator::MetadataManager::Instance().CreateEntry(
            kSchemaManagerMetadataTypeName,
            coordinator::ObjName(db_num, index_name), std::move(any_proto));
        if (!result.ok()) {
          return result.status();
        }
      }
    }

    // Step 2: If old index A exists and A ≠ B, remove alias from A's proto.
    if (!old_index.empty() && old_index != index_name) {
      auto old_entry_or =
          coordinator::MetadataManager::Instance().GetEntryContent(
              kSchemaManagerMetadataTypeName,
              coordinator::ObjName(db_num, old_index));
      if (!old_entry_or.ok()) {
        // If old index was deleted concurrently, that's acceptable — the alias
        // has already been added to B.
        if (!absl::IsNotFound(old_entry_or.status())) {
          VMSDK_LOG(WARNING, nullptr)
              << "UpdateAlias: failed to fetch old index '" << old_index
              << "' proto after alias '" << alias
              << "' already added to target; proceeding as OK";
        }
      } else {
        data_model::IndexSchema old_schema_proto;
        if (!old_entry_or.value().UnpackTo(&old_schema_proto)) {
          return absl::InternalError("Unable to unpack index schema proto");
        }

        // Normalize defaults to match what ToProto() produces.
        NormalizeIndexSchemaProtoDefaults(old_schema_proto);

        auto *aliases = old_schema_proto.mutable_aliases();
        aliases->erase(
            std::remove(aliases->begin(), aliases->end(), std::string(alias)),
            aliases->end());

        auto any_proto = std::make_unique<google::protobuf::Any>();
        any_proto->PackFrom(old_schema_proto);
        auto result = coordinator::MetadataManager::Instance().CreateEntry(
            kSchemaManagerMetadataTypeName,
            coordinator::ObjName(db_num, old_index), std::move(any_proto));
        if (!result.ok()) {
          VMSDK_LOG(WARNING, nullptr)
              << "UpdateAlias: failed to remove alias '" << alias
              << "' from old index '" << old_index
              << "' proto; rolling back add to target: "
              << result.status().message();

          // Roll back: remove the alias we already added to index B.
          auto rollback_entry_or =
              coordinator::MetadataManager::Instance().GetEntryContent(
                  kSchemaManagerMetadataTypeName,
                  coordinator::ObjName(db_num, index_name));
          if (rollback_entry_or.ok()) {
            data_model::IndexSchema rollback_proto;
            if (rollback_entry_or.value().UnpackTo(&rollback_proto)) {
              NormalizeIndexSchemaProtoDefaults(rollback_proto);
              auto *rb_aliases = rollback_proto.mutable_aliases();
              rb_aliases->erase(
                  std::remove(rb_aliases->begin(), rb_aliases->end(),
                              std::string(alias)),
                  rb_aliases->end());
              auto rb_any = std::make_unique<google::protobuf::Any>();
              rb_any->PackFrom(rollback_proto);
              auto rb_result =
                  coordinator::MetadataManager::Instance().CreateEntry(
                      kSchemaManagerMetadataTypeName,
                      coordinator::ObjName(db_num, index_name),
                      std::move(rb_any));
              if (!rb_result.ok()) {
                VMSDK_LOG(WARNING, nullptr)
                    << "UpdateAlias: rollback of alias '" << alias
                    << "' from target index '" << index_name
                    << "' also failed: " << rb_result.status().message();
              }
            }
          }
          return absl::InternalError(
              "Failed to remove alias from old index; update rolled back");
        }
      }
    }

    return absl::OkStatus();
  }

  // Standalone (non-coordinator) mode: modify in-memory state directly.
  absl::MutexLock lock(&db_to_index_schemas_mutex_);

  // Check if alias collides with an existing index name (non-self).
  if (alias != index_name) {
    auto collision = LookupInternal(db_num, alias);
    if (collision.ok()) {
      return absl::AlreadyExistsError(
          "Alias collides with existing index name");
    }
  }

  // Look up if alias currently exists.
  std::string old_index;
  auto db_alias_it = db_to_aliases_.find(db_num);
  if (db_alias_it != db_to_aliases_.end()) {
    auto alias_it = db_alias_it->second.find(alias);
    if (alias_it != db_alias_it->second.end()) {
      // Idempotent: already points to target → OK.
      if (alias_it->second == index_name) {
        return absl::OkStatus();
      }
      old_index = alias_it->second;
    }
    // Reject if index_name is itself an alias.
    if (db_alias_it->second.contains(index_name)) {
      return absl::InvalidArgumentError(
          "Unknown index name or name is an alias");
    }
  }

  // Validate: target index must exist.
  auto target = LookupInternal(db_num, index_name);
  if (!target.ok()) {
    return GenerateIndexNotFoundError(db_num, index_name);
  }

  // Update forward alias map.
  db_to_aliases_[db_num][std::string(alias)] = std::string(index_name);

  // Sync in-memory IndexSchema protos.
  // If old index exists and differs from new target, remove alias from it.
  if (!old_index.empty() && old_index != index_name) {
    auto old_target = LookupInternal(db_num, old_index);
    if (old_target.ok()) {
      auto old_aliases = old_target.value()->GetAliases();
      old_aliases.erase(std::remove(old_aliases.begin(), old_aliases.end(),
                                    std::string(alias)),
                        old_aliases.end());
      old_target.value()->SetAliases(std::move(old_aliases));
    }
  }

  // Add alias to new target's proto and sort.
  auto new_aliases = target.value()->GetAliases();
  new_aliases.emplace_back(alias);
  std::sort(new_aliases.begin(), new_aliases.end());
  target.value()->SetAliases(std::move(new_aliases));

  return absl::OkStatus();
}

std::vector<std::pair<std::string, std::string>> SchemaManager::GetAllAliases(
    uint32_t db_num) const {
  absl::MutexLock lock(&db_to_index_schemas_mutex_);
  std::vector<std::pair<std::string, std::string>> result;
  auto it = db_to_aliases_.find(db_num);
  if (it == db_to_aliases_.end()) {
    return result;
  }
  result.reserve(it->second.size());
  for (const auto &[alias, index_name] : it->second) {
    result.emplace_back(alias, index_name);
  }
  std::sort(result.begin(), result.end());
  return result;
}

}  // namespace valkey_search
