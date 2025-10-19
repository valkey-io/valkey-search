/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/coordinator/metadata_manager.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "command_parser.h"
#include "google/protobuf/any.pb.h"
#include "google/protobuf/util/json_util.h"
#include "grpcpp/support/status.h"
#include "highwayhash/arch_specific.h"
#include "highwayhash/highwayhash.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/coordinator/util.h"
#include "src/metrics.h"
#include "src/rdb_serialization.h"
<<<<<<< HEAD
=======
#include "src/valkey_search_options.h"
#include "vmsdk/src/debug.h"
>>>>>>> main
#include "vmsdk/src/log.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

/*

The original metadata manager was designed to provide a 2-level hierarchy:
<type-name, object-name>. This lead to the wire-format tied to:

 Map<string, Map<string,protobuf>>.

With Valkey 9, the introduction of DB num into CME creates a desire for a
three-level hierarchy: <type-name, db-num, object-name>. This 3-level
internal namespace is mapped into an external two-level namespace to provide
backward and some degree of forward compatibility.

The mapping is done by manipulating the object name. It is known that
pre-Valkey 9 object names cannot contain a valid hash-tag. This provides the
necessary information to distinguish between an object name and an encoded
<db_num, object_name> pair. Objects with a db_num of 0 are stored the same
way as pre-Valkey 9 objects, providing backward compatibility.

*/

namespace valkey_search::coordinator {
namespace {

constexpr mstime_t kMetadataBroadcastIntervalMs = 30000;
constexpr float kMetadataBroadcastJitterRatio = 0.5;

}  // namespace

CONTROLLED_BOOLEAN(PauseHandleClusterMessage, false);

void DelayedClusterMessageTimerCallback(ValkeyModuleCtx *ctx, void *data) {
  auto *params = static_cast<
      std::tuple<std::string, std::unique_ptr<GlobalMetadataVersionHeader>> *>(
      data);
  auto &[sender_id, header] = *params;

  MetadataManager::Instance().DelayHandleClusterMessage(ctx, sender_id.c_str(),
                                                        std::move(header));

  delete params;
}

static absl::NoDestructor<std::unique_ptr<MetadataManager>>
    metadata_manager_instance;

bool MetadataManager::IsInitialized() {
  return *metadata_manager_instance != nullptr;
}

MetadataManager &MetadataManager::Instance() {
  return **metadata_manager_instance;
}
void MetadataManager::InitInstance(std::unique_ptr<MetadataManager> instance) {
  *metadata_manager_instance = std::move(instance);
}

absl::StatusOr<uint64_t> MetadataManager::ComputeFingerprint(
    absl::string_view type_name, const google::protobuf::Any &contents,
    absl::flat_hash_map<std::string, RegisteredType> &registered_types) {
  auto it = registered_types.find(type_name);
  if (it == registered_types.end()) {
    return absl::NotFoundError(
        absl::StrCat("No type registered for: ", type_name));
  }
  return it->second.fingerprint_callback(contents);
}

uint64_t MetadataManager::ComputeTopLevelFingerprint(
    const google::protobuf::Map<std::string, GlobalMetadataEntryMap>
        &type_namespace_map) {
  // We use this struct to summarize each entry without taking any dependency
  // on the contents.
  struct ChildMetadataEntry {
    uint64_t type_name_fingerprint;
    uint64_t id_fingerprint;
    uint64_t version;
    uint64_t fingerprint;
  };
  std::vector<ChildMetadataEntry> child_metadata_entries;
  child_metadata_entries.reserve(type_namespace_map.size());
  for (auto &[type_name, inner_map] : type_namespace_map) {
    uint64_t type_name_fingerprint;
    highwayhash::HHStateT<HH_TARGET> state(kHashKey);
    highwayhash::HighwayHashT(&state, type_name.c_str(), type_name.size(),
                              &type_name_fingerprint);
    for (auto &[id, entry] : inner_map.entries()) {
      uint64_t id_fingerprint;
      highwayhash::HHStateT<HH_TARGET> state(kHashKey);
      highwayhash::HighwayHashT(&state, id.c_str(), id.size(), &id_fingerprint);
      child_metadata_entries.push_back(
          {.type_name_fingerprint = type_name_fingerprint,
           .id_fingerprint = id_fingerprint,
           .version = entry.version(),
           .fingerprint = entry.fingerprint()});
    }
  }
  // Sort the contents to maintain a deterministic ordering.
  std::sort(child_metadata_entries.begin(), child_metadata_entries.end(),
            [](const ChildMetadataEntry &a, const ChildMetadataEntry &b) {
              if (a.type_name_fingerprint == b.type_name_fingerprint) {
                return a.id_fingerprint < b.id_fingerprint;
              }
              return a.type_name_fingerprint < b.type_name_fingerprint;
            });
  uint64_t new_fingerprint;
  highwayhash::HHStateT<HH_TARGET> state(kHashKey);
  highwayhash::HighwayHashT(
      &state, reinterpret_cast<const char *>(child_metadata_entries.data()),
      child_metadata_entries.size() * sizeof(ChildMetadataEntry),
      &new_fingerprint);
  return new_fingerprint;
}

absl::Status MetadataManager::TriggerCallbacks(
    absl::string_view type_name, uint32_t db_num, absl::string_view id,
    const GlobalMetadataEntry &entry) {
  auto &registered_types = registered_types_.Get();
  auto it = registered_types.find(type_name);
  if (it != registered_types.end()) {
    return registered_types.at(type_name).update_callback(
        db_num, id, entry.has_content() ? &entry.content() : nullptr);
  }
  VMSDK_LOG_EVERY_N_SEC(WARNING, detached_ctx_.get(), 10)
      << "No type registered for: " << type_name << ", skipping callback";
  return absl::OkStatus();
}

absl::StatusOr<google::protobuf::Any> MetadataManager::GetEntry(
    absl::string_view type_name, uint32_t db_num, absl::string_view id) {
  auto encoded_id = EncodeDbNum(db_num, id);
  auto &metadata = metadata_.Get();
  if (!metadata.type_namespace_map().contains(type_name) ||
      !metadata.type_namespace_map().at(type_name).entries().contains(id) ||
      !metadata.type_namespace_map()
           .at(type_name)
           .entries()
           .at(encoded_id)
           .has_content()) {
    return absl::NotFoundError(
        absl::StrCat("Entry not found: ", type_name, " ", db_num, " ", id));
  }
  return metadata.type_namespace_map()
      .at(type_name)
      .entries()
      .at(encoded_id)
      .content();
}

absl::StatusOr<IndexFingerprintVersion> MetadataManager::CreateEntry(
    absl::string_view type_name, uint32_t db_num, absl::string_view id,
    std::unique_ptr<google::protobuf::Any> contents) {
  auto encoded_id = EncodeDbNum(db_num, id);
  auto &registered_types = registered_types_.Get();
  auto rt_it = registered_types.find(type_name);
  if (rt_it == registered_types.end()) {
    return absl::NotFoundError(
        absl::StrCat("No type registered for: ", type_name));
  }
  uint32_t version = 0;
  auto &metadata = metadata_.Get();
  auto it = metadata.type_namespace_map().find(type_name);
  if (it != metadata.type_namespace_map().end()) {
    auto inner_it = it->second.entries().find(encoded_id);
    if (inner_it != it->second.entries().end()) {
      version = inner_it->second.version() + 1;
    }
  }
  VMSDK_ASSIGN_OR_RETURN(
      auto fingerprint,
      ComputeFingerprint(type_name, *contents, registered_types));

  GlobalMetadataEntry new_entry;
  new_entry.set_version(version);
  new_entry.set_fingerprint(fingerprint);
  new_entry.set_encoding_version(rt_it->second.encoding_version);
  new_entry.set_allocated_content(contents.release());

  auto callback_status = TriggerCallbacks(type_name, db_num, id, new_entry);
  if (!callback_status.ok()) {
    return callback_status;
  }

  auto insert_result = metadata.mutable_type_namespace_map()->insert(
      {std::string(type_name), GlobalMetadataEntryMap()});
  (*insert_result.first->second.mutable_entries())[encoded_id] = new_entry;
  // NOLINTNEXTLINE
  metadata.mutable_version_header()->set_top_level_version(
      metadata.version_header().top_level_version() + 1);
  metadata.mutable_version_header()->set_top_level_fingerprint(
      ComputeTopLevelFingerprint(metadata.type_namespace_map()));
  BroadcastMetadata(detached_ctx_.get(), metadata.version_header());
  IndexFingerprintVersion index_fingerprint_version;
  index_fingerprint_version.set_fingerprint(fingerprint);
  index_fingerprint_version.set_version(version);
  return index_fingerprint_version;
}

absl::Status MetadataManager::DeleteEntry(absl::string_view type_name,
                                          uint32_t db_num,
                                          absl::string_view id) {
  auto encoded_id = EncodeDbNum(db_num, id);
  auto &metadata = metadata_.Get();
  auto it = metadata.type_namespace_map().find(type_name);
  if (it == metadata.type_namespace_map().end()) {
    return absl::NotFoundError(
        absl::StrCat("Entry not found: ", type_name, " ", id));
  }
  auto inner_it = it->second.entries().find(encoded_id);
  if (inner_it == it->second.entries().end()) {
    return absl::NotFoundError(
        absl::StrCat("Entry not found: ", type_name, " ", db_num, " ", id));
  }
  if (!inner_it->second.has_content()) {
    return absl::NotFoundError(
        absl::StrCat("Entry not found: ", type_name, " ", db_num, " ", id));
  }
  GlobalMetadataEntry new_entry;
  new_entry.set_version(inner_it->second.version() + 1);
  // Note that fingerprint and encoding version are not set and will default
  // to 0.

  auto callback_status = TriggerCallbacks(type_name, db_num, id, new_entry);
  if (!callback_status.ok()) {
    return callback_status;
  }

  (*(*metadata.mutable_type_namespace_map())[type_name]
        .mutable_entries())[encoded_id] = new_entry;

  metadata.mutable_version_header()->set_top_level_version(
      metadata.version_header().top_level_version() + 1);
  metadata.mutable_version_header()->set_top_level_fingerprint(
      ComputeTopLevelFingerprint(metadata.type_namespace_map()));
  BroadcastMetadata(detached_ctx_.get(), metadata.version_header());
  return absl::OkStatus();
}

std::unique_ptr<GlobalMetadata> MetadataManager::GetGlobalMetadata() {
  auto result = std::make_unique<GlobalMetadata>();
  result->CopyFrom(metadata_.Get());
  return result;
}

absl::StatusOr<IndexFingerprintVersion>
MetadataManager::GetFingerprintAndVersion(absl::string_view type_name,
                                          uint32_t db_num,
                                          absl::string_view id) {
  IndexFingerprintVersion result;
  auto encoded_id = EncodeDbNum(db_num, id);
  auto &metadata = metadata_.Get();
  if (metadata.type_namespace_map().contains(type_name) &&
      metadata.type_namespace_map().at(type_name).entries().contains(
          encoded_id)) {
    const auto &entry =
        metadata.type_namespace_map().at(type_name).entries().at(encoded_id);
    IndexFingerprintVersion index_fingerprint_version;
    index_fingerprint_version.set_fingerprint(entry.fingerprint());
    index_fingerprint_version.set_version(entry.version());
    return index_fingerprint_version;
  }
  return absl::NotFoundError(
      absl::StrCat("Entry not found: ", type_name, " ", db_num, " ", id));
}

void MetadataManager::RegisterType(absl::string_view type_name,
                                   uint32_t encoding_version,
                                   FingerprintCallback fingerprint_callback,
                                   MetadataUpdateCallback callback) {
  auto insert_result =
      registered_types_.Get().insert(std::pair<std::string, RegisteredType>{
          type_name, RegisteredType{.encoding_version = encoding_version,
                                    .fingerprint_callback =
                                        std::move(fingerprint_callback),
                                    .update_callback = std::move(callback)}});
  VMSDK_LOG_EVERY_N_SEC(WARNING, detached_ctx_.get(), 10)
      << "Type already registered for: " << type_name;
  DCHECK(insert_result.second);
}

void MetadataManager::BroadcastMetadata(ValkeyModuleCtx *ctx) {
  BroadcastMetadata(ctx, metadata_.Get().version_header());
}

void MetadataManager::BroadcastMetadata(
    ValkeyModuleCtx *ctx, const GlobalMetadataVersionHeader &version_header) {
  if (is_loading_.Get()) {
    VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
        << "Skipping send of metadata header due to loading";
    return;
  }
  std::string payload;
  version_header.SerializeToString(&payload);
  // Nullptr for target means broadcast to all.
  ValkeyModule_SendClusterMessage(ctx, /* target= */ nullptr,
                                  kMetadataBroadcastClusterMessageReceiverId,
                                  payload.c_str(), payload.size());
}

void MetadataManager::DelayHandleClusterMessage(
    ValkeyModuleCtx *ctx, const char *sender_id,
    std::unique_ptr<GlobalMetadataVersionHeader> header) {
  if (PauseHandleClusterMessage.GetValue()) {
    Metrics::GetStats().pause_handle_cluster_message_round_cnt++;
    VMSDK_LOG_EVERY_N_SEC(NOTICE, nullptr, 2)
        << "DEBUG: Paused round is "
        << Metrics::GetStats().pause_handle_cluster_message_round_cnt;
    std::string sender_id_str(sender_id, VALKEYMODULE_NODE_ID_LEN);
    // Use a timer with a small delay (e.g., 100ms) to poll without blocking
    auto *params = new std::tuple<std::string,
                                  std::unique_ptr<GlobalMetadataVersionHeader>>(
        std::move(sender_id_str), std::move(header));

    ValkeyModule_CreateTimer(ctx, 100, DelayedClusterMessageTimerCallback,
                             params);
  } else {
    HandleBroadcastedMetadata(ctx, sender_id, std::move(header));
  }
}

void MetadataManager::HandleClusterMessage(ValkeyModuleCtx *ctx,
                                           const char *sender_id, uint8_t type,
                                           const unsigned char *payload,
                                           uint32_t len) {
  if (type == kMetadataBroadcastClusterMessageReceiverId) {
    auto header = std::make_unique<GlobalMetadataVersionHeader>();
    header->ParseFromString(
        absl::string_view(reinterpret_cast<const char *>(payload), len));
    std::string sender_id_str(sender_id, VALKEYMODULE_NODE_ID_LEN);
    DelayHandleClusterMessage(ctx, sender_id_str.c_str(), std::move(header));
  } else {
    VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 10)
        << "Unsupported message type: " << type;
  }
}

void MetadataManager::HandleBroadcastedMetadata(
    ValkeyModuleCtx *ctx, const char *sender_id,
    std::unique_ptr<GlobalMetadataVersionHeader> header) {
  if (is_loading_.Get()) {
    VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 10)
        << "Ignoring incoming metadata message due to loading...";
    return;
  }
  auto &metadata = metadata_.Get();
  auto top_level_version = metadata.version_header().top_level_version();
  auto top_level_fingerprint =
      metadata.version_header().top_level_fingerprint();
  if (header->top_level_version() < top_level_version) {
    return;
  }
  std::string sender_id_str(sender_id, VALKEYMODULE_NODE_ID_LEN);
  if (header->top_level_version() == top_level_version) {
    if (header->top_level_fingerprint() == top_level_fingerprint) {
      return;
    }
    VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
        << "Got conflicting contents from " << sender_id_str << " for version "
        << top_level_version
        << ": have "
           "fingerprint "
        << top_level_fingerprint << ", got fingerprint "
        << header->top_level_fingerprint()
        << ". Retrieving full "
           "GlobalMetadata.";
  } else {
    VMSDK_LOG_EVERY_N_SEC(NOTICE, ctx, 1)
        << "Got newer version from " << sender_id_str << ": have "
        << top_level_version << ", got " << header->top_level_version()
        << ". Retrieving full GlobalMetadata.";
  }
  // sender_id isn't NULL terminated, so we copy to a std::string to make sure
  // it is properly NULL terminated
  char node_ip[VALKEYMODULE_NODE_ID_LEN];
  int node_port;
  if (ValkeyModule_GetClusterNodeInfo(ctx, sender_id_str.c_str(), node_ip,
                                      nullptr, &node_port,
                                      nullptr) != VALKEYMODULE_OK) {
    VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
        << "Failed to get cluster node info for node " << sender_id
        << " broadcasting "
           "version "
        << header->top_level_version() << ", fingerprint "
        << header->top_level_fingerprint();
    return;
  }
  std::string address =
      absl::StrCat(node_ip, ":", GetCoordinatorPort(node_port));
  auto client = client_pool_.GetClient(address);
  // Capturing "this" should be okay since SchemaManager is program-scoped.
  client->GetGlobalMetadata(
      [address, this](grpc::Status s, GetGlobalMetadataResponse &response) {
        if (!s.ok()) {
          VMSDK_LOG_EVERY_N_SEC(WARNING, detached_ctx_.get(), 1)
              << "Failed to get GlobalMetadata from " << address << ": "
              << s.error_message();
          return;
        }
        vmsdk::RunByMain([ctx = detached_ctx_.get(),
                          schema = std::unique_ptr<GlobalMetadata>(
                              response.release_metadata()),
                          address = std::move(address)] {
          VMSDK_LOG_EVERY_N_SEC(DEBUG, ctx, 1)
              << "Got GlobalMetadata from " << address << ": "
              << schema->DebugString();
          auto &metadata_manager = MetadataManager::Instance();
          auto status = metadata_manager.ReconcileMetadata(*schema);
          if (!status.ok()) {
            VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
                << "Failed to reconcile schemas: " << status.message();
            return;
          }
          VMSDK_LOG_EVERY_N_SEC(DEBUG, ctx, 1)
              << "Successfully reconciled schemas! New GlobalMetadata: "
              << metadata_manager.GetGlobalMetadata()->DebugString();
        });
      });
}

absl::Status MetadataManager::ReconcileMetadata(const GlobalMetadata &proposed,
                                                bool trigger_callbacks,
                                                bool prefer_incoming) {
  // We synthesize the new version in a new variable, so that if we need to
  // fail, the state is unchanged. The new version starts as a copy of the
  // current version.
  GlobalMetadata result;
  result.CopyFrom(metadata_.Get());

  // Merge the result with the incoming metadata
  for (const auto &[type_name, proposed_inner_map] :
       proposed.type_namespace_map()) {
    auto insert_result = result.mutable_type_namespace_map()->insert(
        {type_name, GlobalMetadataEntryMap()});
    auto &existing_inner_map = insert_result.first->second;
    for (const auto &[id, proposed_entry] : proposed_inner_map.entries()) {
      auto it = existing_inner_map.entries().find(id);
      if (it != existing_inner_map.entries().end() && !prefer_incoming) {
        auto &existing_entry = it->second;
        if (proposed_entry.version() < existing_entry.version()) {
          continue;
        }
        if (proposed_entry.version() == existing_entry.version()) {
          // We always want to prefer a higher encoding version. For example,
          // if a new feature is added, we don't want it to be squashed by
          // nodes that don't understand it.
          if (proposed_entry.encoding_version() <
              existing_entry.encoding_version()) {
            continue;
          }
          if (proposed_entry.encoding_version() ==
              existing_entry.encoding_version()) {
            // Simultaneous update. Resolve by ignoring the change if the
            // fingerprint is less than (or equal, if no change) to ours.
            if (proposed_entry.fingerprint() <= existing_entry.fingerprint()) {
              continue;
            }
          }
        }
      }

      auto mutable_entries = existing_inner_map.mutable_entries();
      (*mutable_entries)[id] = proposed_entry;
      auto &registered_types = registered_types_.Get();
      auto rt_it = registered_types.find(type_name);
      if (rt_it != registered_types.end() && proposed_entry.has_content() &&
          proposed_entry.encoding_version() < rt_it->second.encoding_version) {
        // If the encoding version is less than the current version, we need
        // to re-fingerprint the entry. New fields being added may result in
        // unstable fingerprinting.
        //
        // Later, during reconciliation, our fingerprint will be accepted by
        // the other node due to our encoding version being higher.
        VMSDK_ASSIGN_OR_RETURN(
            auto fingerprint,
            ComputeFingerprint(type_name, proposed_entry.content(),
                               registered_types));
        (*mutable_entries)[id].set_fingerprint(fingerprint);
        (*mutable_entries)[id].set_encoding_version(
            rt_it->second.encoding_version);
      }

      if (trigger_callbacks) {
        auto decoded = DecodeDbNum(id);
        auto result = TriggerCallbacks(type_name, decoded.db_num, decoded.id,
                                       proposed_entry);
        if (!result.ok()) {
          VMSDK_LOG(WARNING, detached_ctx_.get())
              << "Failed during reconciliation callback: %s"
              << result.message().data();
          return result;
        }
      }
    }
  }

  // Recompute the top level fingerprint.
  auto &metadata = metadata_.Get();
  auto old_fingerprint = metadata.version_header().top_level_fingerprint();
  auto new_fingerprint =
      ComputeTopLevelFingerprint(result.type_namespace_map());
  result.mutable_version_header()->set_top_level_fingerprint(new_fingerprint);

  // The new version is the max of the old version and the proposed version.
  // We also bump the version if the fingerprint changed, as this indicates a
  // distinct version.
  auto old_version = metadata.version_header().top_level_version();
  auto new_version =
      std::max(old_version, proposed.version_header().top_level_version());
  bool should_broadcast = false;
  if (new_fingerprint != proposed.version_header().top_level_fingerprint() &&
      new_fingerprint != old_fingerprint) {
    new_version = new_version + 1;
    result.mutable_version_header()->set_top_level_version(new_version);
    should_broadcast = true;
  } else {
    result.mutable_version_header()->set_top_level_version(new_version);
  }

  metadata = result;

  // Finally, we broadcast the new version if we bumped the version.
  if (should_broadcast) {
    BroadcastMetadata(detached_ctx_.get(), metadata.version_header());
  }

  // Update the timestamp of the last successful metadata reconciliation
  last_healthy_metadata_millis_.store(ValkeyModule_Milliseconds(),
                                      std::memory_order_relaxed);

  // Increment the completion counter
  metadata_reconciliation_completed_count_.fetch_add(1,
                                                     std::memory_order_relaxed);

  return absl::OkStatus();
}

bool DoesGlobalMetadataContainEntry(const GlobalMetadata &metadata) {
  if (metadata.type_namespace_map().empty()) {
    return false;
  }
  for (const auto &[type_name, inner_map] : metadata.type_namespace_map()) {
    if (!inner_map.entries().empty()) {
      return true;
    }
  }
  return false;
}

int MetadataManager::GetSectionsCount() const {
  return DoesGlobalMetadataContainEntry(metadata_.Get()) ? 1 : 0;
}

absl::Status MetadataManager::SaveMetadata(ValkeyModuleCtx *ctx, SafeRDB *rdb,
                                           int when) {
  if (when == VALKEYMODULE_AUX_BEFORE_RDB) {
    return absl::OkStatus();
  }

  if (!DoesGlobalMetadataContainEntry(metadata_.Get())) {
    // Auxsave2 will ensure nothing is written to the aux section if we write
    // nothing.
    VMSDK_LOG(NOTICE, ctx)
        << "Skipping aux metadata for MetadataManager since there is no "
           "content";
    return absl::OkStatus();
  }

  VMSDK_LOG(NOTICE, ctx)
      << "Saving aux metadata for MetadataManager to aux RDB";
  data_model::RDBSection section;
  std::string serialized_metadata;
  section.set_type(data_model::RDB_SECTION_GLOBAL_METADATA);
  section.mutable_global_metadata_contents()->CopyFrom(metadata_.Get());
  if (!section.SerializeToString(&serialized_metadata)) {
    return absl::InternalError("Failed to serialize metadata");
  }
  VMSDK_RETURN_IF_ERROR(rdb->SaveStringBuffer(serialized_metadata));
  return absl::OkStatus();
}

absl::Status MetadataManager::LoadMetadata(
    ValkeyModuleCtx *ctx, std::unique_ptr<data_model::RDBSection> section,
    SupplementalContentIter &&supplemental_iter) {
  if (section->type() != data_model::RDB_SECTION_GLOBAL_METADATA) {
    return absl::InternalError(
        "Unexpected RDB section type passed to MetadataManager");
  }

  if (staging_metadata_due_to_repl_load_.Get()) {
    staged_metadata_ = section->global_metadata_contents();
  } else {
    // In case we had an existing state, we need to merge the two views. This
    // could happen if a module triggers a load after we have already been
    // running.
    VMSDK_RETURN_IF_ERROR(ReconcileMetadata(section->global_metadata_contents(),
                                            /*trigger_callbacks=*/false,
                                            /*prefer_incoming=*/true));
  }
  return absl::OkStatus();
}

void MetadataManagerOnClusterMessageCallback(ValkeyModuleCtx *ctx,
                                             const char *sender_id,
                                             uint8_t type,
                                             const unsigned char *payload,
                                             uint32_t len) {
  MetadataManager::Instance().HandleClusterMessage(ctx, sender_id, type,
                                                   payload, len);
}

mstime_t GetIntervalWithJitter(mstime_t interval, float jitter_ratio) {
  absl::BitGen gen;
  float jitter = absl::Uniform(gen, -jitter_ratio / 2.0, jitter_ratio / 2.0);
  return interval + interval * jitter;
}

void MetadataManagerSendMetadataBroadcast(ValkeyModuleCtx *ctx, void *data) {
  ValkeyModule_CreateTimer(ctx,
                           GetIntervalWithJitter(kMetadataBroadcastIntervalMs,
                                                 kMetadataBroadcastJitterRatio),
                           &MetadataManagerSendMetadataBroadcast, nullptr);
  MetadataManager::Instance().BroadcastMetadata(ctx);
}

void MetadataManager::OnServerCronCallback(
    ValkeyModuleCtx *ctx, [[maybe_unused]] ValkeyModuleEvent eid,
    [[maybe_unused]] uint64_t subevent, [[maybe_unused]] void *data) {
  static bool timer_started = false;
  if (!timer_started) {
    // The first server cron tick after the FT.CREATE is run needs to kick
    // start the timer. This can't be done during normal server event
    // subscription because timers cannot be safely created in background
    // threads (the GIL does not protect event loop code which uses the
    // timers).
    timer_started = true;
    ValkeyModule_CreateTimer(
        ctx,
        GetIntervalWithJitter(kMetadataBroadcastIntervalMs,
                              kMetadataBroadcastJitterRatio),
        &MetadataManagerSendMetadataBroadcast, nullptr);
  }
}

void MetadataManager::OnLoadingEnded(ValkeyModuleCtx *ctx) {
  // Only on loading ended do we apply the staged changes.
  if (staging_metadata_due_to_repl_load_.Get()) {
    VMSDK_LOG(NOTICE, ctx)
        << "Applying staged metadata at the end of RDB loading";

    // Clear the local metadata, then use ReconcileMetadata to recompute
    // fingerprints in case encoding has changed.
    metadata_ = GlobalMetadata();
    auto status = ReconcileMetadata(staged_metadata_.Get(),
                                    /*trigger_callbacks=*/false,
                                    /*prefer_incoming=*/true);
    if (!status.ok()) {
      VMSDK_LOG(WARNING, ctx)
          << "Failed to apply staged metadata: %s" << status.message().data();
    }
    staged_metadata_ = GlobalMetadata();
    staging_metadata_due_to_repl_load_ = false;
  }
  is_loading_ = false;
}

void MetadataManager::OnReplicationLoadStart(ValkeyModuleCtx *ctx) {
  VMSDK_LOG(NOTICE, ctx) << "Staging metadata during RDB load due to "
                            "replication, will apply on loading finished";
  staging_metadata_due_to_repl_load_ = true;
}

void MetadataManager::OnLoadingStarted(ValkeyModuleCtx *ctx) {
  VMSDK_LOG(NOTICE, ctx)
      << "Loading started, stopping incoming metadata updates";
  is_loading_ = true;
}

void MetadataManager::OnLoadingCallback(ValkeyModuleCtx *ctx,
                                        [[maybe_unused]] ValkeyModuleEvent eid,
                                        uint64_t subevent,
                                        [[maybe_unused]] void *data) {
  if (subevent == VALKEYMODULE_SUBEVENT_LOADING_ENDED) {
    MetadataManager::Instance().OnLoadingEnded(ctx);
    return;
  }
  if (subevent == VALKEYMODULE_SUBEVENT_LOADING_REPL_START) {
    MetadataManager::Instance().OnReplicationLoadStart(ctx);
  }
  if (subevent == VALKEYMODULE_SUBEVENT_LOADING_AOF_START ||
      subevent == VALKEYMODULE_SUBEVENT_LOADING_RDB_START ||
      subevent == VALKEYMODULE_SUBEVENT_LOADING_REPL_START) {
    MetadataManager::Instance().OnLoadingStarted(ctx);
  }
}

int64_t MetadataManager::GetMilliSecondsSinceLastHealthyMetadata() const {
  int64_t last_millis =
      last_healthy_metadata_millis_.load(std::memory_order_relaxed);
  if (last_millis == 0) {
    // No metadata has been successfully received yet
    return -1;
  }

  int64_t current_millis = ValkeyModule_Milliseconds();
  return current_millis - last_millis;
}

int64_t MetadataManager::GetMetadataReconciliationCompletedCount() const {
  return metadata_reconciliation_completed_count_.load(
      std::memory_order_relaxed);
}

void MetadataManager::RegisterForClusterMessages(ValkeyModuleCtx *ctx) {
  ValkeyModule_RegisterClusterMessageReceiver(
      ctx, coordinator::kMetadataBroadcastClusterMessageReceiverId,
      MetadataManagerOnClusterMessageCallback);
}

absl::Status MetadataManager::ShowMetadata(
    ValkeyModuleCtx *ctx, [[maybe_unused]] vmsdk::ArgsIterator &itr) const {
  auto metadata = metadata_.Get().DebugString();
  VMSDK_LOG(WARNING, ctx) << "Metadata: " << metadata;
  google::protobuf::util::JsonPrintOptions options;
  options.always_print_fields_with_no_presence = true;
  [[maybe_unused]] auto status = google::protobuf::util::MessageToJsonString(
      metadata_.Get(), &metadata, options);
  ValkeyModule_ReplyWithStringBuffer(ctx, metadata.data(), metadata.size());
  return absl::OkStatus();
}

/*
An 8/1.0 encoded string won't have a hashtag anywhere and is always for
  db_num == 0 An 9/1.1 encoded string will always have a false-hashtag at
  the START AND may have a real hashtag after that.

Decoded strings that lack a hashtag and are for db_num == 0, are encoded with
the 8/1.0 rules All other decoded strings are encoded according to the 9/1.1
rules.

A pseudo-hashtag is of the format: {dddd}
dddd is the database number, i.e., ascii digits 0-9 ONLY.
Characters after the database number up to the trailing right brace are
explicitly ignored -- but preserved -- allowing for potential future
forward/reverse compatibility.

*/
MetadataManager::DecodedDbNum MetadataManager::DecodeDbNum(
    absl::string_view encoded) {
  auto hash_tag = vmsdk::ParseHashTag(encoded);
  if (hash_tag) {
    std::string_view front_tag = *hash_tag;
    CHECK(encoded.size() >= 3);  // hash_tag means >= 3 chars
    if (front_tag.data() == (encoded.data() + 1)) {
      std::string db_num_str;
      while (!front_tag.empty() && std::isdigit(front_tag.front())) {
        db_num_str += front_tag.front();
        front_tag.remove_prefix(1);
      }
      if (!front_tag.empty()) {
        VMSDK_LOG_EVERY_N(NOTICE, nullptr, 1000)
            << "Ignoring extended index name metadata";
      }
      if (!db_num_str.empty()) {
        // Found valid 9/1.1 encoding.
        return {.db_num = static_cast<uint32_t>(std::stoul(db_num_str)),
                .id = std::string(encoded.substr(hash_tag->size() + 2))};
      }
    }
    VMSDK_LOG_EVERY_N(WARNING, nullptr, 10)
        << "Found invalid encoded index name: " << encoded;
  }
  // Assume 8/1.0 encoding.
  return {.db_num = 0, .id = std::string(encoded)};
}

std::string MetadataManager::EncodeDbNum(uint32_t db_num,
                                         absl::string_view id) {
  if (db_num == 0) {
    // 8/1.0 encoding.
    return std::string(id);
  }
  // 9/1.1 encoding.
  return absl::StrCat("{", db_num, "}", id);
}

}  // namespace valkey_search::coordinator
