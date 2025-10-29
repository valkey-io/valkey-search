/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COORDINATOR_METADATA_MANAGER_H_
#define VALKEYSEARCH_SRC_COORDINATOR_METADATA_MANAGER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "command_parser.h"
#include "google/protobuf/any.pb.h"
#include "highwayhash/hh_types.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/rdb_serialization.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::coordinator {

using FingerprintCallback =
    absl::AnyInvocable<absl::StatusOr<vmsdk::SemanticVersion>(
        const google::protobuf::Any &metadata)>;
using EncodingVersionCallback = absl::AnyInvocable<absl::StatusOr<uint32_t>(
    const google::protobuf::Any &metadata)>;
using MetadataUpdateCallback = absl::AnyInvocable<absl::Status(
    uint32_t db_num, absl::string_view, const google::protobuf::Any *metadata)>;
using AuxSaveCallback = void (*)(ValkeyModuleIO *rdb, int when);
using AuxLoadCallback = int (*)(ValkeyModuleIO *rdb, int encver, int when);
static constexpr int kEncodingVersion = 0;
static constexpr uint8_t kMetadataBroadcastClusterMessageReceiverId = 0x00;

// Randomly generated 32 bit key for fingerprinting the metadata.
static constexpr highwayhash::HHKey kHashKey{
    0x9736bad976c904ea, 0x08f963a1a52eece9, 0x1ea3f3f773f3b510,
    0x9290a6b4e4db3d51};
class MetadataManager {
 public:
  MetadataManager(ValkeyModuleCtx *ctx, ClientPool &client_pool)
      : client_pool_(client_pool),
        detached_ctx_(vmsdk::MakeUniqueValkeyDetachedThreadSafeContext(ctx)) {
    RegisterRDBCallback(
        data_model::RDB_SECTION_GLOBAL_METADATA,
        RDBSectionCallbacks{
            .load = [this](ValkeyModuleCtx *ctx,
                           std::unique_ptr<data_model::RDBSection> section,
                           SupplementalContentIter &&iter) -> absl::Status {
              return LoadMetadata(ctx, std::move(section), std::move(iter));
            },

            .save = [this](ValkeyModuleCtx *ctx, SafeRDB *rdb, int when)
                -> absl::Status { return SaveMetadata(ctx, rdb, when); },

            .section_count = [this](ValkeyModuleCtx *ctx, int when) -> int {
              return GetSectionsCount();
            },
            .minimum_semantic_version = [](ValkeyModuleCtx *ctx, int when)
                -> int { return Instance().ComputeRDBVersion(); }});
  }

  static uint64_t ComputeTopLevelFingerprint(
      const google::protobuf::Map<std::string, GlobalMetadataEntryMap>
          &type_namespace_map);

  vmsdk::SemanticVersion ComputeRDBVersion() const;

  absl::Status TriggerCallbacks(absl::string_view type_name, uint32_t db_num,
                                absl::string_view id,
                                const GlobalMetadataEntry &entry);

  absl::StatusOr<google::protobuf::Any> GetEntry(absl::string_view type_name,
                                                 uint32_t db_num,
                                                 absl::string_view id);

  absl::StatusOr<IndexFingerprintVersion> CreateEntry(
      absl::string_view type_name, uint32_t db_num, absl::string_view id,
      std::unique_ptr<google::protobuf::Any> contents);

  absl::Status DeleteEntry(absl::string_view type_name, uint32_t db_num,
                           absl::string_view id);

  absl::StatusOr<IndexFingerprintVersion> GetFingerprintAndVersion(
      absl::string_view type_name, uint32_t db_num,
      absl::string_view index_name);

  std::unique_ptr<GlobalMetadata> GetGlobalMetadata();

  // RegisterType is used to register a new metadata type in the metadata
  // manager. After registering a type, the metadata manager will be able to
  // accept updates to that type both locally and over the cluster bus.
  //
  // * type_name should be a unique string identifying the type.
  // * fingerprint_callback should be a function for computing the fingerprint
  // of the metadata for the given encoding version. This function can only
  // change when the encoding version is bumped.
  // * update_callback will be called whenever the metadata is updated.
  //
  // Each entry has an encoding version associated with it.  When an entry
  // is created or updated locally, the encoding version is computed using the
  // encoding_version parameter passed to RegisterType. When an entry is updated
  // from the cluster bus, the encoding version is read from the metadata entry
  // itself. If the encoding version in the new metadata entry is greater than
  // the max_encoding_version registered for the type, the update will be
  // rejected.
  //
  void RegisterType(absl::string_view type_name, uint32_t max_encoding_version,
                    FingerprintCallback fingerprint_callback,
                    MetadataUpdateCallback callback,
                    EncodingVersionCallback encoding_version_callback);

  void BroadcastMetadata(ValkeyModuleCtx *ctx);

  void BroadcastMetadata(ValkeyModuleCtx *ctx,
                         const GlobalMetadataVersionHeader &version_header);

  void DelayHandleClusterMessage(
      ValkeyModuleCtx *ctx, const char *sender_id,
      std::unique_ptr<GlobalMetadataVersionHeader> header);

  void HandleClusterMessage(ValkeyModuleCtx *ctx, const char *sender_id,
                            uint8_t type, const unsigned char *payload,
                            uint32_t len);

  void HandleBroadcastedMetadata(
      ValkeyModuleCtx *ctx, const char *sender_id,
      std::unique_ptr<GlobalMetadataVersionHeader> header);

  absl::Status ReconcileMetadata(const GlobalMetadata &proposed,
                                 absl::string_view source,
                                 bool trigger_callbacks = true,
                                 bool prefer_incoming = false);

  void OnServerCronCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                            uint64_t subevent, void *data);

  void OnLoadingEnded(ValkeyModuleCtx *ctx);
  void OnLoadingStarted(ValkeyModuleCtx *ctx);
  void OnReplicationLoadStart(ValkeyModuleCtx *ctx);
  void OnLoadingCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                         uint64_t subevent, void *data);
  absl::Status SaveMetadata(ValkeyModuleCtx *ctx, SafeRDB *rdb, int when);
  absl::Status LoadMetadata(ValkeyModuleCtx *ctx,
                            std::unique_ptr<data_model::RDBSection> section,
                            SupplementalContentIter &&supplemental_iter);
  void RegisterForClusterMessages(ValkeyModuleCtx *ctx);

  int64_t GetMilliSecondsSinceLastHealthyMetadata() const;
  int64_t GetMetadataReconciliationCompletedCount() const;

  static bool IsInitialized();
  static void InitInstance(std::unique_ptr<MetadataManager> instance);
  static MetadataManager &Instance();

  absl::Status ShowMetadata(ValkeyModuleCtx *ctx,
                            vmsdk::ArgsIterator &iter) const;
  static std::string EncodeDbNum(uint32_t db_num, absl::string_view id);
  struct DecodedDbNum {
    uint32_t db_num;
    std::string id;
  };
  static DecodedDbNum DecodeDbNum(absl::string_view encoded_id);

 private:
  struct RegisteredType {
    uint32_t max_encoding_version;
    EncodingVersionCallback encoding_version_callback;
    FingerprintCallback fingerprint_callback;
    MetadataUpdateCallback update_callback;
  };
  absl::StatusOr<uint64_t> ComputeFingerprint(
      absl::string_view type_name, const google::protobuf::Any &contents,
      absl::flat_hash_map<std::string, RegisteredType> &registered_types);
  int GetSectionsCount() const;
  vmsdk::MainThreadAccessGuard<GlobalMetadata> metadata_;
  vmsdk::MainThreadAccessGuard<GlobalMetadata> staged_metadata_;
  vmsdk::MainThreadAccessGuard<bool> staging_metadata_due_to_repl_load_ = false;
  vmsdk::MainThreadAccessGuard<bool> is_loading_ = false;
  vmsdk::MainThreadAccessGuard<absl::flat_hash_map<std::string, RegisteredType>>
      registered_types_;
  coordinator::ClientPool &client_pool_;
  vmsdk::UniqueValkeyDetachedThreadSafeContext detached_ctx_;
  std::atomic_int64_t last_healthy_metadata_millis_{0};
  std::atomic_int64_t metadata_reconciliation_completed_count_{0};
};
}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_SRC_COORDINATOR_METADATA_MANAGER_H_
