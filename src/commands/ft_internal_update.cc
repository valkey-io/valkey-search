/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/commands.h"
#include "src/coordinator/metadata_manager.h"
#include "src/index_schema.pb.h"
#include "src/metrics.h"
#include "src/schema_manager.h"
#include "src/valkey_search_options.h"

namespace valkey_search {

constexpr int kFTInternalUpdateArgCount = 4;

// Handles an FT.INTERNAL_UPDATE failure. The caller must return its result
// immediately (never fall through to applying a half-parsed entry). During
// loading, a failure is either skipped (if the skip-corrupted config is set) or
// returned as a recoverable error -- corrupt persisted data must not abort.
absl::Status HandleInternalUpdateFailure(ValkeyModuleCtx *ctx,
                                         const std::string &operation_type,
                                         const std::string &id,
                                         const absl::Status &error_status) {
  VMSDK_LOG(WARNING, ctx) << "CRITICAL: " << operation_type
                          << " failed in FT.INTERNAL_UPDATE. "
                          << "Index ID: " << vmsdk::config::RedactIfNeeded(id);

  if (operation_type.find("parse") != std::string::npos) {
    Metrics::GetStats().ft_internal_update_parse_failures_cnt++;
  } else {
    Metrics::GetStats().ft_internal_update_process_failures_cnt++;
  }

  if (ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_LOADING) {
    if (options::GetSkipCorruptedInternalUpdateEntries().GetValue()) {
      VMSDK_LOG(WARNING, ctx)
          << "SKIPPING corrupted FT.INTERNAL_UPDATE AOF entry due to "
             "configuration";
      Metrics::GetStats().ft_internal_update_skipped_entries_cnt++;
      ValkeyModule_ReplyWithSimpleString(ctx, "OK");
      return absl::OkStatus();
    }
    return absl::DataLossError(
        "Corrupt FT.INTERNAL_UPDATE entry during loading; set "
        "search.skip-corrupted-internal-update-entries=yes to skip it");
  }

  return error_status;
}

absl::Status FTInternalUpdateCmd(ValkeyModuleCtx *ctx,
                                 ValkeyModuleString **argv, int argc) {
  CHECK_EQ(argc, kFTInternalUpdateArgCount)
      << "FT.INTERNAL_UPDATE called with wrong argument count: " << argc;

  auto id_view = vmsdk::ToStringView(argv[1]);
  std::string id(id_view);

  auto metadata_view = vmsdk::ToStringView(argv[2]);
  coordinator::GlobalMetadataEntry metadata_entry;
  if (!metadata_entry.ParseFromArray(metadata_view.data(),
                                     metadata_view.size())) {
    return HandleInternalUpdateFailure(
        ctx, "GlobalMetadataEntry parse", id,
        absl::InvalidArgumentError("Failed to parse GlobalMetadataEntry"));
  }

  auto header_view = vmsdk::ToStringView(argv[3]);
  coordinator::GlobalMetadataVersionHeader version_header;
  if (!version_header.ParseFromArray(header_view.data(), header_view.size())) {
    return HandleInternalUpdateFailure(
        ctx, "GlobalMetadataVersionHeader parse", id,
        absl::InvalidArgumentError(
            "Failed to parse GlobalMetadataVersionHeader"));
  }

  // The MetadataManager only exists when the coordinator is enabled (cluster
  // mode). Receiving an FT.INTERNAL_UPDATE without it (e.g. a coordinator-mode
  // AOF replayed on a standalone node) is a misconfiguration: there is nowhere
  // to apply the entry. Surface it as an error rather than dereferencing the
  // uninitialized instance; during loading it is skippable like any other
  // corrupt/unexpected entry.
  if (!coordinator::MetadataManager::IsInitialized()) {
    return HandleInternalUpdateFailure(
        ctx, "MetadataManager not initialized", id,
        absl::FailedPreconditionError(
            "FT.INTERNAL_UPDATE received without the coordinator enabled"));
  }

  int flags = ValkeyModule_GetContextFlags(ctx);
  if ((flags & VALKEYMODULE_CTX_FLAGS_SLAVE) ||
      (flags & VALKEYMODULE_CTX_FLAGS_LOADING)) {
    auto status = coordinator::MetadataManager::Instance().CreateEntryOnReplica(
        ctx, kSchemaManagerMetadataTypeName, id, &metadata_entry,
        &version_header);
    if (!status.ok()) {
      return HandleInternalUpdateFailure(ctx, "CreateEntryOnReplica", id,
                                         status);
    }
  }

  ValkeyModule_ReplicateVerbatim(ctx);

  ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  return absl::OkStatus();
}

}  // namespace valkey_search
