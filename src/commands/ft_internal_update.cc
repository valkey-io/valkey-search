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

// Handles a parse/processing failure for an FT.INTERNAL_UPDATE entry. Returns a
// status that the caller propagates by returning immediately (it never falls
// through to applying a half-parsed entry).
//
// During AOF/RDB loading a failure is either skipped (when the operator opts in
// via search.skip-corrupted-internal-update-entries) or surfaced as a
// recoverable error. Corrupt persisted data must not abort the process, so this
// never uses a fatal CHECK.
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
        "Corrupt FT.INTERNAL_UPDATE entry encountered during loading. Set "
        "search.skip-corrupted-internal-update-entries=yes to skip such "
        "entries, or repair/remove the offending AOF entry.");
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

  // FT.INTERNAL_UPDATE only carries coordinator metadata. The MetadataManager
  // is only initialized when the coordinator is enabled (cluster mode). If a
  // node persisted these entries to its AOF and is later loaded without the
  // coordinator (e.g. standalone), there is no MetadataManager to apply them
  // to, so skip rather than dereference an uninitialized instance.
  if (!coordinator::MetadataManager::IsInitialized()) {
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    return absl::OkStatus();
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
