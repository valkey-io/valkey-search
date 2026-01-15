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

// Helper function to handle parse failures with poison pill recovery
absl::Status HandleInternalUpdateFailure(
    ValkeyModuleCtx *ctx, bool success, const std::string &operation_type,
    const std::string &id,
    const absl::Status &error_status = absl::OkStatus()) {
  if (success) {
    return absl::OkStatus();
  }

  VMSDK_LOG(WARNING, ctx) << "CRITICAL: " << operation_type
                          << " failed in FT.INTERNAL_UPDATE. "
                          << "Index ID: " << id;
  if (!error_status.ok()) {
    VMSDK_LOG(WARNING, ctx) << "Error: " << error_status.message();
  }

  if (operation_type.find("parse") != std::string::npos) {
    Metrics::GetStats().ft_internal_update_parse_failures_cnt++;
  } else {
    Metrics::GetStats().ft_internal_update_process_failures_cnt++;
  }

  if (ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_LOADING) {
    if (options::GetSkipCorruptedInternalUpdateEntries().GetValue()) {
      VMSDK_LOG(WARNING, ctx)
          << "SKIPPING corrupted AOF entry due to configuration";
      Metrics::GetStats().ft_internal_update_skipped_entries_cnt++;
      ValkeyModule_ReplyWithSimpleString(ctx, "OK");
      return absl::OkStatus();
    } else {
      CHECK(false)
          << "Internal update failure during AOF loading - cannot continue";
    }
  }

  return error_status.ok()
             ? absl::InternalError("ERR " + operation_type + " failed")
             : error_status;
}

absl::Status FTInternalUpdateCmd(ValkeyModuleCtx *ctx,
                                 ValkeyModuleString **argv, int argc) {
  CHECK_EQ(argc, kFTInternalUpdateArgCount)
      << "FT.INTERNAL_UPDATE called with wrong argument count: " << argc;

  auto id_view = vmsdk::ToStringView(argv[1]);
  std::string id(id_view);

  auto metadata_view = vmsdk::ToStringView(argv[2]);
  coordinator::GlobalMetadataEntry metadata_entry;
  VMSDK_RETURN_IF_ERROR(HandleInternalUpdateFailure(
      ctx,
      metadata_entry.ParseFromArray(metadata_view.data(), metadata_view.size()),
      "GlobalMetadataEntry parse", id));

  auto header_view = vmsdk::ToStringView(argv[3]);
  coordinator::GlobalMetadataVersionHeader version_header;
  VMSDK_RETURN_IF_ERROR(HandleInternalUpdateFailure(
      ctx,
      version_header.ParseFromArray(header_view.data(), header_view.size()),
      "GlobalMetadataVersionHeader parse", id));
  int flags = ValkeyModule_GetContextFlags(ctx);
  if ((flags & VALKEYMODULE_CTX_FLAGS_SLAVE) ||
      (flags & VALKEYMODULE_CTX_FLAGS_LOADING)) {
    auto status = coordinator::MetadataManager::Instance().CreateEntryOnReplica(
        ctx, kSchemaManagerMetadataTypeName, id, &metadata_entry,
        &version_header);
    VMSDK_RETURN_IF_ERROR(HandleInternalUpdateFailure(
        ctx, status.ok(), "CreateEntryOnReplica", id, status));
  }

  ValkeyModule_ReplicateVerbatim(ctx);

  ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  return absl::OkStatus();
}

}  // namespace valkey_search
