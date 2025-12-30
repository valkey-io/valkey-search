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
absl::Status HandleParseFailure(ValkeyModuleCtx *ctx,
                                const std::string &data_type, size_t data_len,
                                const std::string &id) {
  VMSDK_LOG(WARNING, ctx) << "CRITICAL: Failed to parse " << data_type
                          << " in FT.INTERNAL_UPDATE. "
                          << "Data length: " << data_len << ", Index ID: " << id
                          << ". This indicates data corruption or logic bug.";
  Metrics::GetStats().ft_internal_update_parse_failures_cnt++;

  if (ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_LOADING) {
    if (options::GetSkipCorruptedInternalUpdateEntries().GetValue()) {
      VMSDK_LOG(WARNING, ctx)
          << "SKIPPING corrupted AOF entry due to configuration";
      Metrics::GetStats().ft_internal_update_skipped_entries_cnt++;
      ValkeyModule_ReplyWithSimpleString(ctx, "OK");
      return absl::OkStatus();
    } else {
      CHECK(false)
          << "Protobuf parse failure during AOF loading - cannot continue";
    }
  }

  return absl::InvalidArgumentError("ERR failed to parse " + data_type);
}

absl::Status FTInternalUpdateCmd(ValkeyModuleCtx *ctx,
                                 ValkeyModuleString **argv, int argc) {
  if (argc != kFTInternalUpdateArgCount) {
    return absl::InvalidArgumentError(
        "ERR wrong number of arguments for FT_INTERNAL_UPDATE");
  }

  size_t id_len;
  const char *id_data = ValkeyModule_StringPtrLen(argv[1], &id_len);
  std::string id(id_data, id_len);

  size_t metadata_len;
  const char *metadata_data = ValkeyModule_StringPtrLen(argv[2], &metadata_len);
  coordinator::GlobalMetadataEntry metadata_entry;
  if (!metadata_entry.ParseFromArray(metadata_data, metadata_len)) {
    return HandleParseFailure(ctx, "GlobalMetadataEntry", metadata_len, id);
  }

  size_t header_len;
  const char *header_data = ValkeyModule_StringPtrLen(argv[3], &header_len);
  coordinator::GlobalMetadataVersionHeader version_header;
  if (!version_header.ParseFromArray(header_data, header_len)) {
    return HandleParseFailure(ctx, "GlobalMetadataVersionHeader", header_len,
                              id);
  }

  auto status = coordinator::MetadataManager::Instance().ProcessInternalUpdate(
      ctx, kSchemaManagerMetadataTypeName, id, &metadata_entry,
      &version_header);
  if (!status.ok()) {
    VMSDK_LOG(WARNING, ctx)
        << "CRITICAL: ProcessInternalUpdate failed in FT.INTERNAL_UPDATE. "
        << "Index ID: " << id << ", Error: " << status.message();

    Metrics::GetStats().ft_internal_update_process_failures_cnt++;
    if (options::GetSkipCorruptedInternalUpdateEntries().GetValue()) {
      VMSDK_LOG(WARNING, ctx)
          << "SKIPPING corrupted AOF entry due to configuration";
      Metrics::GetStats().ft_internal_update_skipped_entries_cnt++;
      ValkeyModule_ReplyWithSimpleString(ctx, "OK");
      return absl::OkStatus();
    } else {
      CHECK(false)
          << "Protobuf parse failure during AOF loading - cannot continue";
    }

    return status;
  }

  ValkeyModule_ReplicateVerbatim(ctx);

  ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  return absl::OkStatus();
}

}  // namespace valkey_search
