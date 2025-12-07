/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/commands.h"
#include "src/commands/ft_create_parser.h"
#include "src/index_schema.pb.h"
#include "src/schema_manager.h"

namespace valkey_search {

constexpr int kFTInternalUpdateArgCount = 4;

absl::Status FTInternalUpdateCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
  if (argc != kFTInternalUpdateArgCount) {
    return absl::InvalidArgumentError("ERR wrong number of arguments for FT_INTERNAL_UPDATE");
  }
  
  // Parse index ID
  size_t id_len;
  const char *id_data = ValkeyModule_StringPtrLen(argv[1], &id_len);
  std::string id(id_data, id_len);
  
  // Deserialize GlobalMetadataEntry
  size_t metadata_len;
  const char *metadata_data = ValkeyModule_StringPtrLen(argv[2], &metadata_len);
  coordinator::GlobalMetadataEntry metadata_entry;
  if (!metadata_entry.ParseFromArray(metadata_data, metadata_len)) {
    return absl::InvalidArgumentError("ERR failed to parse GlobalMetadataEntry");
  }
  
  // Deserialize GlobalMetadataVersionHeader
  size_t header_len;
  const char *header_data = ValkeyModule_StringPtrLen(argv[3], &header_len);
  coordinator::GlobalMetadataVersionHeader version_header;
  if (!version_header.ParseFromArray(header_data, header_len)) {
    return absl::InvalidArgumentError("ERR failed to parse GlobalMetadataVersionHeader");
  }
  
  auto status = coordinator::MetadataManager::Instance().ProcessInternalUpdate(
      ctx, kSchemaManagerMetadataTypeName, id, &metadata_entry, &version_header);
  if (!status.ok()) {
    return status;
  }

  ValkeyModule_ReplicateVerbatim(ctx);

  ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  return absl::OkStatus();
}

}  // namespace valkey_search
