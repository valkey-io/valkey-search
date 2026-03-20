/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "src/commands/commands.h"
#include "src/schema_manager.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

absl::Status FTAliasAddCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                           int argc) {
  if (argc != 3) {
    return absl::InvalidArgumentError(vmsdk::WrongArity(kAliasAddCommand));
  }
  auto alias = vmsdk::ToStringView(argv[1]);
  auto index_name = vmsdk::ToStringView(argv[2]);

  VMSDK_RETURN_IF_ERROR(SchemaManager::Instance().AddAlias(
      ValkeyModule_GetSelectedDb(ctx), alias, index_name));

  ValkeyModule_ReplyWithSimpleString(ctx, "OK");

  // Aliases have no MetadataManager path, so always replicate verbatim.
  // TODO: propagate alias mutations to other primaries in cluster mode
  // via a dedicated coordinator path (currently only replicas receive this).
  ValkeyModule_ReplicateVerbatim(ctx);
  return absl::OkStatus();
}

}  // namespace valkey_search
