/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "absl/status/status.h"
#include "src/acl.h"
#include "src/commands/commands.h"
#include "src/commands/ft_create_parser.h"
#include "src/schema_manager.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

absl::Status FTCreateCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                         int argc) {
  VMSDK_ASSIGN_OR_RETURN(auto index_schema_proto,
                         ParseFTCreateArgs(ctx, argv + 1, argc - 1));
  index_schema_proto.set_db_num(ValkeyModule_GetSelectedDb(ctx));
  static const auto permissions =
      PrefixACLPermissions(kCreateCmdPermissions, kCreateCommand);
  VMSDK_RETURN_IF_ERROR(AclPrefixCheck(ctx, permissions, index_schema_proto));
  VMSDK_RETURN_IF_ERROR(
      SchemaManager::Instance().CreateIndexSchema(ctx, index_schema_proto));

  ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  ValkeyModule_ReplicateVerbatim(ctx);
  return absl::OkStatus();
}
}  // namespace valkey_search
