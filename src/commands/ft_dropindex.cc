/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "src/acl.h"
#include "src/commands/commands.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

absl::Status FTDropIndexCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                            int argc) {
  if (argc != 2) {
    return absl::InvalidArgumentError(vmsdk::WrongArity(kDropIndexCommand));
  }
  auto index_schema_name = vmsdk::ToStringView(argv[1]);

  VMSDK_ASSIGN_OR_RETURN(
      auto index_schema,
      SchemaManager::Instance().GetIndexSchema(ValkeyModule_GetSelectedDb(ctx),
                                               index_schema_name));
  static const auto permissions =
      PrefixACLPermissions(kDropIndexCmdPermissions, kDropIndexCommand);
  VMSDK_RETURN_IF_ERROR(
      AclPrefixCheck(ctx, permissions, index_schema->GetKeyPrefixes()));

  VMSDK_RETURN_IF_ERROR(SchemaManager::Instance().RemoveIndexSchema(
      ctx, ValkeyModule_GetSelectedDb(ctx), index_schema_name));

  // directly handle reply in standalone mode
  // let fanout operation handle reply in cluster mode
  if (!ValkeySearch::Instance().IsCluster() ||
      !ValkeySearch::Instance().UsingCoordinator()) {
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  }
  ValkeyModule_ReplicateVerbatim(ctx);
  return absl::OkStatus();
}
}  // namespace valkey_search
