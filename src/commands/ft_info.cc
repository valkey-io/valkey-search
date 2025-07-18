/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/string_view.h"
#include "src/acl.h"
#include "src/commands/commands.h"
#include "src/query/info_fanout.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

absl::Status FTInfoCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                       int argc) {
  if (argc < 2) {
    ValkeyModule_ReplyWithError(ctx, vmsdk::WrongArity(kInfoCommand).c_str());
    return absl::OkStatus();
  }

  vmsdk::ArgsIterator itr{argv, argc};
  itr.Next();
  VMSDK_ASSIGN_OR_RETURN(auto itr_arg, itr.Get());
  auto index_schema_name = vmsdk::ToStringView(itr_arg);

  // Parse optional LOCAL/GLOBAL parameter
  bool is_global = false;
  if (argc == 2) {
    // Only 1 parameter provided (index name), default to LOCAL
    is_global = false;
    VMSDK_LOG(NOTICE, ctx) << "==========Using Local Scope==========";
  } else if (argc == 3) {
    // 2 parameters provided, validate the second parameter
    itr.Next();
    VMSDK_ASSIGN_OR_RETURN(auto scope_arg, itr.Get());
    auto scope = vmsdk::ToStringView(scope_arg);

    if (absl::EqualsIgnoreCase(scope, "LOCAL")) {
      VMSDK_LOG(NOTICE, ctx) << "==========Using Local Scope==========";
      is_global = false;
    } else if (absl::EqualsIgnoreCase(scope, "GLOBAL")) {
      // Check if we're in cluster mode and using coordinator
      if (!ValkeySearch::Instance().IsCluster() || 
          !ValkeySearch::Instance().UsingCoordinator()) {
        ValkeyModule_ReplyWithError(
            ctx, "ERR GLOBAL scope requires cluster mode with coordinator enabled");
        return absl::OkStatus();
      }
      is_global = true;
      
      VMSDK_LOG(NOTICE, ctx) << "==========Using Global Scope==========";
      
      // Get and print fanout targets
      auto targets = query::info_fanout::GetInfoTargetsForFanout(ctx);
      VMSDK_LOG(NOTICE, ctx) << "Found " << targets.size() << " fanout targets:";

      for (const auto& target : targets) {
        VMSDK_LOG(NOTICE, ctx)
            << "  Target type: "
            << (target.type == query::fanout::FanoutSearchTarget::Type::kLocal ? "LOCAL" : "REMOTE")
            << ", address: " << target.address;
      }
    } else {
      // Invalid scope parameter
      ValkeyModule_ReplyWithError(
          ctx, "ERR Invalid scope parameter. Must be LOCAL or GLOBAL");
      return absl::OkStatus();
    }
  } else {
    // More than 2 parameters provided
    ValkeyModule_ReplyWithError(ctx, vmsdk::WrongArity(kInfoCommand).c_str());
    return absl::OkStatus();
  }

  // TODO global info fanout

  VMSDK_ASSIGN_OR_RETURN(
      auto index_schema,
      SchemaManager::Instance().GetIndexSchema(ValkeyModule_GetSelectedDb(ctx),
                                               index_schema_name));
  static const auto permissions =
      PrefixACLPermissions(kInfoCmdPermissions, kInfoCommand);
  VMSDK_RETURN_IF_ERROR(
      AclPrefixCheck(ctx, permissions, index_schema->GetKeyPrefixes()));
  index_schema->RespondWithInfo(ctx);

  return absl::OkStatus();
}

}  // namespace valkey_search
