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
#include "src/schema_manager.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

#include <iostream>

namespace valkey_search {

absl::Status FTInfoCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                       int argc) {
  if (argc < 2) {
    return absl::InvalidArgumentError(vmsdk::WrongArity(kInfoCommand));
  }

  vmsdk::ArgsIterator itr{argv, argc};
  itr.Next();
  VMSDK_ASSIGN_OR_RETURN(auto itr_arg, itr.Get());
  auto index_schema_name = vmsdk::ToStringView(itr_arg);

  // Parse optional LOCAL/GLOBAL parameter
  bool is_global = false;
  if (argc > 2) {  // Only parse scope if there's a third argument
    itr.Next();
    VMSDK_ASSIGN_OR_RETURN(auto scope_arg, itr.Get());
    auto scope = vmsdk::ToStringView(scope_arg);

    // default parameter is LOCAL; GLOBAL must be specified to retrieve global info
    if (absl::EqualsIgnoreCase(scope, "GLOBAL")) {
      is_global = true;
      std::cout << "==========Using Global Scope==========" << std::endl;
    }
  }

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
