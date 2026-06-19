/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "absl/status/status.h"
#include "src/commands/commands.h"
#include "src/schema_manager.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

absl::Status FTAliasListCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                            int argc) {
  if (argc != 1) {
    return absl::InvalidArgumentError(vmsdk::WrongArity(kAliasListCommand));
  }
  auto aliases =
      SchemaManager::Instance().GetAllAliases(ValkeyModule_GetSelectedDb(ctx));
  ValkeyModule_ReplyWithArray(ctx, aliases.size() * 2);
  for (const auto &[alias, index_name] : aliases) {
    ValkeyModule_ReplyWithSimpleString(ctx, alias.c_str());
    ValkeyModule_ReplyWithSimpleString(ctx, index_name.c_str());
  }
  return absl::OkStatus();
}

}  // namespace valkey_search
