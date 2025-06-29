/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <string>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "src/commands/commands.h"
#include "src/schema_manager.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

absl::Status FTListCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                       int argc) {
  if (argc > 1) {
    return absl::InvalidArgumentError(vmsdk::WrongArity(kListCommand));
  }
  absl::flat_hash_set<std::string> names =
      SchemaManager::Instance().GetIndexSchemasInDB(
          ValkeyModule_GetSelectedDb(ctx));
  ValkeyModule_ReplyWithArray(ctx, names.size());
  for (const auto &name : names) {
    ValkeyModule_ReplyWithSimpleString(ctx, name.c_str());
  }
  return absl::OkStatus();
}
}  // namespace valkey_search
