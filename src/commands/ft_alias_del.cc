/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "src/commands/commands.h"
#include "src/commands/ft_alias_consistency.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

absl::Status FTAliasDelCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                           int argc) {
  if (argc != 2) {
    return absl::InvalidArgumentError(vmsdk::WrongArity(kAliasDelCommand));
  }
  auto alias = vmsdk::ToStringView(argv[1]);

  // Reject early if inside MULTI/EXEC in CME mode to avoid local mutation
  // without fanout.
  const bool inside_multi_exec = vmsdk::MultiOrLua(ctx);
  if (ABSL_PREDICT_FALSE(inside_multi_exec &&
                         ValkeySearch::Instance().IsCluster() &&
                         ValkeySearch::Instance().UsingCoordinator())) {
    return absl::InvalidArgumentError(
        "MULTI/EXEC or Lua script are not supported in CME mode.");
  }

  VMSDK_RETURN_IF_ERROR(SchemaManager::Instance().RemoveAlias(
      ValkeyModule_GetSelectedDb(ctx), alias));

  // Cluster consistency check — wait for alias deletion to propagate.
  const bool is_loading =
      ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_LOADING;
  if (ValkeySearch::Instance().IsCluster() &&
      ValkeySearch::Instance().UsingCoordinator()) {
    if (!is_loading) {
      unsigned timeout_ms = options::GetFTInfoTimeoutMs().GetValue();
      auto op = new AliasRemovedConsistencyCheckFanoutOperation(
          ValkeyModule_GetSelectedDb(ctx), std::string(alias), timeout_ms);
      op->StartOperation(ctx);
    } else {
      ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    }
  } else {
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  }

  if (!options::GetUseCoordinator().GetValue()) {
    ValkeyModule_ReplicateVerbatim(ctx);
  }
  return absl::OkStatus();
}

}  // namespace valkey_search
