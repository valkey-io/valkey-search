/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "src/acl.h"
#include "src/commands/commands.h"
#include "src/commands/ft_info_parser.h"
#include "src/query/cluster_info_fanout_operation.h"
#include "src/query/primary_info_fanout_operation.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/module_config.h"
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

  InfoCommand cmd;
  auto status = cmd.ParseCommand(ctx, itr);
  if (!status.ok()) {
    ValkeyModule_ReplyWithError(ctx, status.message().data());
    return absl::OkStatus();
  }

  status = cmd.Execute(ctx);
  if (!status.ok()) {
    ValkeyModule_ReplyWithError(ctx, status.message().data());
  }
  return absl::OkStatus();
}

}  // namespace valkey_search
