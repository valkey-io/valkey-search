/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <absl/base/no_destructor.h>

#include "info.h"
#include "src/commands/commands.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/status/status_macros.h"

extern vmsdk::module::Options options;  // Declared in module_loader.cc
namespace valkey_search {

enum SubCommands {
  kShowInfo,
};

const absl::flat_hash_map<absl::string_view, SubCommands> kDebugSubcommands({
    {"SHOW_INFO", SubCommands::kShowInfo},
});

absl::Status FTDebugCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                        int argc) {
  vmsdk::ArgsIterator itr{argv, argc};
  itr.Next();  // Skip the command name
  SubCommands subcommand;
  VMSDK_RETURN_IF_ERROR(
      vmsdk::ParseEnumParam(subcommand, itr, &kDebugSubcommands));
  switch (subcommand) {
    case SubCommands::kShowInfo:
      return vmsdk::info_field::ShowInfo(ctx, itr, options);
    default:
      assert(false);
  }
  return absl::InvalidArgumentError("Unknown command");
}

}  // namespace valkey_search