/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <cstdint>
#include <optional>
#include <string>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "src/commands/commands.h"
#include "src/schema_manager.h"
#include "src/utils/regex_util.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

absl::Status FTListCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                       int argc) {
  vmsdk::ArgsIterator itr{argv, argc};
  itr.Next();  // Skip the command name

  std::optional<utils::Regex> filter_regex;
  if (itr.PopIfNextIgnoreCase("REGEX")) {
    if (!itr.HasNext()) {
      return absl::InvalidArgumentError("Missing pattern for REGEX");
    }
    absl::string_view pattern = *itr.GetStringView();
    const int64_t max_bytes = options::GetMaxRegexPatternBytes().GetValue();
    if (static_cast<int64_t>(pattern.size()) > max_bytes) {
      return absl::InvalidArgumentError(absl::StrCat(
          "REGEX pattern is too long (max ", max_bytes, " bytes)"));
    }
    VMSDK_ASSIGN_OR_RETURN(filter_regex, utils::Regex::Compile(pattern));
    itr.Next();
  }
  if (itr.HasNext()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected argument `", *itr.GetStringView(), "`"));
  }

  absl::flat_hash_set<std::string> names =
      SchemaManager::Instance().GetIndexSchemasInDB(
          ValkeyModule_GetSelectedDb(ctx));
  if (filter_regex.has_value()) {
    absl::erase_if(names, [&](const std::string &name) {
      return !filter_regex->PartialMatch(name);
    });
  }

  ValkeyModule_ReplyWithArray(ctx, names.size());
  for (const auto &name : names) {
    ValkeyModule_ReplyWithSimpleString(ctx, name.c_str());
  }
  return absl::OkStatus();
}
}  // namespace valkey_search
