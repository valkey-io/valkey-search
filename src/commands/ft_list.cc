/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <optional>
#include <regex>
#include <string>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "src/commands/commands.h"
#include "src/schema_manager.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

absl::Status FTListCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                       int argc) {
  if (argc > 1 && argc != 3) {
    return absl::InvalidArgumentError(vmsdk::WrongArity(kListCommand));
  }

  std::optional<std::regex> filter_regex;
  if (argc == 3) {
    if (absl::AsciiStrToUpper(vmsdk::ToStringView(argv[1])) != "REGEX") {
      return absl::InvalidArgumentError(vmsdk::WrongArity(kListCommand));
    }
    try {
      filter_regex.emplace(std::string(vmsdk::ToStringView(argv[2])));
    } catch (const std::regex_error &e) {
      return absl::InvalidArgumentError(
          absl::StrCat("Invalid regular expression: ", e.what()));
    }
  }

  absl::flat_hash_set<std::string> names =
      SchemaManager::Instance().GetIndexSchemasInDB(
          ValkeyModule_GetSelectedDb(ctx));

  absl::flat_hash_set<std::string> filtered_names;
  if (filter_regex.has_value()) {
    for (const auto &name : names) {
      if (std::regex_search(name, *filter_regex)) {
        filtered_names.insert(name);
      }
    }
  } else {
    filtered_names = names;
  }

  ValkeyModule_ReplyWithArray(ctx, filtered_names.size());
  for (const auto &name : filtered_names) {
    ValkeyModule_ReplyWithSimpleString(ctx, name.c_str());
  }
  return absl::OkStatus();
}
}  // namespace valkey_search
