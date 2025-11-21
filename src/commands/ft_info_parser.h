/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_INFO_PARSER_H_
#define VALKEYSEARCH_SRC_COMMANDS_FT_INFO_PARSER_H_

#include <cstdint>
#include <memory>

#include "absl/status/status.h"
#include "src/index_schema.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

enum class InfoScope { kLocal, kPrimary, kCluster };

struct InfoCommand {
  std::shared_ptr<IndexSchema> index_schema;
  std::string index_schema_name;
  InfoScope scope{InfoScope::kLocal};
  bool enable_partial_results{false};
  bool require_consistency{true};
  uint32_t timeout_ms{0};

  absl::Status ParseCommand(ValkeyModuleCtx *ctx, vmsdk::ArgsIterator &itr);
  absl::Status Execute(ValkeyModuleCtx *ctx);
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_INFO_PARSER_H_
