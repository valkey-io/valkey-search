/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <vector>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "src/commands/commands.h"
#include "src/commands/filter_parser.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

absl::Status FTExplainCliCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                             int argc) {
  if (argc != 3) {
    return absl::InvalidArgumentError(
        "Wrong number of arguments for FT.EXPLAINCLI command. "
        "Usage: FT.EXPLAINCLI <index> <query>");
  }

  // Get index name and query string
  size_t index_name_len;
  const char *index_name_str =
      ValkeyModule_StringPtrLen(argv[1], &index_name_len);
  absl::string_view index_name(index_name_str, index_name_len);

  size_t query_len;
  const char *query_str = ValkeyModule_StringPtrLen(argv[2], &query_len);
  absl::string_view query(query_str, query_len);

  // Get the index schema
  VMSDK_ASSIGN_OR_RETURN(
      auto index_schema, SchemaManager::Instance().GetIndexSchema(
                             ValkeyModule_GetSelectedDb(ctx), index_name));

  // Parse the query to build the predicate tree
  TextParsingOptions options{};
  FilterParser parser(*index_schema, query, options);
  
  auto parse_results = parser.Parse();
  if (!parse_results.ok()) {
    ValkeyModule_ReplyWithError(ctx, parse_results.status().message().data());
    return absl::OkStatus();
  }

  // Generate the predicate tree explanation
  std::string explanation;
  if (parse_results->root_predicate) {
    explanation = PrintPredicateTree(parse_results->root_predicate.get());
  } else {
    explanation = "No predicate tree (empty or match-all query)";
  }

  // Split the explanation into lines for better CLI display
  std::vector<std::string> lines;
  size_t start = 0;
  size_t pos = 0;
  
  while ((pos = explanation.find('\n', start)) != std::string::npos) {
    std::string line = explanation.substr(start, pos - start);
    if (!line.empty()) {  // Skip empty lines
      lines.push_back(line);
    }
    start = pos + 1;
  }
  
  // Add the last line if it doesn't end with newline
  if (start < explanation.length()) {
    std::string line = explanation.substr(start);
    if (!line.empty()) {
      lines.push_back(line);
    }
  }
  
  // Reply with an array of lines
  ValkeyModule_ReplyWithArray(ctx, lines.size());
  for (const auto& line : lines) {
    ValkeyModule_ReplyWithStringBuffer(ctx, line.c_str(), line.length());
  }
  
  return absl::OkStatus();
}

}  // namespace valkey_search
