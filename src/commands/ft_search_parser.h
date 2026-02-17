/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_
#define VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_

#include <optional>

#include "src/commands/commands.h"
#include "src/query/search.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace options {
vmsdk::config::Number &GetMaxKnn();
}  // namespace options

absl::Status VerifyQueryString(query::SearchParameters &parameters);

//
// Data Unique to the FT.SEARCH command
//
struct SearchCommand : public QueryCommand {
  SearchCommand(int db_num) : QueryCommand(db_num) {}
  absl::Status ParseCommand(vmsdk::ArgsIterator &itr) override;
  void SendReply(ValkeyModuleCtx *ctx,
                 query::SearchResult &search_result) override;
  absl::Status PostParseQueryString() override;
  // By default, FT.SEARCH does not require complete results and can be
  // optimized with LIMIT based trimming. Implement the correct logic here to
  // return true when those clauses are present.
  bool RequiresCompleteResults() const override {
    return sortby.has_value();
  }

  // Returns the sortby field identifier if sorting is enabled.
  std::optional<std::string> GetSortByIdentifier() const override {
    if (sortby.has_value()) {
      auto schema_identifier = index_schema->GetIdentifier(sortby->field);
      return schema_identifier.ok() ? *schema_identifier : sortby->field;
    }
    return std::nullopt;
  }

  std::optional<query::SortByParameter> sortby;
  bool with_sort_keys{false};
};

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_
