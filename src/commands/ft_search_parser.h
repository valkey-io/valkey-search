/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_
#define VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_

#include <cstdint>

#include "src/commands/commands.h"
#include "src/query/search.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace options {
vmsdk::config::Number &GetMaxKnn();
}  // namespace options

struct LimitParameter {
  uint64_t first_index{0};
  uint64_t number{10};
};

absl::Status PreParseQueryString(query::SearchParameters &parameters);
absl::Status PostParseQueryString(query::SearchParameters &parameters);
absl::Status VerifyQueryString(query::SearchParameters &parameters);

//
// Data Unique to the FT.SEARCH command
//
struct SearchCommand : public QueryCommand {
  SearchCommand(int db_num) : QueryCommand(db_num) {}
  absl::Status ParseCommand(vmsdk::ArgsIterator &itr) override;
  void SendReply(ValkeyModuleCtx *ctx,
                 query::SearchResult &search_result) override;
  // By default, FT.SEARCH does not require complete results and can optimized
  // with LIMIT based trimming.
  // TODO: When SORTBY or similar clauses are supported, implement the correct
  // logic here to return true when those clauses are present.
  bool RequiresCompleteResults() const override { return false; }
};

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_
