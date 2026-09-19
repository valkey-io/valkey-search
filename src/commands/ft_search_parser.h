/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_
#define VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_

#include <optional>
#include <string>

#include "src/commands/commands.h"
#include "src/query/search.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace options {
vmsdk::config::Number& GetMaxKnn();
vmsdk::config::Number& GetMaxTimeoutMs();
}  // namespace options

absl::Status VerifyQueryString(query::SearchParameters& parameters);

//
// Data Unique to the FT.SEARCH command
//
struct SearchCommand : public QueryCommand {
  SearchCommand(int db_num) : QueryCommand(db_num) {}
  absl::Status ParseCommand(vmsdk::ArgsIterator& itr) override;
  void SendReply(ValkeyModuleCtx* ctx,
                 query::SearchResult& search_result) override;
  absl::Status PostParseQueryString() override;
  // By default, FT.SEARCH does not require complete results and can be
  // optimized with LIMIT based trimming. Implement the correct logic here to
  // return true when those clauses are present.
  bool RequiresCompleteResults() const override {
    return sortby_parameter.has_value() || num_vr_predicates > 0;
  }

  query::SerializationRange GetSerializationRange() const;

  bool with_sort_keys{false};
  bool with_scores{false};

  // True when the last RETURN clause was `RETURN 0`; folded into no_content
  // after the full command is parsed (ParseCommand), so it cannot cancel a
  // sticky NOCONTENT keyword set by the NOCONTENT keyword itself.
  // This is separate from SearchParameters.no_content during parsing
  // to allow a later RETURN to override an earlier `RETURN 0`.
  bool return_no_fields{false};

  // Returns true if this is a standalone vector range query (no KNN).
  bool IsVectorRangeQuery() const {
    return IsNonVectorQuery() && num_vr_predicates > 0;
  }
};

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_
