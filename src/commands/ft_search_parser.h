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
vmsdk::config::Number &GetMaxTimeoutMs();
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
    return sortby_parameter.has_value();
  }

  query::SerializationRange GetSerializationRange() const;

  // Replies one array per document in search_result.neighbors[start, end).
  void ReplyRows(ValkeyModuleCtx *ctx, const query::SearchResult &search_result,
                 size_t start, size_t end) const;

  bool with_sort_keys{false};
  bool with_scores{false};

 private:
  // Settings shared by every row of a reply.
  struct RowFormat {
    bool has_relevance{false};
    bool sort_by_vec_score{false};
    std::string sort_key_prefix;
  };
  RowFormat GetRowFormat() const;
  // Replies a document's elements: key, [score], [sort key], fields. Returns
  // the number of elements replied.
  size_t ReplyRowElements(ValkeyModuleCtx *ctx,
                          const indexes::Neighbor &neighbor,
                          const RowFormat &format) const;
};

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_
