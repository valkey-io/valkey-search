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
vmsdk::config::Number &GetMaxKnn();
vmsdk::config::Number &GetMaxTimeoutMs();
}  // namespace options

absl::Status VerifyQueryString(query::SearchParameters &parameters);

class ResultsReply;

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
    return sortby_parameter.has_value() || inkeys.has_value();
  }

  query::SerializationRange GetSerializationRange() const;

  // Adds a row to reply per document in search_result.neighbors[start, end).
  void ReplyRows(ValkeyModuleCtx *ctx, ResultsReply &reply,
                 const query::SearchResult &search_result, size_t start,
                 size_t end) const;

  bool with_sort_keys{false};
  bool with_scores{false};

  // True when the last RETURN clause was `RETURN 0`; folded into no_content
  // after parsing (ParseCommand).
  // This is separate from SearchParameters.no_content during parsing
  // as there can be many or none return clause, and only the last RETURN is
  // effective
  bool return_no_fields{false};

  // Returns true if this is a standalone vector range query (no KNN).
  bool IsVectorRangeQuery() const {
    return IsNonVectorQuery() && has_vector_range;
  }

 private:
  // Settings shared by every row of a reply.
  struct RowFormat {
    bool has_relevance{false};
    bool sort_by_vec_score{false};
    bool nil_absent_sort_key{false};
    std::string sort_key_prefix;
    // The single VR distance field name (query::GetVrScoreFieldName).
    // Empty unless the query named the distance ($yield_distance_as or AS).
    std::string vr_field;
  };
  // RESP3 sort keys always follow Redis Search (see GetRowFormat).
  RowFormat GetRowFormat(bool resp3) const;
  // The document's sort key, or nullopt to reply nil.
  std::optional<std::string> SortKey(const indexes::Neighbor &neighbor,
                                     const RowFormat &format) const;
  // Replies the document's field/value pairs and returns their number. With
  // unique_names, a field named like the reported distance is left out.
  size_t ReplyAttributes(ValkeyModuleCtx *ctx,
                         const indexes::Neighbor &neighbor,
                         const RowFormat &format, bool unique_names) const;
};

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_
