/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_RESULTS_REPLY_H_
#define VALKEYSEARCH_SRC_COMMANDS_RESULTS_REPLY_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "absl/strings/string_view.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

// Writes the results of FT.SEARCH, FT.AGGREGATE or an FT.CURSOR read in the
// client's protocol. Callers describe the results; how they are laid out in
// RESP2 or RESP3 stays in the two implementations.
//
// RESP3 replies the Redis Search envelope:
//   {attributes: [], warning: [], total_results, format: "STRING", results}
// with one map per result: {id, score, sortkey, extra_attributes, values}.
class ResultsReply {
 public:
  // The reply being written, which decides the RESP2 layout. RESP3 always
  // replies the envelope, as [envelope, cursor_id] for kSearchCursor.
  enum class Layout {
    // FT.SEARCH: [total, row elements...], each row's elements inline.
    kSearch,
    // FT.SEARCH ... WITHCURSOR: [total, [row...], cursor_id].
    kSearchCursor,
    // FT.AGGREGATE and FT.CURSOR READ: [count, row...], each row an array.
    kRows,
  };

  // One result. Only FT.SEARCH results have an id; an FT.AGGREGATE result is
  // just its fields.
  struct Row {
    std::optional<absl::string_view> id;
    // WITHSCORES.
    std::optional<double> score;
    // WITHSORTKEYS: the sort key, or nullopt to reply nil.
    std::optional<std::optional<std::string>> sort_key;
    // Replies the field/value pairs and returns their number. Unset under
    // NOCONTENT. With unique_names the pairs go into a map, so a field named
    // like a computed one must be left out.
    std::function<size_t(bool unique_names)> fields;
  };

  // RESP3 clients get the envelope unless search_envelope is false, which
  // keeps FT.HYBRID's RESP2 shape.
  static std::unique_ptr<ResultsReply> Create(ValkeyModuleCtx *ctx,
                                              Layout layout,
                                              bool search_envelope = true);

  virtual ~ResultsReply() = default;
  virtual bool IsResp3() const = 0;
  // Starts the reply of `count` rows out of `total` matches.
  virtual void Begin(size_t total, size_t count) = 0;
  virtual void AddRow(const Row &row) = 0;
  // Ends the reply. kSearchCursor also replies the cursor id, 0 when no cursor
  // was created.
  virtual void End(uint64_t cursor_id = 0) = 0;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_COMMANDS_RESULTS_REPLY_H_
