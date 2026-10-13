/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
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

  // What every row of the reply carries, fixed by the query.
  struct Columns {
    // FT.SEARCH results have an id; an FT.AGGREGATE result is just its fields.
    bool id{false};
    // WITHSCORES.
    bool score{false};
    // WITHSORTKEYS.
    bool sort_key{false};
    // Unset under NOCONTENT.
    bool fields{true};
  };

  // Replies the field/value pairs of one row.
  class Fields {
   public:
    virtual ~Fields() = default;
    // Replies the pairs and returns their number. With unique_names the pairs
    // go into a map, so a field named like a computed one must be left out.
    virtual size_t Reply(ValkeyModuleCtx *ctx, bool unique_names) const = 0;
  };

  // One result. Only the members its Columns name are read.
  struct Row {
    absl::string_view id;
    double score{0};
    // The sort key is sort_key_prefix followed by sort_key, or nil when
    // sort_key_nil is set.
    char sort_key_prefix{0};
    absl::string_view sort_key;
    bool sort_key_nil{false};
    const Fields *fields{nullptr};
  };

  // RESP3 clients get the envelope unless search_envelope is false, which
  // keeps FT.HYBRID's RESP2 shape.
  static std::unique_ptr<ResultsReply> Create(ValkeyModuleCtx *ctx,
                                              Layout layout, Columns columns,
                                              bool search_envelope = true);

  virtual ~ResultsReply() = default;
  // 2 or 3.
  int RespVersion() const { return resp_version_; }
  // Starts the reply of `count` rows out of `total` matches.
  virtual void Begin(size_t total, size_t count) = 0;
  virtual void AddRow(const Row &row) = 0;
  // Ends the reply. kSearchCursor also replies the cursor id, 0 when no cursor
  // was created.
  virtual void End(uint64_t cursor_id = 0) = 0;

 protected:
  ResultsReply(ValkeyModuleCtx *ctx, Layout layout, Columns columns,
               int resp_version)
      : ctx_(ctx),
        layout_(layout),
        columns_(columns),
        resp_version_(resp_version) {}

  void ReplySortKey(const Row &row);

  ValkeyModuleCtx *ctx_;
  Layout layout_;
  Columns columns_;

 private:
  int resp_version_;
  // Joins the sort key prefix and value, reused across rows.
  std::string sort_key_buffer_;
};

}  // namespace valkey_search
