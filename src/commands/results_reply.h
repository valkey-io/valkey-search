/*
 * Copyright (c) 2026, valkey-search contributors
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

// Replies FT.SEARCH, FT.AGGREGATE and FT.CURSOR READ results in RESP2 or RESP3.
//
// RESP3 follows Redis Search:
//   {attributes: [], warning: [], total_results, format: "STRING", results}
// with each result {id, score, sortkey, extra_attributes, values}.
class ResultsReply {
 public:
  // The RESP2 layout. RESP3 always replies the envelope, as
  // [envelope, cursor_id] for kSearchCursor.
  enum class Layout {
    // FT.SEARCH: [total, row elements...], each row's elements inline.
    kSearch,
    // FT.SEARCH ... WITHCURSOR: [total, [row...], cursor_id].
    kSearchCursor,
    // FT.AGGREGATE and FT.CURSOR READ: [count, row...], each row an array.
    kRows,
  };

  struct Columns {
    // FT.SEARCH only.
    bool id{false};
    // WITHSCORES.
    bool score{false};
    // WITHSORTKEYS.
    bool sort_key{false};
    // Unset under NOCONTENT.
    bool fields{true};
  };

  class Fields {
   public:
    virtual ~Fields() = default;
    // Returns the number of pairs replied. unique_names is set for a RESP3 map.
    virtual size_t Reply(ValkeyModuleCtx *ctx, bool unique_names) const = 0;
  };

  // Only the members enabled in Columns are read.
  struct Row {
    absl::string_view id;
    double score{0};
    // sort_key_prefix + sort_key, or nil.
    char sort_key_prefix{0};
    absl::string_view sort_key;
    bool sort_key_nil{false};
    const Fields *fields{nullptr};
  };

  // search_envelope=false keeps the RESP2 reply under RESP3 (FT.HYBRID).
  static std::unique_ptr<ResultsReply> Create(ValkeyModuleCtx *ctx,
                                              Layout layout, Columns columns,
                                              bool search_envelope = true);

  static constexpr int kResp2 = 2;
  static constexpr int kResp3 = 3;

  virtual ~ResultsReply() = default;
  int RespVersion() const { return resp_version_; }
  // Starts the reply of `count` rows out of `total` matches.
  virtual void Begin(size_t total, size_t count) = 0;
  virtual void AddRow(const Row &row) = 0;
  // kSearchCursor also replies cursor_id, 0 when no cursor was created.
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
  // Reused across rows.
  std::string sort_key_buffer_;
};

}  // namespace valkey_search
