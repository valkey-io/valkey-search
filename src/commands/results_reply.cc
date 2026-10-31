/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "results_reply.h"

#include <cstddef>
#include <cstdint>
#include <memory>

#include "src/expr/value.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

void ReplyWithStringView(ValkeyModuleCtx *ctx, absl::string_view s) {
  ValkeyModule_ReplyWithStringBuffer(ctx, s.data(), s.size());
}

class Resp2ResultsReply : public ResultsReply {
 public:
  Resp2ResultsReply(ValkeyModuleCtx *ctx, Layout layout, Columns columns)
      : ResultsReply(ctx, layout, columns, kResp2) {}

  void Begin(size_t total, size_t count) override {
    switch (layout_) {
      case Layout::kSearch:
        // The length is the number of row elements, known at End().
        ValkeyModule_ReplyWithArray(ctx_, VALKEYMODULE_POSTPONED_LEN);
        ValkeyModule_ReplyWithLongLong(ctx_, total);
        elements_ = 1;
        break;
      case Layout::kSearchCursor:
        ValkeyModule_ReplyWithArray(ctx_, 3);
        ValkeyModule_ReplyWithLongLong(ctx_, total);
        ValkeyModule_ReplyWithArray(ctx_, count);
        break;
      case Layout::kRows:
        ValkeyModule_ReplyWithArray(ctx_, 1 + count);
        ValkeyModule_ReplyWithLongLong(ctx_, count);
        break;
    }
  }

  void AddRow(const Row &row) override {
    if (!columns_.id) {
      // FT.AGGREGATE: [field, value...].
      ValkeyModule_ReplyWithArray(ctx_, VALKEYMODULE_POSTPONED_LEN);
      ValkeyModule_ReplySetArrayLength(ctx_,
                                       2 * row.fields->Reply(ctx_, false));
      ++elements_;
      return;
    }
    const bool inline_row = layout_ == Layout::kSearch;
    if (!inline_row) {
      ValkeyModule_ReplyWithArray(ctx_, VALKEYMODULE_POSTPONED_LEN);
    }
    ReplyWithStringView(ctx_, row.id);
    size_t elements = 1;
    if (columns_.score) {
      ReplyWithStringView(ctx_, expr::FormatDouble(row.score));
      ++elements;
    }
    // RESP2 drops the sort key under NOCONTENT.
    if (columns_.fields) {
      if (columns_.sort_key) {
        ReplySortKey(row);
        ++elements;
      }
      ValkeyModule_ReplyWithArray(ctx_, VALKEYMODULE_POSTPONED_LEN);
      ValkeyModule_ReplySetArrayLength(ctx_,
                                       2 * row.fields->Reply(ctx_, false));
      ++elements;
    }
    if (inline_row) {
      elements_ += elements;
    } else {
      ValkeyModule_ReplySetArrayLength(ctx_, elements);
    }
  }

  void End(uint64_t cursor_id) override {
    switch (layout_) {
      case Layout::kSearch:
        ValkeyModule_ReplySetArrayLength(ctx_, elements_);
        break;
      case Layout::kSearchCursor:
        ValkeyModule_ReplyWithLongLong(ctx_, static_cast<long long>(cursor_id));
        break;
      case Layout::kRows:
        break;
    }
  }

 private:
  // kSearch: the elements replied so far.
  size_t elements_{0};
};

class Resp3ResultsReply : public ResultsReply {
 public:
  Resp3ResultsReply(ValkeyModuleCtx *ctx, Layout layout, Columns columns)
      : ResultsReply(ctx, layout, columns, kResp3) {}

  void Begin(size_t total, size_t count) override {
    if (layout_ == Layout::kSearchCursor) {
      ValkeyModule_ReplyWithArray(ctx_, 2);
    }
    ValkeyModule_ReplyWithMap(ctx_, 5);
    ValkeyModule_ReplyWithSimpleString(ctx_, "attributes");
    ValkeyModule_ReplyWithArray(ctx_, 0);
    ValkeyModule_ReplyWithSimpleString(ctx_, "warning");
    ValkeyModule_ReplyWithArray(ctx_, 0);
    ValkeyModule_ReplyWithSimpleString(ctx_, "total_results");
    ValkeyModule_ReplyWithLongLong(ctx_, total);
    ValkeyModule_ReplyWithSimpleString(ctx_, "format");
    ValkeyModule_ReplyWithSimpleString(ctx_, "STRING");
    ValkeyModule_ReplyWithSimpleString(ctx_, "results");
    ValkeyModule_ReplyWithArray(ctx_, count);
  }

  void AddRow(const Row &row) override {
    ValkeyModule_ReplyWithMap(
        ctx_,
        columns_.id + columns_.score + columns_.sort_key + columns_.fields + 1);
    if (columns_.id) {
      ValkeyModule_ReplyWithSimpleString(ctx_, "id");
      ReplyWithStringView(ctx_, row.id);
    }
    if (columns_.score) {
      ValkeyModule_ReplyWithSimpleString(ctx_, "score");
      ValkeyModule_ReplyWithDouble(ctx_, row.score);
    }
    if (columns_.sort_key) {
      ValkeyModule_ReplyWithSimpleString(ctx_, "sortkey");
      ReplySortKey(row);
    }
    if (columns_.fields) {
      ValkeyModule_ReplyWithSimpleString(ctx_, "extra_attributes");
      ValkeyModule_ReplyWithMap(ctx_, VALKEYMODULE_POSTPONED_LEN);
      ValkeyModule_ReplySetMapLength(ctx_, row.fields->Reply(ctx_, true));
    }
    ValkeyModule_ReplyWithSimpleString(ctx_, "values");
    ValkeyModule_ReplyWithArray(ctx_, 0);
  }

  void End(uint64_t cursor_id) override {
    if (layout_ == Layout::kSearchCursor) {
      ValkeyModule_ReplyWithLongLong(ctx_, static_cast<long long>(cursor_id));
    }
  }
};

}  // namespace

void ResultsReply::ReplySortKey(const Row &row) {
  if (row.sort_key_nil) {
    ValkeyModule_ReplyWithNull(ctx_);
    return;
  }
  sort_key_buffer_.assign(1, row.sort_key_prefix);
  sort_key_buffer_.append(row.sort_key);
  ReplyWithStringView(ctx_, sort_key_buffer_);
}

std::unique_ptr<ResultsReply> ResultsReply::Create(ValkeyModuleCtx *ctx,
                                                   Layout layout,
                                                   Columns columns,
                                                   bool search_envelope) {
  if (search_envelope &&
      (ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_RESP3)) {
    return std::make_unique<Resp3ResultsReply>(ctx, layout, columns);
  }
  return std::make_unique<Resp2ResultsReply>(ctx, layout, columns);
}

}  // namespace valkey_search
