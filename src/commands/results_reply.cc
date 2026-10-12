/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/results_reply.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "src/expr/value.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

void ReplyWithStringView(ValkeyModuleCtx *ctx, absl::string_view s) {
  ValkeyModule_ReplyWithStringBuffer(ctx, s.data(), s.size());
}

void ReplySortKey(ValkeyModuleCtx *ctx,
                  const std::optional<std::string> &sort_key) {
  if (sort_key.has_value()) {
    ReplyWithStringView(ctx, *sort_key);
  } else {
    ValkeyModule_ReplyWithNull(ctx);
  }
}

class Resp2ResultsReply : public ResultsReply {
 public:
  Resp2ResultsReply(ValkeyModuleCtx *ctx, Layout layout)
      : ctx_(ctx), layout_(layout) {}

  bool IsResp3() const override { return false; }

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
    if (!row.id.has_value()) {
      // A result without an id is the array of its field/value pairs.
      ValkeyModule_ReplyWithArray(ctx_, VALKEYMODULE_POSTPONED_LEN);
      ValkeyModule_ReplySetArrayLength(ctx_, 2 * row.fields(false));
      ++elements_;
      return;
    }
    const bool inline_row = layout_ == Layout::kSearch;
    if (!inline_row) {
      ValkeyModule_ReplyWithArray(ctx_, VALKEYMODULE_POSTPONED_LEN);
    }
    ReplyWithStringView(ctx_, *row.id);
    size_t elements = 1;
    if (row.score.has_value()) {
      ReplyWithStringView(ctx_, expr::FormatDouble(*row.score));
      ++elements;
    }
    // NOCONTENT leaves out the sort key along with the fields.
    if (row.fields) {
      if (row.sort_key.has_value()) {
        ReplySortKey(ctx_, *row.sort_key);
        ++elements;
      }
      ValkeyModule_ReplyWithArray(ctx_, VALKEYMODULE_POSTPONED_LEN);
      ValkeyModule_ReplySetArrayLength(ctx_, 2 * row.fields(false));
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
  ValkeyModuleCtx *ctx_;
  Layout layout_;
  // kSearch: the elements replied so far.
  size_t elements_{0};
};

class Resp3ResultsReply : public ResultsReply {
 public:
  Resp3ResultsReply(ValkeyModuleCtx *ctx, Layout layout)
      : ctx_(ctx), layout_(layout) {}

  bool IsResp3() const override { return true; }

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
    ValkeyModule_ReplyWithMap(ctx_, (row.id.has_value() ? 1 : 0) +
                                        (row.score.has_value() ? 1 : 0) +
                                        (row.sort_key.has_value() ? 1 : 0) +
                                        (row.fields ? 1 : 0) + 1);
    if (row.id.has_value()) {
      ValkeyModule_ReplyWithSimpleString(ctx_, "id");
      ReplyWithStringView(ctx_, *row.id);
    }
    if (row.score.has_value()) {
      ValkeyModule_ReplyWithSimpleString(ctx_, "score");
      ValkeyModule_ReplyWithDouble(ctx_, *row.score);
    }
    if (row.sort_key.has_value()) {
      ValkeyModule_ReplyWithSimpleString(ctx_, "sortkey");
      ReplySortKey(ctx_, *row.sort_key);
    }
    if (row.fields) {
      ValkeyModule_ReplyWithSimpleString(ctx_, "extra_attributes");
      ValkeyModule_ReplyWithMap(ctx_, VALKEYMODULE_POSTPONED_LEN);
      ValkeyModule_ReplySetMapLength(ctx_, row.fields(true));
    }
    ValkeyModule_ReplyWithSimpleString(ctx_, "values");
    ValkeyModule_ReplyWithArray(ctx_, 0);
  }

  void End(uint64_t cursor_id) override {
    if (layout_ == Layout::kSearchCursor) {
      ValkeyModule_ReplyWithLongLong(ctx_, static_cast<long long>(cursor_id));
    }
  }

 private:
  ValkeyModuleCtx *ctx_;
  Layout layout_;
};

}  // namespace

std::unique_ptr<ResultsReply> ResultsReply::Create(ValkeyModuleCtx *ctx,
                                                   Layout layout,
                                                   bool search_envelope) {
  if (search_envelope &&
      (ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_RESP3)) {
    return std::make_unique<Resp3ResultsReply>(ctx, layout);
  }
  return std::make_unique<Resp2ResultsReply>(ctx, layout);
}

}  // namespace valkey_search
