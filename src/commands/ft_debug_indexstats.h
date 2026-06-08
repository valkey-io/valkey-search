/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_DEBUG_INDEXSTATS_H_
#define VALKEYSEARCH_SRC_COMMANDS_FT_DEBUG_INDEXSTATS_H_

#include <cstdint>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

class IndexSchema;

// Generic ordered key/value tree used as the intermediate result form. Both
// the RESP emitter and the human-readable log builder consume this same shape;
// new statistics can be added with one-line edits to a collector.
struct StatValue;
using StatPairs = std::vector<std::pair<std::string, StatValue>>;

struct StatValue {
  std::variant<int64_t, std::string, StatPairs> v;

  StatValue() : v(int64_t{0}) {}
  // NOLINTBEGIN(google-explicit-constructor)
  StatValue(int n) : v(static_cast<int64_t>(n)) {}
  StatValue(int64_t n) : v(n) {}
  StatValue(uint64_t n) : v(static_cast<int64_t>(n)) {}
  StatValue(const char* s) : v(std::string(s)) {}
  StatValue(std::string s) : v(std::move(s)) {}
  StatValue(absl::string_view s) : v(std::string(s)) {}
  StatValue(StatPairs sp) : v(std::move(sp)) {}
  // NOLINTEND(google-explicit-constructor)
};

// Build the StatPairs tree for the given index, optionally filtered to a
// subset of attributes by alias. Caller must hold a reader lock on the index
// schema's time_sliced_mutex_.
StatPairs CollectIndexStats(const IndexSchema& schema,
                            const std::vector<std::string>& fields);

// Renders a StatPairs as the RESP reply on `ctx`.
void EmitResp(ValkeyModuleCtx* ctx, const StatPairs& kv);

// Renders a StatPairs as a multi-line, indented human-readable string.
std::string BuildLogLine(const StatPairs& kv);

// Top-level handler: FT._DEBUG INDEXSTATS <index> [<field> ...]
absl::Status IndexStatsCmd(ValkeyModuleCtx* ctx, vmsdk::ArgsIterator& itr);

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_DEBUG_INDEXSTATS_H_
