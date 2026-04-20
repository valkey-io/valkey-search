/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_H_
#define VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_H_

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "src/query/search.h"

namespace valkey_search {
class ValkeySearch;

// Remove neighbors whose key is not in the inkeys set.
// Adjusts total_count down by the number removed (floors at 0).
// An empty set drops every neighbor — callers should gate on
// the optional being engaged, not on the set being non-empty.
inline void ApplyInkeysFilter(query::SearchResult &search_result,
                              const absl::flat_hash_set<std::string> &inkeys) {
  auto &neighbors = search_result.neighbors;
  auto it = std::remove_if(neighbors.begin(), neighbors.end(),
                           [&inkeys](const indexes::Neighbor &n) {
                             return !inkeys.contains(n.external_id->Str());
                           });
  size_t removed = std::distance(it, neighbors.end());
  neighbors.erase(it, neighbors.end());
  search_result.total_count = (search_result.total_count >= removed)
                                  ? search_result.total_count - removed
                                  : 0;
}

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_H_
