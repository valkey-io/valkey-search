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
#include "src/indexes/vector_base.h"

namespace valkey_search {
class ValkeySearch;

// Remove neighbors whose key is not in the inkeys set.
// Adjusts total_count down by the number removed (floors at 0).
// No-op when inkeys is empty.
inline void ApplyInkeysFilter(std::vector<indexes::Neighbor> &neighbors,
                              size_t &total_count,
                              const absl::flat_hash_set<std::string> &inkeys) {
  if (inkeys.empty()) {
    return;
  }
  auto it = std::remove_if(neighbors.begin(), neighbors.end(),
                           [&inkeys](const indexes::Neighbor &n) {
                             return !inkeys.contains(n.external_id->Str());
                           });
  size_t removed = std::distance(it, neighbors.end());
  neighbors.erase(it, neighbors.end());
  total_count = (total_count >= removed) ? total_count - removed : 0;
}

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_H_
