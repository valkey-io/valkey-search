/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_SRC_QUERY_CONTENT_RESOLUTION_H_
#define VALKEY_SEARCH_SRC_QUERY_CONTENT_RESOLUTION_H_

#include <memory>

#include "src/indexes/vector_base.h"
#include "src/query/search.h"
#include "src/utils/string_interning.h"

namespace valkey_search::query {

// Prepares `params` to resolve content for `keys` instead of running a search
// (bounded content fan-out recovery). ResolveContent then applies the
// contention check, fetch, revalidation and slot ownership exactly as for a
// search, and drops keys that no longer match. Sequence number 0 makes
// revalidation run for any key written since load (sequence numbers start at
// 1); a key still at 0 is unchanged since load, so its earlier match holds.
template <typename Keys>
void SeedContentFetch(SearchParameters &params, const Keys &keys) {
  // Every key is a global winner: resolve content for all of them.
  params.content_limit.reset();
  auto &neighbors = params.search_result.neighbors;
  neighbors.clear();
  neighbors.reserve(keys.size());
  for (const auto &key : keys) {
    neighbors.emplace_back(StringInternStore::Intern(key), 0.0f);
  }
  params.search_result.total_count = neighbors.size();
}

// Entry point — called on main thread after search completes.
// Handles contention checking (registering with mutation queue if needed),
// content fetching via ProcessNeighborsForReply, and final completion.
void ResolveContent(std::unique_ptr<SearchParameters> params);

}  // namespace valkey_search::query

#endif  // VALKEY_SEARCH_SRC_QUERY_CONTENT_RESOLUTION_H_
