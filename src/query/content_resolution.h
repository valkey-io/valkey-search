/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_SRC_QUERY_CONTENT_RESOLUTION_H_
#define VALKEY_SEARCH_SRC_QUERY_CONTENT_RESOLUTION_H_

#include <memory>

#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query {

struct SearchParameters;

// Entry point — called on main thread after search completes.
// Handles contention checking (registering with mutation queue if needed),
// content fetching via ProcessNeighborsForReply, and final completion.
void ResolveContent(std::unique_ptr<SearchParameters> params);

// Fetches content for neighbors and adjusts total_count for any removed
// neighbors. Can be called directly from the Reply callback when content
// resolution is deferred (content_resolution_pending_ optimization).
void FetchContent(SearchParameters& params, ValkeyModuleCtx* ctx);

}  // namespace valkey_search::query

#endif  // VALKEY_SEARCH_SRC_QUERY_CONTENT_RESOLUTION_H_
