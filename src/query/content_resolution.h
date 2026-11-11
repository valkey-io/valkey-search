/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_SRC_QUERY_CONTENT_RESOLUTION_H_
#define VALKEY_SEARCH_SRC_QUERY_CONTENT_RESOLUTION_H_

#include <memory>
#include <vector>

#include "src/indexes/vector_base.h"
#include "src/query/search.h"

namespace valkey_search::query {

// Prepares `params` to resolve content for `candidates` instead of running a
// search (bounded content fan-out recovery). Each candidate carries its key,
// score, distance and mutation sequence number from the first round, and no
// content. ResolveContent then applies the contention check, fetch,
// revalidation and slot ownership exactly as for a search's own results: a
// candidate that no longer matches is dropped, and only one whose document
// changed since the first round is revalidated and rescored.
void SeedContentFetch(SearchParameters &params,
                      std::vector<indexes::Neighbor> candidates);

// Entry point — called on main thread after search completes.
// Handles contention checking (registering with mutation queue if needed),
// content fetching via ProcessNeighborsForReply, and final completion.
void ResolveContent(std::unique_ptr<SearchParameters> params);

}  // namespace valkey_search::query

#endif  // VALKEY_SEARCH_SRC_QUERY_CONTENT_RESOLUTION_H_
