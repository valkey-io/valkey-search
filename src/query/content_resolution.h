/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_SRC_QUERY_CONTENT_RESOLUTION_H_
#define VALKEY_SEARCH_SRC_QUERY_CONTENT_RESOLUTION_H_

#include <memory>

namespace valkey_search::query {

struct SearchParameters;
struct PendingValidationContext;

// Entry point — called on main thread after search completes.
// Handles contention checking (registering with mutation queue if needed),
// content fetching via ProcessNeighborsForReply, and final completion.
void ResolveContent(std::unique_ptr<SearchParameters> params);

// Called on the main thread after every conflicting key's mutation has
// been drained and writer-side per-neighbor validation has run. Removes
// neighbors that the writer marked kFail, refreshes sequence numbers on
// kPass neighbors so PerformKeyContentionCheck and VerifyFilter both
// short-circuit them, and re-enters ResolveContent. A re-entry will only
// re-attach if a NEW conflict has arrived since the last attachment.
void DispatchValidatedQuery(std::shared_ptr<PendingValidationContext> ctx);

}  // namespace valkey_search::query

#endif  // VALKEY_SEARCH_SRC_QUERY_CONTENT_RESOLUTION_H_
