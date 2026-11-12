/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

// Bounded content fan-out: execution (valkey-search#1324).
//
// Round 1: every shard returns its K candidates, with content for only its
// first E_i (fanout_content_limit.h). The coordinator ranks all of them, as
// the single-round merge does, to find the winners the client will be sent.
//
// Fetch rounds, only while a winner has no content: one fetch per shard
// owning such winners, for exactly those keys -- a FetchContent RPC to a
// remote shard; for the coordinator's own shard, the same content resolution
// in this process, as its round-1 search runs. The shard revalidates each key
// against the query and returns content for those that still match. A winner
// that comes back without content no longer matches: it is removed and no
// longer counted, and the next candidate moves into the window, to be fetched
// in turn if it has no content. A shard whose fetch fails (under partial
// results) has its key-only candidates removed. So the reply is the window of
// candidates that still match, as the single-round fan-out returns it. A
// fetched candidate keeps its recomputed score or distance and the ranking is
// redone. Each candidate is fetched at most once, which bounds the rounds.
//
// Shard failures follow the single-round rules.

#ifndef VALKEYSEARCH_SRC_QUERY_CONTENT_BOUNDED_FANOUT_H_
#define VALKEYSEARCH_SRC_QUERY_CONTENT_BOUNDED_FANOUT_H_

#include <memory>
#include <optional>
#include <vector>

#include "absl/status/status.h"
#include "src/coordinator/client_pool.h"
#include "src/query/fanout_content_limit.h"
#include "src/query/search.h"
#include "vmsdk/src/cluster_map.h"
#include "vmsdk/src/thread_pool.h"

namespace valkey_search::query::fanout {

// The plan for fanning `parameters` out to `targets` with bounded content, or
// nullopt to use the single-round fan-out (disabled, or not worth it).
std::optional<BoundedContentPlan> PlanBoundedContentFanout(
    const SearchParameters &parameters,
    const std::vector<vmsdk::cluster_map::NodeInfo> &targets);

// Runs a bounded content fan-out. Takes ownership of `parameters` and
// completes it exactly once, on any outcome.
absl::Status PerformBoundedContentFanoutAsync(
    const std::vector<vmsdk::cluster_map::NodeInfo> &targets,
    coordinator::ClientPool *client_pool,
    std::unique_ptr<SearchParameters> parameters, BoundedContentPlan plan,
    vmsdk::ThreadPool *thread_pool);

}  // namespace valkey_search::query::fanout

#endif  // VALKEYSEARCH_SRC_QUERY_CONTENT_BOUNDED_FANOUT_H_
