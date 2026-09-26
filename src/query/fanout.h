/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_FANOUT_H_
#define VALKEYSEARCH_SRC_QUERY_FANOUT_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "absl/status/status.h"
#include "src/coordinator/client_pool.h"
#include "src/query/multi_search.h"
#include "src/query/search.h"
#include "vmsdk/src/cluster_map.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query::fanout {

absl::Status PerformSearchFanoutAsync(
    ValkeyModuleCtx* ctx,
    std::vector<vmsdk::cluster_map::NodeInfo>& search_targets,
    coordinator::ClientPool* coordinator_client_pool,
    std::unique_ptr<query::SearchParameters> parameters,
    vmsdk::ThreadPool* thread_pool);

// Cluster fanout for FT.HYBRID multi-arm search. Sends ONE
// MultiSearchIndexPartition RPC per shard carrying every arm's
// SearchIndexPartitionRequest, demuxes per-arm responses into per-arm result
// trackers, and triggers the MultiSearchTracker when all arms have completed
// across all shards.
absl::Status PerformMultiSearchFanoutAsync(
    ValkeyModuleCtx* ctx,
    std::vector<vmsdk::cluster_map::NodeInfo>& search_targets,
    coordinator::ClientPool* coordinator_client_pool,
    std::unique_ptr<query::MultiSearchParameters> parameters,
    vmsdk::ThreadPool* thread_pool);

// The k each of `num_shards` shards is asked for under SHARD_K_RATIO:
// max(ceil(k / num_shards), ceil(k * ratio)). The ratio is applied in double
// precision, as the reference engine does, so a product such as 50 * 0.56
// rounds up to 29. `ratio` must be in (0, 1]; k is 32-bit to match the RPC.
uint32_t ShardKForRatio(uint32_t k, size_t num_shards, double ratio);

// Utility function to check if system is under low utilization
bool IsSystemUnderLowUtilization();

}  // namespace valkey_search::query::fanout

#endif  // VALKEYSEARCH_SRC_QUERY_FANOUT_H_
