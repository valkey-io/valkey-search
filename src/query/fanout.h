/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_FANOUT_H_
#define VALKEYSEARCH_SRC_QUERY_FANOUT_H_

#include <memory>
#include <vector>

#include "absl/status/status.h"
#include "src/coordinator/client_pool.h"
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

// Utility function to check if system is under low utilization
bool IsSystemUnderLowUtilization();

}  // namespace valkey_search::query::fanout

#endif  // VALKEYSEARCH_SRC_QUERY_FANOUT_H_
