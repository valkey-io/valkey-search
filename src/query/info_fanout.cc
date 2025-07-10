/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/info_fanout.h"

#include "src/query/fanout.h"
#include "absl/status/status.h"

namespace valkey_search::query::info_fanout {

absl::Status PerformInfoFanoutAsync(
    ValkeyModuleCtx* ctx, 
    std::vector<fanout::FanoutSearchTarget>& info_targets,
    coordinator::ClientPool* coordinator_client_pool,
    std::unique_ptr<InfoParameters> parameters,
    vmsdk::ThreadPool* thread_pool, 
    InfoResponseCallback callback) {
  // TODO: Implement fanout logic
  return absl::UnimplementedError("Info fanout not yet implemented");
}

std::vector<fanout::FanoutSearchTarget> GetInfoTargetsForFanout(ValkeyModuleCtx* ctx) {
  // TODO: Implement target discovery
  // For now, reuse existing search fanout logic
  return fanout::GetSearchTargetsForFanout(ctx);
}

}  // namespace valkey_search::query::info_fanout