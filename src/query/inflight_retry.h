/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_SRC_QUERY_INFLIGHT_RETRY_H_
#define VALKEY_SEARCH_SRC_QUERY_INFLIGHT_RETRY_H_

#include <vector>

#include "src/index_schema.h"
#include "src/metrics.h"
#include "src/query/search.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query {

// Helper to check for in-flight conflicts and schedule retry if needed.
// Returns true if retry was scheduled, false if no conflicts found.
template <typename RetryContext>
bool CheckInFlightAndScheduleRetry(
    ValkeyModuleCtx* ctx, RetryContext* retry_ctx,
    const std::vector<InternedStringPtr>& neighbor_keys,
    const std::shared_ptr<IndexSchema>& index_schema,
    void (*timer_callback)(ValkeyModuleCtx*, void*), const char* log_prefix) {
  if (index_schema->HasAnyConflictingInFlightKeys(neighbor_keys)) {
    ++Metrics::GetStats().fulltext_query_retry_cnt;
    VMSDK_LOG(DEBUG, ctx)
        << log_prefix << " has conflicting in-flight keys, scheduling retry";
    ValkeyModule_CreateTimer(ctx, kInFlightRetryIntervalMs, timer_callback,
                             retry_ctx);
    return true;
  }
  return false;
}

// Helper to schedule initial retry on main thread
template <typename RetryContext>
void ScheduleInFlightRetryOnMain(RetryContext* retry_ctx,
                                 void (*timer_callback)(ValkeyModuleCtx*,
                                                        void*)) {
  vmsdk::RunByMain(
      [retry_ctx, timer_callback]() {
        auto ctx = vmsdk::MakeUniqueValkeyThreadSafeContext(nullptr);
        ValkeyModule_CreateTimer(ctx.get(), kInFlightRetryIntervalMs,
                                 timer_callback, retry_ctx);
      },
      true);
}

}  // namespace valkey_search::query

#endif  // VALKEY_SEARCH_SRC_QUERY_INFLIGHT_RETRY_H_
