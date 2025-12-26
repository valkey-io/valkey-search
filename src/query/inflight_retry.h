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

// Check for in-flight conflicts and schedule retry timer if conflicts exist.
// Returns true if retry was scheduled (conflicts found), false otherwise.
// Must be called on main thread.
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

// Schedule the retry context to be processed on the main thread.
// - If has_conflicts is true: schedules with delay (background saw conflicts)
// - If has_conflicts is false: schedules immediately (background saw no
//   conflicts, but main thread must still verify before completing)
//
// The timer_callback will be invoked on the main thread and should call
// CheckInFlightAndScheduleRetry to verify conflicts before completing.
template <typename RetryContext>
void ScheduleOnMainThread(RetryContext* retry_ctx,
                          void (*timer_callback)(ValkeyModuleCtx*, void*),
                          bool has_conflicts) {
  if (has_conflicts) {
    ++Metrics::GetStats().fulltext_query_blocked_cnt;
  }
  vmsdk::RunByMain(
      [retry_ctx, timer_callback, has_conflicts]() {
        auto ctx = vmsdk::MakeUniqueValkeyThreadSafeContext(nullptr);
        if (has_conflicts) {
          // Delay before first check since we know there are conflicts
          ValkeyModule_CreateTimer(ctx.get(), kInFlightRetryIntervalMs,
                                   timer_callback, retry_ctx);
        } else {
          // Execute callback immediately - it will do the main thread check
          timer_callback(ctx.get(), retry_ctx);
        }
      },
      true);
}

}  // namespace valkey_search::query

#endif  // VALKEY_SEARCH_SRC_QUERY_INFLIGHT_RETRY_H_
