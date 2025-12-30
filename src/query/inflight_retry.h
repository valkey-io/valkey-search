/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_SRC_QUERY_INFLIGHT_RETRY_H_
#define VALKEY_SEARCH_SRC_QUERY_INFLIGHT_RETRY_H_

#include <memory>
#include <vector>

#include "src/index_schema.h"
#include "src/metrics.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query {

// Base class for in-flight retry contexts. Provides common interface for
// checking conflicts and scheduling retries.
//
// Usage pattern:
// 1. Create derived context with case-specific data
// 2. Call ScheduleOnMainThread() to start the retry loop
// 3. The timer callback calls ProcessRetry() which handles:
//    - Cancellation check -> `OnCancelled()`
//    - Conflict check -> schedule retry timer
//    - No conflicts -> `OnComplete()`
class InFlightRetryContextBase {
 public:
  explicit InFlightRetryContextBase(std::vector<InternedStringPtr> keys)
      : neighbor_keys_(std::move(keys)) {}

  virtual ~InFlightRetryContextBase() = default;
  virtual void OnComplete() = 0;
  virtual void OnCancelled() = 0;
  virtual bool IsCancelled() const = 0;
  virtual const std::shared_ptr<IndexSchema>& GetIndexSchema() const = 0;
  virtual const char* GetDesc() const = 0;
  const std::vector<InternedStringPtr>& GetNeighborKeys() const {
    return neighbor_keys_;
  }

 private:
  std::vector<InternedStringPtr> neighbor_keys_;
};

// Timer callback for retry scheduling
void InFlightRetryCallback(ValkeyModuleCtx* ctx, void* data);

// Process a retry attempt. Handles cancellation, conflict checking, and
// completion. This is the main entry point called by timer callbacks.
void ProcessRetry(ValkeyModuleCtx* ctx, InFlightRetryContextBase* retry_ctx);

// Schedule the retry context to be processed on the main thread.
// - If `has_conflicts` is true: schedules with delay (background saw conflicts)
// - If `has_conflicts` is false: schedules immediately (background saw no
//   conflicts, but main thread must still verify before completing)
void ScheduleOnMainThread(InFlightRetryContextBase* retry_ctx,
                          bool has_conflicts);

}  // namespace valkey_search::query

#endif  // VALKEY_SEARCH_SRC_QUERY_INFLIGHT_RETRY_H_
