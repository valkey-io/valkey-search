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
#include "vmsdk/src/log.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query {

// Base class for in-flight retry contexts using event-driven notification.
// Uses shared_ptr and enable_shared_from_this for safe async callbacks.
//
// Usage pattern:
// 1. Create derived context via make_shared
// 2. Call ScheduleOnMainThread() to start processing
// 3. If conflicts exist, registers with mutation entry for callback
// 4. When mutation completes, OnMutationComplete() triggers retry
// 5. On no conflicts -> OnComplete()
class InFlightRetryContextBase
    : public std::enable_shared_from_this<InFlightRetryContextBase> {
 public:
  explicit InFlightRetryContextBase(std::vector<InternedStringPtr>&& keys)
      : neighbor_keys_(std::move(keys)) {}

  virtual ~InFlightRetryContextBase() = default;
  virtual void OnComplete() = 0;
  virtual void OnCancelled() = 0;
  virtual bool IsCancelled() const = 0;
  virtual const std::shared_ptr<IndexSchema>& GetIndexSchema() const = 0;
  virtual const char* GetDesc() const = 0;

  // Process a retry attempt on main thread
  void ProcessRetry();

  // Schedule this context to be processed on the main thread
  void ScheduleOnMainThread();

  // Called by IndexSchema when a conflicting mutation completes
  void OnMutationComplete();

  const std::vector<InternedStringPtr>& GetNeighborKeys() const {
    return neighbor_keys_;
  }

 private:
  std::vector<InternedStringPtr> neighbor_keys_;
  bool blocked_{false};
};

}  // namespace valkey_search::query

#endif  // VALKEY_SEARCH_SRC_QUERY_INFLIGHT_RETRY_H_
