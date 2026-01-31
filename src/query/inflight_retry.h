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

#include "src/indexes/vector_base.h"
#include "src/metrics.h"
#include "vmsdk/src/utils.h"

namespace valkey_search {
class IndexSchema;
}

namespace valkey_search::query {

struct SearchParameters;

// Context for in-flight retry using event-driven notification.
// Uses shared_ptr and enable_shared_from_this for safe async callbacks.
//
// The completion behavior is determined by the SearchParameters subclass:
// - InitiatorSearch (initiator): unblocks client
// - LocalResponderSearch (local fanout): adds results to tracker
// - RemoteResponderSearch (remote responder): sends gRPC response
//
// Usage pattern:
// 1. Create context via make_shared with parameters (which owns neighbors)
// 2. Call ScheduleOnMainThread() to start processing
// 3. If conflicts exist, registers with mutation entry for callback
// 4. When mutation completes, OnMutationComplete() triggers retry
// 5. On no conflicts -> parameters->OnComplete()
class InFlightRetryContext
    : public std::enable_shared_from_this<InFlightRetryContext> {
 public:
  explicit InFlightRetryContext(std::unique_ptr<SearchParameters> params);

  ~InFlightRetryContext() = default;

  // Process a retry attempt on main thread
  void ProcessRetry();

  // Schedule this context to be processed on the main thread
  void ScheduleOnMainThread();

  // Called by IndexSchema when a conflicting mutation completes
  void OnMutationComplete();

  const std::vector<indexes::Neighbor>& GetNeighbors() const;

 private:
  std::unique_ptr<SearchParameters> parameters_;
  bool blocked_{false};
};

}  // namespace valkey_search::query

#endif  // VALKEY_SEARCH_SRC_QUERY_INFLIGHT_RETRY_H_
