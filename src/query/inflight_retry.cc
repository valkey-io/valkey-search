/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/inflight_retry.h"

namespace valkey_search::query {

void InFlightRetryContextBase::ProcessRetry() {
  if (IsCancelled()) {
    OnCancelled();
    return;
  }

  // Try to register with a conflicting mutation entry
  if (GetIndexSchema()->RegisterWaitingQuery(GetNeighborKeys(),
                                             shared_from_this())) {
    if (!blocked_) {
      blocked_ = true;
      ++Metrics::GetStats().fulltext_query_blocked_cnt;
    }
    ++Metrics::GetStats().fulltext_query_retry_cnt;
    return;  // Will be called back via OnMutationComplete()
  }

  // No conflicts - complete the query
  OnComplete();
}

void InFlightRetryContextBase::OnMutationComplete() {
  ScheduleOnMainThread();
}

void InFlightRetryContextBase::ScheduleOnMainThread() {
  auto self = shared_from_this();
  vmsdk::RunByMain([self]() { self->ProcessRetry(); }, true);
}

}  // namespace valkey_search::query
