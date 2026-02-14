/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/inflight_retry.h"

#include "src/index_schema.h"
#include "src/query/search.h"
#include "vmsdk/src/log.h"

namespace valkey_search::query {

InFlightRetryContext::InFlightRetryContext(
    std::unique_ptr<SearchParameters> params)
    : parameters_(std::move(params)) {}

void InFlightRetryContext::ProcessRetry() {
  auto &params = parameters_->GetParameters();
  if (params.cancellation_token->IsCancelled()) {
    VMSDK_LOG(DEBUG, nullptr)
        << "In-flight retry cancelled for " << parameters_->GetDesc();
    parameters_->OnCancelled();
    return;
  }

  // Try to register with a conflicting mutation entry
  if (params.index_schema->RegisterWaitingQuery(parameters_->GetNeighbors(),
                                                shared_from_this())) {
    if (!blocked_) {
      blocked_ = true;
      ++Metrics::GetStats().fulltext_query_blocked_cnt;
      VMSDK_LOG(DEBUG, nullptr)
          << "In-flight retry blocked for " << parameters_->GetDesc();
    }
    ++Metrics::GetStats().fulltext_query_retry_cnt;
    return;  // Will be called back via OnMutationComplete()
  }

  // No conflicts - complete the query
  VMSDK_LOG(DEBUG, nullptr)
      << "In-flight retry complete for " << parameters_->GetDesc();
  parameters_->OnComplete();
}

void InFlightRetryContext::OnMutationComplete() { ScheduleOnMainThread(); }

const std::vector<indexes::Neighbor> &InFlightRetryContext::GetNeighbors()
    const {
  return parameters_->GetNeighbors();
}

void InFlightRetryContext::ScheduleOnMainThread() {
  auto self = shared_from_this();
  vmsdk::RunByMain([self]() { self->ProcessRetry(); }, true);
}

}  // namespace valkey_search::query
