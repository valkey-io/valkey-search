/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/inflight_retry.h"

namespace valkey_search::query {

void InFlightRetryCallback(ValkeyModuleCtx* ctx, void* data) {
  static_cast<InFlightRetryContextBase*>(data)->ProcessRetry(ctx);
}

void InFlightRetryContextBase::ProcessRetry(ValkeyModuleCtx* ctx) {
  if (IsCancelled()) {
    OnCancelled();
    delete this;
    return;
  }

  if (GetIndexSchema()->HasAnyConflictingInFlightKeys(GetNeighborKeys())) {
    ++Metrics::GetStats().fulltext_query_retry_cnt;
    VMSDK_LOG(DEBUG, ctx)
        << GetDesc() << " has conflicting in-flight keys, scheduling retry";
    ValkeyModule_CreateTimer(ctx,
                             options::GetInFlightRetryIntervalMs().GetValue(),
                             InFlightRetryCallback, this);
    return;
  }

  OnComplete();
  delete this;
}

void InFlightRetryContextBase::ScheduleOnMainThread(bool has_conflicts) {
  if (has_conflicts) {
    ++Metrics::GetStats().fulltext_query_blocked_cnt;
  }
  auto* self = this;
  vmsdk::RunByMain(
      [self, has_conflicts]() {
        auto ctx = vmsdk::MakeUniqueValkeyThreadSafeContext(nullptr);
        if (has_conflicts) {
          ValkeyModule_CreateTimer(
              ctx.get(), options::GetInFlightRetryIntervalMs().GetValue(),
              InFlightRetryCallback, self);
        } else {
          self->ProcessRetry(ctx.get());
        }
      },
      true);
}

}  // namespace valkey_search::query
