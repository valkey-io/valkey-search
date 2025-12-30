/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/inflight_retry.h"

namespace valkey_search::query {

void InFlightRetryCallback(ValkeyModuleCtx* ctx, void* data) {
  ProcessRetry(ctx, static_cast<InFlightRetryContextBase*>(data));
}

void ProcessRetry(ValkeyModuleCtx* ctx, InFlightRetryContextBase* retry_ctx) {
  if (retry_ctx->IsCancelled()) {
    retry_ctx->OnCancelled();
    delete retry_ctx;
    return;
  }

  if (retry_ctx->GetIndexSchema()->HasAnyConflictingInFlightKeys(
          retry_ctx->GetNeighborKeys())) {
    ++Metrics::GetStats().fulltext_query_retry_cnt;
    VMSDK_LOG(DEBUG, ctx)
        << retry_ctx->GetDesc()
        << " has conflicting in-flight keys, scheduling retry";
    ValkeyModule_CreateTimer(ctx,
                             options::GetInFlightRetryIntervalMs().GetValue(),
                             InFlightRetryCallback, retry_ctx);
    return;
  }

  retry_ctx->OnComplete();
  delete retry_ctx;
}

void ScheduleOnMainThread(InFlightRetryContextBase* retry_ctx,
                          bool has_conflicts) {
  if (has_conflicts) {
    ++Metrics::GetStats().fulltext_query_blocked_cnt;
  }
  vmsdk::RunByMain(
      [retry_ctx, has_conflicts]() {
        auto ctx = vmsdk::MakeUniqueValkeyThreadSafeContext(nullptr);
        if (has_conflicts) {
          ValkeyModule_CreateTimer(
              ctx.get(), options::GetInFlightRetryIntervalMs().GetValue(),
              InFlightRetryCallback, retry_ctx);
        } else {
          InFlightRetryCallback(ctx.get(), retry_ctx);
        }
      },
      true);
}

}  // namespace valkey_search::query
