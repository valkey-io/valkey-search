/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/defrag.h"

#include <atomic>
#include <cstdint>

#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::defrag {

namespace {

// Counters backing Stats. Atomic because the callback runs on the main thread
// while FT._DEBUG DEFRAG_STATS reads them from a command thread.
std::atomic<uint64_t> callback_invocations{0};
std::atomic<uint64_t> cursor_reads{0};
std::atomic<uint64_t> deadline_checks{0};
std::atomic<uint64_t> deadline_stops{0};
std::atomic<uint64_t> completed_passes{0};

}  // namespace

Stats GetStats() {
  Stats s;
  s.callback_invocations = callback_invocations.load(std::memory_order_relaxed);
  s.cursor_reads = cursor_reads.load(std::memory_order_relaxed);
  s.deadline_checks = deadline_checks.load(std::memory_order_relaxed);
  s.deadline_stops = deadline_stops.load(std::memory_order_relaxed);
  s.completed_passes = completed_passes.load(std::memory_order_relaxed);
  return s;
}

void ResetStats() {
  callback_invocations.store(0, std::memory_order_relaxed);
  cursor_reads.store(0, std::memory_order_relaxed);
  deadline_checks.store(0, std::memory_order_relaxed);
  deadline_stops.store(0, std::memory_order_relaxed);
  completed_passes.store(0, std::memory_order_relaxed);
}

int OnGlobalDefragCallback(ValkeyModuleDefragCtx *ctx) {
  callback_invocations.fetch_add(1, std::memory_order_relaxed);

  // Step 1: recover where the previous invocation stopped. On the first call of
  // a pass the stored cursor is 0, meaning "start from the beginning". A
  // non-zero value is whatever we saved last time. DefragCursorGet fails only
  // if core gave us no cursor at all, which is the case this module cannot
  // resume in; treat it as "start over" and keep going.
  unsigned long resume_from = 0;  // NOLINT(runtime/int) - core API type
  if (ValkeyModule_DefragCursorGet(ctx, &resume_from) == VALKEYMODULE_OK) {
    cursor_reads.fetch_add(1, std::memory_order_relaxed);
  } else {
    resume_from = 0;
  }

  // Step 2: poll the deadline. A real implementation does a bounded chunk of
  // work per iteration and re-checks this between chunks, so it can hand the
  // main thread back on time. There is no index work here yet, so we check once
  // and record the answer.
  deadline_checks.fetch_add(1, std::memory_order_relaxed);
  const bool out_of_time = ValkeyModule_DefragShouldStop(ctx) != 0;
  if (out_of_time) {
    deadline_stops.fetch_add(1, std::memory_order_relaxed);
    // Out of time with work still outstanding: save the position and leave the
    // cursor non-zero so core calls us again. resume_from + 1 keeps it non-zero
    // even at position 0, which matters because 0 means "done".
    ValkeyModule_DefragCursorSet(ctx, resume_from + 1);
    return 0;
  }

  // Step 3: the pass is complete. Resetting the cursor to 0 is what tells core
  // this module is finished for this cycle. Skipping this would leave core
  // rescheduling the global defrag stage forever.
  ValkeyModule_DefragCursorSet(ctx, 0);
  completed_passes.fetch_add(1, std::memory_order_relaxed);
  return 0;
}

bool RegisterGlobalDefragCallback(ValkeyModuleCtx *ctx) {
  // Module API pointers are bound from the running server at load time. A core
  // without module global defrag leaves this null, in which case we simply do
  // not register and the module behaves exactly as before.
  if (ValkeyModule_RegisterDefragFunc == nullptr) {
    return false;
  }
  return ValkeyModule_RegisterDefragFunc(ctx, OnGlobalDefragCallback) ==
         VALKEYMODULE_OK;
}

}  // namespace valkey_search::defrag
