/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_DEFRAG_H_
#define VALKEYSEARCH_SRC_DEFRAG_H_

#include <cstdint>

#include "vmsdk/src/valkey_module_api/valkey_module.h"

// Module-global active defragmentation.
//
// Valkey's active defrag relocates live allocations out of sparsely-used
// jemalloc slabs so the pages underneath can be released. It handles its own
// data and module *keys* on its own, but anything a module allocates outside
// the keyspace is invisible to it. For valkey-search that is most of what
// fragments over time: HNSW graphs, tag and text indexes, the interned-string
// pool. Core exposes one hook for that memory, a single global callback
// registered with ValkeyModule_RegisterDefragFunc.
//
// Contract of that callback (see valkey/src/module.c moduleDefragGlobals):
//
//   * It runs on the main thread, inside the core defrag cycle, and is given a
//     deadline. ValkeyModule_DefragShouldStop() reports when that deadline has
//     passed; the callback is expected to poll it and return promptly.
//   * It is given a cursor that persists across invocations within a cycle
//     (ValkeyModule_DefragCursorGet / ...Set), so work can be split over many
//     calls instead of blocking the main thread once. Core discards the cursor
//     when a cycle ends or is aborted, so the next cycle starts again from 0.
//   * The cursor doubles as the "am I done" signal, the same convention every
//     other scanner in defrag.c uses:
//         cursor != 0  -> more work remains, call me again
//         cursor == 0  -> done for this cycle
//     A callback that never stores 0 will be invoked forever, so resetting the
//     cursor on completion is mandatory, not optional.
//
// This file implements the protocol end of that contract. It deliberately does
// no index work: the sampling and rebuild path (reingestion) and the jemalloc
// tcache interaction are handled separately. What lives here is the part core
// talks to, kept small so it is easy to reason about and to test.
namespace valkey_search::defrag {

// Counters describing what the global defrag callback did. Every field is
// observable from tests and from FT._DEBUG DEFRAG_STATS, so each element of the
// core contract above can be asserted independently rather than inferred.
struct Stats {
  // Number of times core invoked our callback. Non-zero proves the core-side
  // registration and defrag cycle actually reach this module.
  uint64_t callback_invocations = 0;
  // Number of times we successfully read the persistent cursor. Non-zero proves
  // core handed us a real cursor (before the core change this returned an
  // error, because global callbacks were invoked with cursor == NULL).
  uint64_t cursor_reads = 0;
  // Number of times we polled the deadline via DefragShouldStop.
  uint64_t deadline_checks = 0;
  // Number of times DefragShouldStop reported the deadline had passed, i.e. we
  // were asked to yield the main thread mid-pass.
  uint64_t deadline_stops = 0;
  // Number of times we finished a pass and reset the cursor to 0 ("done").
  uint64_t completed_passes = 0;
};

// Snapshot of the counters. Safe to call from any thread.
Stats GetStats();

// Zero the counters. Used by tests and by FT._DEBUG to get a clean baseline.
void ResetStats();

// The global defrag callback core invokes. Exposed (rather than kept static) so
// it can be unit tested directly without standing up a full module load.
//
// The "more work" signal travels through the cursor, not a return value: core
// types this callback as returning void (see the contract above).
void OnGlobalDefragCallback(ValkeyModuleDefragCtx *ctx);

// Register OnGlobalDefragCallback with core.
//
// The registration API is only present on cores that support module global
// defrag, so the pointer is checked first: on an older core we simply do not
// participate in defrag and the module loads and runs normally. Returns true if
// the callback was registered.
bool RegisterGlobalDefragCallback(ValkeyModuleCtx *ctx);

}  // namespace valkey_search::defrag

#endif  // VALKEYSEARCH_SRC_DEFRAG_H_
