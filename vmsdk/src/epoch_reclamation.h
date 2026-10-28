/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_EPOCH_RECLAMATION_H_
#define VMSDK_SRC_EPOCH_RECLAMATION_H_

#include <atomic>
#include <cstdint>

// Epoch-based reclamation (EBR).
//
// Lets threads read shared records without atomics or reference counting,
// while other threads replace those records concurrently. A replaced record is
// not released immediately; it is retired and released once every thread that
// could still be reading it has finished.
//
// Usage:
//  - Readers wrap each operation that reads shared records in an EpochGuard.
//  - A writer swaps a record out of shared storage, then calls RetireEpoch()
//    and keeps the old record together with the returned tag.
//  - The old record may be released once MinActiveEpoch() > tag. Call
//    MinActiveEpoch() only after the retire is visible to the calling thread
//    (e.g. under the mutex protecting the retire list).
//
// Correctness (store-buffering with seq_cst fences on both sides):
//  - Reader: announce epoch; fence; read records.
//  - Retirer: swap the record; fence; tag = epoch++.
//  - Reclaimer (ordered after the retire): fence; scan the announcements.
// A reader that read the old record announced before the retirer's fence, so
// the reclaimer's scan observes its epoch, which is <= tag. A reader that
// observed the incremented epoch synchronizes with the retirer's fence and can
// only read the new record.
//
// Cost: entering and leaving the outermost guard is a relaxed load, two stores
// and one seq_cst fence, with no shared cache line written. Nested guards are
// free. RetireEpoch() is one fence and one atomic increment. MinActiveEpoch()
// scans one announcement per thread that has ever used a guard and is alive.

namespace vmsdk::epoch {

namespace internal {

// Starts at 1: an announced epoch of 0 means "not in a guard".
extern std::atomic<uint64_t> global_epoch;

// Per-thread announcement. It lives in TLS, so announcements of different
// threads never share a cache line.
struct ThreadState {
  std::atomic<uint64_t> epoch{0};
  uint32_t depth{0};
  bool registered{false};

  ~ThreadState();
};

inline ThreadState &LocalState() {
  thread_local ThreadState state;
  return state;
}

// Makes `state` visible to MinActiveEpoch(). Called once per thread.
void Register(ThreadState *state);

}  // namespace internal

// Announces the current thread as reading for the guard's lifetime. Nested
// guards are free; only the outermost one announces.
class EpochGuard {
 public:
  EpochGuard() {
    internal::ThreadState &state = internal::LocalState();
    if (state.depth++ != 0) {
      return;
    }
    if (!state.registered) {
      internal::Register(&state);
    }
    state.epoch.store(internal::global_epoch.load(std::memory_order_acquire),
                      std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_seq_cst);
  }

  ~EpochGuard() {
    internal::ThreadState &state = internal::LocalState();
    if (--state.depth == 0) {
      state.epoch.store(0, std::memory_order_release);
    }
  }

  EpochGuard(const EpochGuard &) = delete;
  EpochGuard &operator=(const EpochGuard &) = delete;
};

// True if the current thread is inside an EpochGuard.
inline bool InGuard() { return internal::LocalState().depth != 0; }

// Call after swapping a record out of shared storage. Returns the tag to
// retire the old record with.
inline uint64_t RetireEpoch() {
  std::atomic_thread_fence(std::memory_order_seq_cst);
  return internal::global_epoch.fetch_add(1, std::memory_order_relaxed);
}

// Smallest epoch announced by a thread inside a guard, or UINT64_MAX if none.
// A record retired with tag t may be released once MinActiveEpoch() > t.
uint64_t MinActiveEpoch();

}  // namespace vmsdk::epoch

#endif  // VMSDK_SRC_EPOCH_RECLAMATION_H_
