/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/epoch_reclamation.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <limits>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"

namespace vmsdk::epoch {
namespace internal {

// Constant-initialized: no load-time static initializer.
constinit std::atomic<uint64_t> global_epoch{1};

namespace {

// Announcements of all live threads that have used a guard.
class Registry {
 public:
  static Registry &Get() {
    static absl::NoDestructor<Registry> registry;
    return *registry;
  }

  void Add(ThreadState *state) ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    threads_.push_back(state);
  }

  void Remove(ThreadState *state) ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    threads_.erase(std::remove(threads_.begin(), threads_.end(), state),
                   threads_.end());
  }

  uint64_t MinActiveEpoch() const ABSL_LOCKS_EXCLUDED(mutex_) {
    uint64_t min_epoch = std::numeric_limits<uint64_t>::max();
    absl::ReaderMutexLock lock(&mutex_);
    for (const ThreadState *state : threads_) {
      const uint64_t e = state->epoch.load(std::memory_order_relaxed);
      if (e != 0) {
        min_epoch = std::min(min_epoch, e);
      }
    }
    return min_epoch;
  }

 private:
  mutable absl::Mutex mutex_;
  std::vector<ThreadState *> threads_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace

ThreadState::~ThreadState() {
  if (registered) {
    Registry::Get().Remove(this);
  }
}

void Register(ThreadState *state) {
  Registry::Get().Add(state);
  state->registered = true;
}

}  // namespace internal

uint64_t MinActiveEpoch() {
  std::atomic_thread_fence(std::memory_order_seq_cst);
  return internal::Registry::Get().MinActiveEpoch();
}

}  // namespace vmsdk::epoch
