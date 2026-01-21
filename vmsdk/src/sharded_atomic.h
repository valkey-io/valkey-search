/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_SHARED_ATOMIC_H_
#define VMSDK_SRC_SHARED_ATOMIC_H_

#include <algorithm>
#include <atomic>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "vmsdk/src/memory_allocation_overrides.h"

namespace vmsdk {

// A high-performance sharded atomic counter that supports template types.
// It uses Thread-Local Storage (TLS) to allow zero-contention writes.
// Note: This implements a Global Counter behavior per type T.
// All instances of ShardedAtomic<T> will share the same underlying sum.
template <typename T>
class ShardedAtomic {
 public:
  // ------------------------------------------------------------------------
  // Public API
  // ------------------------------------------------------------------------

  // THE HOT PATH (Write)
  // Cost: ~1-2 CPU Cycles (MOV + ADD). No LOCK prefix.
  inline void Add(T n) {
    ThreadLocalNode& node = GetLocalNode();

    // Optimistic Load -> Add -> Store
    // Since this thread is the exclusive writer to 'node', we don't need atomic
    // RMW. We use relaxed ordering to avoid memory fences, relying on the
    // single-writer invariant.
    T current = node.value.load(std::memory_order_relaxed);
    node.value.store(current + n, std::memory_order_relaxed);
  }

  inline void Subtract(T n) {
    ThreadLocalNode& node = GetLocalNode();

    T current = node.value.load(std::memory_order_relaxed);
    node.value.store(current - n, std::memory_order_relaxed);
  }

  // THE COLD PATH (Read)
  // Cost: Mutex Lock + Vector Iteration (O(N) active threads)
  // Default allows fuzzy reads (relaxed) for maximum performance.
  T GetTotal(std::memory_order order = std::memory_order_relaxed) const {
    return CounterRegistry::Instance().GetTotal(order);
  }

  void Reset() const { return CounterRegistry::Instance().Reset(); }

 private:
  // ------------------------------------------------------------------------
  // Private Implementation Details
  // ------------------------------------------------------------------------

  // Forward declaration
  struct ThreadLocalNode;

  // Private Registry: Manages active nodes for this specific type T
  class CounterRegistry {
   public:
    static CounterRegistry& Instance() {
      static CounterRegistry instance;
      return instance;
    }

    void Register(ThreadLocalNode* node) {
      absl::MutexLock lock(&mutex_);
      nodes_.push_back(node);
    }

    void Unregister(ThreadLocalNode* node) {
      absl::MutexLock lock(&mutex_);
      auto it = std::find(nodes_.begin(), nodes_.end(), node);
      if (it != nodes_.end()) {
        *it = nodes_.back();
        nodes_.pop_back();
      }
    }

    T GetTotal(std::memory_order order) const {
      T total = 0;
      // Block if a thread is currently registering/unregistering.
      absl::ReaderMutexLock lock(&mutex_);

      for (const auto* node : nodes_) {
        total += node->value.load(order);
      }
      return total;
    }

    void Reset() {
      // We lock purely to ensure the vector doesn't change size (iterators
      // validity)
      absl::ReaderMutexLock lock(&mutex_);
      for (auto* node : nodes_) {
        // Forcibly set value to 0.
        // We use relaxed because exact ordering across threads usually
        // doesn't matter for a "hard reset" in tests.
        node->value.store(0, std::memory_order_relaxed);
      }
    }

   private:
    mutable absl::Mutex mutex_;
    std::vector<ThreadLocalNode*,
                RawSystemAllocator<ThreadLocalNode*,
                                   DisableRawSystemAllocatorReporting>>
        nodes_ ABSL_GUARDED_BY(mutex_);
  };

  // Private Node: The Thread-Local storage container
  struct alignas(64) ThreadLocalNode {
    std::atomic<T> value{0};

    ThreadLocalNode() { CounterRegistry::Instance().Register(this); }

    ~ThreadLocalNode() { CounterRegistry::Instance().Unregister(this); }
  };

  // Helper to access the unique TLS node for the current thread
  static ThreadLocalNode& GetLocalNode() {
    // C++ guarantees this is initialized once per thread (lazily)
    // and destroyed when the thread exits.
    static thread_local ThreadLocalNode node;
    return node;
  }
};

}  // namespace vmsdk

#endif  // VMSDK_SRC_SHARED_ATOMIC_H_