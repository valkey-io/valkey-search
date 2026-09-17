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
#include <memory>
#include <new>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/inlined_vector.h"
#include "absl/synchronization/mutex.h"
#include "vmsdk/src/memory_allocation_overrides.h"

namespace vmsdk {

// A high-performance sharded atomic counter that supports template types.
// It uses Thread-Local Storage (TLS) to allow zero-contention writes.
// Each instance of ShardedAtomic receives a unique dynamic index, allowing
// independent counters without needing template tags.
template <typename T>
class ShardedAtomic {
 public:
  // ------------------------------------------------------------------------
  // Public API
  // ------------------------------------------------------------------------

  ShardedAtomic() : index_(CounterRegistry::Instance().AllocateIndex()) {
    ref_keeper_ = std::shared_ptr<size_t>(new size_t(index_), [](size_t *p) {
      CounterRegistry::Instance().FreeIndex(*p);
      delete p;
    });
  }

  // THE HOT PATH (Write)
  inline void Add(T n) {
    if (ABSL_PREDICT_FALSE(node_destroyed_)) {
      return;
    }
    ThreadLocalNode &node = GetLocalNode();
    if (ABSL_PREDICT_FALSE(index_ >= node.capacity)) {
      node.EnsureCapacity(index_ + 1);
    }
    T current = node.values[index_].load(std::memory_order_relaxed);
    node.values[index_].store(current + n, std::memory_order_relaxed);
  }

  inline void Subtract(T n) {
    if (ABSL_PREDICT_FALSE(node_destroyed_)) {
      return;
    }
    ThreadLocalNode &node = GetLocalNode();
    if (ABSL_PREDICT_FALSE(index_ >= node.capacity)) {
      node.EnsureCapacity(index_ + 1);
    }
    T current = node.values[index_].load(std::memory_order_relaxed);
    node.values[index_].store(current - n, std::memory_order_relaxed);
  }

  // Prefix increment
  inline ShardedAtomic &operator++() {
    Add(1);
    return *this;
  }

  // Postfix increment
  inline void operator++(int) { Add(1); }

  // Prefix decrement
  inline ShardedAtomic &operator--() {
    Subtract(1);
    return *this;
  }

  // Postfix decrement
  inline void operator--(int) { Subtract(1); }

  // THE COLD PATH (Read)
  T GetTotal(std::memory_order order = std::memory_order_relaxed) const {
    return CounterRegistry::Instance().GetTotal(index_, order);
  }

  T GetNonNegativeTotal(
      std::memory_order order = std::memory_order_relaxed) const {
    T total = GetTotal(order);
    return total > 0 ? total : 0;
  }

  void Reset() const { CounterRegistry::Instance().Reset(index_); }

 private:
  // ThreadLocalNode is the TLS container for unbounded dynamic counters
  struct alignas(64) ThreadLocalNode {
    std::atomic<T> *values{nullptr};
    size_t capacity{0};
    mutable absl::Mutex resize_mutex;

    ThreadLocalNode();
    ~ThreadLocalNode();
    void EnsureCapacity(size_t min_capacity);
  };

  // Private Registry: Manages active nodes and dynamic indices for this
  // specific type T
  class CounterRegistry {
   public:
    // Never destroyed, deliberately. A thread's ThreadLocalNode unregisters
    // itself here from its destructor, so the registry has to outlive every
    // node. glibc guarantees that on its own -- it runs thread_local
    // destructors before static ones -- but musl has no
    // __cxa_thread_atexit_impl, so libstdc++'s fallback puts both in one LIFO
    // list. There the order depends on which thread first reached Instance():
    // if that was a worker thread, this registry is destroyed before the main
    // thread's node, whose destructor then walks a freed vector. It shows up
    // as a SIGSEGV in exit() after every test has passed.
    //
    // NoDestructor also keeps the initialization off the heap: a new here
    // would allocate through the module allocator, which reports the
    // allocation, which reaches this function again.
    static CounterRegistry &Instance() {
      static absl::NoDestructor<CounterRegistry> instance;
      return *instance;
    }

    size_t AllocateIndex() {
      absl::MutexLock lock(&mutex_);
      size_t index;
      if (!free_indices_.empty()) {
        index = free_indices_.back();
        free_indices_.pop_back();
      } else {
        index = next_index_++;
        retired_totals_.push_back(0);
      }
      return index;
    }

    void FreeIndex(size_t index) {
      absl::MutexLock lock(&mutex_);
      if (index < retired_totals_.size()) {
        retired_totals_[index] = 0;
        for (auto *node : nodes_) {
          absl::MutexLock node_lock(&node->resize_mutex);
          if (index < node->capacity) {
            node->values[index].store(0, std::memory_order_relaxed);
          }
        }
        free_indices_.push_back(index);
      }
    }

    void Register(ThreadLocalNode *node) {
      absl::MutexLock lock(&mutex_);
      nodes_.push_back(node);
    }

    void Unregister(ThreadLocalNode *node) {
      absl::MutexLock lock(&mutex_);
      absl::MutexLock node_lock(&node->resize_mutex);
      for (size_t i = 0; i < node->capacity; ++i) {
        if (i < retired_totals_.size()) {
          retired_totals_[i] += node->values[i].load(std::memory_order_relaxed);
        }
      }
      auto it = std::find(nodes_.begin(), nodes_.end(), node);
      if (it != nodes_.end()) {
        *it = nodes_.back();
        nodes_.pop_back();
      }
    }

    T GetTotal(size_t index, std::memory_order order) const {
      absl::ReaderMutexLock lock(&mutex_);
      T total = 0;
      if (index < retired_totals_.size()) {
        total = retired_totals_[index];
      }
      for (const auto *node : nodes_) {
        absl::MutexLock node_lock(&node->resize_mutex);
        if (index < node->capacity) {
          total += node->values[index].load(order);
        }
      }
      return total;
    }

    void Reset(size_t index) {
      absl::ReaderMutexLock lock(&mutex_);
      if (index < retired_totals_.size()) {
        retired_totals_[index] = 0;
      }
      for (auto *node : nodes_) {
        absl::MutexLock node_lock(&node->resize_mutex);
        if (index < node->capacity) {
          node->values[index].store(0, std::memory_order_seq_cst);
        }
      }
    }

   private:
    // Inline capacity, so that constructing a ShardedAtomic allocates nothing.
    // Every instance's constructor calls AllocateIndex, which appends to
    // retired_totals_, and the instances that matter are globals -- so without
    // this, the first allocation of the process happens during static
    // initialization, before anything has established an allocator. Eight
    // covers the three counters the module defines today with room to spare;
    // beyond that these grow on the heap as before, by which time the
    // allocator is in place.
    static constexpr size_t kInlineCapacity = 8;

    mutable absl::Mutex mutex_;
    absl::InlinedVector<ThreadLocalNode *, kInlineCapacity,
                        RawSystemAllocator<ThreadLocalNode *>>
        nodes_ ABSL_GUARDED_BY(mutex_);

    absl::InlinedVector<T, kInlineCapacity, RawSystemAllocator<T>>
        retired_totals_ ABSL_GUARDED_BY(mutex_);

    absl::InlinedVector<size_t, kInlineCapacity, RawSystemAllocator<size_t>>
        free_indices_ ABSL_GUARDED_BY(mutex_);

    size_t next_index_ ABSL_GUARDED_BY(mutex_){0};
  };

  // Set once this thread's node has been destroyed, after which Add and
  // Subtract must not touch it.
  //
  // A thread_local is destroyed in reverse order of construction, and this
  // node is constructed on the thread's first accounted allocation -- so any
  // thread_local built before that one is destroyed after it. If such an
  // object frees memory from its destructor, free() reports the size here and
  // GetLocalNode() hands back the destroyed node, whose values array has
  // already been returned to the system allocator. glibc leaves that memory
  // mapped and the write silently lands in a freed chunk; musl unmaps it and
  // the process dies, which is how this was found (valkey-server SIGSEGVs in
  // a worker thread during SHUTDOWN on Alpine).
  //
  // The flag is a separate object rather than a values=nullptr store in
  // ~ThreadLocalNode because a store to the object being destroyed is a dead
  // store the compiler may drop (gcc -flifetime-dse); verified that clearing
  // values there does not stop the crash. It is trivially destructible, so it
  // stays readable for the lifetime of the thread and needs no ordering of
  // its own.
  //
  // Counts already accumulated survive: ~ThreadLocalNode folds them into the
  // registry's retired totals. What is dropped is allocation activity after
  // this thread's node is gone, which is thread-exit teardown only.
  static inline thread_local bool node_destroyed_{false};

  static ThreadLocalNode &GetLocalNode() {
    static thread_local ThreadLocalNode node;
    return node;
  }

 private:
  size_t index_;
  std::shared_ptr<size_t> ref_keeper_;
};

// ------------------------------------------------------------------------
// Out-of-line method definitions
// ------------------------------------------------------------------------

template <typename T>
ShardedAtomic<T>::ThreadLocalNode::ThreadLocalNode() {
  CounterRegistry::Instance().Register(this);
}

template <typename T>
ShardedAtomic<T>::ThreadLocalNode::~ThreadLocalNode() {
  // Before anything else: Unregister and deallocate below may themselves
  // allocate or free, and must not re-enter this node.
  node_destroyed_ = true;
  CounterRegistry::Instance().Unregister(this);
  if (values) {
    RawSystemAllocator<std::atomic<T>> alloc;
    alloc.deallocate(values, capacity);
  }
}

template <typename T>
void ShardedAtomic<T>::ThreadLocalNode::EnsureCapacity(size_t min_capacity) {
  if (ABSL_PREDICT_TRUE(min_capacity <= capacity)) {
    return;
  }

  absl::MutexLock lock(&resize_mutex);
  if (min_capacity <= capacity) {
    return;
  }

  size_t new_capacity =
      std::max(capacity * 2, std::max(min_capacity, (size_t)64));

  RawSystemAllocator<std::atomic<T>> alloc;
  std::atomic<T> *new_values = alloc.allocate(new_capacity);

  for (size_t i = 0; i < capacity; ++i) {
    new (new_values + i)
        std::atomic<T>(values[i].load(std::memory_order_relaxed));
  }
  for (size_t i = capacity; i < new_capacity; ++i) {
    new (new_values + i) std::atomic<T>(0);
  }

  std::atomic<T> *old_values = values;
  size_t old_capacity = capacity;

  values = new_values;
  capacity = new_capacity;

  if (old_values) {
    alloc.deallocate(old_values, old_capacity);
  }
}

}  // namespace vmsdk

#endif  // VMSDK_SRC_SHARED_ATOMIC_H_
