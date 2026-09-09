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

#include "absl/base/thread_annotations.h"
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
    ThreadLocalNode &node = GetLocalNode();
    if (ABSL_PREDICT_FALSE(index_ >= node.capacity)) {
      node.EnsureCapacity(index_ + 1);
    }
    T current = node.values[index_].load(std::memory_order_relaxed);
    node.values[index_].store(current + n, std::memory_order_relaxed);
  }

  inline void Subtract(T n) {
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
    static CounterRegistry &Instance() {
      static CounterRegistry instance;
      return instance;
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
    mutable absl::Mutex mutex_;
    std::vector<ThreadLocalNode *,
                RawSystemAllocator<ThreadLocalNode *,
                                   DisableRawSystemAllocatorReporting>>
        nodes_ ABSL_GUARDED_BY(mutex_);

    std::vector<T, RawSystemAllocator<T, DisableRawSystemAllocatorReporting>>
        retired_totals_ ABSL_GUARDED_BY(mutex_);

    std::vector<size_t,
                RawSystemAllocator<size_t, DisableRawSystemAllocatorReporting>>
        free_indices_ ABSL_GUARDED_BY(mutex_);

    size_t next_index_ ABSL_GUARDED_BY(mutex_){0};
  };

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
  CounterRegistry::Instance().Unregister(this);
  if (values) {
    RawSystemAllocator<std::atomic<T>, DisableRawSystemAllocatorReporting>
        alloc;
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

  RawSystemAllocator<std::atomic<T>, DisableRawSystemAllocatorReporting> alloc;
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
