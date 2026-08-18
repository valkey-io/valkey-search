/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VMSDK_SRC_LOCK_FREE_QUEUE_H_
#define VMSDK_SRC_LOCK_FREE_QUEUE_H_

#include <atomic>
#include <cstddef>
#include <utility>
#include <vector>

namespace vmsdk {

// High-performance Bounded MPSC / Work-Stealing Lock-Free Queue
template <typename T, size_t Capacity = 65536>
class alignas(64) LockFreeQueue {
 public:
  LockFreeQueue() : head_(0), tail_(0), sequence_(Capacity), buffer_(Capacity) {
    for (size_t i = 0; i < Capacity; ++i) {
      sequence_[i].store(i, std::memory_order_relaxed);
    }
  }

  // Non-copyable, non-movable
  LockFreeQueue(const LockFreeQueue&) = delete;
  LockFreeQueue& operator=(const LockFreeQueue&) = delete;

  // Lock-free Enqueue (Multi-Producer safe)
  bool TryEnqueue(T&& item) {
    size_t pos = tail_.load(std::memory_order_relaxed);
    while (true) {
      size_t index = pos % Capacity;
      size_t seq = sequence_[index].load(std::memory_order_acquire);
      intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
      if (diff == 0) {
        if (tail_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
          buffer_[index] = std::move(item);
          sequence_[index].store(pos + 1, std::memory_order_release);
          return true;
        }
      } else if (diff < 0) {
        return false; // Queue is full
      } else {
        pos = tail_.load(std::memory_order_relaxed);
      }
    }
  }

  // Lock-free Dequeue / Steal (Single-Consumer or Work-Stealing safe)
  bool TryDequeue(T& item) {
    size_t pos = head_.load(std::memory_order_relaxed);
    while (true) {
      size_t index = pos % Capacity;
      size_t seq = sequence_[index].load(std::memory_order_acquire);
      intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);
      if (diff == 0) {
        if (head_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
          item = std::move(buffer_[index]);
          sequence_[index].store(pos + Capacity, std::memory_order_release);
          return true;
        }
      } else if (diff < 0) {
        return false; // Queue is empty
      } else {
        pos = head_.load(std::memory_order_relaxed);
      }
    }
  }

  bool IsEmpty() const {
    size_t head = head_.load(std::memory_order_relaxed);
    size_t tail = tail_.load(std::memory_order_relaxed);
    return head >= tail;
  }

  size_t Size() const {
    size_t head = head_.load(std::memory_order_relaxed);
    size_t tail = tail_.load(std::memory_order_relaxed);
    return tail > head ? (tail - head) : 0;
  }

 private:
  alignas(64) std::atomic<size_t> head_{0};
  alignas(64) std::atomic<size_t> tail_{0};
  std::vector<std::atomic<size_t>> sequence_;
  std::vector<T> buffer_;
};

}  // namespace vmsdk

#endif  // VMSDK_SRC_LOCK_FREE_QUEUE_H_
