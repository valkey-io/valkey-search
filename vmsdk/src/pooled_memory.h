/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_POOLED_MEMORY_H_
#define VALKEYSEARCH_SRC_POOLED_MEMORY_H_

#include <memory_resource>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"

namespace vmsdk {

class PooledMemory : public std::pmr::memory_resource {
 public:
  PooledMemory(size_t chunk_size);
  ~PooledMemory() override;
  size_t GetAllocated() const { return allocated_; }
  size_t GetInUse() const { return inuse_; }
  size_t GetFreed() const { return freed_; }

 private:
  void* do_allocate(size_t bytes,
                    size_t alignment = alignof(std::max_align_t)) override;

  void do_deallocate(void* p, size_t bytes,
                     size_t alignment = alignof(std::max_align_t)) override;
  bool do_is_equal(
      const std::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }

  void NewChunk(size_t min_size);

  size_t chunk_size_;
  size_t inuse_{0};
  size_t allocated_{0};
  size_t freed_{0};
  struct Chunk {
    size_t size_;
    size_t leftoff_;
    char data_[0];
  };
  absl::InlinedVector<Chunk*, 10> chunks_;
};

template <typename T>
using PooledMemoryAllocator = std::pmr::polymorphic_allocator<T>;

//
// Define convenience container types
//
template <typename T>
using PooledVector = std::vector<T, PooledMemoryAllocator<T>>;

template <typename T, size_t N>
using PooledInlinedVector = absl::InlinedVector<T, N, PooledMemoryAllocator<T>>;

using PooledString = std::basic_string<char, std::char_traits<char>,
                                       PooledMemoryAllocator<char>>;

template <typename K, typename V>
using PooledFlatHashMap =
    absl::flat_hash_map<K, V, PooledMemoryAllocator<std::pair<K, V>>>;

}  // namespace vmsdk

#endif