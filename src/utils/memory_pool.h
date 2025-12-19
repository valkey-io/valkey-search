/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_UTILS_MEMPOOL_H_
#define VALKEYSEARCH_SRC_UTILS_MEMPOOL_H_

#include <memory_resource>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "vmsdk/src/command_parser.h"

// See vmsdk/src/memory_allocation_overrides.cc
namespace vmsdk {
extern void (*malloc_hook)();
};

namespace valkey_search {

class MemoryPool : public std::pmr::memory_resource {
 public:
  MemoryPool(size_t chunk_size);
  ~MemoryPool() override;
  size_t GetAllocated() const { return allocated_; }
  size_t GetInUse() const { return inuse_; }
  size_t GetFreed() const { return freed_; }

  //
  // Interface to other modules
  //
  //
  // Debugging infrastructure for MemoryPool
  //
  // If this is in the callstack, then non-Pooled allocations are
  // conditionally captured and made visible via the FT._DEBUG command
  //
  class EnableCapture {
   public:
    EnableCapture() {
      vmsdk::malloc_hook = &Capture;
      CaptureEnabled = CaptureRequested;
    }
    ~EnableCapture() { CaptureEnabled = false; }
  };

  inline static void Capture() {
    if (CaptureEnabled) {
      DoCapture();
    }
  }

  static absl::Status DebugCmd(ValkeyModuleCtx* ctx, vmsdk::ArgsIterator& itr);

 private:
  static bool thread_local CaptureEnabled;
  static bool CaptureRequested;
  static void DoCapture();

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
using MemoryPoolAllocator = std::pmr::polymorphic_allocator<T>;

//
// Define convenience container types
//
template <typename T>
using PooledVector = std::vector<T, MemoryPoolAllocator<T>>;

template <typename T, size_t N>
using PooledInlinedVector = absl::InlinedVector<T, N, MemoryPoolAllocator<T>>;

template <typename K, typename V>
using PoolFlatHashMap =
    absl::flat_hash_map<K, V, MemoryPoolAllocator<std::pair<K, V>>>;

}  // namespace valkey_search

#endif