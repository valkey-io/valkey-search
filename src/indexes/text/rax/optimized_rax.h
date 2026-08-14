/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_RAX_OPTIMIZED_RAX_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_RAX_OPTIMIZED_RAX_H_

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "src/indexes/text/rax/rax.h"
#include "src/utils/allocator.h"

namespace valkey_search {

// OptimizedRax implements a high-performance C++ wrapper around the Rax radix tree with the following optimizations:
// * Capacity Quantization & Growth Over-allocation: Rounds node allocations up to power-of-two capacity buckets
//   (32B, 64B, 128B, ...), ensuring child edge additions do not trigger repeated reallocations or memory copies.
// * Quantized Size-Class Free-Lists: Maintains an array of LIFO free-lists indexed by power-of-two bucket size,
//   enabling O(1) recycling of split and compressed nodes without invoking allocator functions.
// * Thread-Local Memory Resource Routing (RaxPmrGuard): Routes C-style malloc/free hooks directly to thread-local
//   memory pools, avoiding global heap mutex contention during multi-threaded indexing.
// * Callback-Based Bulk Deallocation: Supports custom deallocation callbacks (`FreeWithCallback`) during tree
//   teardown to safely clean up associated leaf set structures in a single pass.
class OptimizedRax {
 public:
  explicit OptimizedRax();
  ~OptimizedRax();

  OptimizedRax(const OptimizedRax &) = delete;
  OptimizedRax &operator=(const OptimizedRax &) = delete;

  int Insert(std::string_view key, void *data, void **old_data = nullptr);
  int Remove(std::string_view key, void **old_data = nullptr);
  void *Find(std::string_view key) const;
  int Mutate(std::string_view key, RaxMutateCallback mutate,
             void *caller_context, item_count_op op = kNone);
  size_t GetSubtreeItemCount(std::string_view prefix) const;
  Rax *GetRax() const { return rax_; }
  uint64_t Size() const;
  void FreeWithCallback(void (*free_callback)(void *));

  // Custom allocation hooks used by this tree instance
  void *AllocateNode(size_t size);
  void *ReallocateNode(void *ptr, size_t new_size);
  void FreeNode(void *ptr);
  int UsableSize(void *ptr);

 private:
  static constexpr int kNumBuckets = 8;
  static size_t GetBucketIndex(size_t size);
  static size_t GetBucketSize(size_t bucket_idx);

  Rax *rax_{nullptr};
  std::vector<void *> free_lists_[kNumBuckets];
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_RAX_OPTIMIZED_RAX_H_

