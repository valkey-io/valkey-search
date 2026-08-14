/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_UTILS_ALLOCATOR_H_
#define VALKEYSEARCH_SRC_UTILS_ALLOCATOR_H_

#include <atomic>
#include <cstddef>
#include <memory>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "src/utils/intrusive_list.h"
#include "src/utils/intrusive_ref_count.h"

namespace valkey_search {
constexpr size_t kFreeEntriesPerChunkGroupSize = 7;
constexpr size_t kChunkBufferPages = 10;
constexpr size_t kChunkBufferMinEntriesPerChunk = 8;
constexpr size_t kEntriesPerBitmapWord = 64;

/*
FixedSizeAllocator is responsible for allocating and managing contiguous
memory chunks for a specific buffer size. Grouping buffers of identical sizes in
the same allocator minimizes maintenance overhead and improves CPU cache
locality, benefiting applications like vector search that frequently access
same-sized buffers.

The `FixedSizeAllocator` uses an atomic bitvector per chunk for lock-free
allocations and frees, minimizing mutex contention under multi-threading.
*/

struct AllocatorChunk;

class Allocator {
 public:
  virtual char *Allocate(size_t size) = 0;
  virtual char *Allocate(size_t size, size_t alignment) {
    return Allocate(size);
  }
  static bool Free(char *ptr);
  static size_t GetAllocatedSize(char *ptr);
  virtual ~Allocator() = default;
  virtual size_t ChunkSize() const = 0;
  virtual size_t Alignment() const { return 8; }

 protected:
  friend class SegregatedFixedSizeAllocator;
  virtual void Free(AllocatorChunk *chunk, char *ptr) = 0;
};

class FixedSizeAllocator;

struct AllocatorChunk {
  AllocatorChunk(Allocator *allocator, size_t size, size_t alignment = 8);
  ~AllocatorChunk();
  void Retain() { ref_count.fetch_add(1, std::memory_order_relaxed); }
  bool TryRetain() {
    uint32_t count = ref_count.load(std::memory_order_relaxed);
    while (count > 0) {
      if (ref_count.compare_exchange_weak(count, count + 1,
                                          std::memory_order_relaxed)) {
        return true;
      }
    }
    return false;
  }
  void Release() {
    if (ref_count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      delete this;
    }
  }

  size_t entries_in_chunk;
  size_t entry_size;
  size_t alignment;

  struct AlignedDeleter {
    size_t alignment;
    void operator()(char *ptr) const {
      ::operator delete[](ptr, std::align_val_t{alignment});
    }
  };
  std::unique_ptr<char[], AlignedDeleter> raw_data;
  char *data{nullptr};

  size_t num_bitmap_words;
  // Bitvector: 0 = free, 1 = occupied
  std::unique_ptr<std::atomic<uint64_t>[]> bitmap;
  std::atomic<size_t> scan_hint{0};

  std::atomic<uint32_t> allocated_count{0};
  std::atomic<bool> retired{false};
  std::atomic<uint32_t> ref_count{1};
  int current_group{-1};
  Allocator *allocator;

  // Intrusive linked list.
  AllocatorChunk *next{nullptr};
  AllocatorChunk *prev{nullptr};
};

class FixedSizeAllocator : public IntrusiveRefCount, public Allocator {
 public:
  friend class IntrusiveRefCount;
  explicit FixedSizeAllocator(size_t size, size_t alignment = 8);
  char *Allocate(size_t size) override;
  char *Allocate(size_t size, size_t alignment) override;
  char *Allocate();
  size_t ActiveAllocations() const {
    return active_allocations_.load(std::memory_order_relaxed);
  }
  size_t ChunkCount() const ABSL_LOCKS_EXCLUDED(mutex_);
  ~FixedSizeAllocator() override;
  size_t ChunkSize() const override { return size_; }
  size_t Alignment() const override { return alignment_; }

 protected:
  void Free(AllocatorChunk *chunk, char *ptr) override;

 private:
  IntrusiveList<AllocatorChunk> chunks_grouped_by_free_entries_
      [kFreeEntriesPerChunkGroupSize] ABSL_GUARDED_BY(mutex_);
  size_t size_;
  size_t alignment_{8};
  IntrusiveList<AllocatorChunk> fully_used_chunks_ ABSL_GUARDED_BY(mutex_);
  std::atomic<AllocatorChunk *> current_chunk_{nullptr};
  std::atomic<size_t> active_allocations_{0};
  mutable absl::Mutex mutex_;
  void UpdateChunkGroup(AllocatorChunk *chunk)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void AllocateChunk() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
};

DEFINE_UNIQUE_PTR_TYPE(Allocator);
DEFINE_UNIQUE_PTR_TYPE(FixedSizeAllocator);

class SegregatedFixedSizeAllocator : public Allocator {
 public:
  static constexpr size_t kSizeClasses[] = {
      2,   4,   6,   8,   10,  12,   14,   16,   18,   20,  22,  24,
      28,  32,  40,  48,  56,  64,   72,   80,   88,   96,  104, 112,
      120, 128, 144, 160, 176, 192,  208,  224,  240,  256, 320, 384,
      448, 512, 640, 768, 896, 1024, 1536, 2048, 3072, 4096};
  static constexpr size_t kNumClasses =
      sizeof(kSizeClasses) / sizeof(kSizeClasses[0]);
  static constexpr size_t kMaxSize = kSizeClasses[kNumClasses - 1];

  explicit SegregatedFixedSizeAllocator(size_t default_alignment = 8);
  ~SegregatedFixedSizeAllocator() override = default;

  char *Allocate(size_t size) override;
  char *Allocate(size_t size, size_t alignment) override;
  static bool Free(char *ptr);
  char *Reallocate(char *ptr, size_t new_size);
  static size_t UsableSize(char *ptr);
  size_t ChunkSize() const override { return kMaxSize; }
  size_t Alignment() const override { return 32; }

 protected:
  void Free(AllocatorChunk *chunk, char *ptr) override;

 private:
  static size_t GetSizeClassIndex(size_t size);
  std::vector<UniqueFixedSizeAllocatorPtr> allocators_;
};

SegregatedFixedSizeAllocator &GetDefaultSegregatedAllocator();
SegregatedFixedSizeAllocator &GetThreadLocalSegregatedAllocator();

DEFINE_UNIQUE_PTR_TYPE(SegregatedFixedSizeAllocator);

size_t BufferSize(size_t size);
size_t EntriesFitInChunk(size_t size, size_t num_pages);

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_UTILS_ALLOCATOR_H_
