/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/allocator.h"

#include <algorithm>
#include <atomic>
#include <bit>
#include <cmath>
#include <cstddef>
#include <map>
#include <memory>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/log/check.h"
#include "absl/synchronization/mutex.h"

extern "C" {
// NOLINTNEXTLINE(readability-identifier-naming)
void *__wrap_malloc(size_t size);
// NOLINTNEXTLINE(readability-identifier-naming)
void __wrap_free(void *ptr);
// NOLINTNEXTLINE(readability-identifier-naming)
void *__wrap_realloc(void *ptr, size_t size);
// NOLINTNEXTLINE(readability-identifier-naming)
int __wrap_malloc_usable_size(void *ptr);
}

namespace valkey_search {

size_t BufferSize(size_t entries_in_chunk, size_t size) {
  return entries_in_chunk * size;
}

class ChunkTracker {
 public:
  ChunkTracker() = default;
  void Track(const AllocatorChunk *chunk) ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::WriterMutexLock lock(&mutex_);
    chunks_by_data_.insert(std::make_pair(chunk->data, chunk));
  }
  AllocatorChunk *FindAndRetainChunk(char *ptr) const
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::ReaderMutexLock lock(&mutex_);

    auto it = chunks_by_data_.upper_bound(ptr);
    if (it != chunks_by_data_.begin()) {
      --it;
      if (ptr >= it->second->data &&
          ptr <
              it->second->data + BufferSize(it->second->entries_in_chunk,
                                                  it->second->entry_size)) {
        auto chunk = const_cast<AllocatorChunk *>(it->second);
        chunk->Retain();
        return chunk;
      }
    }
    return nullptr;
  }
  void Untrack(const AllocatorChunk *chunk) ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::WriterMutexLock lock(&mutex_);
    chunks_by_data_.erase(chunk->data);
  }

 private:
  std::map<char *, const AllocatorChunk *> chunks_by_data_
      ABSL_GUARDED_BY(mutex_);
  mutable absl::Mutex mutex_;
};

ChunkTracker chunk_tracker;

int CalcChunkFreeGroup(size_t free_cnt) {
  if (free_cnt == 0) {
    return -1;
  }
  auto log2 = static_cast<size_t>(std::ceil(std::log2(free_cnt)));
  return static_cast<int>(std::min(log2, kFreeEntriesPerChunkGroupSize - 1));
}

size_t AlignUp(size_t size, size_t alignment) {
  if (alignment <= 1) {
    return size;
  }
  return (size + alignment - 1) & ~(alignment - 1);
}

size_t NormalizeAlignment(size_t alignment) {
  if (alignment <= 8) {
    return 8;
  }
  size_t a = 8;
  while (a < alignment) {
    a <<= 1;
  }
  return a;
}

size_t GetPageSize() { return static_cast<size_t>(sysconf(_SC_PAGESIZE)); }

size_t EntriesFitInChunk(size_t size, size_t num_pages) {
  static const size_t page_size = GetPageSize();
  size_t total_bytes = num_pages * page_size;
  return std::max<size_t>(kChunkBufferMinEntriesPerChunk, total_bytes / size);
}

AllocatorChunk::AllocatorChunk(Allocator *allocator, size_t size,
                               size_t alignment)
    : entries_in_chunk(
          EntriesFitInChunk(AlignUp(size, alignment), kChunkBufferPages)),
      entry_size(AlignUp(size, alignment)),
      alignment(alignment),
      raw_data(static_cast<char *>(::operator new[](
                   BufferSize(entries_in_chunk, entry_size) + alignment,
                   std::align_val_t{alignment})),
               AlignedDeleter{alignment}),
      data(reinterpret_cast<char *>(
          AlignUp(reinterpret_cast<uintptr_t>(raw_data.get()), alignment))),
      num_bitmap_words((entries_in_chunk + kEntriesPerBitmapWord - 1) /
                       kEntriesPerBitmapWord),
      bitmap(std::make_unique<std::atomic<uint64_t>[]>(num_bitmap_words)),
      allocator(allocator) {
  for (size_t word_index = 0; word_index < num_bitmap_words; ++word_index) {
    bitmap[word_index].store(0, std::memory_order_relaxed);
  }
  size_t valid_bits_in_last_word = entries_in_chunk % kEntriesPerBitmapWord;
  if (valid_bits_in_last_word != 0) {
    uint64_t unused_mask = ~((1ULL << valid_bits_in_last_word) - 1);
    bitmap[num_bitmap_words - 1].store(unused_mask, std::memory_order_relaxed);
  }
  chunk_tracker.Track(this);
}

AllocatorChunk::~AllocatorChunk() { chunk_tracker.Untrack(this); }

FixedSizeAllocator::FixedSizeAllocator(size_t size, size_t alignment)
    : size_(alignment <= 1 ? size
                           : AlignUp(size, NormalizeAlignment(alignment))),
      alignment_(alignment <= 1 ? 1 : NormalizeAlignment(alignment)) {}

FixedSizeAllocator::~FixedSizeAllocator() {
  auto clear_list = [](IntrusiveList<AllocatorChunk> &list) {
    while (!list.Empty()) {
      auto chunk = list.Front();
      list.Remove(chunk);
      chunk_tracker.Untrack(chunk);
      chunk->Release();
    }
  };
  clear_list(fully_used_chunks_);
  for (auto &chunk_group : chunks_grouped_by_free_entries_) {
    clear_list(chunk_group);
  }
}

size_t FixedSizeAllocator::ChunkCount() const {
  absl::MutexLock lock(&mutex_);
  auto size = fully_used_chunks_.Size();
  for (auto &chunk_group : chunks_grouped_by_free_entries_) {
    size += chunk_group.Size();
  }
  return size;
}

static char *TryAllocateFromChunk(AllocatorChunk *chunk) {
  if (!chunk || chunk->retired.load(std::memory_order_acquire)) {
    return nullptr;
  }
  // Speculatively increment allocated_count to establish liveness and prevent
  // concurrent Free() from observing a zero count and retiring the chunk.
  uint32_t prev_count =
      chunk->allocated_count.fetch_add(1, std::memory_order_acq_rel);
  if (prev_count >= chunk->entries_in_chunk ||
      chunk->retired.load(std::memory_order_acquire)) {
    chunk->allocated_count.fetch_sub(1, std::memory_order_relaxed);
    return nullptr;
  }

  size_t start_word = chunk->scan_hint.load(std::memory_order_relaxed) %
                      chunk->num_bitmap_words;
  for (size_t i = 0; i < chunk->num_bitmap_words; ++i) {
    if (chunk->retired.load(std::memory_order_acquire)) {
      chunk->allocated_count.fetch_sub(1, std::memory_order_relaxed);
      return nullptr;
    }
    size_t word_index = (start_word + i) % chunk->num_bitmap_words;
    uint64_t old_val =
        chunk->bitmap[word_index].load(std::memory_order_relaxed);
    while (old_val != ~0ULL) {
      if (chunk->retired.load(std::memory_order_acquire)) {
        chunk->allocated_count.fetch_sub(1, std::memory_order_relaxed);
        return nullptr;
      }
      int bit_index = std::countr_zero(~old_val);
      size_t entry_idx = word_index * kEntriesPerBitmapWord + bit_index;
      uint64_t new_val = old_val | (1ULL << bit_index);
      if (chunk->bitmap[word_index].compare_exchange_weak(
              old_val, new_val, std::memory_order_acquire,
              std::memory_order_relaxed)) {
        if (chunk->retired.load(std::memory_order_acquire)) {
          uint64_t rollback_mask = ~(1ULL << bit_index);
          chunk->bitmap[word_index].fetch_and(rollback_mask,
                                              std::memory_order_relaxed);
          chunk->allocated_count.fetch_sub(1, std::memory_order_relaxed);
          return nullptr;
        }
        chunk->scan_hint.store((word_index + 1) % chunk->num_bitmap_words,
                               std::memory_order_relaxed);
        return chunk->data + entry_idx * chunk->entry_size;
      }
    }
  }
  // No free slot found in this chunk; revert speculative count increment.
  chunk->allocated_count.fetch_sub(1, std::memory_order_relaxed);
  return nullptr;
}

char *FixedSizeAllocator::Allocate(size_t size) {
  return Allocate(size, alignment_);
}

char *FixedSizeAllocator::Allocate(size_t size, size_t alignment) {
  size_t aligned_size = AlignUp(size, alignment);
  CHECK_LE(aligned_size, size_);
  return Allocate();
}

char *FixedSizeAllocator::Allocate() {
  AllocatorChunk *curr = current_chunk_.load(std::memory_order_acquire);
  if (curr && curr->TryRetain()) {
    char *ptr = TryAllocateFromChunk(curr);
    if (curr->allocated_count.load(std::memory_order_relaxed) >=
        curr->entries_in_chunk) {
      AllocatorChunk *expected = curr;
      current_chunk_.compare_exchange_strong(expected, nullptr,
                                             std::memory_order_release,
                                             std::memory_order_relaxed);
    }
    curr->Release();
    if (ptr) {
      active_allocations_.fetch_add(1, std::memory_order_relaxed);
      IncrementRef();
      return ptr;
    }
  }

  absl::MutexLock lock(&mutex_);
  curr = current_chunk_.load(std::memory_order_acquire);
  if (curr) {
    char *ptr = TryAllocateFromChunk(curr);
    if (ptr) {
      if (curr->allocated_count.load(std::memory_order_relaxed) >=
          curr->entries_in_chunk) {
        UpdateChunkGroup(curr);
        current_chunk_.store(nullptr, std::memory_order_relaxed);
      }
      active_allocations_.fetch_add(1, std::memory_order_relaxed);
      IncrementRef();
      return ptr;
    }
    UpdateChunkGroup(curr);
    current_chunk_.store(nullptr, std::memory_order_relaxed);
  }

  for (auto &chunk_group : chunks_grouped_by_free_entries_) {
    while (!chunk_group.Empty()) {
      auto chunk = chunk_group.Front();
      char *ptr = TryAllocateFromChunk(chunk);
      if (ptr) {
        if (chunk->allocated_count.load(std::memory_order_relaxed) <
            chunk->entries_in_chunk) {
          current_chunk_.store(chunk, std::memory_order_release);
        } else {
          UpdateChunkGroup(chunk);
          current_chunk_.store(nullptr, std::memory_order_relaxed);
        }
        active_allocations_.fetch_add(1, std::memory_order_relaxed);
        IncrementRef();
        return ptr;
      }
      UpdateChunkGroup(chunk);
    }
  }

  AllocateChunk();
  curr = current_chunk_.load(std::memory_order_acquire);
  char *ptr = TryAllocateFromChunk(curr);
  CHECK_NE(ptr, nullptr);
  if (curr->allocated_count.load(std::memory_order_relaxed) >=
      curr->entries_in_chunk) {
    UpdateChunkGroup(curr);
    current_chunk_.store(nullptr, std::memory_order_relaxed);
  }
  active_allocations_.fetch_add(1, std::memory_order_relaxed);
  IncrementRef();
  return ptr;
}

void FixedSizeAllocator::Free(AllocatorChunk *chunk, char *ptr) {
  size_t offset = ptr - chunk->data;
  size_t entry_idx = offset / chunk->entry_size;
  size_t word_index = entry_idx / kEntriesPerBitmapWord;
  size_t bit_index = entry_idx % kEntriesPerBitmapWord;

  uint64_t mask = ~(1ULL << bit_index);
  chunk->bitmap[word_index].fetch_and(mask, std::memory_order_release);

  uint32_t prev_allocations =
      chunk->allocated_count.fetch_sub(1, std::memory_order_acq_rel);
  active_allocations_.fetch_sub(1, std::memory_order_relaxed);

  uint32_t curr_allocations = prev_allocations - 1;

  int prev_group =
      CalcChunkFreeGroup(chunk->entries_in_chunk - prev_allocations);
  int curr_group =
      CalcChunkFreeGroup(chunk->entries_in_chunk - curr_allocations);

  if (prev_group != curr_group || curr_allocations == 0) {
    absl::MutexLock lock(&mutex_);
    if (chunk->current_group == -2) {
      DecrementRef();
      return;
    }
    if (curr_allocations == 0 &&
        chunk->allocated_count.load(std::memory_order_acquire) == 0) {
      if (chunk->current_group >= 0) {
        chunks_grouped_by_free_entries_[chunk->current_group].Remove(chunk);
      } else if (chunk->current_group == -1) {
        fully_used_chunks_.Remove(chunk);
      }
      if (current_chunk_.load(std::memory_order_relaxed) == chunk) {
        current_chunk_.store(nullptr, std::memory_order_release);
      }
      chunk->current_group = -2;
      chunk->retired.store(true, std::memory_order_release);
      chunk_tracker.Untrack(chunk);
      chunk->Release();
    } else {
      UpdateChunkGroup(chunk);
    }
  }
  DecrementRef();
}

void FixedSizeAllocator::UpdateChunkGroup(AllocatorChunk *chunk) {
  if (chunk->current_group == -1) {
    fully_used_chunks_.Remove(chunk);
  } else if (chunk->current_group >= 0) {
    chunks_grouped_by_free_entries_[chunk->current_group].Remove(chunk);
  }

  size_t free_cnt = chunk->entries_in_chunk -
                    chunk->allocated_count.load(std::memory_order_relaxed);
  if (free_cnt == 0) {
    fully_used_chunks_.PushBack(chunk);
    chunk->current_group = -1;
    if (current_chunk_.load(std::memory_order_relaxed) == chunk) {
      current_chunk_.store(nullptr, std::memory_order_relaxed);
    }
  } else {
    int new_group = CalcChunkFreeGroup(free_cnt);
    chunks_grouped_by_free_entries_[new_group].PushBack(chunk);
    chunk->current_group = new_group;
  }
}

void FixedSizeAllocator::AllocateChunk() {
  auto new_chunk = new AllocatorChunk(this, size_, alignment_);
  size_t free_group = CalcChunkFreeGroup(new_chunk->entries_in_chunk);
  chunks_grouped_by_free_entries_[free_group].PushBack(new_chunk);
  new_chunk->current_group = free_group;
  current_chunk_.store(new_chunk, std::memory_order_release);
}

bool Allocator::Free(char *ptr) {
  auto chunk = chunk_tracker.FindAndRetainChunk(ptr);
  if (!chunk) {
    return false;
  }
  chunk->allocator->Free(chunk, ptr);
  chunk->Release();
  return true;
}

size_t Allocator::GetAllocatedSize(char *ptr) {
  auto chunk = chunk_tracker.FindAndRetainChunk(ptr);
  if (!chunk) {
    return 0;
  }
  size_t entry_size = chunk->entry_size;
  chunk->Release();
  return entry_size;
}

SegregatedFixedSizeAllocator::SegregatedFixedSizeAllocator(
    size_t default_alignment) {
  allocators_.reserve(kNumClasses);
  for (unsigned long kSizeClasse : kSizeClasses) {
    size_t alignment = (kSizeClasse >= 128) ? 32 : default_alignment;
    allocators_.push_back(
        CREATE_UNIQUE_PTR(FixedSizeAllocator, kSizeClasse, alignment));
  }
}

size_t SegregatedFixedSizeAllocator::GetSizeClassIndex(size_t size) {
  for (size_t i = 0; i < kNumClasses; ++i) {
    if (size <= kSizeClasses[i]) {
      return i;
    }
  }
  return kNumClasses;
}

char *SegregatedFixedSizeAllocator::Allocate(size_t size) {
  return Allocate(size, 8);
}

char *SegregatedFixedSizeAllocator::Allocate(size_t size, size_t alignment) {
  if (size == 0) {
    return nullptr;
  }
  size_t aligned_size = AlignUp(size, alignment);
  size_t idx = GetSizeClassIndex(aligned_size);
  if (idx < kNumClasses) {
    return allocators_[idx]->Allocate();
  }
  return static_cast<char *>(__wrap_malloc(aligned_size));
}

bool SegregatedFixedSizeAllocator::Free(char *ptr) {
  if (!ptr) {
    return true;
  }
  if (Allocator::Free(ptr)) {
    return true;
  }
  __wrap_free(ptr);
  return false;
}

char *SegregatedFixedSizeAllocator::Reallocate(char *ptr, size_t new_size) {
  if (!ptr) {
    return GetThreadLocalSegregatedAllocator().Allocate(new_size);
  }
  if (new_size == 0) {
    Free(ptr);
    return nullptr;
  }
  size_t old_size = UsableSize(ptr);
  if (old_size >= new_size) {
    return ptr;
  }
  char *new_ptr = GetThreadLocalSegregatedAllocator().Allocate(new_size);
  if (!new_ptr) {
    return nullptr;
  }
  if (old_size > 0) {
    std::memcpy(new_ptr, ptr, std::min(old_size, new_size));
  }
  Free(ptr);
  return new_ptr;
}

size_t SegregatedFixedSizeAllocator::UsableSize(char *ptr) {
  if (!ptr) {
    return 0;
  }
  size_t size = Allocator::GetAllocatedSize(ptr);
  if (size > 0) {
    return size;
  }
  return static_cast<size_t>(__wrap_malloc_usable_size(ptr));
}

void SegregatedFixedSizeAllocator::Free(AllocatorChunk *chunk, char *ptr) {
  chunk->allocator->Free(chunk, ptr);
}

SegregatedFixedSizeAllocator &GetThreadLocalSegregatedAllocator() {
  static thread_local SegregatedFixedSizeAllocator *thread_allocator =
      new SegregatedFixedSizeAllocator();
  return *thread_allocator;
}

SegregatedFixedSizeAllocator &GetDefaultSegregatedAllocator() {
  return GetThreadLocalSegregatedAllocator();
}

}  // namespace valkey_search
