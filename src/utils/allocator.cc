/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/allocator.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <map>
#include <memory>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/log/check.h"
#include "absl/synchronization/mutex.h"

namespace valkey_search {

size_t BufferSize(size_t entries_in_chunk, size_t size) {
  return entries_in_chunk * size;
}

class ChunkTracker {
 public:
  ChunkTracker() = default;
  void Track(const AllocatorChunk *chunk) ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    chunks_by_data_.insert(std::make_pair(chunk->data.get(), chunk));
  }
  const AllocatorChunk *FindChunk(char *ptr) const ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);

    auto it = chunks_by_data_.upper_bound(ptr);
    if (it != chunks_by_data_.begin()) {
      --it;
      if (it->second->data.get() <= ptr) {
        DCHECK_GT(it->second->data.get() +
                      BufferSize(it->second->entries_in_chunk,
                                 it->second->allocator->ChunkSize()),
                  ptr);
        return it->second;
      }
    }
    return nullptr;
  }
  void Untrack(const AllocatorChunk *chunk) ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    chunks_by_data_.erase(chunk->data.get());
  }

 private:
  std::map<char *, const AllocatorChunk *> chunks_by_data_
      ABSL_GUARDED_BY(mutex_);
  mutable absl::Mutex mutex_;
};

ChunkTracker chunk_tracker;

size_t CalcChunkFreeGroup(size_t free_cnt) {
  if (free_cnt == 0) {
    return -1;
  }
  auto log2 = static_cast<size_t>(std::ceil(std::log2(free_cnt)));
  return std::min(log2, kFreeEntriesPerChunkGroupSize - 1);
}

int UpperBoundToMultipleOf8(int num) { return (num + 7) & ~7; }

// TODO: allow deletion of chunks when they are empty
FixedSizeAllocator::FixedSizeAllocator(size_t size, bool require_ptr_alignment)
    : size_(size), require_ptr_alignment_(require_ptr_alignment) {
  if (require_ptr_alignment_) {
    size_ = UpperBoundToMultipleOf8(size);
  }
}

FixedSizeAllocator::~FixedSizeAllocator() {
  CHECK(fully_used_chunks_.Empty());
  for (auto &chunk_group : chunks_grouped_by_free_entries_) {
    CHECK(chunk_group.Empty());
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

char *FixedSizeAllocator::Allocate(size_t size) {
  if (require_ptr_alignment_) {
    size = UpperBoundToMultipleOf8(size);
  }
  CHECK_EQ(size, size_);
  return Allocate();
}

char *FixedSizeAllocator::Allocate() {
  absl::MutexLock lock(&mutex_);
  if (current_chunk_ == nullptr) {
    AllocateChunk();
  }
  int old_free_group = CalcChunkFreeGroup(current_chunk_->free_list.size());
  CHECK_GT(old_free_group, -1);
  auto ptr = current_chunk_->free_list.top();
  current_chunk_->free_list.pop();
  ++active_allocations_;

  HandleChunkEntryUsageChange(current_chunk_, old_free_group);
  if (!current_chunk_) {
    SelectCurrentChunk();
  }
  IncrementRef();
  return ptr;
}

void FixedSizeAllocator::HandleChunkEntryUsageChange(AllocatorChunk *chunk,
                                                     int old_free_group) {
  if (old_free_group == -1) {
    fully_used_chunks_.Remove(chunk);
    int new_free_group = CalcChunkFreeGroup(chunk->free_list.size());
    chunks_grouped_by_free_entries_[new_free_group].PushBack(chunk);
    return;
  }
  if (chunk->free_list.empty()) {
    chunks_grouped_by_free_entries_[old_free_group].Remove(chunk);
    if (chunk == current_chunk_) {
      current_chunk_ = nullptr;
    }
    fully_used_chunks_.PushBack(chunk);
    return;
  }
  int new_free_group = CalcChunkFreeGroup(chunk->free_list.size());
  CHECK_GT(new_free_group, -1);
  if (new_free_group != old_free_group) {
    chunks_grouped_by_free_entries_[old_free_group].Remove(chunk);
    chunks_grouped_by_free_entries_[new_free_group].PushBack(chunk);
  }
}

void FixedSizeAllocator::SelectCurrentChunk() {
  auto current_chunk_group =
      current_chunk_ ? CalcChunkFreeGroup(current_chunk_->free_list.size())
                     : kFreeEntriesPerChunkGroupSize;
  for (size_t i = 0; i < current_chunk_group; ++i) {
    if (!chunks_grouped_by_free_entries_[i].Empty()) {
      current_chunk_ = chunks_grouped_by_free_entries_[i].Front();
      break;
    }
  }
}

void FixedSizeAllocator::AllocateChunk() {
  current_chunk_ = new AllocatorChunk(this, size_);
  chunks_grouped_by_free_entries_[CalcChunkFreeGroup(
                                      current_chunk_->entries_in_chunk)]
      .PushBack(current_chunk_);
}

void FixedSizeAllocator::Free(AllocatorChunk *chunk, char *ptr) {
  {
    absl::MutexLock lock(&mutex_);
    --active_allocations_;

    int free_group = CalcChunkFreeGroup(chunk->free_list.size());
    chunk->free_list.push(ptr);
    HandleChunkEntryUsageChange(chunk, free_group);
    if (chunk->free_list.size() == chunk->entries_in_chunk) {
      chunks_grouped_by_free_entries_[CalcChunkFreeGroup(
                                          chunk->free_list.size())]
          .Remove(chunk);
      if (chunk == current_chunk_) {
        current_chunk_ = nullptr;
      }
      delete chunk;
    }
    SelectCurrentChunk();
  }
  DecrementRef();
}

size_t GetPageSize() { return static_cast<size_t>(sysconf(_SC_PAGESIZE)); }

size_t EntriesFitInChunk(size_t size, size_t num_pages) {
  static const size_t page_size = GetPageSize();
  size_t total_bytes = num_pages * page_size;
  return std::max<size_t>(kChunkBufferMinEntriesPerChunk, total_bytes / size);
}

AllocatorChunk::AllocatorChunk(Allocator *allocator, size_t size)
    : entries_in_chunk(EntriesFitInChunk(size, kChunkBufferPages)),
      // Note: Using new[] to avoid calling constructor of char[].
      data(std::unique_ptr<char[]>(
          new char[BufferSize(entries_in_chunk, size)])),
      allocator(allocator) {
  for (size_t i = 0; i < entries_in_chunk; ++i) {
    free_list.push(data.get() + i * size);
  }
  chunk_tracker.Track(this);
}

AllocatorChunk::~AllocatorChunk() { chunk_tracker.Untrack(this); }

bool Allocator::Free(char *ptr) {
  auto chunk = chunk_tracker.FindChunk(ptr);
  if (!chunk) {
    return false;
  }
  chunk->allocator->Free(const_cast<AllocatorChunk *>(chunk), ptr);
  return true;
}

}  // namespace valkey_search
