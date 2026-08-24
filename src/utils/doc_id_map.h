/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_SRC_UTILS_DOC_ID_MAP_H_
#define VALKEYSEARCH_SRC_UTILS_DOC_ID_MAP_H_

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <limits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "src/utils/string_interning.h"

namespace valkey_search {

using DocId = uint32_t;
constexpr DocId kInvalidDocId = 0;

struct FreeBatch {
  static constexpr size_t kCapacity = 128;
  DocId entries[kCapacity];
  uint32_t count = 0;
  FreeBatch *next = nullptr;
};

class DocIdMap {
 public:
  static constexpr size_t kChunkShift = 16;
  static constexpr size_t kChunkSize =
      1ULL << kChunkShift;  // 65,536 elements per chunk
  static constexpr size_t kMaxChunks =
      1ULL << (32 - kChunkShift);  // 65,536 chunks (Supports up to 4.29B docs)

  static DocIdMap &Instance() {
    static DocIdMap instance;
    return instance;
  }

  DocId GetOrAssign(const InternedStringPtr &doc_key) {
    if (!doc_key) {
      return kInvalidDocId;
    }
    size_t shard_idx = doc_key.Hash() % kNumShards;
    Shard &target_shard = shards_[shard_idx];

    absl::MutexLock shard_lock(&target_shard.mutex);
    auto [iter, inserted] = target_shard.key_to_id.try_emplace(doc_key, 0);
    if (!inserted) {
      return iter->second;
    }

    DocId assigned_doc_id = AllocateOrRecycleDocId();
    if (ABSL_PREDICT_FALSE(assigned_doc_id == kInvalidDocId)) {
      target_shard.key_to_id.erase(iter);
      return kInvalidDocId;
    }
    iter->second = assigned_doc_id;

    EnsureChunkAllocated(assigned_doc_id);

    size_t chunk_idx = assigned_doc_id >> kChunkShift;
    size_t offset = assigned_doc_id & (kChunkSize - 1);

    if (ABSL_PREDICT_TRUE(chunk_idx < kMaxChunks)) {
      InternedStringPtr *target_chunk =
          chunks_[chunk_idx].load(std::memory_order_acquire);
      if (ABSL_PREDICT_TRUE(target_chunk != nullptr)) {
        target_chunk[offset] = doc_key;
      }
    }
    active_doc_count_.fetch_add(1, std::memory_order_relaxed);

    return assigned_doc_id;
  }

  bool Remove(const InternedStringPtr &doc_key) {
    if (!doc_key) {
      return false;
    }
    size_t shard_idx = doc_key.Hash() % kNumShards;
    Shard &target_shard = shards_[shard_idx];

    DocId doc_id_to_free = kInvalidDocId;
    {
      absl::MutexLock shard_lock(&target_shard.mutex);
      auto iter = target_shard.key_to_id.find(doc_key);
      if (iter == target_shard.key_to_id.end()) {
        return false;
      }
      doc_id_to_free = iter->second;
      target_shard.key_to_id.erase(iter);
    }

    size_t chunk_idx = doc_id_to_free >> kChunkShift;
    size_t offset = doc_id_to_free & (kChunkSize - 1);
    if (chunk_idx < kMaxChunks) {
      InternedStringPtr *target_chunk =
          chunks_[chunk_idx].load(std::memory_order_acquire);
      if (target_chunk != nullptr) {
        target_chunk[offset] = InternedStringPtr{};
      }
    }

    if (active_doc_count_.load(std::memory_order_relaxed) > 0) {
      active_doc_count_.fetch_sub(1, std::memory_order_relaxed);
    }

    RecycleId(doc_id_to_free);
    return true;
  }

  DocId GetDocId(const InternedStringPtr &doc_key) const {
    if (!doc_key) {
      return kInvalidDocId;
    }
    size_t shard_idx = doc_key.Hash() % kNumShards;
    const Shard &target_shard = shards_[shard_idx];
    absl::ReaderMutexLock shard_lock(&target_shard.mutex);
    auto found_iter = target_shard.key_to_id.find(doc_key);
    if (found_iter != target_shard.key_to_id.end()) {
      return found_iter->second;
    }
    return kInvalidDocId;
  }

  const InternedStringPtr &GetKey(DocId doc_id) const {
    size_t chunk_idx = doc_id >> kChunkShift;
    size_t offset = doc_id & (kChunkSize - 1);

    if (chunk_idx < kMaxChunks) {
      InternedStringPtr *target_chunk =
          chunks_[chunk_idx].load(std::memory_order_acquire);
      if (target_chunk != nullptr) {
        return target_chunk[offset];
      }
    }
    static const InternedStringPtr empty_key;
    return empty_key;
  }

  size_t Size() const {
    return active_doc_count_.load(std::memory_order_relaxed);
  }

  size_t GlobalFreeBatchesCount() const {
    return global_free_batches_count_.load(std::memory_order_relaxed);
  }

  void PushBatchToGlobal(FreeBatch *batch) {
    if (!batch || batch->count == 0) {
      delete batch;
      return;
    }
    batch->next = global_free_stack_.load(std::memory_order_relaxed);
    while (!global_free_stack_.compare_exchange_weak(
        batch->next, batch, std::memory_order_release,
        std::memory_order_relaxed)) {
    }
    global_free_batches_count_.fetch_add(1, std::memory_order_relaxed);
  }

  FreeBatch *PopBatchFromGlobal() {
    if (global_free_batches_count_.load(std::memory_order_relaxed) == 0) {
      return nullptr;
    }
    FreeBatch *head = global_free_stack_.load(std::memory_order_acquire);
    while (head != nullptr && !global_free_stack_.compare_exchange_weak(
                                  head, head->next, std::memory_order_acquire,
                                  std::memory_order_relaxed)) {
    }
    if (head != nullptr) {
      global_free_batches_count_.fetch_sub(1, std::memory_order_relaxed);
      head->next = nullptr;
    }
    return head;
  }

  void Clear() {
    for (auto &shard : shards_) {
      absl::MutexLock shard_lock(&shard.mutex);
      shard.key_to_id.clear();
    }

    absl::MutexLock alloc_lock(&alloc_mutex_);
    for (auto &chunk : chunks_) {
      InternedStringPtr *allocated_chunk =
          chunk.load(std::memory_order_relaxed);
      if (allocated_chunk != nullptr) {
        delete[] allocated_chunk;
        chunk.store(nullptr, std::memory_order_relaxed);
      }
    }

    FreeBatch *curr =
        global_free_stack_.exchange(nullptr, std::memory_order_relaxed);
    while (curr != nullptr) {
      FreeBatch *nxt = curr->next;
      delete curr;
      curr = nxt;
    }
    global_free_batches_count_.store(0, std::memory_order_relaxed);

    ThreadLocalCache &tl = GetThreadLocalCache();
    if (tl.active_batch) {
      tl.active_batch->count = 0;
    }
    if (tl.spare_batch) {
      tl.spare_batch->count = 0;
    }

    next_id_.store(1, std::memory_order_relaxed);
    active_doc_count_.store(0, std::memory_order_relaxed);
  }

  ~DocIdMap() { Clear(); }

 private:
  DocIdMap()
      : next_id_(1), active_doc_count_(0), global_free_batches_count_(0) {
    for (auto &chunk : chunks_) {
      chunk.store(nullptr, std::memory_order_relaxed);
    }
    global_free_stack_.store(nullptr, std::memory_order_relaxed);
  }

  struct ThreadLocalCache {
    FreeBatch *active_batch = nullptr;
    FreeBatch *spare_batch = nullptr;

    ThreadLocalCache() {
      active_batch = new FreeBatch();
      spare_batch = new FreeBatch();
    }

    ~ThreadLocalCache() {
      if (active_batch) {
        if (active_batch->count > 0) {
          DocIdMap::Instance().PushBatchToGlobal(active_batch);
        } else {
          delete active_batch;
        }
        active_batch = nullptr;
      }
      if (spare_batch) {
        if (spare_batch->count > 0) {
          DocIdMap::Instance().PushBatchToGlobal(spare_batch);
        } else {
          delete spare_batch;
        }
        spare_batch = nullptr;
      }
    }
  };

  static ThreadLocalCache &GetThreadLocalCache() {
    thread_local ThreadLocalCache instance;
    return instance;
  }

  DocId AllocateOrRecycleDocId() {
    ThreadLocalCache &tl = GetThreadLocalCache();
    if (tl.active_batch->count > 0) {
      return tl.active_batch->entries[--tl.active_batch->count];
    }

    if (tl.spare_batch->count > 0) {
      std::swap(tl.active_batch, tl.spare_batch);
      return tl.active_batch->entries[--tl.active_batch->count];
    }

    FreeBatch *batch = PopBatchFromGlobal();
    if (batch != nullptr) {
      delete tl.active_batch;
      tl.active_batch = batch;
      return tl.active_batch->entries[--tl.active_batch->count];
    }

    DocId assigned_doc_id = next_id_.fetch_add(1, std::memory_order_relaxed);
    if (ABSL_PREDICT_FALSE(assigned_doc_id >=
                           std::numeric_limits<uint32_t>::max() - kChunkSize)) {
      next_id_.store(kInvalidDocId, std::memory_order_relaxed);
      return kInvalidDocId;
    }
    return assigned_doc_id;
  }

  void RecycleId(DocId doc_id) {
    ThreadLocalCache &tl = GetThreadLocalCache();
    if (tl.active_batch->count < FreeBatch::kCapacity) {
      tl.active_batch->entries[tl.active_batch->count++] = doc_id;
      return;
    }

    if (tl.spare_batch->count < FreeBatch::kCapacity) {
      std::swap(tl.active_batch, tl.spare_batch);
      tl.active_batch->entries[tl.active_batch->count++] = doc_id;
      return;
    }

    // Both batches are full, push
    // spare_batch to global lock-free stack
    PushBatchToGlobal(tl.spare_batch);
    tl.spare_batch = new FreeBatch();

    // Swap active and spare
    std::swap(tl.active_batch, tl.spare_batch);
    tl.active_batch->entries[tl.active_batch->count++] = doc_id;
  }

  void EnsureChunkAllocated(DocId doc_id) {
    size_t chunk_idx = doc_id >> kChunkShift;
    if (chunk_idx >= kMaxChunks) {
      return;
    }

    if (chunks_[chunk_idx].load(std::memory_order_acquire) == nullptr) {
      absl::MutexLock alloc_lock(&alloc_mutex_);
      if (chunks_[chunk_idx].load(std::memory_order_relaxed) == nullptr) {
        InternedStringPtr *new_chunk = new InternedStringPtr[kChunkSize];
        chunks_[chunk_idx].store(new_chunk, std::memory_order_release);
      }
    }
  }

  static constexpr size_t kNumShards = 256;
  struct alignas(64) Shard {
    mutable absl::Mutex mutex;
    absl::flat_hash_map<InternedStringPtr, DocId> key_to_id
        ABSL_GUARDED_BY(mutex);
  };

  Shard shards_[kNumShards];
  std::atomic<InternedStringPtr *> chunks_[kMaxChunks];
  mutable absl::Mutex alloc_mutex_;
  std::atomic<DocId> next_id_;
  std::atomic<size_t> active_doc_count_;

  std::atomic<FreeBatch *> global_free_stack_;
  std::atomic<size_t> global_free_batches_count_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_UTILS_DOC_ID_MAP_H_
