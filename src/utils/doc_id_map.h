#ifndef VALKEYSEARCH_SRC_UTILS_DOC_ID_MAP_H_
#define VALKEYSEARCH_SRC_UTILS_DOC_ID_MAP_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"

namespace valkey_search {

using DocId = uint32_t;
constexpr DocId kInvalidDocId = 0;

class DocIdMap {
 public:
  static constexpr size_t kChunkSize = 4096;
  static constexpr size_t kMaxChunks = 16384;  // Supports up to ~67 million documents

  static DocIdMap& Instance() {
    static DocIdMap instance;
    return instance;
  }

  DocId GetOrAssign(absl::string_view doc_key) {
    size_t shard = std::hash<absl::string_view>{}(doc_key) % kNumShards;
    auto& s = shards_[shard];
    
    // First check existing entry under shard lock
    {
      absl::MutexLock lock(&s.mutex);
      auto it = s.key_to_id.find(doc_key);
      if (it != s.key_to_id.end()) {
        return it->second;
      }
    }

    // Allocate new ID
    DocId id = next_id_.fetch_add(1, std::memory_order_relaxed);
    
    // Ensure reverse mapping chunk exists
    EnsureChunkAllocated(id);
    
    // Store reverse mapping string in lock-free chunk
    size_t chunk_idx = id / kChunkSize;
    size_t offset = id % kChunkSize;
    std::string* chunk = chunks_[chunk_idx].load(std::memory_order_acquire);
    chunk[offset] = std::string(doc_key);

    // Insert into forward lookup map
    {
      absl::MutexLock lock(&s.mutex);
      auto [it, inserted] = s.key_to_id.emplace(std::string(doc_key), id);
      if (!inserted) {
        // Double-check race condition: another thread inserted the key first
        return it->second;
      }
    }
    return id;
  }

  DocId GetOrCreateDocId(absl::string_view doc_key) {
    return GetOrAssign(doc_key);
  }

  DocId GetDocId(absl::string_view doc_key) const {
    size_t shard = std::hash<absl::string_view>{}(doc_key) % kNumShards;
    auto& s = shards_[shard];
    absl::MutexLock lock(&s.mutex);
    auto it = s.key_to_id.find(doc_key);
    if (it != s.key_to_id.end()) {
      return it->second;
    }
    return kInvalidDocId;
  }

  // Lock-free O(1) reverse lookup by DocId
  const std::string& GetKey(DocId id) const {
    size_t chunk_idx = id / kChunkSize;
    size_t offset = id % kChunkSize;

    if (chunk_idx < kMaxChunks) {
      std::string* chunk = chunks_[chunk_idx].load(std::memory_order_acquire);
      if (chunk != nullptr) {
        return chunk[offset];
      }
    }
    static const std::string empty;
    return empty;
  }

  size_t Size() const {
    return next_id_.load(std::memory_order_relaxed) - 1;
  }

  void Clear() {
    for (size_t i = 0; i < kNumShards; ++i) {
      absl::MutexLock lock(&shards_[i].mutex);
      shards_[i].key_to_id.clear();
    }

    absl::MutexLock alloc_lock(&alloc_mutex_);
    for (size_t i = 0; i < kMaxChunks; ++i) {
      std::string* chunk = chunks_[i].load(std::memory_order_relaxed);
      if (chunk != nullptr) {
        delete[] chunk;
        chunks_[i].store(nullptr, std::memory_order_relaxed);
      }
    }
    next_id_.store(1, std::memory_order_relaxed);
  }

  ~DocIdMap() {
    Clear();
  }

 private:
  DocIdMap() : next_id_(1) {
    for (size_t i = 0; i < kMaxChunks; ++i) {
      chunks_[i].store(nullptr, std::memory_order_relaxed);
    }
  }

  void EnsureChunkAllocated(DocId id) {
    size_t chunk_idx = id / kChunkSize;
    if (chunk_idx >= kMaxChunks) return;

    if (chunks_[chunk_idx].load(std::memory_order_acquire) == nullptr) {
      absl::MutexLock lock(&alloc_mutex_);
      if (chunks_[chunk_idx].load(std::memory_order_relaxed) == nullptr) {
        std::string* new_chunk = new std::string[kChunkSize];
        chunks_[chunk_idx].store(new_chunk, std::memory_order_release);
      }
    }
  }

  static constexpr size_t kNumShards = 32;
  struct Shard {
    mutable absl::Mutex mutex;
    absl::flat_hash_map<std::string, DocId> key_to_id ABSL_GUARDED_BY(mutex);
  };

  Shard shards_[kNumShards];
  std::atomic<std::string*> chunks_[kMaxChunks];
  mutable absl::Mutex alloc_mutex_;
  std::atomic<DocId> next_id_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_UTILS_DOC_ID_MAP_H_
