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
#include "src/utils/string_interning.h"

namespace valkey_search {

using DocId = uint32_t;
constexpr DocId kInvalidDocId = 0;

class DocIdMap {
 public:
  static constexpr size_t kChunkShift = 16;
  static constexpr size_t kChunkSize = 1ULL << kChunkShift;                  // 65,536 elements per chunk
  static constexpr size_t kMaxChunks = 1ULL << (32 - kChunkShift);           // 65,536 chunks (Supports up to 4.29B docs)

  static DocIdMap& Instance() {
    static DocIdMap instance;
    return instance;
  }

  DocId GetOrAssign(absl::string_view doc_key) {
    InternedStringPtr interned_key = StringInternStore::Intern(doc_key);
    return GetOrAssign(interned_key);
  }

  DocId GetOrAssign(const InternedStringPtr& interned_key) {
    if (!interned_key) return kInvalidDocId;
    size_t shard = interned_key.Hash() % kNumShards;
    auto& s = shards_[shard];
    
    // First check existing entry under shard lock
    {
      absl::MutexLock lock(&s.mutex);
      auto it = s.key_to_id.find(interned_key);
      if (it != s.key_to_id.end()) {
        return it->second;
      }
    }

    // Allocate new ID
    DocId id = next_id_.fetch_add(1, std::memory_order_relaxed);
    
    // Ensure reverse mapping chunk exists
    EnsureChunkAllocated(id);
    
    // Store reverse mapping interned string in lock-free chunk
    size_t chunk_idx = id >> kChunkShift;
    size_t offset = id & (kChunkSize - 1);
    InternedStringPtr* chunk = chunks_[chunk_idx].load(std::memory_order_acquire);
    chunk[offset] = interned_key;

    // Insert into forward lookup map
    {
      absl::MutexLock lock(&s.mutex);
      auto [it, inserted] = s.key_to_id.emplace(interned_key, id);
      if (!inserted) {
        // Double-check race condition: another thread inserted the key first
        return it->second;
      }
    }
    return id;
  }

  DocId GetDocId(absl::string_view doc_key) const {
    InternedStringPtr interned_key = StringInternStore::Intern(doc_key);
    return GetDocId(interned_key);
  }

  DocId GetDocId(const InternedStringPtr& interned_key) const {
    if (!interned_key) return kInvalidDocId;
    size_t shard = interned_key.Hash() % kNumShards;
    auto& s = shards_[shard];
    absl::MutexLock lock(&s.mutex);
    auto it = s.key_to_id.find(interned_key);
    if (it != s.key_to_id.end()) {
      return it->second;
    }
    return kInvalidDocId;
  }

  // Lock-free O(1) reverse lookup using bitwise chunk/offset mask
  const InternedStringPtr& GetKey(DocId id) const {
    size_t chunk_idx = id >> kChunkShift;
    size_t offset = id & (kChunkSize - 1);

    if (chunk_idx < kMaxChunks) {
      InternedStringPtr* chunk = chunks_[chunk_idx].load(std::memory_order_acquire);
      if (chunk != nullptr) {
        return chunk[offset];
      }
    }
    static const InternedStringPtr empty;
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
      InternedStringPtr* chunk = chunks_[i].load(std::memory_order_relaxed);
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
    size_t chunk_idx = id >> kChunkShift;
    if (chunk_idx >= kMaxChunks) return;

    if (chunks_[chunk_idx].load(std::memory_order_acquire) == nullptr) {
      absl::MutexLock lock(&alloc_mutex_);
      if (chunks_[chunk_idx].load(std::memory_order_relaxed) == nullptr) {
        InternedStringPtr* new_chunk = new InternedStringPtr[kChunkSize];
        chunks_[chunk_idx].store(new_chunk, std::memory_order_release);
      }
    }
  }

  static constexpr size_t kNumShards = 32;
  struct Shard {
    mutable absl::Mutex mutex;
    absl::flat_hash_map<InternedStringPtr, DocId> key_to_id ABSL_GUARDED_BY(mutex);
  };

  Shard shards_[kNumShards];
  std::atomic<InternedStringPtr*> chunks_[kMaxChunks];
  mutable absl::Mutex alloc_mutex_;
  std::atomic<DocId> next_id_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_UTILS_DOC_ID_MAP_H_
