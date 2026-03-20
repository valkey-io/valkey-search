/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_INDEX_H_
#define VALKEY_SEARCH_INDEXES_TEXT_INDEX_H_

#include <algorithm>
#include <array>
#include <atomic>
#include <bitset>
#include <cctype>
#include <memory>
#include <optional>

#include "absl/base/thread_annotations.h"
#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_map.h"
#include "absl/functional/function_ref.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/invasive_ptr.h"
#include "src/indexes/text/lexer.h"
#include "src/indexes/text/posting.h"
#include "src/indexes/text/rax_target_mutex_pool.h"
#include "src/indexes/text/rax_wrapper.h"

struct sb_stemmer;

namespace valkey_search::indexes::text {

// Forward declarations for helper functions
static void FreePostingsCallback(void *target);
static void FreeStemParentsCallback(void *target);

// Helper functions implementation
inline void FreePostingsCallback(void *target) {
  if (target) {
    auto raw = static_cast<InvasivePtrRaw<Postings>>(target);
    InvasivePtr<Postings>::AdoptRaw(raw);
  }
}

inline void FreeStemParentsCallback(void *target) {
  if (target) {
    auto raw = static_cast<InvasivePtrRaw<StemParents>>(target);
    InvasivePtr<StemParents>::AdoptRaw(raw);
  }
}

// Helper to create target set function for Rax mutations
inline auto CreatePostingsTargetSetFn(const InvasivePtr<Postings> &target) {
  return [&target](void *old_val) -> void * {
    if (old_val) {
      InvasivePtr<Postings>::AdoptRaw(
          static_cast<InvasivePtrRaw<Postings>>(old_val));
    }
    if (!target) return nullptr;
    InvasivePtr<Postings> copy = target;
    return static_cast<void *>(std::move(copy).ReleaseRaw());
  };
}

// Inline capacity for stem variants extracted from stem tree
constexpr size_t kStemVariantsInlineCapacity = 20;

// Compile-time shard counts
constexpr size_t kSchemaTextIndexShards = 256;  // Schema-level: many shards

// token -> (PositionMap, suffix support)
using TokenPositions =
    absl::flat_hash_map<std::string, std::pair<PositionMap, bool>>;

class TextIndexSchema;

// FT.INFO counters for text info fields and memory pools
struct TextIndexMetadata {
  std::atomic<uint64_t> total_positions{0};
  std::atomic<uint64_t> num_unique_terms{0};
  std::atomic<uint64_t> total_term_frequency{0};

  // Memory pools for text index components
  MemoryPool posting_memory_pool_{0};
  MemoryPool radix_memory_pool_{0};
  MemoryPool text_index_memory_pool_{0};
};

// Template TextIndex for compile-time shard optimization
// Schema-level: TextIndex<256> (many shards)
// Per-key: TextIndex<1> (single shard, zero overhead)
template <size_t NumShards>
class TextIndex {
  //
  // Sharded radix trees: words with same first byte share a shard.
  // Each shard has independent tree + mutex, enabling true write parallelism.
  // Words with different first bytes can modify their shards concurrently
  // without blocking. Eliminates global text_index_mutex_ bottleneck.
  //

 public:
  // Constructor
  explicit TextIndex(bool suffix) : shards_{} {
    static_assert(NumShards > 1,
                  "Use PerKeyTextIndex for single-shard instances, not "
                  "TextIndex<1> (avoids 40-byte mutex overhead)");
    for (size_t i = 0; i < NumShards; ++i) {
      shards_[i].Initialize(FreePostingsCallback, suffix);
    }
  }

  // Get shard index for a word (based on first byte)
  size_t GetShardIndex(absl::string_view word) const;

  // Check if this is a sharded index
  bool IsSharded() const { return NumShards > 1; }

  // Accessors for specific shard
  Rax &GetPrefixShard(size_t shard_index);
  const Rax &GetPrefixShard(size_t shard_index) const;
  std::optional<std::reference_wrapper<Rax>> GetSuffixShard(size_t shard_index);
  std::optional<std::reference_wrapper<const Rax>> GetSuffixShard(
      size_t shard_index) const;

  // Applies target mutation to appropriate shard for |word|
  // If reverse_word is provided, also adds to suffix tree (if enabled)
  void MutateTarget(
      absl::string_view word, const InvasivePtr<Postings> &target,
      const std::optional<std::string> &reverse_word = std::nullopt,
      item_count_op op = NONE);

  // Get mutex for shard protecting this word
  absl::Mutex &GetShardMutex(absl::string_view word);
  const absl::Mutex &GetShardMutex(absl::string_view word) const;

  size_t GetNumShards() const { return NumShards; }

  // API compatibility: delegate to shard[0] - NEEDED for BuildTextIterator
  Rax &GetPrefix() { return GetPrefixShard(0); }
  const Rax &GetPrefix() const { return GetPrefixShard(0); }
  std::optional<std::reference_wrapper<Rax>> GetSuffix() {
    return GetSuffixShard(0);
  }
  std::optional<std::reference_wrapper<const Rax>> GetSuffix() const {
    return GetSuffixShard(0);
  }

  struct Shard {
    Rax prefix_tree;
    std::unique_ptr<Rax> suffix_tree;
    mutable absl::Mutex mutex;

    // Default constructor for std::array compatibility
    Shard() : prefix_tree(FreePostingsCallback), suffix_tree(nullptr) {}

    // Initialize method called after default construction
    void Initialize(void (*free_callback)(void *), bool with_suffix) {
      // Rax already constructed with FreePostingsCallback in default ctor
      // Just need to create suffix tree if requested
      if (with_suffix) {
        suffix_tree = std::make_unique<Rax>(free_callback);
      }
    }
  };

  // Compile-time sized array - eliminates per-instance overhead
  // For TextIndex<1>: just one Shard inline, no heap allocation
  // For TextIndex<256>: 256 Shards, but only one schema instance
  std::array<Shard, NumShards> shards_;
};

// Template member function implementations (must be in header)
template <size_t NumShards>
size_t TextIndex<NumShards>::GetShardIndex(absl::string_view word) const {
  // Use first byte for deterministic sharding
  // Modulo mathematically guarantees result < NumShards (no runtime check
  // needed)
  unsigned char first_byte =
      word.empty() ? 0 : static_cast<unsigned char>(word[0]);
  return first_byte % NumShards;
}

template <size_t NumShards>
Rax &TextIndex<NumShards>::GetPrefixShard(size_t shard_index) {
  return shards_[shard_index].prefix_tree;
}

template <size_t NumShards>
const Rax &TextIndex<NumShards>::GetPrefixShard(size_t shard_index) const {
  return shards_[shard_index].prefix_tree;
}

template <size_t NumShards>
std::optional<std::reference_wrapper<Rax>> TextIndex<NumShards>::GetSuffixShard(
    size_t shard_index) {
  if (!shards_[shard_index].suffix_tree) {
    return std::nullopt;
  }
  return std::ref(*shards_[shard_index].suffix_tree);
}

template <size_t NumShards>
std::optional<std::reference_wrapper<const Rax>>
TextIndex<NumShards>::GetSuffixShard(size_t shard_index) const {
  if (!shards_[shard_index].suffix_tree) {
    return std::nullopt;
  }
  return std::ref(*shards_[shard_index].suffix_tree);
}

template <size_t NumShards>
void TextIndex<NumShards>::MutateTarget(
    absl::string_view word, const InvasivePtr<Postings> &target,
    const std::optional<std::string> &reverse_word, item_count_op op) {
  auto CreateTargetSetFn = CreatePostingsTargetSetFn(target);

  size_t prefix_shard = GetShardIndex(word);
  size_t suffix_shard = prefix_shard;
  if (reverse_word.has_value() && shards_[0].suffix_tree) {
    suffix_shard = GetShardIndex(*reverse_word);
  }

  // Lock shards in sorted order to prevent deadlock
  if (reverse_word.has_value() && suffix_shard != prefix_shard) {
    size_t first = std::min(prefix_shard, suffix_shard);
    size_t second = std::max(prefix_shard, suffix_shard);
    absl::MutexLock lock1(&shards_[first].mutex);
    absl::MutexLock lock2(&shards_[second].mutex);
    shards_[prefix_shard].prefix_tree.MutateTarget(word, CreateTargetSetFn, op);
    shards_[suffix_shard].suffix_tree->MutateTarget(*reverse_word,
                                                    CreateTargetSetFn, op);
  } else {
    absl::MutexLock lock(&shards_[prefix_shard].mutex);
    shards_[prefix_shard].prefix_tree.MutateTarget(word, CreateTargetSetFn, op);
    if (reverse_word.has_value() && shards_[prefix_shard].suffix_tree) {
      shards_[prefix_shard].suffix_tree->MutateTarget(*reverse_word,
                                                      CreateTargetSetFn, op);
    }
  }
}

template <size_t NumShards>
absl::Mutex &TextIndex<NumShards>::GetShardMutex(absl::string_view word) {
  return shards_[GetShardIndex(word)].mutex;
}

template <size_t NumShards>
const absl::Mutex &TextIndex<NumShards>::GetShardMutex(
    absl::string_view word) const {
  return shards_[GetShardIndex(word)].mutex;
}

// Lightweight per-key text index: ~56 bytes (Rax + unique_ptr), no mutex,
// moveable Separate from template TextIndex<N> to avoid Shard mutex overhead
class PerKeyTextIndex {
 public:
  explicit PerKeyTextIndex(bool suffix)
      : prefix_tree_(FreePostingsCallback),
        suffix_tree_(suffix ? std::make_unique<Rax>(FreePostingsCallback)
                            : nullptr) {}

  Rax &GetPrefix() { return prefix_tree_; }
  const Rax &GetPrefix() const { return prefix_tree_; }

  std::optional<std::reference_wrapper<Rax>> GetSuffix() {
    if (!suffix_tree_) return std::nullopt;
    return std::ref(*suffix_tree_);
  }

  std::optional<std::reference_wrapper<const Rax>> GetSuffix() const {
    if (!suffix_tree_) return std::nullopt;
    return std::ref(*suffix_tree_);
  }

  void MutateTarget(
      absl::string_view word, const InvasivePtr<Postings> &target,
      const std::optional<std::string> &reverse_word = std::nullopt,
      item_count_op op = NONE) {
    auto CreateTargetSetFn = CreatePostingsTargetSetFn(target);

    prefix_tree_.MutateTarget(word, CreateTargetSetFn, op);
    if (suffix_tree_ && reverse_word.has_value()) {
      suffix_tree_->MutateTarget(*reverse_word, CreateTargetSetFn, op);
    }
  }

 private:
  Rax prefix_tree_;
  std::unique_ptr<Rax> suffix_tree_;
};

class TextIndexSchema {
 public:
  TextIndexSchema(data_model::Language language, const std::string &punctuation,
                  bool with_offsets, const std::vector<std::string> &stop_words,
                  uint32_t min_stem_size);

  absl::StatusOr<bool> StageAttributeData(const InternedStringPtr &key,
                                          absl::string_view data,
                                          size_t text_field_number, bool stem,
                                          bool suffix);
  void CommitKeyData(const InternedStringPtr &key);
  void DeleteKeyData(const InternedStringPtr &key);

  uint8_t AllocateTextFieldNumber() { return num_text_fields_++; }
  bool HasTextOffsets() const { return with_offsets_; }
  uint8_t GetNumTextFields() const { return num_text_fields_; }
  std::shared_ptr<TextIndex<kSchemaTextIndexShards>> GetTextIndex() const {
    return text_index_;
  }
  Lexer GetLexer() const { return lexer_; }

  // Access to metadata for memory pool usage
  TextIndexMetadata &GetMetadata() { return metadata_; }

  // Access stem tree for word expansion during search
  const Rax &GetStemTree() const { return stem_tree_; }

  // Get stem root and all stem parents for a search term
  std::string GetAllStemVariants(
      absl::string_view search_term,
      absl::InlinedVector<absl::string_view, kStemVariantsInlineCapacity>
          &words_to_search,
      uint64_t stem_enabled_mask, bool lock_needed);

  // Get the minimum stem size across all fields
  uint32_t GetMinStemSize() const { return min_stem_size_; }

  // Schema-level stem field mask (mirrored from
  // IndexSchema::stem_text_field_mask_)
  void SetStemTextFieldMask(uint64_t mask) { stem_text_field_mask_ = mask; }
  uint64_t GetStemTextFieldMask() const { return stem_text_field_mask_; }

  // Enable suffix trie.
  void EnableSuffix();

 private:
  uint8_t num_text_fields_ = 0;

  // Each schema instance has its own metadata with memory pools
  TextIndexMetadata metadata_;

  //
  // This is the main index of all Text fields in this index schema
  // Sharded by first byte for parallel writes (per-shard locks in TextIndex)
  // Uses TextIndex<256> for many shards
  //
  std::shared_ptr<TextIndex<kSchemaTextIndexShards>> text_index_;

  //
  // Stem tree: maps stem roots to their parent words
  // Example: "happi" → {"happy", "happiness", "happily"}
  //
  Rax stem_tree_;

  // Guards structural changes to stem_tree_.
  mutable absl::Mutex stem_tree_mutex_;

  // Per-word bucket locks for concurrent Rax target updates.
  RaxTargetMutexPool rax_target_mutex_pool_;

  //
  // To support the Delete record and the post-filtering case, there is a
  // separate table of postings that are indexed by Key.
  // Uses lightweight PerKeyTextIndex (no mutex, same as current merged ~56
  // bytes)
  //
  // This object must also ensure that updates of this object are multi-thread
  // safe.
  //
  absl::node_hash_map<Key, PerKeyTextIndex> per_key_text_indexes_;

  // Prevent concurrent mutations to per-key text index map
  std::mutex per_key_text_indexes_mutex_;

  Lexer lexer_;

  // Key updates are fanned out to each attribute's IndexBase object. Since text
  // indexing operates at the schema-level, any new text data to insert for a
  // key is accumulated across all attributes here and committed into the text
  // index structures at the end for efficiency.
  absl::node_hash_map<Key, TokenPositions> in_progress_key_updates_;

  // Prevent concurrent mutations to in-progress key updates map
  std::mutex in_progress_key_updates_mutex_;

  // Temporary storage for stem mappings during indexing
  // Maps key -> (stemmed_word -> list of original words that stem to it)
  absl::node_hash_map<Key, InProgressStemMap> in_progress_stem_mappings_;

  // Prevent concurrent mutations to in-progress stem mappings map
  std::mutex in_progress_stem_mappings_mutex_;

  // Whether to store position offsets for phrase queries
  bool with_offsets_ = false;

  // True if any text attributes of the schema have suffix search enabled.
  bool with_suffix_trie_ = false;

  // Minimum word length for stemming (schema-level configuration)
  uint32_t min_stem_size_;

  // Schema-level stem field mask (mirrored from
  // IndexSchema::stem_text_field_mask_)
  uint64_t stem_text_field_mask_ = 0;

  // We track subtree items if the index has a HNSW field to enable filtering
  // planning decisions with prefix/suffix text filtering.
  bool track_subtree_item_counts_ = false;

 public:
  // FT.INFO stats for text index
  uint64_t GetTotalPositions() const;
  uint64_t GetNumUniqueTerms() const;
  uint64_t GetTotalTermFrequency() const;
  // TODO: Implement the following APIs when we want granular memory metrics for
  // text index components
  uint64_t GetPostingsMemoryUsage() const;
  uint64_t GetRadixTreeMemoryUsage() const;
  uint64_t GetPositionMemoryUsage() const;
  uint64_t GetTotalTextIndexMemoryUsage() const;

  // Thread-safe accessor for per-key text indexes. Executes the provided
  // function while holding the mutex lock, ensuring safe concurrent access.
  template <typename Func>
  auto WithPerKeyTextIndexes(Func &&func)
      -> decltype(func(per_key_text_indexes_)) {
    std::lock_guard<std::mutex> guard(per_key_text_indexes_mutex_);
    return func(per_key_text_indexes_);
  }

  // Direct accessor for per-key text indexes.
  // Assumes that lock is already acquired earlier.
  const absl::node_hash_map<Key, PerKeyTextIndex> &GetPerKeyTextIndexes()
      const {
    return per_key_text_indexes_;
  }

  // Total number of keys with text fields indexed in this schema.
  // No locking needed because only called from read phase.
  size_t GetTrackedKeyCount() const { return per_key_text_indexes_.size(); }

  // Helper function to lookup text index for a key
  static const PerKeyTextIndex *LookupTextIndex(
      const absl::node_hash_map<Key, PerKeyTextIndex> &per_key_indexes,
      const Key &key) {
    if (!key) {
      CHECK(false) << "Invalid null key passed to LookupTextIndex";
      return nullptr;
    }
    if (auto it = per_key_indexes.find(key); it != per_key_indexes.end()) {
      return &it->second;
    }
    // Key not found in text indexes - this is normal for keys without text data
    return nullptr;
  }
  // Locking-enabled version of GetTrackedKeyCount.
  size_t GetTrackedKeyCount(bool lock) {
    std::optional<std::lock_guard<std::mutex>> per_key_guard;
    if (lock) per_key_guard.emplace(per_key_text_indexes_mutex_);
    return GetTrackedKeyCount();
  }

  // Lookup per-key text index for a key
  const PerKeyTextIndex *GetPerKeyTextIndex(const Key &key, bool lock);

  // TODO: remove this because we'll always track the counts once it's optimized
  bool TrackSubtreeItemsCountEnabled() const {
    return track_subtree_item_counts_;
  }
};

}  // namespace valkey_search::indexes::text

#endif
