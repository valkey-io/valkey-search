/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_INDEX_H_
#define VALKEY_SEARCH_INDEXES_TEXT_INDEX_H_

#include <atomic>
#include <bitset>
#include <cctype>
#include <memory>
#include <mutex>
#include <optional>

#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_map.h"
#include "absl/functional/function_ref.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/invasive_ptr.h"
#include "src/indexes/text/lexer.h"
#include "src/indexes/text/posting.h"
#include "src/indexes/text/radix_tree.h"

struct sb_stemmer;

namespace valkey_search::indexes::text {

// token -> (PositionMap, suffix support)
using TokenPositions =
    absl::flat_hash_map<std::string, std::pair<PositionMap, bool>>;

// Stem tree target: maps stem root to set of parent words that stem to it
// Example: "happi" → {"happy", "happiness", "happily"}
// Using optional so RadixTree can check emptiness via boolean conversion
using StemParents = std::optional<absl::flat_hash_set<std::string>>;

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

class TextIndex {
  //
  // The main query data structure maps Words into Postings objects. This
  // is always done with a prefix tree. Optionally, a suffix tree can also be
  // maintained. But in any case for the same word the two trees must point to
  // the same Postings object, which is owned by this pair of trees. Plus,
  // updates to these two trees need to be atomic when viewed externally. The
  // locking provided by the RadixTree object is NOT quite sufficient to
  // guarantee that the two trees are always in lock step. thus this object
  // becomes responsible for cross-tree locking issues. Multiple locking
  // strategies are possible. TBD (a shared-ed word lock table should work well)
  //

 public:
  explicit TextIndex(bool suffix);
  RadixTree<InvasivePtr<Postings>> &GetPrefix();
  const RadixTree<InvasivePtr<Postings>> &GetPrefix() const;
  std::optional<std::reference_wrapper<RadixTree<InvasivePtr<Postings>>>>
  GetSuffix();
  std::optional<std::reference_wrapper<const RadixTree<InvasivePtr<Postings>>>>
  GetSuffix() const;

 private:
  RadixTree<InvasivePtr<Postings>> prefix_tree_;
  std::unique_ptr<RadixTree<InvasivePtr<Postings>>> suffix_tree_;
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
  std::shared_ptr<TextIndex> GetTextIndex() const { return text_index_; }
  Lexer GetLexer() const { return lexer_; }

  // Access to metadata for memory pool usage
  TextIndexMetadata &GetMetadata() { return metadata_; }

  // Access stem tree for word expansion during search
  const RadixTree<StemParents> &GetStemTree() const { return stem_tree_; }

  // Get all stem variants for a search term (including parent words from stem
  // tree)
  std::string GetAllStemVariants(const std::string &search_term,
                          std::vector<std::string> &words_to_search);

  // Get the mask of fields that have stemming enabled
  uint64_t GetStemmingFieldMask() const { return stemming_enabled_fields_; }

  // Enable suffix trie.
  void EnableSuffix() {
    with_suffix_trie_ = true;
    text_index_ = std::make_shared<TextIndex>(true);
  }

 private:
  uint8_t num_text_fields_ = 0;

  // Each schema instance has its own metadata with memory pools
  TextIndexMetadata metadata_;

  //
  // This is the main index of all Text fields in this index schema
  //
  std::shared_ptr<TextIndex> text_index_ = std::make_shared<TextIndex>(false);

  // Prevent concurrent mutations to schema-level text index
  // TODO: develop a finer-grained TextIndex locking scheme
  std::mutex text_index_mutex_;

  //
  // Stem tree: maps stem roots to their parent words
  // Example: "happi" → {"happy", "happiness", "happily"}
  //
  RadixTree<StemParents> stem_tree_;

  // Prevent concurrent mutations to stem tree
  std::mutex stem_tree_mutex_;

  //
  // To support the Delete record and the post-filtering case, there is a
  // separate table of postings that are indexed by Key.
  //
  // This object must also ensure that updates of this object are multi-thread
  // safe.
  //
  absl::node_hash_map<Key, TextIndex> per_key_text_indexes_;

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
  // Maps key -> (stemmed_word -> set of original words that stem to it)
  absl::node_hash_map<
      Key, absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>>>
      in_progress_stem_mappings_;

  // Prevent concurrent mutations to in-progress stem mappings map
  std::mutex in_progress_stem_mappings_mutex_;

  // Whether to store position offsets for phrase queries
  bool with_offsets_ = false;

  // True if any text attributes of the schema have suffix search enabled.
  bool with_suffix_trie_ = false;

  // Minimum word length for stemming (schema-level configuration)
  uint32_t min_stem_size_;

  // Tracks which field numbers have stemming enabled (bit mask)
  uint64_t stemming_enabled_fields_ = 0;

 public:
  // FT.INFO memory stats for text index
  uint64_t GetTotalPositions() const;
  uint64_t GetNumUniqueTerms() const;
  uint64_t GetTotalTermFrequency() const;
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
  const absl::node_hash_map<Key, TextIndex> &GetPerKeyTextIndexes() const {
    return per_key_text_indexes_;
  }

  // Helper function to lookup text index for a key
  static const TextIndex *LookupTextIndex(
      const absl::node_hash_map<Key, TextIndex> &per_key_indexes,
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
};

}  // namespace valkey_search::indexes::text

#endif
