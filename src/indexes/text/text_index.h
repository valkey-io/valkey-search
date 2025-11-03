/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_INDEX_H_
#define VALKEY_SEARCH_INDEXES_TEXT_INDEX_H_

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
#include "src/indexes/text/lexer.h"
#include "src/indexes/text/posting.h"
#include "src/indexes/text/radix_tree.h"

struct sb_stemmer;

namespace valkey_search::indexes::text {

// token -> (PositionMap, suffix support)
using TokenPositions =
    absl::flat_hash_map<std::string, std::pair<PositionMap, bool>>;

struct TextIndex {
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
  RadixTree<std::shared_ptr<Postings>, false> prefix_;
  std::optional<RadixTree<std::shared_ptr<Postings>, true>> suffix_;
};

class TextIndexSchema {
 public:
  TextIndexSchema(data_model::Language language, const std::string& punctuation,
                  bool with_offsets,
                  const std::vector<std::string>& stop_words);

  absl::StatusOr<bool> StageAttributeData(const InternedStringPtr& key,
                                          absl::string_view data,
                                          size_t text_field_number, bool stem,
                                          size_t min_stem_size, bool suffix);
  void CommitKeyData(const InternedStringPtr& key);
  void DeleteKeyData(const InternedStringPtr& key);

  uint8_t AllocateTextFieldNumber() { return num_text_fields_++; }

  uint8_t GetNumTextFields() const { return num_text_fields_; }
  std::shared_ptr<TextIndex> GetTextIndex() const { return text_index_; }

 private:
  uint8_t num_text_fields_ = 0;

  //
  // This is the main index of all Text fields in this index schema
  //
  std::shared_ptr<TextIndex> text_index_ = std::make_shared<TextIndex>();

  // Prevent concurrent mutations to schema-level text index
  // TODO: develop a finer-grained TextIndex locking scheme
  std::mutex text_index_mutex_;

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

  // Whether to store position offsets for phrase queries
  bool with_offsets_ = false;
};

}  // namespace valkey_search::indexes::text

#endif
