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
#include <optional>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/function_ref.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/posting.h"
#include "src/indexes/text/radix_tree.h"

struct sb_stemmer;

namespace valkey_search::indexes::text {

using Key = valkey_search::InternedStringPtr;
using Position = uint32_t;
using PunctuationBitmap = std::bitset<256>;

struct TextIndex {
  TextIndex() = default;
  ~TextIndex() = default;
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
  TextIndexSchema(const data_model::IndexSchema& index_schema_proto);
  ~TextIndexSchema();

  uint8_t AllocateTextFieldNumber() { return num_text_fields_++; }

  uint8_t GetNumTextFields() const { return num_text_fields_; }
  std::shared_ptr<TextIndex> GetTextIndex() const { return text_index_; }
  const std::string GetPunctuationString() const { return punct_str_; }
  const PunctuationBitmap& GetPunctuationBitmap() const {
    return punct_bitmap_;
  }
  const absl::flat_hash_set<std::string>& GetStopWordsSet() const {
    return stop_words_set_;
  }
  data_model::Language GetLanguage() const { return language_; }
  sb_stemmer* GetStemmer() const { return stemmer_; };
  bool GetWithOffsets() const { return with_offsets_; }

 private:
  uint8_t num_text_fields_ = 0;

  //
  // This is the main index of all Text fields in this index schema
  //
  std::shared_ptr<TextIndex> text_index_ = std::make_shared<TextIndex>();

  //
  // To support the Delete record and the post-filtering case, there is a
  // separate table of postings that are indexed by Key.
  //
  // This object must also ensure that updates of this object are multi-thread
  // safe.
  //
  absl::flat_hash_map<Key, TextIndex> by_key_;

  // Punctuation string
  std::string punct_str_;

  // Punctuation bitmap
  PunctuationBitmap punct_bitmap_;

  // Stop words set for filtering during tokenization
  absl::flat_hash_set<std::string> stop_words_set_;

  // Language needed for stemmer creation
  data_model::Language language_ = data_model::LANGUAGE_UNSPECIFIED;

  // Stemmer reused across all operations for this index
  mutable sb_stemmer* stemmer_ = nullptr;

  // Whether to store position offsets for phrase queries
  bool with_offsets_ = false;

  const char* GetLanguageString() const;
};

}  // namespace valkey_search::indexes::text

#endif
