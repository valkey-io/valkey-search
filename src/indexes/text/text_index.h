/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_INDEX_H_
#define VALKEY_SEARCH_INDEXES_TEXT_INDEX_H_

#include <bitset>
#include <memory>
#include <optional>
#include <cctype>
#include <unordered_set>

#include "absl/container/flat_hash_map.h"
#include "src/indexes/text/radix_tree.h"
#include "src/indexes/text/posting.h"
#include "src/index_schema.pb.h"

struct sb_stemmer;

namespace valkey_search::indexes::text {

namespace {

bool IsWhitespace(unsigned char c) {
  return std::isspace(c) || std::iscntrl(c);
}

}  // namespace

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

  // Prefix tree
  RadixTree<std::shared_ptr<Postings>, false> prefix_;
  
  // Suffix tree
  std::optional<RadixTree<std::shared_ptr<Postings>, true>> suffix_;
};

struct TextIndexSchema {
  TextIndexSchema(data_model::Language language,
                  const std::string& punctuation,
                  bool with_offsets,
                  const std::vector<std::string>& stop_words)
      : num_text_fields_(0), 
        text_index_(std::make_shared<TextIndex>()),
        language_(language),
        with_offsets_(with_offsets) {
    BuildPunctuationBitmap(punctuation);
    BuildStopWordsSet(stop_words);
  }

  ~TextIndexSchema();

  uint8_t num_text_fields_;
  //
  // This is the main index of all Text fields in this index schema
  //
  std::shared_ptr<TextIndex> text_index_;
  //
  // To support the Delete record and the post-filtering case, there is a
  // separate table of postings that are indexed by Key.
  //
  // This object must also ensure that updates of this object are multi-thread
  // safe.
  //
  absl::flat_hash_map<Key, TextIndex> by_key_;

  // Optimized structures (shared across all text fields)
  PunctuationBitmap punct_bitmap_;

  // Stop words set for filtering during tokenization
  std::unordered_set<std::string> stop_words_set_;

  // Language needed for stemmer creation
  data_model::Language language_ = data_model::LANGUAGE_UNSPECIFIED;
  
  // Stemmer reused across all operations for this index
  mutable sb_stemmer* stemmer_ = nullptr;

  // Whether to store position offsets for phrase queries
  bool with_offsets_ = false;

  uint8_t AllocateTextFieldNumber() {
    return num_text_fields_++;
  }

  sb_stemmer* GetStemmer() const;

  const PunctuationBitmap& GetPunctuationBitmap() const {
    return punct_bitmap_;
  }

  bool GetWithOffsets() const {
    return with_offsets_;
  }

  std::string GetLanguageString() const {
    switch (language_) {
      case data_model::LANGUAGE_ENGLISH:
        return "english";
      default:
        return "english";
    }
  }

  const std::unordered_set<std::string>& GetStopWordsSet() const {
    return stop_words_set_;
  }

 private:
  void BuildPunctuationBitmap(const std::string& punctuation) {
    punct_bitmap_.reset();

    // Add all whitespace characters as word separators (RFC requirement)
    for (int i = 0; i < 256; ++i) {
      if (IsWhitespace(static_cast<unsigned char>(i))) {
        punct_bitmap_.set(i);
      }
    }

    // Add user-specified punctuation characters
    for (char c : punctuation) {
      punct_bitmap_.set(static_cast<unsigned char>(c));
    }
  }

  void BuildStopWordsSet(const std::vector<std::string>& stop_words);
};

}  // namespace valkey_search::indexes::text

#endif
