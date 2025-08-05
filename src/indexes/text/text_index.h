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

#include "absl/container/flat_hash_map.h"
#include "src/indexes/text/radix_tree.h"
#include "src/indexes/text/posting.h"
#include "src/index_schema.pb.h"

struct sb_stemmer;

namespace valkey_search::indexes::text {

namespace {

bool IsWhitespace(char c) {
  return std::isspace(static_cast<unsigned char>(c)) || std::iscntrl(static_cast<unsigned char>(c));
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
        stemmer_(nullptr),
        language_(language),
        punctuation_(punctuation),
        with_offsets_(with_offsets),
        stop_words_(stop_words) {
    BuildOptimizedStructures();
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

  // Raw configuration (for RDB persistence)
  data_model::Language language_ = data_model::LANGUAGE_UNSPECIFIED;
  std::string punctuation_;
  bool with_offsets_ = true;
  std::vector<std::string> stop_words_;

  // Optimized structures (shared across all text fields)
  PunctuationBitmap punct_bitmap_;

  // Stemmer reused across all operations for this index
  mutable sb_stemmer* stemmer_;

  uint8_t AllocateTextFieldNumber() {
    return num_text_fields_++;
  }

  sb_stemmer* GetStemmer() const;

  std::string GetPunctuation() const {
    return punctuation_;
  }

  data_model::Language GetLanguage() const {
    return language_;
  }

  bool GetWithOffsets() const {
    return with_offsets_;
  }

  const PunctuationBitmap& GetPunctuationBitmap() const {
    return punct_bitmap_;
  }

  std::string GetLanguageString() const {
    switch (language_) {
      case data_model::LANGUAGE_ENGLISH:
        return "english";
      default:
        return "english";
    }
  }

 private:
  void BuildOptimizedStructures() {
    punct_bitmap_.reset();

    // Add all whitespace characters as word separators (RFC requirement)
    for (int i = 0; i < 256; ++i) {
      if (IsWhitespace(static_cast<char>(i))) {
        punct_bitmap_.set(i);
      }
    }

    // Add user-specified punctuation characters
    for (char c : punctuation_) {
      if (static_cast<unsigned char>(c) < 256) {  // Only ASCII for now
        punct_bitmap_.set(static_cast<unsigned char>(c));
      }
    }

    // TODO: Build hash set for stop words
  }
};

}  // namespace valkey_search::indexes::text

#endif
