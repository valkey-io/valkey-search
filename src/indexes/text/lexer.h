/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_LEXER_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_LEXER_H_

/*

STATELESS LEXER DESIGN

The Lexer is a stateless processor that takes configuration parameters
and produces tokenized output. Configuration is stored in TextIndexSchema
and Text classes, then passed to lexer methods as parameters.

Tokenization Pipeline:
1. Split text on punctuation characters (configurable)
2. Convert to lowercase
3. Stop word removal (filter out common words)
4. Apply stemming based on language and field settings

*/

#include <bitset>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"

struct sb_stemmer;

namespace valkey_search::indexes::text {

// Inline capacity for per-document stem mapping
constexpr size_t kInProgressStemVariantsInlineCapacity = 4;

// Per-document stem mappings: stemmed_word -> list of original words that stem
// to it
using InProgressStemMap = absl::flat_hash_map<
    std::string,
    absl::InlinedVector<std::string, kInProgressStemVariantsInlineCapacity>>;

struct Lexer {
  Lexer(data_model::Language language, const std::string& punctuation,
        const std::vector<std::string>& stop_words);
  ~Lexer() = default;

  absl::StatusOr<std::vector<std::string>> Tokenize(
      absl::string_view text, bool stemming_enabled, uint32_t min_stem_size,
      InProgressStemMap* stem_mappings = nullptr) const;

  template <typename Callback>
  absl::Status Tokenize(
      absl::string_view text, bool stemming_enabled, uint32_t min_stem_size,
      InProgressStemMap* stem_mappings, Callback&& callback) const;

  bool IsPunctuation(char c) const {
    return punct_bitmap_[static_cast<unsigned char>(c)];
  }

  bool IsStopWord(absl::string_view lowercase_word) const {
    return stop_words_set_.contains(lowercase_word);
  }
  sb_stemmer* GetStemmer() const;
  void NormalizeLowerCaseInPlace(std::string& str) const;
  void StemWordInPlace(std::string& word, sb_stemmer* stemmer,
                       uint32_t min_stem_size = 0) const;
  void UpdateStemMap(absl::string_view original_word, sb_stemmer* stemmer,
                     uint32_t min_stem_size,
                     InProgressStemMap& stem_mappings) const;

 private:
  data_model::Language language_;
  std::bitset<256> punct_bitmap_;
  absl::flat_hash_set<std::string> stop_words_set_;

  // UTF-8 processing helpers
  bool IsValidUtf8(absl::string_view text) const;
  // Common stemming logic
  std::string_view DoStemming(absl::string_view word, sb_stemmer* stemmer,
                              uint32_t min_stem_size) const;
};

template <typename Callback>
inline absl::Status Lexer::Tokenize(
    absl::string_view text, bool stemming_enabled, uint32_t min_stem_size,
    InProgressStemMap* stem_mappings, Callback&& callback) const {
  if (stemming_enabled) {
    CHECK(stem_mappings) << "stem_mappings must not be null";
  }
  if (!IsValidUtf8(text)) {
    return absl::InvalidArgumentError("Invalid UTF-8");
  }

  sb_stemmer* stemmer = stemming_enabled ? GetStemmer() : nullptr;
  std::string word;
  word.reserve(64);
  size_t pos = 0;
  uint32_t token_index = 0;
  while (pos < text.size()) {
    while (pos < text.size() && IsPunctuation(text[pos])) {
      if (text[pos] == '\\' && pos + 1 < text.size()) {
        break;
      }
      pos++;
    }

    word.clear();

    while (pos < text.size()) {
      char ch = text[pos];
      if (ch == '\\' && pos + 1 < text.size()) {
        char next_ch = text[pos + 1];
        pos++;
        if (next_ch == '\\' || IsPunctuation(next_ch)) {
          word.push_back(text[pos++]);
        } else {
          if (IsPunctuation('\\')) {
            break;
          } else {
            word.push_back(text[pos++]);
          }
        }
      } else if (IsPunctuation(ch)) {
        break;
      } else {
        word.push_back(ch);
        pos++;
      }
    }

    if (!word.empty()) {
      NormalizeLowerCaseInPlace(word);

      if (IsStopWord(word)) {
        continue;
      }

      if (stemming_enabled) {
        UpdateStemMap(word, stemmer, min_stem_size, *stem_mappings);
      }
      callback(word, token_index++);
      word.clear();
    }
  }

  return absl::OkStatus();
}

}  // namespace valkey_search::indexes::text

#endif
