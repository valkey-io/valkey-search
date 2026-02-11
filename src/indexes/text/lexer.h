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

#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"

struct sb_stemmer;

namespace valkey_search::indexes::text {

struct Lexer {
  Lexer(data_model::Language language, const std::string& punctuation,
        const std::vector<std::string>& stop_words);
  ~Lexer() = default;

  absl::StatusOr<std::vector<std::string>> Tokenize(
      absl::string_view text, bool stemming_enabled, uint32_t min_stem_size,
      absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>>*
          stem_mappings = nullptr) const;

  std::string StemWord(const std::string& word, sb_stemmer* stemmer,
                       uint32_t min_stem_size = 0) const;
  bool IsPunctuation(char c) const {
    return punct_bitmap_[static_cast<unsigned char>(c)];
  }

  bool IsStopWord(const std::string& lowercase_word) const {
    return stop_words_set_.contains(lowercase_word);
  }
  sb_stemmer* GetStemmer() const;

 private:
  data_model::Language language_;
  std::bitset<256> punct_bitmap_;
  absl::flat_hash_set<std::string> stop_words_set_;

  // UTF-8 processing helpers
  bool IsValidUtf8(absl::string_view text) const;
};

}  // namespace valkey_search::indexes::text

#endif
