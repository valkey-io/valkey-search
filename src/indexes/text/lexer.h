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
#include <unordered_map>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

struct sb_stemmer;

namespace valkey_search::indexes::text {

struct Lexer {
  Lexer(const char*);
  ~Lexer();

  absl::StatusOr<std::vector<std::string>> Tokenize(
      absl::string_view text, const std::bitset<256>& punct_bitmap,
      bool stemming_enabled, uint32_t min_stem_size,
      const absl::flat_hash_set<std::string>& stop_words_set) const;

  // Punctuation checking API
  static bool IsPunctuation(char c, const std::bitset<256>& punct_bitmap) {
    return punct_bitmap[static_cast<unsigned char>(c)];
  }

  // Stop word checking API (expects lowercase input)
  static bool IsStopWord(
      const std::string& lowercase_word,
      const absl::flat_hash_set<std::string>& stop_words_set) {
    return stop_words_set.contains(lowercase_word);
  }

 private:
  const char* language_;

  class ThreadLocalStemmerCache {
   private:
    std::unordered_map<std::string, sb_stemmer*> cache_;

   public:
    ~ThreadLocalStemmerCache();
    sb_stemmer* GetOrCreateStemmer(const std::string& language);
  };

  // Thread-local stemmer cache - each ingestion worker thread gets a stemmer
  // for each language it tokenizes at least once
  static thread_local ThreadLocalStemmerCache stemmer_cache_;

  std::string StemWord(const std::string& word, bool stemming_enabled,
                       uint32_t min_stem_size, sb_stemmer* stemmer) const;

  // UTF-8 processing helpers
  bool IsValidUtf8(absl::string_view text) const;
};

}  // namespace valkey_search::indexes::text

#endif
