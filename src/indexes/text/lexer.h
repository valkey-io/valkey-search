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
#include "src/index_schema.pb.h"

struct sb_stemmer;

namespace valkey_search::indexes::text {

struct Lexer {
  Lexer(data_model::Language language, const std::string& punctuation,
        const std::vector<std::string>& stop_words);
  ~Lexer() = default;

  absl::StatusOr<std::vector<std::string>> Tokenize(
      absl::string_view text, bool stemming_enabled,
      uint32_t min_stem_size) const;

 private:
  data_model::Language language_;
  std::bitset<256> punct_bitmap_;
  absl::flat_hash_set<std::string> stop_words_set_;

  // Thread-local stemmer cache. Stemmers will be owned by threads and shared
  // amongst the Lexer instances, since a stemmer instance is not thread-safe.
  // Each ingestion worker thread gets a stemmer for each language it tokenizes
  // at least once.
  class ThreadLocalStemmerCache {
   private:
    std::unordered_map<data_model::Language, sb_stemmer*> cache_;

   public:
    ~ThreadLocalStemmerCache();
    sb_stemmer* GetOrCreateStemmer(data_model::Language language);
  };
  static thread_local ThreadLocalStemmerCache stemmer_cache_;

  std::string StemWord(const std::string& word, bool stemming_enabled,
                       uint32_t min_stem_size, sb_stemmer* stemmer) const;

  // Private helper methods operating on instance data
  bool IsPunctuation(char c) const {
    return punct_bitmap_[static_cast<unsigned char>(c)];
  }

  bool IsStopWord(const std::string& lowercase_word) const {
    return stop_words_set_.contains(lowercase_word);
  }

  // UTF-8 processing helpers
  bool IsValidUtf8(absl::string_view text) const;
};

}  // namespace valkey_search::indexes::text

#endif
