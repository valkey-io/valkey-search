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
3. TODO: stop word removal (not implemented in this phase)
4. Apply stemming based on language and field settings

*/

#include <bitset>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

struct sb_stemmer;

namespace valkey_search::indexes::text {

struct Lexer {
  absl::StatusOr<std::vector<std::string>> Tokenize(
      absl::string_view text, const std::bitset<256>& punct_bitmap,
      sb_stemmer* stemmer, bool stemming_enabled, uint32_t min_stem_size) const;

  // Punctuation checking API
  static bool IsPunctuation(char c, const std::bitset<256>& punct_bitmap) {
    return punct_bitmap[static_cast<unsigned char>(c)];
  }

 private:
  std::string StemWord(const std::string& word, sb_stemmer* stemmer,
                       bool stemming_enabled, uint32_t min_stem_size) const;

  // UTF-8 processing helpers
  bool IsValidUtf8(absl::string_view text) const;
};

}  // namespace valkey_search::indexes::text

#endif
