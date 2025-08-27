/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/indexes/text/lexer.h"

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "libstemmer.h"
#include "src/utils/scanner.h"

namespace valkey_search::indexes::text {

absl::StatusOr<std::vector<std::string>> Lexer::Tokenize(
    absl::string_view text,
    const std::bitset<256>& punct_bitmap,
    sb_stemmer* stemmer,
    bool stemming_enabled,
    uint32_t min_stem_size,
    const std::unordered_set<std::string>& stop_words_set) const {

  if (!IsValidUtf8(text)) {
    return absl::InvalidArgumentError("Invalid UTF-8");
  }

  std::vector<std::string> tokens;

  size_t pos = 0;
  while (pos < text.size()) {
    while (pos < text.size() && Lexer::IsPunctuation(text[pos], punct_bitmap)) {
      pos++;
    }

    size_t word_start = pos;
    while (pos < text.size() && !Lexer::IsPunctuation(text[pos], punct_bitmap)) {
      pos++;
    }

    if (pos > word_start) {
      absl::string_view word_view(text.data() + word_start, pos - word_start);

      std::string word = absl::AsciiStrToLower(word_view);

      if (Lexer::IsStopWord(word, stop_words_set)) {
        continue;  // Skip stop words
      }

      word = StemWord(word, stemmer, stemming_enabled, min_stem_size);

      tokens.push_back(std::move(word));
    }
  }
  
  return tokens;
}

std::string Lexer::StemWord(
    const std::string& word,
    sb_stemmer* stemmer,
    bool stemming_enabled,
    uint32_t min_stem_size) const {
  
  if (word.empty() || !stemming_enabled || word.length() < min_stem_size) {
    return word;
  }
  
  CHECK(stemmer) << "Stemmer not initialized";

  const sb_symbol* stemmed = sb_stemmer_stem(
    stemmer,
    reinterpret_cast<const sb_symbol*>(word.c_str()),
    word.length()
  );
  
  DCHECK(stemmed) << "Stemming failed for word: " + word;
  
  int stemmed_length = sb_stemmer_length(stemmer);
  return std::string(reinterpret_cast<const char*>(stemmed), stemmed_length);
}

// UTF-8 validation using Scanner
bool Lexer::IsValidUtf8(absl::string_view text) const {
  valkey_search::utils::Scanner scanner(text);

  // Try to parse each UTF-8 character - Scanner counts invalid sequences
  while (scanner.GetPosition() < text.size()) {
    valkey_search::utils::Scanner::Char ch = scanner.NextUtf8();
    if (ch == valkey_search::utils::Scanner::kEOF) {
      break;
    }
  }

  // If any invalid UTF-8 sequences were encountered, text is invalid
  return scanner.GetInvalidUtf8Count() == 0 && scanner.GetPosition() == text.size();
}
}  // namespace valkey_search::indexes::text
