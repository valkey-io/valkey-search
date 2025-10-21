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

thread_local Lexer::ThreadLocalStemmerCache Lexer::stemmer_cache_;

// Clean up thread-local stemmers when the thread exits
Lexer::ThreadLocalStemmerCache::~ThreadLocalStemmerCache() {
  for (auto& [lang, stemmer] : cache_) {
    if (stemmer) {
      sb_stemmer_delete(stemmer);
    }
  }
}

sb_stemmer* Lexer::ThreadLocalStemmerCache::GetOrCreateStemmer(
    const std::string& language) {
  auto it = cache_.find(language);
  if (it == cache_.end()) {
    sb_stemmer* stemmer = sb_stemmer_new(language.c_str(), "UTF_8");
    cache_[language] = stemmer;
    return stemmer;
  }
  return it->second;
}

Lexer::Lexer(const char* language) : language_(language) {}

Lexer::~Lexer() {}

absl::StatusOr<std::vector<std::string>> Lexer::Tokenize(
    absl::string_view text, const std::bitset<256>& punct_bitmap,
    bool stemming_enabled, uint32_t min_stem_size,
    const absl::flat_hash_set<std::string>& stop_words_set) const {
  if (!IsValidUtf8(text)) {
    return absl::InvalidArgumentError("Invalid UTF-8");
  }

  // Get or create the thread-local stemmer for this lexer's language
  sb_stemmer* stemmer =
      stemming_enabled ? stemmer_cache_.GetOrCreateStemmer(language_) : nullptr;
  std::vector<std::string> tokens;
  size_t pos = 0;
  while (pos < text.size()) {
    while (pos < text.size() && Lexer::IsPunctuation(text[pos], punct_bitmap)) {
      pos++;
    }

    size_t word_start = pos;
    while (pos < text.size() &&
           !Lexer::IsPunctuation(text[pos], punct_bitmap)) {
      pos++;
    }

    if (pos > word_start) {
      absl::string_view word_view(text.data() + word_start, pos - word_start);

      std::string word = absl::AsciiStrToLower(word_view);

      if (Lexer::IsStopWord(word, stop_words_set)) {
        continue;  // Skip stop words
      }

      word = StemWord(word, stemming_enabled, min_stem_size, stemmer);

      tokens.push_back(std::move(word));
    }
  }

  return tokens;
}

std::string Lexer::StemWord(const std::string& word, bool stemming_enabled,
                            uint32_t min_stem_size, sb_stemmer* stemmer) const {
  if (word.empty() || !stemming_enabled || word.length() < min_stem_size) {
    return word;
  }

  DCHECK(stemmer) << "Stemmer is null";

  // Use the passed stemmer (already retrieved from cache in Tokenize)
  const sb_symbol* stemmed = sb_stemmer_stem(
      stemmer, reinterpret_cast<const sb_symbol*>(word.c_str()), word.length());

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
  return scanner.GetInvalidUtf8Count() == 0 &&
         scanner.GetPosition() == text.size();
}
}  // namespace valkey_search::indexes::text
