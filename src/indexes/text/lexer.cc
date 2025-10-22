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

using PunctuationBitmap = std::bitset<256>;

namespace {

bool IsWhitespace(unsigned char c) {
  return std::isspace(c) || std::iscntrl(c);
}

PunctuationBitmap BuildPunctuationBitmap(const std::string& punctuation) {
  PunctuationBitmap bitmap;
  bitmap.reset();

  for (int i = 0; i < 256; ++i) {
    if (IsWhitespace(static_cast<unsigned char>(i))) {
      bitmap.set(i);
    }
  }

  for (char c : punctuation) {
    bitmap.set(static_cast<unsigned char>(c));
  }

  return bitmap;
}

absl::flat_hash_set<std::string> BuildStopWordsSet(
    const std::vector<std::string>& stop_words) {
  absl::flat_hash_set<std::string> stop_words_set;
  for (const auto& word : stop_words) {
    stop_words_set.insert(absl::AsciiStrToLower(word));
  }
  return stop_words_set;
}

const char* GetLanguageString(data_model::Language language) {
  switch (language) {
    case data_model::LANGUAGE_ENGLISH:
      return "english";
    default:
      return "english";
  }
}

}  // namespace

thread_local Lexer::StemmerCache Lexer::stemmer_cache_;

// Clean up thread-local stemmers when the thread exits
Lexer::StemmerCache::~StemmerCache() {
  for (auto& [lang, stemmer] : cache_) {
    sb_stemmer_delete(stemmer);
  }
}

sb_stemmer* Lexer::StemmerCache::GetOrCreateStemmer(
    data_model::Language language) {
  auto it = cache_.find(language);
  if (it == cache_.end()) {
    sb_stemmer* stemmer = sb_stemmer_new(GetLanguageString(language), "UTF_8");
    cache_[language] = stemmer;
    return stemmer;
  }
  return it->second;
}

Lexer::Lexer(data_model::Language language, const std::string& punctuation,
             const std::vector<std::string>& stop_words)
    : language_(language),
      punct_bitmap_(BuildPunctuationBitmap(punctuation)),
      stop_words_set_(BuildStopWordsSet(stop_words)) {}

absl::StatusOr<std::vector<std::string>> Lexer::Tokenize(
    absl::string_view text, bool stemming_enabled,
    uint32_t min_stem_size) const {
  if (!IsValidUtf8(text)) {
    return absl::InvalidArgumentError("Invalid UTF-8");
  }

  // Get or create the thread-local stemmer for this lexer's language
  sb_stemmer* stemmer =
      stemming_enabled ? stemmer_cache_.GetOrCreateStemmer(language_) : nullptr;
  std::vector<std::string> tokens;
  size_t pos = 0;
  while (pos < text.size()) {
    while (pos < text.size() && IsPunctuation(text[pos])) {
      pos++;
    }

    size_t word_start = pos;
    while (pos < text.size() && !IsPunctuation(text[pos])) {
      pos++;
    }

    if (pos > word_start) {
      absl::string_view word_view(text.data() + word_start, pos - word_start);

      std::string word = absl::AsciiStrToLower(word_view);

      if (IsStopWord(word)) {
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
