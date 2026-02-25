/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/indexes/text/lexer.h"

#include <memory>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "libstemmer.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "src/utils/scanner.h"
#include "vmsdk/src/log.h"

namespace valkey_search::indexes::text {

namespace {

bool IsWhitespace(unsigned char c) {
  return std::isspace(c) || std::iscntrl(c);
}

using PunctuationBitmap = std::bitset<256>;

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
      CHECK(false) << "Unexpected language";
  }
}

struct StemmerDeleter {
  void operator()(sb_stemmer* stemmer) const { sb_stemmer_delete(stemmer); }
};

using StemmerPtr = std::unique_ptr<sb_stemmer, StemmerDeleter>;

// Thread-local stemmer cache. Since a stemmer instance is not thread-safe,
// stemmers will be owned by threads and shared amongst the Lexer instances.
// Each ingestion worker thread gets a stemmer for each language it tokenizes
// at least once.
thread_local absl::flat_hash_map<data_model::Language, StemmerPtr> stemmers_;

}  // namespace

Lexer::Lexer(data_model::Language language, const std::string& punctuation,
             const std::vector<std::string>& stop_words)
    : language_(language),
      punct_bitmap_(BuildPunctuationBitmap(punctuation)),
      stop_words_set_(BuildStopWordsSet(stop_words)) {}

absl::StatusOr<std::vector<std::string>> Lexer::Tokenize(
    absl::string_view text, bool stemming_enabled, uint32_t min_stem_size,
    absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>>*
        stem_mappings) const {
  VMSDK_LOG(NOTICE, nullptr) << "Tokenizing text content: " << text;
  if (!IsValidUtf8(text)) {
    return absl::InvalidArgumentError("Invalid UTF-8");
  }

  // Get or create the thread-local stemmer for this lexer's language
  sb_stemmer* stemmer = stemming_enabled ? GetStemmer() : nullptr;
  std::vector<std::string> tokens;
  size_t pos = 0;
  while (pos < text.size()) {
    // Skip leading punctuation, but check for backslash escape sequences
    while (pos < text.size() && IsPunctuation(text[pos])) {
      if (text[pos] == '\\' && pos + 1 < text.size()) {
        // Backslash at start - let word building handle escape
        break;
      }
      pos++;
    }

    std::string word_buffer;

    // Build word, handling backslash escape sequences
    while (pos < text.size()) {
      char ch = text[pos];
      if (ch == '\\' && pos + 1 < text.size()) {
        char next_ch = text[pos + 1];
        pos++;  // Consume the backslash
        if (next_ch == '\\' || IsPunctuation(next_ch)) {
          // Backslash escapes backslash or punctuation
          word_buffer.push_back(text[pos++]);  // Keep the escaped character
        } else {
          // Backslash before non-punctuation
          if (IsPunctuation('\\')) {
            // Backslash is punctuation → end token (Standard Unicode
            // segmentation)
            break;
          } else {
            // Backslash not punctuation → keep letter
            word_buffer.push_back(text[pos++]);
          }
        }
      } else if (IsPunctuation(ch)) {
        // Regular punctuation - end of word
        break;
      } else {
        // Regular character
        word_buffer.push_back(ch);
        pos++;
      }
    }

    if (!word_buffer.empty()) {
      std::string word = NormalizeLowerCase(word_buffer);

      if (IsStopWord(word)) {
        continue;  // Skip stop words
      }

      if (stemming_enabled) {
        std::string stemmed_word = StemWord(word, stemmer, min_stem_size);
        if (word != stemmed_word) {
          CHECK(stem_mappings) << "stem_mappings must not be null";
          (*stem_mappings)[stemmed_word].insert(word);
        }
      }
      tokens.push_back(std::move(word));
    }
  }

  return tokens;
}

// Returns a thread-local cached stemmer for this lexer's language, creating it
// on first access.
sb_stemmer* Lexer::GetStemmer() const {
  auto it = stemmers_.find(language_);
  if (it == stemmers_.end()) {
    StemmerPtr stemmer(sb_stemmer_new(GetLanguageString(language_), "UTF_8"));
    sb_stemmer* raw_ptr = stemmer.get();
    stemmers_[language_] = std::move(stemmer);
    return raw_ptr;
  }
  return it->second.get();
}

std::string Lexer::StemWord(const std::string& word, sb_stemmer* stemmer,
                            uint32_t min_stem_size) const {
  if (word.empty() || word.length() < min_stem_size) {
    return word;
  }

  CHECK(stemmer) << "Stemmer is not initialized";

  const sb_symbol* stemmed = sb_stemmer_stem(
      stemmer, reinterpret_cast<const sb_symbol*>(word.c_str()), word.length());

  CHECK(stemmed) << "Stemming failed";

  int stemmed_length = sb_stemmer_length(stemmer);
  CHECK(stemmed_length > 0 && stemmed_length <= word.length())
      << "Stemming failed";

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

std::string Lexer::NormalizeLowerCase(absl::string_view str) const {
  return absl::c_all_of(str, absl::ascii_isascii)
             ? absl::AsciiStrToLower(str)
             : UnicodeNormalizer::CaseFold(str);
}
}  // namespace valkey_search::indexes::text
