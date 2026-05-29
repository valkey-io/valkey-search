/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/indexes/text/lexer.h"

#include <memory>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "libstemmer.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "src/utils/scanner.h"
#include "src/utils/utf8_iterator.h"

namespace valkey_search::indexes::text {

namespace {

bool IsWhitespace(unsigned char c) {
  return std::isspace(c) || std::iscntrl(c);
}

// Build PunctuationSet from the PUNCTUATION string. Iterates as code points
// (not bytes) so multi-byte chars like U+060C are stored correctly.
PunctuationSet BuildPunctuationSet(const std::string& punctuation) {
  PunctuationSet result;

  // ASCII whitespace and control characters are always word boundaries.
  for (int i = 0; i < 128; ++i) {
    if (IsWhitespace(static_cast<unsigned char>(i))) {
      result.ascii.set(i);
    }
  }

  // Iterate the user-supplied punctuation as code points, not bytes.
  utils::Utf8Iterator it(punctuation);
  while (!it.Done()) {
    auto [cp, byte_len] = it.Next();
    if (utils::Utf8Iterator::IsAscii(cp)) {
      result.ascii.set(cp);
    } else {
      result.non_ascii.insert(cp);
    }
  }

  return result;
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
      punct_set_(BuildPunctuationSet(punctuation)),
      stop_words_set_(BuildStopWordsSet(stop_words)) {}

absl::StatusOr<std::vector<std::string>> Lexer::Tokenize(
    absl::string_view text, bool stemming_enabled, uint32_t min_stem_size,
    InProgressStemMap* stem_mappings) const {
  if (stemming_enabled) {
    CHECK(stem_mappings) << "stem_mappings must not be null";
  }
  if (!IsValidUtf8(text)) {
    return absl::InvalidArgumentError("Invalid UTF-8");
  }

  // Get or create the thread-local stemmer for this lexer's language
  sb_stemmer* stemmer = stemming_enabled ? GetStemmer() : nullptr;
  // Deque grows by adding new blocks—avoids the cost of copying
  // existing elements during reallocation.
  std::vector<std::string> tokens;
  std::string word;
  word.reserve(64);
  size_t pos = 0;
  while (pos < text.size()) {
    // Skip leading punctuation. Decode code points so multi-byte chars are
    // never confused with ASCII punctuation.
    while (pos < text.size()) {
      if (text[pos] == '\\' && pos + 1 < text.size()) {
        break;  // Let word-building handle escape
      }
      utils::Utf8Iterator peek_it(text.substr(pos));
      auto [cp, byte_len] = peek_it.Next();
      if (!IsPunctuation(cp)) break;
      pos += byte_len;
    }

    word.clear();

    // Build word. Decode code points so multi-byte chars are treated correctly
    // by IsPunctuation (which handles both ASCII and non-ASCII via
    // PunctuationSet).
    while (pos < text.size()) {
      if (text[pos] == '\\' && pos + 1 < text.size()) {
        pos++;  // Consume the backslash
        utils::Utf8Iterator esc_it(text.substr(pos));
        auto [next_cp, next_byte_len] = esc_it.Next();
        if (next_cp == '\\' || IsPunctuation(next_cp)) {
          word.append(text.data() + pos, next_byte_len);
          pos += next_byte_len;
        } else {
          if (IsPunctuation('\\')) {
            break;
          } else {
            word.append(text.data() + pos, next_byte_len);
            pos += next_byte_len;
          }
        }
        continue;
      }

      utils::Utf8Iterator cp_it(text.substr(pos));
      auto [cp, byte_len] = cp_it.Next();
      if (IsPunctuation(cp)) {
        break;
      }
      word.append(text.data() + pos, byte_len);
      pos += byte_len;
    }

    if (!word.empty()) {
      NormalizeLowerCaseInPlace(word);

      if (IsStopWord(word)) {
        continue;  // Skip stop words
      }

      if (stemming_enabled) {
        UpdateStemMap(word, stemmer, min_stem_size, *stem_mappings);
      }
      tokens.push_back(std::move(word));
      word.clear();
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

void Lexer::NormalizeLowerCaseInPlace(std::string& str) const {
  if (absl::c_all_of(str, absl::ascii_isascii)) {
    absl::AsciiStrToLower(&str);
  } else {
    UnicodeNormalizer::CaseFoldInPlace(str);
  }
}

std::string_view Lexer::DoStemming(absl::string_view word, sb_stemmer* stemmer,
                                   uint32_t min_stem_size) const {
  if (word.empty()) {
    return word;
  }
  // min_stem_size is a code point count, not byte count. "été" = 3 cps, 5
  // bytes.
  if (utils::Utf8Iterator::CodePointCount(word) < min_stem_size) {
    return word;
  }
  CHECK(stemmer) << "Stemmer is not initialized";
  const sb_symbol* stemmed = sb_stemmer_stem(
      stemmer, reinterpret_cast<const sb_symbol*>(word.data()), word.length());
  CHECK(stemmed) << "Stemming failed";
  int stemmed_length = sb_stemmer_length(stemmer);
  CHECK(stemmed_length > 0) << "Stemming failed";
  return std::string_view(reinterpret_cast<const char*>(stemmed),
                          stemmed_length);
}

void Lexer::StemWordInPlace(std::string& word, sb_stemmer* stemmer,
                            uint32_t min_stem_size) const {
  std::string_view stemmed_view = DoStemming(word, stemmer, min_stem_size);
  if (stemmed_view != word) {
    word.assign(stemmed_view);
  }
}

void Lexer::UpdateStemMap(absl::string_view original_word, sb_stemmer* stemmer,
                          uint32_t min_stem_size,
                          InProgressStemMap& stem_mappings) const {
  std::string_view stemmed_view =
      DoStemming(original_word, stemmer, min_stem_size);
  if (stemmed_view != original_word) {
    auto it = stem_mappings.find(stemmed_view);
    if (it == stem_mappings.end()) {
      // New Stem Root: create a new map entry
      it = stem_mappings.try_emplace(std::string(stemmed_view)).first;
    }
    // Add original word to the list of variants for this stem root
    auto& variants = it->second;
    if (std::find(variants.begin(), variants.end(), original_word) ==
        variants.end()) {
      variants.emplace_back(original_word);
    }
  }
}
}  // namespace valkey_search::indexes::text
