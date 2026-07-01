/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/snowball_processor.h"

#include <algorithm>
#include <memory>

#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "libstemmer.h"
#include "src/indexes/text/punctuation.h"
#include "src/indexes/text/stop_words.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "src/utils/scanner.h"

namespace valkey_search::indexes::text {

namespace {

struct StemmerDeleter {
  void operator()(sb_stemmer* stemmer) const { sb_stemmer_delete(stemmer); }
};

using StemmerPtr = std::unique_ptr<sb_stemmer, StemmerDeleter>;

thread_local absl::flat_hash_map<data_model::Language, StemmerPtr> stemmers_;

}  // namespace

SnowballProcessor::SnowballProcessor(data_model::Language language,
                                     const std::string& punctuation,
                                     const std::vector<std::string>& stop_words)
    : language_(language),
      norm_form_(language == data_model::LANGUAGE_ARABIC
                     ? NormalizationForm::NFKC
                     : NormalizationForm::NFC),
      punct_set_(BuildPunctuationSet(punctuation)),
      stop_words_set_(BuildStopWordsSet(stop_words)) {}

const char* SnowballProcessor::GetLanguageString(
    data_model::Language language) {
  switch (language) {
    case data_model::LANGUAGE_ENGLISH:
      return "english";
    case data_model::LANGUAGE_FRENCH:
      return "french";
    case data_model::LANGUAGE_GERMAN:
      return "german";
    case data_model::LANGUAGE_SPANISH:
      return "spanish";
    case data_model::LANGUAGE_ITALIAN:
      return "italian";
    case data_model::LANGUAGE_PORTUGUESE:
      return "portuguese";
    case data_model::LANGUAGE_RUSSIAN:
      return "russian";
    case data_model::LANGUAGE_SWEDISH:
      return "swedish";
    case data_model::LANGUAGE_TURKISH:
      return "turkish";
    case data_model::LANGUAGE_DUTCH:
      return "dutch";
    case data_model::LANGUAGE_INDONESIAN:
      return "indonesian";
    case data_model::LANGUAGE_ARABIC:
      return "arabic";
    default:
      CHECK(false) << "Unexpected language for SnowballProcessor";
      __builtin_unreachable();
  }
}

sb_stemmer* SnowballProcessor::GetStemmer() const {
  auto it = stemmers_.find(language_);
  if (it == stemmers_.end()) {
    StemmerPtr stemmer(sb_stemmer_new(GetLanguageString(language_), "UTF_8"));

    // sb_stemmer_new() returns nullptr for languages not yet registered in
    // third_party/snowball/libstemmer/modules.h. Run add_language.sh to
    // generate stemmer source files for new languages.
    CHECK(stemmer) << "Failed to create stemmer for language: "
                   << GetLanguageString(language_);

    sb_stemmer* raw_ptr = stemmer.get();
    stemmers_[language_] = std::move(stemmer);
    return raw_ptr;
  }
  return it->second.get();
}

std::string_view SnowballProcessor::DoStemming(absl::string_view word,
                                               sb_stemmer* stemmer,
                                               uint32_t min_stem_size) const {
  if (word.empty() ||
      !utils::Scanner::AtLeastNCodepoints(word, min_stem_size)) {
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

void SnowballProcessor::StemWordInPlace(std::string& word,
                                        uint32_t min_stem_size) const {
  sb_stemmer* stemmer = GetStemmer();
  std::string_view stemmed_view = DoStemming(word, stemmer, min_stem_size);
  if (stemmed_view != word) {
    word.assign(stemmed_view);
  }
}

void SnowballProcessor::BuildStemMap(const std::vector<std::string>& tokens,
                                     uint32_t min_stem_size,
                                     InProgressStemMap& stem_mappings) const {
  sb_stemmer* stemmer = GetStemmer();
  for (const auto& token : tokens) {
    std::string_view stemmed_view = DoStemming(token, stemmer, min_stem_size);
    if (stemmed_view != token) {
      auto it = stem_mappings.find(stemmed_view);
      if (it == stem_mappings.end()) {
        it = stem_mappings.try_emplace(std::string(stemmed_view)).first;
      }
      auto& variants = it->second;
      if (std::find(variants.begin(), variants.end(), token) ==
          variants.end()) {
        variants.emplace_back(token);
      }
    }
  }
}

absl::StatusOr<std::vector<std::string>> SnowballProcessor::Tokenize(
    absl::string_view text, bool stemming_enabled, uint32_t min_stem_size,
    InProgressStemMap* stem_map) const {
  if (!utils::Scanner::IsValidUtf8(text)) {
    return absl::InvalidArgumentError("Invalid UTF-8");
  }

  sb_stemmer* stemmer = stemming_enabled ? GetStemmer() : nullptr;
  std::vector<std::string> tokens;
  std::string word;
  word.reserve(64);
  size_t pos = 0;

  while (pos < text.size()) {
    // Skip leading punctuation (code-point aware)
    while (pos < text.size()) {
      if (text[pos] == '\\' && pos + 1 < text.size()) break;
      utils::Scanner s(text.substr(pos));
      auto cp = s.NextUtf8();
      CHECK(cp != utils::Scanner::kInvalidCp)
          << "Tokenize decoded invalid UTF-8 after IsValidUtf8 passed";
      if (!IsPunctuation(cp)) break;
      pos += s.LastUtf8ByteLen();
    }

    word.clear();

    // Build word until next punctuation boundary
    while (pos < text.size()) {
      if (text[pos] == '\\' && pos + 1 < text.size()) {
        pos++;
        utils::Scanner s(text.substr(pos));
        auto esc_cp = s.NextUtf8();
        CHECK(esc_cp != utils::Scanner::kInvalidCp)
            << "Tokenize decoded invalid UTF-8 after IsValidUtf8 passed";
        uint8_t esc_len = s.LastUtf8ByteLen();
        if (esc_cp != '\\' && !IsPunctuation(esc_cp) && IsPunctuation('\\')) {
          break;
        }
        word.append(text.data() + pos, esc_len);
        pos += esc_len;
        continue;
      }

      utils::Scanner s(text.substr(pos));
      auto cp = s.NextUtf8();
      CHECK(cp != utils::Scanner::kInvalidCp)
          << "Tokenize decoded invalid UTF-8 after IsValidUtf8 passed";
      if (IsPunctuation(cp)) break;
      uint8_t len = s.LastUtf8ByteLen();
      word.append(text.data() + pos, len);
      pos += len;
    }

    if (!word.empty()) {
      NormalizeLowerCaseInPlace(word);

      if (IsStopWord(word)) {
        word.clear();
        continue;
      }

      if (stemming_enabled && stem_map) {
        std::string_view stemmed = DoStemming(word, stemmer, min_stem_size);
        if (stemmed != word) {
          auto it = stem_map->find(stemmed);
          if (it == stem_map->end()) {
            it = stem_map->try_emplace(std::string(stemmed)).first;
          }
          auto& variants = it->second;
          if (std::find(variants.begin(), variants.end(), word) ==
              variants.end()) {
            variants.emplace_back(word);
          }
        }
      }

      tokens.push_back(std::move(word));
      word.clear();
    }
  }
  return tokens;
}

void SnowballProcessor::NormalizeLowerCaseInPlace(std::string& word) const {
  if (absl::c_all_of(word, absl::ascii_isascii)) {
    absl::AsciiStrToLower(&word);
  } else {
    word = UnicodeNormalizer::Normalize(word, norm_form_);
    UnicodeNormalizer::CaseFoldInPlace(word);
  }
}

bool SnowballProcessor::IsPunctuation(uint32_t cp) const {
  return punct_set_.Contains(cp);
}

bool SnowballProcessor::IsStopWord(absl::string_view word) const {
  return stop_words_set_.contains(word);
}

}  // namespace valkey_search::indexes::text
