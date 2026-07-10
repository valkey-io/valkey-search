/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/snowball_stem.h"

#include <algorithm>
#include <memory>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "libstemmer.h"
#include "src/utils/scanner.h"

namespace valkey_search::indexes::text {

namespace {

struct StemmerDeleter {
  void operator()(sb_stemmer* stemmer) const { sb_stemmer_delete(stemmer); }
};

using StemmerPtr = std::unique_ptr<sb_stemmer, StemmerDeleter>;

thread_local absl::flat_hash_map<data_model::Language, StemmerPtr> stemmers_;

}  // namespace

SnowballStemFilter::SnowballStemFilter(data_model::Language language,
                                       uint32_t default_min_stem_size)
    : language_(language), default_min_stem_size_(default_min_stem_size) {}

const char* SnowballStemFilter::GetLanguageString(
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
      CHECK(false) << "Unexpected language for SnowballStemFilter";
      __builtin_unreachable();
  }
}

sb_stemmer* SnowballStemFilter::GetStemmer() const {
  auto it = stemmers_.find(language_);
  if (it == stemmers_.end()) {
    StemmerPtr stemmer(sb_stemmer_new(GetLanguageString(language_), "UTF_8"));
    CHECK(stemmer) << "Failed to create stemmer for language: "
                   << GetLanguageString(language_);
    sb_stemmer* raw_ptr = stemmer.get();
    stemmers_[language_] = std::move(stemmer);
    return raw_ptr;
  }
  return it->second.get();
}

std::string_view SnowballStemFilter::DoStemming(absl::string_view word,
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
  return {reinterpret_cast<const char*>(stemmed),
          static_cast<std::string_view::size_type>(stemmed_length)};
}

bool SnowballStemFilter::Apply(std::string& token) const {
  sb_stemmer* stemmer = GetStemmer();
  std::string_view stemmed = DoStemming(token, stemmer, default_min_stem_size_);
  if (stemmed.data() != token.data() || stemmed.size() != token.size()) {
    token.assign(stemmed.data(), stemmed.size());
  }
  return true;
}

std::string SnowballStemFilter::GetStemRoot(absl::string_view token,
                                            uint32_t min_stem_size) const {
  sb_stemmer* stemmer = GetStemmer();
  std::string_view stemmed = DoStemming(token, stemmer, min_stem_size);
  return std::string(stemmed);
}

void SnowballStemFilter::BuildStemMap(const std::vector<std::string>& tokens,
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

}  // namespace valkey_search::indexes::text
