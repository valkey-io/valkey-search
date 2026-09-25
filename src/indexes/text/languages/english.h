/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_ENGLISH_H_
#define VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_ENGLISH_H_

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/snowball_language.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {

// English: ASCII punctuation only (backward compatibility).
inline const std::string kEnglishPunctuation =
    ",.<>{}[]\"':;!@#$%^&*()-+=~/\\|?";

// English stop words (33 words) — sourced from Apache Lucene.
inline const std::vector<std::string> kEnglishStopWords{
    "a",    "is",   "the", "an",   "and",  "are",   "as",   "at",    "be",
    "but",  "by",   "for", "if",   "in",   "into",  "it",   "no",    "not",
    "of",   "on",   "or",  "such", "that", "their", "then", "there", "these",
    "they", "this", "to",  "was",  "will", "with"};

class EnglishLanguage final : public SnowballLanguage {
 public:
  EnglishLanguage()
      : SnowballLanguage(data_model::LANGUAGE_ENGLISH, kEnglishPunctuation,
                         kEnglishStopWords, NormalizationForm::NFC, "",
                         "english") {}

  data_model::Language Id() const override {
    return data_model::LANGUAGE_ENGLISH;
  }
  absl::string_view Name() const override { return "english"; }
  const std::string& GetDefaultPunctuation() const override {
    return kEnglishPunctuation;
  }
  const std::vector<std::string>& GetDefaultStopWords() const override {
    return kEnglishStopWords;
  }
  NormalizationForm GetNormalizationForm() const override {
    return NormalizationForm::NFC;
  }
  absl::string_view CaseFoldLocale() const override { return ""; }
  vmsdk::ValkeyVersion MinRequiredVersion() const override {
    return vmsdk::ValkeyVersion(0, 0, 0);
  }
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_ENGLISH_H_
