/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_DUTCH_H_
#define VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_DUTCH_H_

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/snowball_language.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "src/version.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {

// Dutch: ASCII + common typographic punctuation.
// Punctuation characters sourced from Unicode CLDR Punctuation Exemplars (v46).
inline const std::string kDutchPunctuation =
    ",.<>{}[]\"':;!@#$%^&*()-+=~/\\|?"
    "\xe2\x80\x93"  // – U+2013 EN DASH
    "\xe2\x80\x94"  // — U+2014 EM DASH
    "\xe2\x80\xa6"  // … U+2026 HORIZONTAL ELLIPSIS
    "\xe2\x80\x98"  // ' U+2018 LEFT SINGLE QUOTATION MARK
    "\xe2\x80\x99"  // ' U+2019 RIGHT SINGLE QUOTATION MARK
    "\xe2\x80\x9c"  // " U+201C LEFT DOUBLE QUOTATION MARK
    "\xe2\x80\x9d"  // " U+201D RIGHT DOUBLE QUOTATION MARK
    "\xc2\xab"      // « U+00AB LEFT-POINTING DOUBLE ANGLE QUOTATION MARK
    "\xc2\xbb";     // » U+00BB RIGHT-POINTING DOUBLE ANGLE QUOTATION MARK

// Dutch stop words (101 words) — sourced from Apache Lucene.
inline const std::vector<std::string> kDutchStopWords{
    "de",     "en",      "van",   "ik",     "te",     "dat",   "die",
    "in",     "een",     "hij",   "het",    "niet",   "zijn",  "is",
    "was",    "op",      "aan",   "met",    "als",    "voor",  "had",
    "er",     "maar",    "om",    "hem",    "dan",    "zou",   "of",
    "wat",    "mijn",    "men",   "dit",    "zo",     "door",  "over",
    "ze",     "zich",    "bij",   "ook",    "tot",    "je",    "mij",
    "uit",    "der",     "daar",  "haar",   "naar",   "heb",   "hoe",
    "heeft",  "hebben",  "deze",  "u",      "want",   "nog",   "zal",
    "me",     "zij",     "nu",    "ge",     "geen",   "omdat", "iets",
    "worden", "toch",    "al",    "waren",  "veel",   "meer",  "doen",
    "toen",   "moet",    "ben",   "zonder", "kan",    "hun",   "dus",
    "alles",  "onder",   "ja",    "eens",   "hier",   "wie",   "werd",
    "altijd", "doch",    "wordt", "wezen",  "kunnen", "ons",   "zelf",
    "tegen",  "na",      "reeds", "wil",    "kon",    "niets", "uw",
    "iemand", "geweest", "andere"};

class DutchLanguage final : public SnowballLanguage {
 public:
  DutchLanguage()
      : SnowballLanguage(data_model::LANGUAGE_DUTCH, kDutchPunctuation,
                         kDutchStopWords, NormalizationForm::NFC, "", "dutch") {
  }

  data_model::Language Id() const override {
    return data_model::LANGUAGE_DUTCH;
  }
  absl::string_view Name() const override { return "dutch"; }
  const std::string& GetDefaultPunctuation() const override {
    return kDutchPunctuation;
  }
  const std::vector<std::string>& GetDefaultStopWords() const override {
    return kDutchStopWords;
  }
  NormalizationForm GetNormalizationForm() const override {
    return NormalizationForm::NFC;
  }
  absl::string_view CaseFoldLocale() const override { return ""; }
  vmsdk::ValkeyVersion MinRequiredVersion() const override {
    return valkey_search::kRelease13;
  }
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_DUTCH_H_
