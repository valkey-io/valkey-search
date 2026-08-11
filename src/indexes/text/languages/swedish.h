/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_SWEDISH_H_
#define VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_SWEDISH_H_

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/snowball_language.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "src/version.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {

// Swedish: ASCII + common typographic punctuation.
// Punctuation characters sourced from Unicode CLDR Punctuation Exemplars (v46).
inline const std::string kSwedishPunctuation =
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

// Swedish stop words (114 words) — sourced from Apache Lucene.
inline const std::vector<std::string> kSwedishStopWords{
    "och",    "det",    "att",    "i",      "en",    "jag",    "hon",
    "som",    "han",    "på",     "den",    "med",   "var",    "sig",
    "för",    "så",     "till",   "är",     "men",   "ett",    "om",
    "hade",   "de",     "av",     "icke",   "mig",   "du",     "henne",
    "då",     "sin",    "nu",     "har",    "inte",  "hans",   "honom",
    "skulle", "hennes", "där",    "min",    "man",   "ej",     "vid",
    "kunde",  "något",  "från",   "ut",     "när",   "efter",  "upp",
    "vi",     "dem",    "vara",   "vad",    "över",  "än",     "dig",
    "kan",    "sina",   "här",    "ha",     "mot",   "alla",   "under",
    "någon",  "eller",  "allt",   "mycket", "sedan", "ju",     "denna",
    "själv",  "detta",  "åt",     "utan",   "varit", "hur",    "ingen",
    "mitt",   "ni",     "bli",    "blev",   "oss",   "din",    "dessa",
    "några",  "deras",  "blir",   "mina",   "samma", "vilken", "er",
    "sådan",  "vår",    "blivit", "dess",   "inom",  "mellan", "sådant",
    "varför", "varje",  "vilka",  "ditt",   "vem",   "vilket", "sitt",
    "sådana", "vart",   "dina",   "vars",   "vårt",  "våra",   "ert",
    "era",    "vilkas"};

class SwedishLanguage final : public SnowballLanguage {
 public:
  SwedishLanguage()
      : SnowballLanguage(data_model::LANGUAGE_SWEDISH, kSwedishPunctuation,
                         kSwedishStopWords, NormalizationForm::NFC, "",
                         "swedish") {}

  data_model::Language Id() const override {
    return data_model::LANGUAGE_SWEDISH;
  }
  absl::string_view Name() const override { return "swedish"; }
  const std::string& GetDefaultPunctuation() const override {
    return kSwedishPunctuation;
  }
  const std::vector<std::string>& GetDefaultStopWords() const override {
    return kSwedishStopWords;
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

#endif  // VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_SWEDISH_H_
