/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_FRENCH_H_
#define VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_FRENCH_H_

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/snowball_language.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "src/version.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {

// French: ASCII + common typographic punctuation.
// Punctuation characters sourced from Unicode CLDR Punctuation Exemplars (v46).
inline const std::string kFrenchPunctuation =
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

// French stop words (154 words) — sourced from Apache Lucene.
inline const std::vector<std::string> kFrenchStopWords{
    "au",      "aux",     "avec",     "ce",      "ces",      "dans",
    "de",      "des",     "du",       "elle",    "en",       "et",
    "eux",     "il",      "je",       "la",      "le",       "leur",
    "lui",     "ma",      "mais",     "me",      "même",     "mes",
    "moi",     "mon",     "ne",       "nos",     "notre",    "nous",
    "on",      "ou",      "par",      "pas",     "pour",     "qu",
    "que",     "qui",     "sa",       "se",      "ses",      "sur",
    "ta",      "te",      "tes",      "toi",     "ton",      "tu",
    "un",      "une",     "vos",      "votre",   "vous",     "c",
    "d",       "j",       "l",        "à",       "m",        "n",
    "s",       "t",       "y",        "étée",    "étées",    "étant",
    "suis",    "es",      "êtes",     "sont",    "serai",    "seras",
    "sera",    "serons",  "serez",    "seront",  "serais",   "serait",
    "serions", "seriez",  "seraient", "étais",   "était",    "étions",
    "étiez",   "étaient", "fus",      "fut",     "fûmes",    "fûtes",
    "furent",  "sois",    "soit",     "soyons",  "soyez",    "soient",
    "fusse",   "fusses",  "fussions", "fussiez", "fussent",  "ayant",
    "eu",      "eue",     "eues",     "eus",     "ai",       "avons",
    "avez",    "ont",     "aurai",    "aurons",  "aurez",    "auront",
    "aurais",  "aurait",  "aurions",  "auriez",  "auraient", "avais",
    "avait",   "aviez",   "avaient",  "eut",     "eûmes",    "eûtes",
    "eurent",  "aie",     "aies",     "ait",     "ayons",    "ayez",
    "aient",   "eusse",   "eusses",   "eût",     "eussions", "eussiez",
    "eussent", "ceci",    "cela",     "celà",    "cet",      "cette",
    "ici",     "ils",     "les",      "leurs",   "quel",     "quels",
    "quelle",  "quelles", "sans",     "soi"};

class FrenchLanguage final : public SnowballLanguage {
 public:
  FrenchLanguage()
      : SnowballLanguage(data_model::LANGUAGE_FRENCH, kFrenchPunctuation,
                         kFrenchStopWords, NormalizationForm::NFC, "",
                         "french") {}

  data_model::Language Id() const override {
    return data_model::LANGUAGE_FRENCH;
  }
  absl::string_view Name() const override { return "french"; }
  const std::string& GetDefaultPunctuation() const override {
    return kFrenchPunctuation;
  }
  const std::vector<std::string>& GetDefaultStopWords() const override {
    return kFrenchStopWords;
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

#endif  // VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_FRENCH_H_
