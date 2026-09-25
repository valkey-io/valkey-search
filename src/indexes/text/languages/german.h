/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_GERMAN_H_
#define VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_GERMAN_H_

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/snowball_language.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "src/version.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {

// German: ASCII + common typographic + low-9 quotation marks.
// Punctuation characters sourced from Unicode CLDR Punctuation Exemplars (v46).
inline const std::string kGermanPunctuation =
    ",.<>{}[]\"':;!@#$%^&*()-+=~/\\|?"
    "\xe2\x80\x93"   // – U+2013 EN DASH
    "\xe2\x80\x94"   // — U+2014 EM DASH
    "\xe2\x80\xa6"   // … U+2026 HORIZONTAL ELLIPSIS
    "\xe2\x80\x98"   // ' U+2018 LEFT SINGLE QUOTATION MARK
    "\xe2\x80\x99"   // ' U+2019 RIGHT SINGLE QUOTATION MARK
    "\xe2\x80\x9c"   // " U+201C LEFT DOUBLE QUOTATION MARK
    "\xe2\x80\x9d"   // " U+201D RIGHT DOUBLE QUOTATION MARK
    "\xc2\xab"       // « U+00AB LEFT-POINTING DOUBLE ANGLE QUOTATION MARK
    "\xc2\xbb"       // » U+00BB RIGHT-POINTING DOUBLE ANGLE QUOTATION MARK
    "\xe2\x80\x9e"   // „ U+201E DOUBLE LOW-9 QUOTATION MARK
    "\xe2\x80\x9a";  // ‚ U+201A SINGLE LOW-9 QUOTATION MARK

// German stop words (231 words) — sourced from Apache Lucene.
inline const std::vector<std::string> kGermanStopWords{
    "aber",     "alle",      "allem",     "allen",     "aller",     "alles",
    "als",      "also",      "am",        "an",        "ander",     "andere",
    "anderem",  "anderen",   "anderer",   "anderes",   "anderm",    "andern",
    "anderr",   "anders",    "auch",      "auf",       "aus",       "bei",
    "bin",      "bis",       "bist",      "da",        "damit",     "dann",
    "der",      "den",       "des",       "dem",       "die",       "das",
    "daß",      "derselbe",  "derselben", "denselben", "desselben", "demselben",
    "dieselbe", "dieselben", "dasselbe",  "dazu",      "dein",      "deine",
    "deinem",   "deinen",    "deiner",    "deines",    "denn",      "derer",
    "dessen",   "dich",      "dir",       "du",        "dies",      "diese",
    "diesem",   "diesen",    "dieser",    "dieses",    "doch",      "dort",
    "durch",    "ein",       "eine",      "einem",     "einen",     "einer",
    "eines",    "einig",     "einige",    "einigem",   "einigen",   "einiger",
    "einiges",  "einmal",    "er",        "ihn",       "ihm",       "es",
    "etwas",    "euer",      "eure",      "eurem",     "euren",     "eurer",
    "eures",    "für",       "gegen",     "gewesen",   "hab",       "habe",
    "haben",    "hat",       "hatte",     "hatten",    "hier",      "hin",
    "hinter",   "ich",       "mich",      "mir",       "ihr",       "ihre",
    "ihrem",    "ihren",     "ihrer",     "ihres",     "euch",      "im",
    "in",       "indem",     "ins",       "ist",       "jede",      "jedem",
    "jeden",    "jeder",     "jedes",     "jene",      "jenem",     "jenen",
    "jener",    "jenes",     "jetzt",     "kann",      "kein",      "keine",
    "keinem",   "keinen",    "keiner",    "keines",    "können",    "könnte",
    "machen",   "man",       "manche",    "manchem",   "manchen",   "mancher",
    "manches",  "mein",      "meine",     "meinem",    "meinen",    "meiner",
    "meines",   "mit",       "muss",      "musste",    "nach",      "nicht",
    "nichts",   "noch",      "nun",       "nur",       "ob",        "oder",
    "ohne",     "sehr",      "sein",      "seine",     "seinem",    "seinen",
    "seiner",   "seines",    "selbst",    "sich",      "sie",       "ihnen",
    "sind",     "so",        "solche",    "solchem",   "solchen",   "solcher",
    "solches",  "soll",      "sollte",    "sondern",   "sonst",     "über",
    "um",       "und",       "uns",       "unse",      "unsem",     "unsen",
    "unser",    "unses",     "unter",     "viel",      "vom",       "von",
    "vor",      "während",   "war",       "waren",     "warst",     "was",
    "weg",      "weil",      "weiter",    "welche",    "welchem",   "welchen",
    "welcher",  "welches",   "wenn",      "werde",     "werden",    "wie",
    "wieder",   "will",      "wir",       "wird",      "wirst",     "wo",
    "wollen",   "wollte",    "würde",     "würden",    "zu",        "zum",
    "zur",      "zwar",      "zwischen"};

class GermanLanguage final : public SnowballLanguage {
 public:
  GermanLanguage()
      : SnowballLanguage(data_model::LANGUAGE_GERMAN, kGermanPunctuation,
                         kGermanStopWords, NormalizationForm::NFC, "",
                         "german") {}

  data_model::Language Id() const override {
    return data_model::LANGUAGE_GERMAN;
  }
  absl::string_view Name() const override { return "german"; }
  const std::string& GetDefaultPunctuation() const override {
    return kGermanPunctuation;
  }
  const std::vector<std::string>& GetDefaultStopWords() const override {
    return kGermanStopWords;
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

#endif  // VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_GERMAN_H_
