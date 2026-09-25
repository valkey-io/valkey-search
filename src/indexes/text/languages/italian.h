/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_ITALIAN_H_
#define VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_ITALIAN_H_

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/snowball_language.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "src/version.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {

// Italian: ASCII + common typographic punctuation.
// Punctuation characters sourced from Unicode CLDR Punctuation Exemplars (v46).
inline const std::string kItalianPunctuation =
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

// Italian stop words (279 words) — sourced from Apache Lucene.
inline const std::vector<std::string> kItalianStopWords{
    "ad",        "al",       "allo",      "ai",         "agli",     "all",
    "agl",       "alla",     "alle",      "con",        "col",      "coi",
    "da",        "dal",      "dallo",     "dai",        "dagli",    "dall",
    "dagl",      "dalla",    "dalle",     "di",         "del",      "dello",
    "dei",       "degli",    "dell",      "degl",       "della",    "delle",
    "in",        "nel",      "nello",     "nei",        "negli",    "nell",
    "negl",      "nella",    "nelle",     "su",         "sul",      "sullo",
    "sui",       "sugli",    "sull",      "sugl",       "sulla",    "sulle",
    "per",       "tra",      "contro",    "io",         "tu",       "lui",
    "lei",       "noi",      "voi",       "loro",       "mio",      "mia",
    "miei",      "mie",      "tuo",       "tua",        "tuoi",     "tue",
    "suo",       "sua",      "suoi",      "sue",        "nostro",   "nostra",
    "nostri",    "nostre",   "vostro",    "vostra",     "vostri",   "vostre",
    "mi",        "ti",       "ci",        "vi",         "lo",       "la",
    "li",        "le",       "gli",       "ne",         "il",       "un",
    "uno",       "una",      "ma",        "ed",         "se",       "perché",
    "anche",     "come",     "dov",       "dove",       "che",      "chi",
    "cui",       "non",      "più",       "quale",      "quanto",   "quanti",
    "quanta",    "quante",   "quello",    "quelli",     "quella",   "quelle",
    "questo",    "questi",   "questa",    "queste",     "si",       "tutto",
    "tutti",     "a",        "c",         "e",          "i",        "l",
    "o",         "ho",       "hai",       "ha",         "abbiamo",  "avete",
    "hanno",     "abbia",    "abbiate",   "abbiano",    "avrò",     "avrai",
    "avrà",      "avremo",   "avrete",    "avranno",    "avrei",    "avresti",
    "avrebbe",   "avremmo",  "avreste",   "avrebbero",  "avevo",    "avevi",
    "aveva",     "avevamo",  "avevate",   "avevano",    "ebbi",     "avesti",
    "ebbe",      "avemmo",   "aveste",    "ebbero",     "avessi",   "avesse",
    "avessimo",  "avessero", "avendo",    "avuto",      "avuta",    "avuti",
    "avute",     "sono",     "sei",       "è",          "siamo",    "siete",
    "sia",       "siate",    "siano",     "sarò",       "sarai",    "sarà",
    "saremo",    "sarete",   "saranno",   "sarei",      "saresti",  "sarebbe",
    "saremmo",   "sareste",  "sarebbero", "ero",        "eri",      "era",
    "eravamo",   "eravate",  "erano",     "fui",        "fosti",    "fu",
    "fummo",     "foste",    "furono",    "fossi",      "fosse",    "fossimo",
    "fossero",   "essendo",  "faccio",    "fai",        "facciamo", "fanno",
    "faccia",    "facciate", "facciano",  "farò",       "farai",    "farà",
    "faremo",    "farete",   "faranno",   "farei",      "faresti",  "farebbe",
    "faremmo",   "fareste",  "farebbero", "facevo",     "facevi",   "faceva",
    "facevamo",  "facevate", "facevano",  "feci",       "facesti",  "fece",
    "facemmo",   "faceste",  "fecero",    "facessi",    "facesse",  "facessimo",
    "facessero", "facendo",  "sto",       "stai",       "sta",      "stiamo",
    "stanno",    "stia",     "stiate",    "stiano",     "starò",    "starai",
    "starà",     "staremo",  "starete",   "staranno",   "starei",   "staresti",
    "starebbe",  "staremmo", "stareste",  "starebbero", "stavo",    "stavi",
    "stava",     "stavamo",  "stavate",   "stavano",    "stetti",   "stesti",
    "stette",    "stemmo",   "steste",    "stettero",   "stessi",   "stesse",
    "stessimo",  "stessero", "stando"};

class ItalianLanguage final : public SnowballLanguage {
 public:
  ItalianLanguage()
      : SnowballLanguage(data_model::LANGUAGE_ITALIAN, kItalianPunctuation,
                         kItalianStopWords, NormalizationForm::NFC, "",
                         "italian") {}

  data_model::Language Id() const override {
    return data_model::LANGUAGE_ITALIAN;
  }
  absl::string_view Name() const override { return "italian"; }
  const std::string& GetDefaultPunctuation() const override {
    return kItalianPunctuation;
  }
  const std::vector<std::string>& GetDefaultStopWords() const override {
    return kItalianStopWords;
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

#endif  // VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_ITALIAN_H_
