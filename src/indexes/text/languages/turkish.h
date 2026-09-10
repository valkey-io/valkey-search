/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_TURKISH_H_
#define VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_TURKISH_H_

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/snowball_language.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "src/version.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {

// Turkish: ASCII + common typographic punctuation.
// Punctuation characters sourced from Unicode CLDR Punctuation Exemplars (v46).
inline const std::string kTurkishPunctuation =
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

// Turkish stop words (209 words) — sourced from Apache Lucene.
inline const std::vector<std::string> kTurkishStopWords{
    "acaba",      "altmış",      "altı",      "ama",         "ancak",
    "arada",      "aslında",     "ayrıca",    "bana",        "bazı",
    "belki",      "ben",         "benden",    "beni",        "benim",
    "beri",       "beş",         "bile",      "bin",         "bir",
    "birçok",     "biri",        "birkaç",    "birkez",      "birşey",
    "birşeyi",    "biz",         "bize",      "bizden",      "bizi",
    "bizim",      "böyle",       "böylece",   "bu",          "buna",
    "bunda",      "bundan",      "bunlar",    "bunları",     "bunların",
    "bunu",       "bunun",       "burada",    "çok",         "çünkü",
    "da",         "daha",        "dahi",      "de",          "defa",
    "değil",      "diğer",       "diye",      "doksan",      "dokuz",
    "dolayı",     "dolayısıyla", "dört",      "edecek",      "eden",
    "ederek",     "edilecek",    "ediliyor",  "edilmesi",    "ediyor",
    "eğer",       "elli",        "en",        "etmesi",      "etti",
    "ettiği",     "ettiğini",    "gibi",      "göre",        "halen",
    "hangi",      "hatta",       "hem",       "henüz",       "hep",
    "hepsi",      "her",         "herhangi",  "herkesin",    "hiç",
    "hiçbir",     "için",        "iki",       "ile",         "ilgili",
    "ise",        "işte",        "itibaren",  "itibariyle",  "kadar",
    "karşın",     "katrilyon",   "kendi",     "kendilerine", "kendini",
    "kendisi",    "kendisine",   "kendisini", "kez",         "ki",
    "kim",        "kimden",      "kime",      "kimi",        "kimse",
    "kırk",       "milyar",      "milyon",    "mu",          "mü",
    "mı",         "nasıl",       "ne",        "neden",       "nedenle",
    "nerde",      "nerede",      "nereye",    "niye",        "niçin",
    "o",          "olan",        "olarak",    "oldu",        "olduğu",
    "olduğunu",   "olduklarını", "olmadı",    "olmadığı",    "olmak",
    "olması",     "olmayan",     "olmaz",     "olsa",        "olsun",
    "olup",       "olur",        "olursa",    "oluyor",      "on",
    "ona",        "ondan",       "onlar",     "onlardan",    "onları",
    "onların",    "onu",         "onun",      "otuz",        "oysa",
    "öyle",       "pek",         "rağmen",    "sadece",      "sanki",
    "sekiz",      "seksen",      "sen",       "senden",      "seni",
    "senin",      "siz",         "sizden",    "sizi",        "sizin",
    "şey",        "şeyden",      "şeyi",      "şeyler",      "şöyle",
    "şu",         "şuna",        "şunda",     "şundan",      "şunları",
    "şunu",       "tarafından",  "trilyon",   "tüm",         "üç",
    "üzere",      "var",         "vardı",     "ve",          "veya",
    "ya",         "yani",        "yapacak",   "yapılan",     "yapılması",
    "yapıyor",    "yapmak",      "yaptı",     "yaptığı",     "yaptığını",
    "yaptıkları", "yedi",        "yerine",    "yetmiş",      "yine",
    "yirmi",      "yoksa",       "yüz",       "zaten"};

class TurkishLanguage final : public SnowballLanguage {
 public:
  TurkishLanguage()
      : SnowballLanguage(data_model::LANGUAGE_TURKISH, kTurkishPunctuation,
                         kTurkishStopWords, NormalizationForm::NFC, "tr",
                         "turkish") {}

  data_model::Language Id() const override {
    return data_model::LANGUAGE_TURKISH;
  }
  absl::string_view Name() const override { return "turkish"; }
  const std::string& GetDefaultPunctuation() const override {
    return kTurkishPunctuation;
  }
  const std::vector<std::string>& GetDefaultStopWords() const override {
    return kTurkishStopWords;
  }
  NormalizationForm GetNormalizationForm() const override {
    return NormalizationForm::NFC;
  }
  absl::string_view CaseFoldLocale() const override { return "tr"; }
  vmsdk::ValkeyVersion MinRequiredVersion() const override {
    return valkey_search::kRelease13;
  }
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_TURKISH_H_
