/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_PORTUGUESE_H_
#define VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_PORTUGUESE_H_

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/snowball_language.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "src/version.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {

// Portuguese: ASCII + common typographic punctuation.
// Punctuation characters sourced from Unicode CLDR Punctuation Exemplars (v46).
inline const std::string kPortuguesePunctuation =
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

// Portuguese stop words (203 words) — sourced from Apache Lucene.
inline const std::vector<std::string> kPortugueseStopWords{
    "de",          "a",         "o",
    "que",         "e",         "do",
    "da",          "em",        "um",
    "para",        "com",       "não",
    "uma",         "os",        "no",
    "se",          "na",        "por",
    "mais",        "as",        "dos",
    "como",        "mas",       "ao",
    "ele",         "das",       "à",
    "seu",         "sua",       "ou",
    "quando",      "muito",     "nos",
    "já",          "eu",        "também",
    "só",          "pelo",      "pela",
    "até",         "isso",      "ela",
    "entre",       "depois",    "sem",
    "mesmo",       "aos",       "seus",
    "quem",        "nas",       "me",
    "esse",        "eles",      "você",
    "essa",        "num",       "nem",
    "suas",        "meu",       "às",
    "minha",       "numa",      "pelos",
    "elas",        "qual",      "nós",
    "lhe",         "deles",     "essas",
    "esses",       "pelas",     "este",
    "dele",        "tu",        "te",
    "vocês",       "vos",       "lhes",
    "meus",        "minhas",    "teu",
    "tua",         "teus",      "tuas",
    "nosso",       "nossa",     "nossos",
    "nossas",      "dela",      "delas",
    "esta",        "estes",     "estas",
    "aquele",      "aquela",    "aqueles",
    "aquelas",     "isto",      "aquilo",
    "estou",       "está",      "estamos",
    "estão",       "estive",    "esteve",
    "estivemos",   "estiveram", "estava",
    "estávamos",   "estavam",   "estivera",
    "estivéramos", "esteja",    "estejamos",
    "estejam",     "estivesse", "estivéssemos",
    "estivessem",  "estiver",   "estivermos",
    "estiverem",   "hei",       "há",
    "havemos",     "hão",       "houve",
    "houvemos",    "houveram",  "houvera",
    "houvéramos",  "haja",      "hajamos",
    "hajam",       "houvesse",  "houvéssemos",
    "houvessem",   "houver",    "houvermos",
    "houverem",    "houverei",  "houverá",
    "houveremos",  "houverão",  "houveria",
    "houveríamos", "houveriam", "sou",
    "somos",       "são",       "era",
    "éramos",      "eram",      "fui",
    "foi",         "fomos",     "foram",
    "fora",        "fôramos",   "seja",
    "sejamos",     "sejam",     "fosse",
    "fôssemos",    "fossem",    "for",
    "formos",      "forem",     "serei",
    "será",        "seremos",   "serão",
    "seria",       "seríamos",  "seriam",
    "tenho",       "tem",       "temos",
    "tém",         "tinha",     "tínhamos",
    "tinham",      "tive",      "teve",
    "tivemos",     "tiveram",   "tivera",
    "tivéramos",   "tenha",     "tenhamos",
    "tenham",      "tivesse",   "tivéssemos",
    "tivessem",    "tiver",     "tivermos",
    "tiverem",     "terei",     "terá",
    "teremos",     "terão",     "teria",
    "teríamos",    "teriam"};

class PortugueseLanguage final : public SnowballLanguage {
 public:
  PortugueseLanguage()
      : SnowballLanguage(data_model::LANGUAGE_PORTUGUESE,
                         kPortuguesePunctuation, kPortugueseStopWords,
                         NormalizationForm::NFC, "", "portuguese") {}

  data_model::Language Id() const override {
    return data_model::LANGUAGE_PORTUGUESE;
  }
  absl::string_view Name() const override { return "portuguese"; }
  const std::string& GetDefaultPunctuation() const override {
    return kPortuguesePunctuation;
  }
  const std::vector<std::string>& GetDefaultStopWords() const override {
    return kPortugueseStopWords;
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

#endif  // VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_PORTUGUESE_H_
