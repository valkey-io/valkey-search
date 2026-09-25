/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_SPANISH_H_
#define VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_SPANISH_H_

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/snowball_language.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "src/version.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {

// Spanish: ASCII + common typographic + inverted punctuation marks.
// Punctuation characters sourced from Unicode CLDR Punctuation Exemplars (v46).
inline const std::string kSpanishPunctuation =
    ",.<>{}[]\"':;!@#$%^&*()-+=~/\\|?"
    "\xe2\x80\x93"  // – U+2013 EN DASH
    "\xe2\x80\x94"  // — U+2014 EM DASH
    "\xe2\x80\xa6"  // … U+2026 HORIZONTAL ELLIPSIS
    "\xe2\x80\x98"  // ' U+2018 LEFT SINGLE QUOTATION MARK
    "\xe2\x80\x99"  // ' U+2019 RIGHT SINGLE QUOTATION MARK
    "\xe2\x80\x9c"  // " U+201C LEFT DOUBLE QUOTATION MARK
    "\xe2\x80\x9d"  // " U+201D RIGHT DOUBLE QUOTATION MARK
    "\xc2\xab"      // « U+00AB LEFT-POINTING DOUBLE ANGLE QUOTATION MARK
    "\xc2\xbb"      // » U+00BB RIGHT-POINTING DOUBLE ANGLE QUOTATION MARK
    "\xc2\xa1"      // ¡ U+00A1 INVERTED EXCLAMATION MARK
    "\xc2\xbf";     // ¿ U+00BF INVERTED QUESTION MARK

// Spanish stop words (308 words) — sourced from Apache Lucene.
inline const std::vector<std::string> kSpanishStopWords{
    "de",        "la",         "que",          "el",          "en",
    "y",         "a",          "los",          "del",         "se",
    "las",       "por",        "un",           "para",        "con",
    "no",        "una",        "su",           "al",          "lo",
    "como",      "más",        "pero",         "sus",         "le",
    "ya",        "o",          "este",         "sí",          "porque",
    "esta",      "entre",      "cuando",       "muy",         "sin",
    "sobre",     "también",    "me",           "hasta",       "hay",
    "donde",     "quien",      "desde",        "todo",        "nos",
    "durante",   "todos",      "uno",          "les",         "ni",
    "contra",    "otros",      "ese",          "eso",         "ante",
    "ellos",     "e",          "esto",         "mí",          "antes",
    "algunos",   "qué",        "unos",         "yo",          "otro",
    "otras",     "otra",       "él",           "tanto",       "esa",
    "estos",     "mucho",      "quienes",      "nada",        "muchos",
    "cual",      "poco",       "ella",         "estar",       "estas",
    "algunas",   "algo",       "nosotros",     "mi",          "mis",
    "tú",        "te",         "ti",           "tu",          "tus",
    "ellas",     "nosotras",   "vosotros",     "vosotras",    "os",
    "mío",       "mía",        "míos",         "mías",        "tuyo",
    "tuya",      "tuyos",      "tuyas",        "suyo",        "suya",
    "suyos",     "suyas",      "nuestro",      "nuestra",     "nuestros",
    "nuestras",  "vuestro",    "vuestra",      "vuestros",    "vuestras",
    "esos",      "esas",       "estoy",        "estás",       "está",
    "estamos",   "estáis",     "están",        "esté",        "estés",
    "estemos",   "estéis",     "estén",        "estaré",      "estarás",
    "estará",    "estaremos",  "estaréis",     "estarán",     "estaría",
    "estarías",  "estaríamos", "estaríais",    "estarían",    "estaba",
    "estabas",   "estábamos",  "estabais",     "estaban",     "estuve",
    "estuviste", "estuvo",     "estuvimos",    "estuvisteis", "estuvieron",
    "estuviera", "estuvieras", "estuviéramos", "estuvierais", "estuvieran",
    "estuviese", "estuvieses", "estuviésemos", "estuvieseis", "estuviesen",
    "estando",   "estado",     "estada",       "estados",     "estadas",
    "estad",     "he",         "has",          "ha",          "hemos",
    "habéis",    "han",        "haya",         "hayas",       "hayamos",
    "hayáis",    "hayan",      "habré",        "habrás",      "habrá",
    "habremos",  "habréis",    "habrán",       "habría",      "habrías",
    "habríamos", "habríais",   "habrían",      "había",       "habías",
    "habíamos",  "habíais",    "habían",       "hube",        "hubiste",
    "hubo",      "hubimos",    "hubisteis",    "hubieron",    "hubiera",
    "hubieras",  "hubiéramos", "hubierais",    "hubieran",    "hubiese",
    "hubieses",  "hubiésemos", "hubieseis",    "hubiesen",    "habiendo",
    "habido",    "habida",     "habidos",      "habidas",     "soy",
    "eres",      "es",         "somos",        "sois",        "son",
    "sea",       "seas",       "seamos",       "seáis",       "sean",
    "seré",      "serás",      "será",         "seremos",     "seréis",
    "serán",     "sería",      "serías",       "seríamos",    "seríais",
    "serían",    "era",        "eras",         "éramos",      "erais",
    "eran",      "fui",        "fuiste",       "fue",         "fuimos",
    "fuisteis",  "fueron",     "fuera",        "fueras",      "fuéramos",
    "fuerais",   "fueran",     "fuese",        "fueses",      "fuésemos",
    "fueseis",   "fuesen",     "siendo",       "sido",        "tengo",
    "tienes",    "tiene",      "tenemos",      "tenéis",      "tienen",
    "tenga",     "tengas",     "tengamos",     "tengáis",     "tengan",
    "tendré",    "tendrás",    "tendrá",       "tendremos",   "tendréis",
    "tendrán",   "tendría",    "tendrías",     "tendríamos",  "tendríais",
    "tendrían",  "tenía",      "tenías",       "teníamos",    "teníais",
    "tenían",    "tuve",       "tuviste",      "tuvo",        "tuvimos",
    "tuvisteis", "tuvieron",   "tuviera",      "tuvieras",    "tuviéramos",
    "tuvierais", "tuvieran",   "tuviese",      "tuvieses",    "tuviésemos",
    "tuvieseis", "tuviesen",   "teniendo",     "tenido",      "tenida",
    "tenidos",   "tenidas",    "tened"};

class SpanishLanguage final : public SnowballLanguage {
 public:
  SpanishLanguage()
      : SnowballLanguage(data_model::LANGUAGE_SPANISH, kSpanishPunctuation,
                         kSpanishStopWords, NormalizationForm::NFC, "",
                         "spanish") {}

  data_model::Language Id() const override {
    return data_model::LANGUAGE_SPANISH;
  }
  absl::string_view Name() const override { return "spanish"; }
  const std::string& GetDefaultPunctuation() const override {
    return kSpanishPunctuation;
  }
  const std::vector<std::string>& GetDefaultStopWords() const override {
    return kSpanishStopWords;
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

#endif  // VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_SPANISH_H_
