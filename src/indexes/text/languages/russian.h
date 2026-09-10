/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_RUSSIAN_H_
#define VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_RUSSIAN_H_

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/snowball_language.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "src/version.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {

// Russian: ASCII + common typographic + low-9 quotation marks.
// Punctuation characters sourced from Unicode CLDR Punctuation Exemplars (v46).
inline const std::string kRussianPunctuation =
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

// Russian stop words (159 words) — sourced from Apache Lucene.
inline const std::vector<std::string> kRussianStopWords{
    "и",      "в",       "во",      "не",      "что",     "он",      "на",
    "я",      "с",       "со",      "как",     "а",       "то",      "все",
    "она",    "так",     "его",     "но",      "да",      "ты",      "к",
    "у",      "же",      "вы",      "за",      "бы",      "по",      "только",
    "ее",     "мне",     "было",    "вот",     "от",      "меня",    "еще",
    "нет",    "о",       "из",      "ему",     "теперь",  "когда",   "даже",
    "ну",     "вдруг",   "ли",      "если",    "уже",     "или",     "ни",
    "быть",   "был",     "него",    "до",      "вас",     "нибудь",  "опять",
    "уж",     "вам",     "сказал",  "ведь",    "там",     "потом",   "себя",
    "ничего", "ей",      "может",   "они",     "тут",     "где",     "есть",
    "надо",   "ней",     "для",     "мы",      "тебя",    "их",      "чем",
    "была",   "сам",     "чтоб",    "без",     "будто",   "человек", "чего",
    "раз",    "тоже",    "себе",    "под",     "жизнь",   "будет",   "ж",
    "тогда",  "кто",     "этот",    "говорил", "того",    "потому",  "этого",
    "какой",  "совсем",  "ним",     "здесь",   "этом",    "один",    "почти",
    "мой",    "тем",     "чтобы",   "нее",     "кажется", "сейчас",  "были",
    "куда",   "зачем",   "сказать", "всех",    "никогда", "сегодня", "можно",
    "при",    "наконец", "два",     "об",      "другой",  "хоть",    "после",
    "над",    "больше",  "тот",     "через",   "эти",     "нас",     "про",
    "всего",  "них",     "какая",   "много",   "разве",   "сказала", "три",
    "эту",    "моя",     "впрочем", "хорошо",  "свою",    "этой",    "перед",
    "иногда", "лучше",   "чуть",    "том",     "нельзя",  "такой",   "им",
    "более",  "всегда",  "конечно", "всю",     "между"};

class RussianLanguage final : public SnowballLanguage {
 public:
  RussianLanguage()
      : SnowballLanguage(data_model::LANGUAGE_RUSSIAN, kRussianPunctuation,
                         kRussianStopWords, NormalizationForm::NFC, "",
                         "russian") {}

  data_model::Language Id() const override {
    return data_model::LANGUAGE_RUSSIAN;
  }
  absl::string_view Name() const override { return "russian"; }
  const std::string& GetDefaultPunctuation() const override {
    return kRussianPunctuation;
  }
  const std::vector<std::string>& GetDefaultStopWords() const override {
    return kRussianStopWords;
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

#endif  // VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_RUSSIAN_H_
