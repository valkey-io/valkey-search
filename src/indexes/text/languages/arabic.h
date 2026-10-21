/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_ARABIC_H_
#define VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_ARABIC_H_

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/snowball_language.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "src/version.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {

// Arabic: ASCII + common typographic + Arabic comma, semicolon, question mark.
// Language-specific characters come from the Unicode CLDR v46 punctuation
// exemplars; the common typographic set is shared by every non-English
// language.
inline const std::string kArabicPunctuation =
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
    "\xd8\x8c"      // ، U+060C ARABIC COMMA
    "\xd8\x9b"      // ؛ U+061B ARABIC SEMICOLON
    "\xd8\x9f";     // ؟ U+061F ARABIC QUESTION MARK

// Arabic stop words (119 words) — sourced from Apache Lucene.
inline const std::vector<std::string> kArabicStopWords{
    "من",   "ومن",  "منها",  "منه",  "في",   "وفي",  "فيها",  "فيه",  "و",
    "ف",    "ثم",   "او",    "أو",   "ب",    "بها",  "به",    "ا",    "أ",
    "اى",   "اي",   "أي",    "أى",   "لا",   "ولا",  "الا",   "ألا",  "إلا",
    "لكن",  "ما",   "وما",   "كما",  "فما",  "عن",   "مع",    "اذا",  "إذا",
    "ان",   "أن",   "إن",    "انها", "أنها", "إنها", "انه",   "أنه",  "إنه",
    "بان",  "بأن",  "فان",   "فأن",  "وان",  "وأن",  "وإن",   "التى", "التي",
    "الذى", "الذي", "الذين", "الى",  "الي",  "إلى",  "إلي",   "على",  "عليها",
    "عليه", "اما",  "أما",   "إما",  "ايضا", "أيضا", "كل",    "وكل",  "لم",
    "ولم",  "لن",   "ولن",   "هى",   "هي",   "هو",   "وهى",   "وهي",  "وهو",
    "فهى",  "فهي",  "فهو",   "انت",  "أنت",  "لك",   "لها",   "له",   "هذه",
    "هذا",  "تلك",  "ذلك",   "هناك", "كانت", "كان",  "يكون",  "تكون", "وكانت",
    "وكان", "غير",  "بعض",   "قد",   "نحو",  "بين",  "بينما", "منذ",  "ضمن",
    "حيث",  "الان", "الآن",  "خلال", "بعد",  "قبل",  "حتى",   "عند",  "عندما",
    "لدى",  "جميع"};

class ArabicLanguage final : public SnowballLanguage {
 public:
  ArabicLanguage()
      : SnowballLanguage(data_model::LANGUAGE_ARABIC, kArabicPunctuation,
                         kArabicStopWords, NormalizationForm::NFKC, "",
                         "arabic", DelimiterScope::kUnicode) {}

  absl::string_view Name() const override { return "arabic"; }
  vmsdk::ValkeyVersion MinRequiredVersion() const override {
    return valkey_search::kRelease13;
  }
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_ARABIC_H_
