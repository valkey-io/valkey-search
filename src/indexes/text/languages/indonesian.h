/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_INDONESIAN_H_
#define VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_INDONESIAN_H_

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/snowball_language.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "src/version.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {

// Indonesian: ASCII + common typographic punctuation.
// Punctuation characters sourced from Unicode CLDR Punctuation Exemplars (v46).
inline const std::string kIndonesianPunctuation =
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

// Indonesian stop words (93 words) — sourced from Apache Lucene.
inline const std::vector<std::string> kIndonesianStopWords{
    "yang",     "dan",      "di",        "dari",      "ini",       "pada",
    "kepada",   "ada",      "adalah",    "dengan",    "untuk",     "dalam",
    "oleh",     "sebagai",  "juga",      "ke",        "atau",      "tidak",
    "itu",      "sebuah",   "tersebut",  "dapat",     "ia",        "telah",
    "satu",     "memiliki", "mereka",    "bahwa",     "lebih",     "karena",
    "seorang",  "akan",     "seperti",   "secara",    "kemudian",  "beberapa",
    "banyak",   "antara",   "setelah",   "yaitu",     "hanya",     "hingga",
    "serta",    "sama",     "dia",       "tetapi",    "namun",     "melalui",
    "bisa",     "sehingga", "ketika",    "suatu",     "sendiri",   "bagi",
    "semua",    "harus",    "setiap",    "maka",      "maupun",    "tanpa",
    "saja",     "jika",     "bukan",     "belum",     "sedangkan", "yakni",
    "meskipun", "hampir",   "kita",      "demikian",  "daripada",  "apa",
    "ialah",    "sana",     "begitu",    "seseorang", "selain",    "terlalu",
    "ataupun",  "saya",     "bila",      "bagaimana", "tapi",      "apabila",
    "kalau",    "kami",     "melainkan", "boleh",     "aku",       "anda",
    "kamu",     "beliau",   "kalian"};

class IndonesianLanguage final : public SnowballLanguage {
 public:
  IndonesianLanguage()
      : SnowballLanguage(data_model::LANGUAGE_INDONESIAN,
                         kIndonesianPunctuation, kIndonesianStopWords,
                         NormalizationForm::NFC, "", "indonesian") {}

  data_model::Language Id() const override {
    return data_model::LANGUAGE_INDONESIAN;
  }
  absl::string_view Name() const override { return "indonesian"; }
  const std::string& GetDefaultPunctuation() const override {
    return kIndonesianPunctuation;
  }
  const std::vector<std::string>& GetDefaultStopWords() const override {
    return kIndonesianStopWords;
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

#endif  // VALKEY_SEARCH_INDEXES_TEXT_LANGUAGES_INDONESIAN_H_
