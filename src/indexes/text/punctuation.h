/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

//
// Per-language default punctuation character sets.
//
// Punctuation characters sourced from the Unicode CLDR Punctuation Exemplars
// (v46). Licensed under the Unicode License V3.
// https://www.unicode.org/cldr/charts/46/by_type/core_data.alphabetic_information.punctuation.html
// https://www.unicode.org/license.txt
//
// Each language's default punctuation extends the ASCII base set with
// script-specific and typographic punctuation characters commonly used as
// word boundaries in that language's written text.
//

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_PUNCTUATION_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_PUNCTUATION_H_

#include <bitset>
#include <cstdint>
#include <string>

#include "absl/container/flat_hash_set.h"
#include "src/index_schema.pb.h"
#include "src/utils/scanner.h"

namespace valkey_search::indexes::text {

// Base ASCII punctuation used as the default for English and
// LANGUAGE_UNSPECIFIED
inline const std::string kAsciiPunctuation = ",.<>{}[]\"':;!@#$%^&*()-+=~/\\|?";

// Common typographic punctuation shared across Latin/Cyrillic-script languages.
// U+2013 EN DASH, U+2014 EM DASH, U+2026 HORIZONTAL ELLIPSIS,
// U+2018 LEFT SINGLE QUOTATION MARK, U+2019 RIGHT SINGLE QUOTATION MARK,
// U+201C LEFT DOUBLE QUOTATION MARK, U+201D RIGHT DOUBLE QUOTATION MARK,
// U+00AB LEFT-POINTING DOUBLE ANGLE QUOTATION MARK (guillemet),
// U+00BB RIGHT-POINTING DOUBLE ANGLE QUOTATION MARK (guillemet)
inline const std::string kCommonMultiBytePunctuation =
    "\xe2\x80\x93"  // – U+2013 EN DASH
    "\xe2\x80\x94"  // — U+2014 EM DASH
    "\xe2\x80\xa6"  // … U+2026 HORIZONTAL ELLIPSIS
    "\xe2\x80\x98"  // ' U+2018 LEFT SINGLE QUOTATION MARK
    "\xe2\x80\x99"  // ' U+2019 RIGHT SINGLE QUOTATION MARK
    "\xe2\x80\x9c"  // " U+201C LEFT DOUBLE QUOTATION MARK
    "\xe2\x80\x9d"  // " U+201D RIGHT DOUBLE QUOTATION MARK
    "\xc2\xab"      // « U+00AB LEFT-POINTING DOUBLE ANGLE QUOTATION MARK
    "\xc2\xbb";     // » U+00BB RIGHT-POINTING DOUBLE ANGLE QUOTATION MARK

// Spanish-specific additions:
// U+00A1 INVERTED EXCLAMATION MARK, U+00BF INVERTED QUESTION MARK
inline const std::string kSpanishExtra =
    "\xc2\xa1"   // ¡ U+00A1
    "\xc2\xbf";  // ¿ U+00BF

// German/Russian-specific additions:
// U+201E DOUBLE LOW-9 QUOTATION MARK, U+201A SINGLE LOW-9 QUOTATION MARK
inline const std::string kLowQuoteExtra =
    "\xe2\x80\x9e"   // „ U+201E DOUBLE LOW-9 QUOTATION MARK
    "\xe2\x80\x9a";  // ‚ U+201A SINGLE LOW-9 QUOTATION MARK

// Arabic-specific punctuation:
// U+060C ARABIC COMMA, U+061B ARABIC SEMICOLON, U+061F ARABIC QUESTION MARK
inline const std::string kArabicExtra =
    "\xd8\x8c"   // ، U+060C ARABIC COMMA
    "\xd8\x9b"   // ؛ U+061B ARABIC SEMICOLON
    "\xd8\x9f";  // ؟ U+061F ARABIC QUESTION MARK

// --- Composed per-language punctuation strings ---

// English: ASCII only (backward compatibility)
inline const std::string kEnglishPunctuation = kAsciiPunctuation;

// French: ASCII + common typographic
inline const std::string kFrenchPunctuation =
    kAsciiPunctuation + kCommonMultiBytePunctuation;

// German: ASCII + common typographic + low quotes
inline const std::string kGermanPunctuation =
    kAsciiPunctuation + kCommonMultiBytePunctuation + kLowQuoteExtra;

// Spanish: ASCII + common typographic + inverted marks
inline const std::string kSpanishPunctuation =
    kAsciiPunctuation + kCommonMultiBytePunctuation + kSpanishExtra;

// Italian: ASCII + common typographic
inline const std::string kItalianPunctuation =
    kAsciiPunctuation + kCommonMultiBytePunctuation;

// Portuguese: ASCII + common typographic
inline const std::string kPortuguesePunctuation =
    kAsciiPunctuation + kCommonMultiBytePunctuation;

// Russian: ASCII + common typographic + low quotes
inline const std::string kRussianPunctuation =
    kAsciiPunctuation + kCommonMultiBytePunctuation + kLowQuoteExtra;

// Swedish: ASCII + common typographic
inline const std::string kSwedishPunctuation =
    kAsciiPunctuation + kCommonMultiBytePunctuation;

// Turkish: ASCII + common typographic
inline const std::string kTurkishPunctuation =
    kAsciiPunctuation + kCommonMultiBytePunctuation;

// Dutch: ASCII + common typographic
inline const std::string kDutchPunctuation =
    kAsciiPunctuation + kCommonMultiBytePunctuation;

// Indonesian: ASCII + common typographic
inline const std::string kIndonesianPunctuation =
    kAsciiPunctuation + kCommonMultiBytePunctuation;

// Arabic: ASCII + Arabic-specific (no guillemets — Arabic uses its own marks)
inline const std::string kArabicPunctuation = kAsciiPunctuation + kArabicExtra;

// Returns the default punctuation string for the given language.
// Returns English (ASCII-only) punctuation for LANGUAGE_UNSPECIFIED.
inline const std::string& GetDefaultPunctuation(data_model::Language language) {
  switch (language) {
    case data_model::LANGUAGE_FRENCH:
      return kFrenchPunctuation;
    case data_model::LANGUAGE_GERMAN:
      return kGermanPunctuation;
    case data_model::LANGUAGE_SPANISH:
      return kSpanishPunctuation;
    case data_model::LANGUAGE_ITALIAN:
      return kItalianPunctuation;
    case data_model::LANGUAGE_PORTUGUESE:
      return kPortuguesePunctuation;
    case data_model::LANGUAGE_RUSSIAN:
      return kRussianPunctuation;
    case data_model::LANGUAGE_SWEDISH:
      return kSwedishPunctuation;
    case data_model::LANGUAGE_TURKISH:
      return kTurkishPunctuation;
    case data_model::LANGUAGE_DUTCH:
      return kDutchPunctuation;
    case data_model::LANGUAGE_INDONESIAN:
      return kIndonesianPunctuation;
    case data_model::LANGUAGE_ARABIC:
      return kArabicPunctuation;
    case data_model::LANGUAGE_ENGLISH:
    case data_model::LANGUAGE_UNSPECIFIED:
    default:
      return kEnglishPunctuation;
  }
}

// Punctuation lookup set. ASCII code points use a bitset; non-ASCII code
// points (e.g. Arabic ، U+060C) use a hash set.
struct PunctuationSet {
  std::bitset<128> ascii;                   // Code points 0x00..0x7F
  absl::flat_hash_set<uint32_t> non_ascii;  // Code points >= 0x80

  bool Contains(uint32_t cp) const {
    if (utils::Scanner::IsAscii(cp)) return ascii[cp];
    return non_ascii.contains(cp);
  }
};

// Build a PunctuationSet from a punctuation string. Iterates as code points
// (not bytes) so multi-byte chars like U+060C are stored correctly.
// ASCII whitespace and control characters are always included as boundaries.
inline PunctuationSet BuildPunctuationSet(const std::string& punctuation) {
  PunctuationSet result;
  for (int i = 0; i < 128; ++i) {
    if (std::isspace(static_cast<unsigned char>(i)) ||
        std::iscntrl(static_cast<unsigned char>(i))) {
      result.ascii.set(i);
    }
  }
  utils::Scanner scanner(punctuation);
  utils::Scanner::Char cp;
  while ((cp = scanner.NextUtf8()) != utils::Scanner::kEOF) {
    if (utils::Scanner::IsAscii(cp)) {
      result.ascii.set(cp);
    } else {
      result.non_ascii.insert(cp);
    }
  }
  return result;
}

}  // namespace valkey_search::indexes::text

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_PUNCTUATION_H_
