/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/unicode_normalizer.h"

#include <string>

#include "absl/strings/string_view.h"
#include "gtest/gtest.h"
#include "src/utils/scanner.h"

namespace valkey_search::indexes::text {
namespace {

// Byte-literal helpers so the cases never depend on this source file's own
// encoding. Code points are written as their exact UTF-8 byte sequences.
// NOTE: C++ hex escapes are greedy, so a \xNN escape immediately followed by a
// hex-digit letter must be split across adjacent string literals.
//
// e + combining acute (U+0065 U+0301) -- decomposed "é"
constexpr absl::string_view kEDecomposed = "e\xCC\x81";
// precomposed "é" (U+00E9)
constexpr absl::string_view kEPrecomposed = "\xC3\xA9";
// "résumé" precomposed (6 code points) / decomposed (8 code points)
constexpr absl::string_view kResumePrecomposed = "r\xC3\xA9sum\xC3\xA9";
constexpr absl::string_view kResumeDecomposed = "re\xCC\x81sume\xCC\x81";
// Arabic kaf: base letter U+0643 and its initial presentation form U+FEDB.
// NFKC maps the presentation form back to the base letter.
constexpr absl::string_view kKafBase = "\xD9\x83";
constexpr absl::string_view kKafPresentationForm = "\xEF\xBB\x9B";
// Latin small ligature fi (U+FB01) and its NFKC expansion "fi".
constexpr absl::string_view kLigatureFi = "\xEF\xAC\x81";
// "été" with MIXED encodings in one token: precomposed é (C3 A9) + "t" (74) +
// decomposed e + combining acute (65 CC 81). NFC must produce a uniform
// precomposed result regardless of each character's input form.
constexpr absl::string_view kEteMixed =
    "\xC3\xA9t"
    "e\xCC\x81";
constexpr absl::string_view kEtePrecomposed = "\xC3\xA9t\xC3\xA9";

struct NormalizeCase {
  std::string name;
  std::string input;
  NormalizationForm form;
  std::string expected;
};

class NormalizeTest : public ::testing::TestWithParam<NormalizeCase> {};

TEST_P(NormalizeTest, ProducesExpectedForm) {
  const auto& c = GetParam();
  EXPECT_EQ(UnicodeNormalizer::Normalize(c.input, c.form), c.expected)
      << "case: " << c.name;
}

// Normalization applied to its own output is a no-op for the same form.
TEST_P(NormalizeTest, IsIdempotent) {
  const auto& c = GetParam();
  std::string once = UnicodeNormalizer::Normalize(c.input, c.form);
  EXPECT_EQ(UnicodeNormalizer::Normalize(once, c.form), once)
      << "case: " << c.name;
}

INSTANTIATE_TEST_SUITE_P(
    UnicodeNormalizer, NormalizeTest,
    ::testing::ValuesIn<NormalizeCase>({
        // ASCII is unchanged for every form (no decomposition exists).
        {"ascii_nfc", "hello", NormalizationForm::NFC, "hello"},
        {"ascii_nfd", "hello", NormalizationForm::NFD, "hello"},
        {"ascii_nfkc", "hello", NormalizationForm::NFKC, "hello"},
        {"ascii_nfkd", "hello", NormalizationForm::NFKD, "hello"},

        // Empty input -> empty output.
        {"empty_nfc", "", NormalizationForm::NFC, ""},

        // NFC composes a decomposed sequence; NFD decomposes a precomposed one.
        {"nfc_compose_e_acute", std::string(kEDecomposed),
         NormalizationForm::NFC, std::string(kEPrecomposed)},
        {"nfd_decompose_e_acute", std::string(kEPrecomposed),
         NormalizationForm::NFD, std::string(kEDecomposed)},

        // résumé round-trips between the 6- and 8-code-point representations.
        {"nfc_resume", std::string(kResumeDecomposed), NormalizationForm::NFC,
         std::string(kResumePrecomposed)},
        {"nfd_resume", std::string(kResumePrecomposed), NormalizationForm::NFD,
         std::string(kResumeDecomposed)},

        // NFKC collapses the Arabic presentation form to the base letter;
        // NFC leaves it unchanged (proving NFC != NFKC, which is why Task 9
        // needs NFKC for Arabic).
        {"nfkc_arabic_presentation_form", std::string(kKafPresentationForm),
         NormalizationForm::NFKC, std::string(kKafBase)},
        {"nfc_keeps_arabic_presentation_form",
         std::string(kKafPresentationForm), NormalizationForm::NFC,
         std::string(kKafPresentationForm)},

        // NFKC expands the fi ligature; NFC leaves it intact.
        {"nfkc_fi_ligature", std::string(kLigatureFi), NormalizationForm::NFKC,
         "fi"},
        {"nfc_keeps_fi_ligature", std::string(kLigatureFi),
         NormalizationForm::NFC, std::string(kLigatureFi)},

        // Mixed precomposed + decomposed in one token: NFC unifies both to the
        // precomposed form, so concatenated text from different sources
        // matches.
        {"nfc_mixed_forms_unify", std::string(kEteMixed),
         NormalizationForm::NFC, std::string(kEtePrecomposed)},
    }),
    [](const ::testing::TestParamInfo<NormalizeCase>& info) {
      return info.param.name;
    });

// NFC reorders combining marks into canonical order. Input has the acute
// (above, ccc=230) before the dot-below (below, ccc=220); canonical order puts
// the lower-class mark first. This is distinct from simple composition.
TEST(UnicodeNormalizerReorderTest, NfcReordersCombiningMarks) {
  // "a" + U+0301 (acute, above) + U+0323 (dot below)
  const std::string input = "a\xCC\x81\xCC\xA3";
  // Canonical: "a" composes with the below mark first, then the above mark.
  // U+1EA1 (a with dot below) + U+0301.
  const std::string expected = "\xE1\xBA\xA1\xCC\x81";
  EXPECT_EQ(UnicodeNormalizer::Normalize(input, NormalizationForm::NFC),
            expected);
}

// résumé has 6 code points in NFC and 8 in NFD.
TEST(UnicodeNormalizerReorderTest, ResumeCodePointCounts) {
  std::string nfc = UnicodeNormalizer::Normalize(std::string(kResumeDecomposed),
                                                 NormalizationForm::NFC);
  std::string nfd = UnicodeNormalizer::Normalize(
      std::string(kResumePrecomposed), NormalizationForm::NFD);
  EXPECT_EQ(utils::Scanner::CodePointCount(nfc), 6u);
  EXPECT_EQ(utils::Scanner::CodePointCount(nfd), 8u);
}

// CaseFoldInPlace is unchanged by this spec; assert representative folding
// still works (German ß -> ss) so a refactor that touched it would be caught.
TEST(CaseFoldInPlaceTest, UnchangedFoldingBehavior) {
  std::string s =
      "Stra\xC3\x9F"
      "e";  // "Straße" (split avoids greedy \x escape)
  UnicodeNormalizer::CaseFoldInPlace(s);
  EXPECT_EQ(s, "strasse");
}

// Turkish dotted/dotless I, folded LOCALE-INDEPENDENTLY (our CaseFoldInPlace is
// the Unicode default fold, not Turkish-locale-aware). This documents and locks
// in current behavior; when locale-aware folding is added later, this test will
// flag the change so it gets compatibility treatment.
//   İ (U+0130, capital I with dot above) -> i (U+0069) + combining dot above
//      (U+0307). The default fold decomposes it; it does NOT become plain "i".
//   ı (U+0131, dotless small i) -> unchanged (no case mapping under default
//   fold).
TEST(CaseFoldInPlaceTest, TurkishDottedAndDotlessILocaleIndependent) {
  std::string dotted_capital = "\xC4\xB0";  // İ U+0130
  UnicodeNormalizer::CaseFoldInPlace(dotted_capital);
  EXPECT_EQ(dotted_capital, "i\xCC\x87");  // U+0069 U+0307

  std::string dotless_small = "\xC4\xB1";  // ı U+0131
  UnicodeNormalizer::CaseFoldInPlace(dotless_small);
  EXPECT_EQ(dotless_small, "\xC4\xB1");  // unchanged
}

}  // namespace
}  // namespace valkey_search::indexes::text
