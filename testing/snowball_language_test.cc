/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/snowball_language.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/language.h"
#include "src/indexes/text/language_registry.h"
#include "src/indexes/text/languages/arabic.h"
#include "src/indexes/text/languages/english.h"
#include "src/indexes/text/languages/french.h"
#include "src/indexes/text/languages/german.h"
#include "src/indexes/text/languages/spanish.h"
#include "src/indexes/text/languages/turkish.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {
namespace {

class TestEnglishLanguage final : public SnowballLanguage {
 public:
  TestEnglishLanguage()
      : SnowballLanguage(data_model::LANGUAGE_ENGLISH, kEnglishPunctuation,
                         kEnglishStopWords, NormalizationForm::NFC, "",
                         "english") {}

  data_model::Language Id() const override {
    return data_model::LANGUAGE_ENGLISH;
  }
  absl::string_view Name() const override { return "english"; }
  const std::string& GetDefaultPunctuation() const override {
    return kEnglishPunctuation;
  }
  const std::vector<std::string>& GetDefaultStopWords() const override {
    return kEnglishStopWords;
  }
  NormalizationForm GetNormalizationForm() const override {
    return NormalizationForm::NFC;
  }
  absl::string_view CaseFoldLocale() const override { return ""; }
  vmsdk::ValkeyVersion MinRequiredVersion() const override {
    return vmsdk::ValkeyVersion(0, 0, 0);
  }
};

class TestTurkishLanguage final : public SnowballLanguage {
 public:
  TestTurkishLanguage()
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
    return vmsdk::ValkeyVersion(1, 3, 0);
  }
};

class TestArabicLanguage final : public SnowballLanguage {
 public:
  TestArabicLanguage()
      : SnowballLanguage(data_model::LANGUAGE_ARABIC, kArabicPunctuation,
                         kArabicStopWords, NormalizationForm::NFKC, "",
                         "arabic") {}

  data_model::Language Id() const override {
    return data_model::LANGUAGE_ARABIC;
  }
  absl::string_view Name() const override { return "arabic"; }
  const std::string& GetDefaultPunctuation() const override {
    return kArabicPunctuation;
  }
  const std::vector<std::string>& GetDefaultStopWords() const override {
    return kArabicStopWords;
  }
  NormalizationForm GetNormalizationForm() const override {
    return NormalizationForm::NFKC;
  }
  absl::string_view CaseFoldLocale() const override { return ""; }
  vmsdk::ValkeyVersion MinRequiredVersion() const override {
    return vmsdk::ValkeyVersion(1, 3, 0);
  }
};

class TestFrenchLanguage final : public SnowballLanguage {
 public:
  TestFrenchLanguage()
      : SnowballLanguage(data_model::LANGUAGE_FRENCH, kFrenchPunctuation,
                         kFrenchStopWords, NormalizationForm::NFC, "",
                         "french") {}

  data_model::Language Id() const override {
    return data_model::LANGUAGE_FRENCH;
  }
  absl::string_view Name() const override { return "french"; }
  const std::string& GetDefaultPunctuation() const override {
    return kFrenchPunctuation;
  }
  const std::vector<std::string>& GetDefaultStopWords() const override {
    return kFrenchStopWords;
  }
  NormalizationForm GetNormalizationForm() const override {
    return NormalizationForm::NFC;
  }
  absl::string_view CaseFoldLocale() const override { return ""; }
  vmsdk::ValkeyVersion MinRequiredVersion() const override {
    return vmsdk::ValkeyVersion(1, 3, 0);
  }
};

class TestGermanLanguage final : public SnowballLanguage {
 public:
  TestGermanLanguage()
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
    return vmsdk::ValkeyVersion(1, 3, 0);
  }
};

class SnowballLanguageTest : public ::testing::Test {
 protected:
  TestEnglishLanguage english_;
  TestTurkishLanguage turkish_;
  TestArabicLanguage arabic_;
};

// --- Tokenize: full pipeline ---

TEST_F(SnowballLanguageTest, EmptyString) {
  auto result = english_.Tokenize("");
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result->empty());
}

TEST_F(SnowballLanguageTest, OnlyPunctuation) {
  auto result = english_.Tokenize("   \t\n!@#$%^&*()   ");
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result->empty());
}

TEST_F(SnowballLanguageTest, PunctuationSplitting) {
  auto result = english_.Tokenize("hello,world!nice-day.today");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>(
                         {"hello", "world", "nice", "day", "today"}));
}

TEST_F(SnowballLanguageTest, CaseFolding) {
  auto result = english_.Tokenize("HELLO World miXeD");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world", "mixed"}));
}

TEST_F(SnowballLanguageTest, StopWordsFiltered) {
  auto result = english_.Tokenize("the cat and dog");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"cat", "dog"}));
}

TEST_F(SnowballLanguageTest, AllStopWordsProducesEmpty) {
  auto result = english_.Tokenize("the and or is");
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result->empty());
}

TEST_F(SnowballLanguageTest, InvalidUtf8ReturnsError) {
  auto result = english_.Tokenize("hello \xFF\xFE world");
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_F(SnowballLanguageTest, Utf8ContentPreserved) {
  auto result = english_.Tokenize("hello \xe4\xb8\x96\xe7\x95\x8c test");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>(
                         {"hello", "\xe4\xb8\x96\xe7\x95\x8c", "test"}));
}

TEST_F(SnowballLanguageTest, TabsAndNewlines) {
  auto result = english_.Tokenize("hello\tworld\ntest");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world", "test"}));
}

TEST_F(SnowballLanguageTest, NonAsciiNotTreatedAsPunctuation) {
  auto result = english_.Tokenize("hello\xf0\x9f\x99\x82world");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello\xf0\x9f\x99\x82world"}));
}

TEST_F(SnowballLanguageTest, CanonicallyEquivalentFormsNormalize) {
  auto precomposed = english_.Tokenize("caf\xc3\xa9");
  auto decomposed = english_.Tokenize("cafe\xcc\x81");
  ASSERT_TRUE(precomposed.ok());
  ASSERT_TRUE(decomposed.ok());
  EXPECT_EQ(*precomposed, *decomposed);
  EXPECT_EQ((*precomposed)[0], "caf\xc3\xa9");
}

TEST_F(SnowballLanguageTest, LongWord) {
  std::string long_word(1000, 'a');
  auto result = english_.Tokenize(long_word);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({long_word}));
}

// --- Backslash escape handling ---

TEST_F(SnowballLanguageTest, EscapedPunctuationIncluded) {
  auto result = english_.Tokenize("hello\\,world");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello,world"}));
}

TEST_F(SnowballLanguageTest, DoubleBackslash) {
  auto result = english_.Tokenize("hello\\\\world");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello\\world"}));
}

TEST_F(SnowballLanguageTest, EscapedMultiBytePunctuation) {
  auto result = arabic_.Tokenize("hello\\\xd8\x8cworld");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello\xd8\x8cworld"}));
}

TEST_F(SnowballLanguageTest, BackslashBeforeNonPunctuationBreaksToken) {
  // When backslash IS punctuation (English) and the next char is NOT
  // punctuation, the backslash acts as a word boundary.
  auto result = english_.Tokenize("hello\\world");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world"}));
}

TEST_F(SnowballLanguageTest, TrailingBackslashIgnored) {
  // Backslash at end with no following char — no escape triggered.
  // The backslash itself is punctuation in English, so it just splits.
  auto result = english_.Tokenize("hello\\");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello"}));
}

// --- Multi-byte punctuation ---

TEST_F(SnowballLanguageTest, ArabicCommaSpitsTokens) {
  auto result = arabic_.Tokenize("hello\xd8\x8cworld");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world"}));
}

// --- TokenizeWithStemMap ---

TEST_F(SnowballLanguageTest, TokenizeWithStemMapBuildsMappings) {
  InProgressStemMap stem_map;
  auto result = english_.TokenizeWithStemMap("running jumps", 0, stem_map);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ((*result)[0], "running");
  EXPECT_EQ((*result)[1], "jumps");
  EXPECT_TRUE(stem_map.contains("run"));
  EXPECT_TRUE(stem_map.contains("jump"));
}

TEST_F(SnowballLanguageTest, TokenizeWithStemMapLargeMinSize) {
  InProgressStemMap stem_map;
  auto result = english_.TokenizeWithStemMap("running jumps", 100, stem_map);
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(stem_map.empty());
}

TEST_F(SnowballLanguageTest, TokenizeWithStemMapInvalidUtf8) {
  InProgressStemMap stem_map;
  auto result = english_.TokenizeWithStemMap("\xFF\xFE", 0, stem_map);
  EXPECT_FALSE(result.ok());
}

TEST_F(SnowballLanguageTest, TokenizeWithStemMapDeduplicatesVariants) {
  InProgressStemMap stem_map;
  auto result =
      english_.TokenizeWithStemMap("running running running", 0, stem_map);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->size(), 3);
  ASSERT_TRUE(stem_map.contains("run"));
  EXPECT_EQ(stem_map["run"].size(), 1);
  EXPECT_EQ(stem_map["run"][0], "running");
}

TEST_F(SnowballLanguageTest, TokenizeWithStemMapFiltersStopWords) {
  InProgressStemMap stem_map;
  auto result =
      english_.TokenizeWithStemMap("the running and jumping", 0, stem_map);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"running", "jumping"}));
  EXPECT_TRUE(stem_map.contains("run"));
  EXPECT_TRUE(stem_map.contains("jump"));
}

TEST_F(SnowballLanguageTest, TokenizeWithStemMapNoEntryWhenStemMatchesToken) {
  InProgressStemMap stem_map;
  // "run" stems to "run" — no mapping needed
  auto result = english_.TokenizeWithStemMap("run", 0, stem_map);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"run"}));
  EXPECT_FALSE(stem_map.contains("run"));
}

// --- QueryTokenize: normalizes but does NOT filter stop words ---

TEST_F(SnowballLanguageTest, QueryTokenizeKeepsStopWords) {
  auto result = english_.QueryTokenize("THE cat AND dog");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"the", "cat", "and", "dog"}));
}

TEST_F(SnowballLanguageTest, QueryTokenizeSplitsOnPunctuation) {
  auto result = english_.QueryTokenize("hello,world!test");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world", "test"}));
}

TEST_F(SnowballLanguageTest, QueryTokenizeInvalidUtf8) {
  auto result = english_.QueryTokenize("\xFF\xFE");
  EXPECT_FALSE(result.ok());
}

TEST_F(SnowballLanguageTest, QueryTokenizeDoesNotHandleEscapes) {
  // Backslash is punctuation in English, so it acts as a word boundary.
  // QueryTokenize does NOT handle escape sequences — backslash splits tokens.
  auto result = english_.QueryTokenize("hello\\,world");
  ASSERT_TRUE(result.ok());
  // Backslash splits "hello" from ",world". Comma then splits further.
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world"}));
}

TEST_F(SnowballLanguageTest, QueryTokenizeNoStopWordFiltering) {
  // All tokens preserved, even stop words
  auto result = english_.QueryTokenize("the and is are");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"the", "and", "is", "are"}));
}

// --- IsQueryDelimiter ---

TEST_F(SnowballLanguageTest, AsciiPunctuationIsDelimiter) {
  EXPECT_TRUE(english_.IsQueryDelimiter(','));
  EXPECT_TRUE(english_.IsQueryDelimiter(' '));
  EXPECT_TRUE(english_.IsQueryDelimiter('\t'));
}

TEST_F(SnowballLanguageTest, LettersAreNotDelimiters) {
  EXPECT_FALSE(english_.IsQueryDelimiter('a'));
  EXPECT_FALSE(english_.IsQueryDelimiter('Z'));
}

TEST_F(SnowballLanguageTest, MultiBytePunctuationPerLanguage) {
  EXPECT_TRUE(turkish_.IsQueryDelimiter(0x2013));   // EN DASH in Turkish
  EXPECT_FALSE(english_.IsQueryDelimiter(0x2013));  // Not in English
}

// --- NormalizeInPlace ---

TEST_F(SnowballLanguageTest, NormalizeAscii) {
  std::string token = "HELLO";
  english_.NormalizeInPlace(token);
  EXPECT_EQ(token, "hello");
}

TEST_F(SnowballLanguageTest, NormalizeNFCComposition) {
  std::string token = "CAFE\xcc\x81";  // CAFE + combining acute
  english_.NormalizeInPlace(token);
  EXPECT_EQ(token, "caf\xc3\xa9");
}

TEST_F(SnowballLanguageTest, TurkishDotlessI) {
  std::string token = "I";
  turkish_.NormalizeInPlace(token);
  EXPECT_EQ(token, "\xc4\xb1");  // ı U+0131
}

TEST_F(SnowballLanguageTest, TurkishDottedI) {
  std::string token = "\xc4\xb0";  // İ U+0130
  turkish_.NormalizeInPlace(token);
  EXPECT_EQ(token, "i");
}

// --- Arabic NFKC: presentation forms collapse to base ---

TEST_F(SnowballLanguageTest, ArabicPresentationFormsCollapse) {
  std::string presentation = "\xef\xbb\x9b\xef\xba\x98\xef\xba\x8e\xef\xba\x8f";
  std::string base = "\xd9\x83\xd8\xaa\xd8\xa7\xd8\xa8";

  auto result1 = arabic_.Tokenize(presentation);
  auto result2 = arabic_.Tokenize(base);
  ASSERT_TRUE(result1.ok());
  ASSERT_TRUE(result2.ok());
  ASSERT_EQ(result1->size(), 1);
  ASSERT_EQ(result2->size(), 1);
  EXPECT_EQ((*result1)[0], (*result2)[0]);
}

// --- Arabic NFKC: normalize-before-split prevents delimiter injection ---

TEST_F(SnowballLanguageTest, ArabicNfkcFullwidthCommaSplitsTokens) {
  // U+FF0C (fullwidth comma, \xef\xbc\x8c) is NOT in the punctuation set,
  // but NFKC maps it to U+002C (ASCII comma) which IS a delimiter.
  // Normalizing before segmentation ensures the split happens correctly.
  auto result = arabic_.Tokenize(
      "abc\xef\xbc\x8c"
      "def");
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 2)
      << "Fullwidth comma should split into two tokens after NFKC";
  EXPECT_EQ((*result)[0], "abc");
  EXPECT_EQ((*result)[1], "def");
}

TEST_F(SnowballLanguageTest, ArabicNfkcFullwidthSemicolonSplitsTokens) {
  // U+FF1B (fullwidth semicolon) → U+003B (ASCII semicolon) under NFKC.
  auto result = arabic_.Tokenize(
      "hello\xef\xbc\x9b"
      "world");
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 2);
  EXPECT_EQ((*result)[0], "hello");
  EXPECT_EQ((*result)[1], "world");
}

TEST_F(SnowballLanguageTest, ArabicNbspSplitsAfterNfkc) {
  // U+00A0 (NBSP, \xc2\xa0) is already in punct_set_ via White_Space, but
  // NFKC also maps it to U+0020 (space). Either way it must split. This test
  // ensures no embedded space byte ends up inside a token.
  auto result = arabic_.Tokenize(
      "\xd8\xa8\xd8\xa7\xd8\xad\xd8\xb1\xd9\x85"  // "باحرم"
      "\xc2\xa0"                                  // U+00A0 NBSP
      "\xd8\xa8\xd8\xa7\xd9\x84\xd9\x85\xd9\x84\xd8\xa7\xd8\xb9");  // "بالملاع"
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 2) << "NBSP should produce two tokens";
  for (const auto& token : *result) {
    EXPECT_EQ(token.find(' '), std::string::npos)
        << "Token contains embedded space: " << token;
    EXPECT_EQ(token.find("\xc2\xa0"), std::string::npos)
        << "Token contains NBSP bytes: " << token;
  }
}

TEST_F(SnowballLanguageTest, ArabicQueryTokenizeNfkcFullwidthComma) {
  // Query path must also normalize before split to match indexed content.
  auto result = arabic_.QueryTokenize(
      "abc\xef\xbc\x8c"
      "def");
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 2);
  EXPECT_EQ((*result)[0], "abc");
  EXPECT_EQ((*result)[1], "def");
}

TEST_F(SnowballLanguageTest, FrenchNfcPreservesFullwidthComma) {
  // Contrast: NFC does NOT decompose U+FF0C, so French keeps it as part of
  // the token (it's not in French punctuation set either).
  TestFrenchLanguage french;
  auto result = french.Tokenize(
      "abc\xef\xbc\x8c"
      "def");
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 1)
      << "Fullwidth comma should NOT split under NFC (French)";
}

// --- GetStemmer ---

TEST_F(SnowballLanguageTest, StemmerNonNull) {
  EXPECT_NE(english_.GetStemmer(), nullptr);
}

TEST_F(SnowballLanguageTest, StemmerProducesCorrectRoots) {
  auto* stemmer = english_.GetStemmer();
  EXPECT_EQ(stemmer->GetStemRoot("running"), "run");
  EXPECT_EQ(stemmer->GetStemRoot("jumps"), "jump");
}

// --- IsStopWord ---

TEST_F(SnowballLanguageTest, EnglishStopWords) {
  EXPECT_TRUE(english_.IsStopWord("the"));
  EXPECT_TRUE(english_.IsStopWord("and"));
  EXPECT_FALSE(english_.IsStopWord("hello"));
}

TEST_F(SnowballLanguageTest, TurkishStopWords) {
  EXPECT_TRUE(turkish_.IsStopWord("ve"));
  EXPECT_TRUE(turkish_.IsStopWord("bir"));
  EXPECT_FALSE(turkish_.IsStopWord("hello"));
}

// --- Version gating ---

TEST_F(SnowballLanguageTest, EnglishAlwaysSupported) {
  EXPECT_TRUE(english_.IsSupported());
  EXPECT_EQ(english_.MinRequiredVersion(), vmsdk::ValkeyVersion(0, 0, 0));
}

TEST_F(SnowballLanguageTest, NonEnglishRequiresRelease13) {
  EXPECT_EQ(turkish_.MinRequiredVersion(), vmsdk::ValkeyVersion(1, 3, 0));
  EXPECT_TRUE(turkish_.IsSupported());
}

// --- Cross-language isolation ---

TEST_F(SnowballLanguageTest, FrenchStopWordsFilteredByFrench) {
  TestFrenchLanguage french;
  auto result = french.Tokenize("dans la maison");
  ASSERT_TRUE(result.ok());
  for (const auto& token : *result) {
    EXPECT_NE(token, "dans");
    EXPECT_NE(token, "la");
  }
}

TEST_F(SnowballLanguageTest, FrenchStopWordsKeptByEnglish) {
  auto result = english_.Tokenize("dans la maison");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->size(), 3);
}

TEST_F(SnowballLanguageTest, FrenchApostropheSplitsToken) {
  TestFrenchLanguage french;
  auto result = french.Tokenize(
      "l'\xc3\xa9"
      "cole");
  ASSERT_TRUE(result.ok());
  bool found_ecole = false;
  for (const auto& token : *result) {
    if (token ==
        "\xc3\xa9"
        "cole") {
      found_ecole = true;
    }
  }
  EXPECT_TRUE(found_ecole)
      << "Apostrophe should split l'école, making 'école' an independent token";
}

// --- Unicode whitespace as word boundaries ---

TEST_F(SnowballLanguageTest, PunctuationSetContainsUnicodeWhitespace) {
  PunctuationSet ps = BuildPunctuationSet(kAsciiPunctuation);
  // ASCII space is in the ascii bitset.
  EXPECT_TRUE(ps.Contains(0x0020));
  // Non-ASCII Unicode White_Space code points must be in the non_ascii set.
  EXPECT_TRUE(ps.Contains(0x00A0));  // NO-BREAK SPACE (NBSP)
  EXPECT_TRUE(ps.Contains(0x1680));  // OGHAM SPACE MARK
  EXPECT_TRUE(ps.Contains(0x2000));  // EN QUAD
  EXPECT_TRUE(ps.Contains(0x200A));  // HAIR SPACE
  EXPECT_TRUE(ps.Contains(0x202F));  // NARROW NO-BREAK SPACE
  EXPECT_TRUE(ps.Contains(0x205F));  // MEDIUM MATHEMATICAL SPACE
  EXPECT_TRUE(ps.Contains(0x3000));  // IDEOGRAPHIC SPACE
  EXPECT_TRUE(ps.Contains(0x2028));  // LINE SEPARATOR
  EXPECT_TRUE(ps.Contains(0x2029));  // PARAGRAPH SEPARATOR
  EXPECT_TRUE(ps.Contains(0x0085));  // NEXT LINE (NEL)
}

TEST_F(SnowballLanguageTest, FrenchNBSPTreatedAsWordBoundary) {
  TestFrenchLanguage french;
  // "Il a dit\u00A0: «\u00A0Bonjour\u00A0!\u00A0»"
  // This is the canonical French typesetting with NBSP before : and inside « »
  auto result = french.Tokenize(
      "Il a dit\xc2\xa0: \xc2\xab\xc2\xa0"
      "Bonjour\xc2\xa0!\xc2\xa0\xc2\xbb");
  ASSERT_TRUE(result.ok());
  // "il" is a stop word but "dit" and "bonjour" should be clean tokens.
  bool found_dit = false;
  bool found_bonjour = false;
  for (const auto& token : *result) {
    if (token == "dit") found_dit = true;
    if (token == "bonjour") found_bonjour = true;
    // No token should contain NBSP bytes (\xc2\xa0).
    EXPECT_EQ(token.find("\xc2\xa0"), std::string::npos)
        << "Token '" << token << "' contains NBSP bytes";
  }
  EXPECT_TRUE(found_dit) << "Expected 'dit' as a clean token";
  EXPECT_TRUE(found_bonjour) << "Expected 'bonjour' as a clean token";
}

TEST_F(SnowballLanguageTest, FrenchNarrowNBSPTreatedAsWordBoundary) {
  TestFrenchLanguage french;
  // U+202F (NARROW NO-BREAK SPACE) is used in modern French typography
  // "\xE2\x80\xAF" is UTF-8 for U+202F
  auto result = french.Tokenize("mot1\xe2\x80\xaf mot2");
  ASSERT_TRUE(result.ok());
  bool found_mot1 = false;
  bool found_mot2 = false;
  for (const auto& token : *result) {
    if (token == "mot1") found_mot1 = true;
    if (token == "mot2") found_mot2 = true;
  }
  EXPECT_TRUE(found_mot1) << "Expected 'mot1' split by NNBSP";
  EXPECT_TRUE(found_mot2) << "Expected 'mot2' split by NNBSP";
}

TEST_F(SnowballLanguageTest, QueryTokenizeSplitsOnNBSP) {
  TestFrenchLanguage french;
  // Query path must also split on NBSP so queries match indexed content.
  auto result = french.QueryTokenize(
      "dit\xc2\xa0"
      "bonjour");
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 2);
  EXPECT_EQ((*result)[0], "dit");
  EXPECT_EQ((*result)[1], "bonjour");
}

TEST_F(SnowballLanguageTest, IdeographicSpaceSplitsTokens) {
  // U+3000 IDEOGRAPHIC SPACE (\xe3\x80\x80) should act as a word boundary.
  auto result = english_.Tokenize("hello\xe3\x80\x80world");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world"}));
}

TEST_F(SnowballLanguageTest, GermanCompoundWordNotDecomposed) {
  TestGermanLanguage german;
  auto result = german.Tokenize("Donaudampfschifffahrtsgesellschaft");
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 1);
  EXPECT_EQ((*result)[0], "donaudampfschifffahrtsgesellschaft");
}

// --- Stop word list snapshot regression ---

struct StopWordSnapshotCase {
  std::string test_name;
  data_model::Language language;
  size_t expected_count;
  std::vector<std::string> must_contain;
};

class StopWordSnapshotTest
    : public ::testing::TestWithParam<StopWordSnapshotCase> {};

TEST_P(StopWordSnapshotTest, ListSizeAndSentinelsMatch) {
  const auto& tc = GetParam();
  const auto& stop_words =
      LanguageRegistry::Instance().Get(tc.language)->GetDefaultStopWords();

  EXPECT_EQ(stop_words.size(), tc.expected_count)
      << "Stop word list size changed for " << tc.test_name
      << ". If intentional, update this snapshot.";

  for (const auto& word : tc.must_contain) {
    EXPECT_NE(std::find(stop_words.begin(), stop_words.end(), word),
              stop_words.end())
        << "Expected stop word '" << word << "' missing from " << tc.test_name;
  }
}

const std::vector<StopWordSnapshotCase> kStopWordSnapshots = {
    {"english", data_model::LANGUAGE_ENGLISH, 33, {"the", "and", "is"}},
    {"french", data_model::LANGUAGE_FRENCH, 154, {"dans", "avec", "pour"}},
    {"german", data_model::LANGUAGE_GERMAN, 231, {"und", "der", "die"}},
    {"spanish", data_model::LANGUAGE_SPANISH, 308, {"de", "que", "por"}},
    {"italian", data_model::LANGUAGE_ITALIAN, 279, {"con", "per", "non"}},
    {"portuguese", data_model::LANGUAGE_PORTUGUESE, 203, {"de", "que", "para"}},
    {"russian",
     data_model::LANGUAGE_RUSSIAN,
     159,
     {"\xd0\xb8", "\xd0\xb2", "\xd0\xbd\xd0\xb5"}},
    {"swedish", data_model::LANGUAGE_SWEDISH, 114, {"och", "att", "som"}},
    {"turkish", data_model::LANGUAGE_TURKISH, 209, {"bir", "ve", "bu"}},
    {"dutch", data_model::LANGUAGE_DUTCH, 101, {"de", "en", "van"}},
    {"indonesian",
     data_model::LANGUAGE_INDONESIAN,
     93,
     {"yang", "dan", "dari"}},
    {"arabic",
     data_model::LANGUAGE_ARABIC,
     119,
     {"\xd9\x85\xd9\x86", "\xd9\x81\xd9\x8a", "\xd9\x88"}},
};

INSTANTIATE_TEST_SUITE_P(
    PerLanguage, StopWordSnapshotTest, ::testing::ValuesIn(kStopWordSnapshots),
    [](const ::testing::TestParamInfo<StopWordSnapshotCase>& info) {
      return info.param.test_name;
    });

// --- LANGUAGE_UNSPECIFIED defaults to English ---

TEST(UnspecifiedLanguageTest, StopWordsMatchEnglish) {
  const auto& unspecified = LanguageRegistry::Instance()
                                .Get(data_model::LANGUAGE_UNSPECIFIED)
                                ->GetDefaultStopWords();
  const auto& english = LanguageRegistry::Instance()
                            .Get(data_model::LANGUAGE_ENGLISH)
                            ->GetDefaultStopWords();
  EXPECT_EQ(&unspecified, &english)
      << "LANGUAGE_UNSPECIFIED must return the same stop word list as ENGLISH";
}

// =========================================================================
// Parameterized tests for language-specific behavior
//
// Only parameterized where config causes different code paths per language:
// - Non-ASCII punctuation splitting (different punctuation sets)
// - Locale-aware case folding (Turkish vs generic)
// - NFKC vs NFC normalization (Arabic vs others)
// =========================================================================

// --- Non-ASCII punctuation: splits tokens in languages that include it ---

struct NonAsciiPunctuationCase {
  std::string test_name;
  data_model::Language language;
  std::string punctuation;
  std::string stop_words_dummy;  // not used, empty vector below
  NormalizationForm norm_form;
  std::string locale;
  std::string stemmer_algorithm;
  std::string input;
  // Codepoint that is punctuation in this language
  uint32_t punct_codepoint;
  bool expect_split;
};

const std::vector<NonAsciiPunctuationCase> kNonAsciiPunctuationCases = {
    // EN DASH U+2013 splits in French but not English
    {"french_en_dash_splits", data_model::LANGUAGE_FRENCH, kFrenchPunctuation,
     "", NormalizationForm::NFC, "", "french", "hello\xe2\x80\x93world", 0x2013,
     true},
    {"english_en_dash_no_split", data_model::LANGUAGE_ENGLISH,
     kEnglishPunctuation, "", NormalizationForm::NFC, "", "english",
     "hello\xe2\x80\x93world", 0x2013, false},
    // Arabic comma U+060C splits in Arabic but not English
    {"arabic_comma_splits", data_model::LANGUAGE_ARABIC, kArabicPunctuation, "",
     NormalizationForm::NFKC, "", "arabic", "hello\xd8\x8cworld", 0x060C, true},
    {"english_arabic_comma_no_split", data_model::LANGUAGE_ENGLISH,
     kEnglishPunctuation, "", NormalizationForm::NFC, "", "english",
     "hello\xd8\x8cworld", 0x060C, false},
    // German low-9 quotation mark U+201E splits in German but not English
    {"german_low_quote_splits", data_model::LANGUAGE_GERMAN, kGermanPunctuation,
     "", NormalizationForm::NFC, "", "german", "hello\xe2\x80\x9eworld", 0x201E,
     true},
    {"english_low_quote_no_split", data_model::LANGUAGE_ENGLISH,
     kEnglishPunctuation, "", NormalizationForm::NFC, "", "english",
     "hello\xe2\x80\x9eworld", 0x201E, false},
    // Spanish inverted question mark U+00BF splits in Spanish
    {"spanish_inverted_question_splits", data_model::LANGUAGE_SPANISH,
     kSpanishPunctuation, "", NormalizationForm::NFC, "", "spanish",
     "hello\xc2\xbfworld", 0x00BF, true},
    {"english_inverted_question_no_split", data_model::LANGUAGE_ENGLISH,
     kEnglishPunctuation, "", NormalizationForm::NFC, "", "english",
     "hello\xc2\xbfworld", 0x00BF, false},
};

class NonAsciiPunctuationLanguage final : public SnowballLanguage {
 public:
  NonAsciiPunctuationLanguage(const NonAsciiPunctuationCase& tc)
      : SnowballLanguage(tc.language, tc.punctuation, {}, tc.norm_form,
                         tc.locale, tc.stemmer_algorithm),
        tc_(tc) {}

  data_model::Language Id() const override { return tc_.language; }
  absl::string_view Name() const override { return tc_.stemmer_algorithm; }
  const std::string& GetDefaultPunctuation() const override {
    return tc_.punctuation;
  }
  const std::vector<std::string>& GetDefaultStopWords() const override {
    static const std::vector<std::string> empty;
    return empty;
  }
  NormalizationForm GetNormalizationForm() const override {
    return tc_.norm_form;
  }
  absl::string_view CaseFoldLocale() const override { return tc_.locale; }
  vmsdk::ValkeyVersion MinRequiredVersion() const override {
    return vmsdk::ValkeyVersion(0, 0, 0);
  }

 private:
  NonAsciiPunctuationCase tc_;
};

class NonAsciiPunctuationTest
    : public ::testing::TestWithParam<NonAsciiPunctuationCase> {};

TEST_P(NonAsciiPunctuationTest, SplitBehavior) {
  const auto& tc = GetParam();
  NonAsciiPunctuationLanguage lang(tc);

  EXPECT_EQ(lang.IsQueryDelimiter(tc.punct_codepoint), tc.expect_split);

  auto result = lang.Tokenize(tc.input);
  ASSERT_TRUE(result.ok());
  if (tc.expect_split) {
    EXPECT_EQ(result->size(), 2) << "Expected split for " << tc.test_name;
  } else {
    EXPECT_EQ(result->size(), 1) << "Expected no split for " << tc.test_name;
  }
}

INSTANTIATE_TEST_SUITE_P(
    PerLanguage, NonAsciiPunctuationTest,
    ::testing::ValuesIn(kNonAsciiPunctuationCases),
    [](const ::testing::TestParamInfo<NonAsciiPunctuationCase>& info) {
      return info.param.test_name;
    });

// --- Locale-aware case folding: Turkish vs generic ---

struct LocaleCaseFoldCase {
  std::string test_name;
  std::string locale;
  std::string stemmer_algorithm;
  std::string input;
  std::string expected;
};

const std::vector<LocaleCaseFoldCase> kLocaleCaseFoldCases = {
    // Turkish: I → ı (dotless i)
    {"turkish_upper_I_to_dotless_i", "tr", "turkish", "I", "\xc4\xb1"},
    // Turkish: İ (U+0130) → i
    {"turkish_dotted_I_to_i", "tr", "turkish", "\xc4\xb0", "i"},
    // Generic (English): I → i (standard lowercase)
    {"generic_upper_I_to_i", "", "english", "I", "i"},
    // Generic: İ (U+0130) → i̇ (i + combining dot above in NFC)
    {"generic_dotted_I_to_i_dot", "", "english", "\xc4\xb0", "i\xcc\x87"},
};

class LocaleCaseFoldLanguage final : public SnowballLanguage {
 public:
  LocaleCaseFoldLanguage(const LocaleCaseFoldCase& tc)
      : SnowballLanguage(tc.locale == "tr" ? data_model::LANGUAGE_TURKISH
                                           : data_model::LANGUAGE_ENGLISH,
                         kEnglishPunctuation, {}, NormalizationForm::NFC,
                         tc.locale, tc.stemmer_algorithm),
        locale_(tc.locale) {}

  data_model::Language Id() const override {
    return locale_ == "tr" ? data_model::LANGUAGE_TURKISH
                           : data_model::LANGUAGE_ENGLISH;
  }
  absl::string_view Name() const override {
    return locale_ == "tr" ? "turkish" : "english";
  }
  const std::string& GetDefaultPunctuation() const override {
    return kEnglishPunctuation;
  }
  const std::vector<std::string>& GetDefaultStopWords() const override {
    static const std::vector<std::string> empty;
    return empty;
  }
  NormalizationForm GetNormalizationForm() const override {
    return NormalizationForm::NFC;
  }
  absl::string_view CaseFoldLocale() const override { return locale_; }
  vmsdk::ValkeyVersion MinRequiredVersion() const override {
    return vmsdk::ValkeyVersion(0, 0, 0);
  }

 private:
  std::string locale_;
};

class LocaleCaseFoldTest : public ::testing::TestWithParam<LocaleCaseFoldCase> {
};

TEST_P(LocaleCaseFoldTest, ProducesExpectedOutput) {
  const auto& tc = GetParam();
  LocaleCaseFoldLanguage lang(tc);
  std::string token = tc.input;
  lang.NormalizeInPlace(token);
  EXPECT_EQ(token, tc.expected);
}

INSTANTIATE_TEST_SUITE_P(
    PerLocale, LocaleCaseFoldTest, ::testing::ValuesIn(kLocaleCaseFoldCases),
    [](const ::testing::TestParamInfo<LocaleCaseFoldCase>& info) {
      return info.param.test_name;
    });

// --- NFKC vs NFC normalization: Arabic decomposes presentation forms ---

struct NormFormCase {
  std::string test_name;
  NormalizationForm norm_form;
  std::string stemmer_algorithm;
  std::string input;
  std::string expected;
};

const std::vector<NormFormCase> kNormFormCases = {
    // Arabic NFKC: presentation form FBxx → base
    // U+FB56 (ARABIC LETTER PEH ISOLATED FORM) → U+067E (peh)
    {"nfkc_arabic_peh_presentation_form", NormalizationForm::NFKC, "arabic",
     "\xef\xad\x96", "\xd9\xbe"},
    // NFC leaves presentation forms unchanged
    {"nfc_preserves_presentation_form", NormalizationForm::NFC, "english",
     "\xef\xad\x96", "\xef\xad\x96"},
    // Both NFC and NFKC compose combining sequences
    {"nfkc_composes_combining", NormalizationForm::NFKC, "arabic", "e\xcc\x81",
     "\xc3\xa9"},
    {"nfc_composes_combining", NormalizationForm::NFC, "english", "e\xcc\x81",
     "\xc3\xa9"},
};

class NormFormLanguage final : public SnowballLanguage {
 public:
  NormFormLanguage(const NormFormCase& tc)
      : SnowballLanguage(tc.norm_form == NormalizationForm::NFKC
                             ? data_model::LANGUAGE_ARABIC
                             : data_model::LANGUAGE_ENGLISH,
                         kEnglishPunctuation, {}, tc.norm_form, "",
                         tc.stemmer_algorithm),
        norm_form_(tc.norm_form) {}

  data_model::Language Id() const override {
    return norm_form_ == NormalizationForm::NFKC ? data_model::LANGUAGE_ARABIC
                                                 : data_model::LANGUAGE_ENGLISH;
  }
  absl::string_view Name() const override {
    return norm_form_ == NormalizationForm::NFKC ? "arabic" : "english";
  }
  const std::string& GetDefaultPunctuation() const override {
    return kEnglishPunctuation;
  }
  const std::vector<std::string>& GetDefaultStopWords() const override {
    static const std::vector<std::string> empty;
    return empty;
  }
  NormalizationForm GetNormalizationForm() const override { return norm_form_; }
  absl::string_view CaseFoldLocale() const override { return ""; }
  vmsdk::ValkeyVersion MinRequiredVersion() const override {
    return vmsdk::ValkeyVersion(0, 0, 0);
  }

 private:
  NormalizationForm norm_form_;
};

class NormFormTest : public ::testing::TestWithParam<NormFormCase> {};

TEST_P(NormFormTest, ProducesExpectedNormalization) {
  const auto& tc = GetParam();
  NormFormLanguage lang(tc);
  std::string token = tc.input;
  lang.NormalizeInPlace(token);
  EXPECT_EQ(token, tc.expected);
}

INSTANTIATE_TEST_SUITE_P(
    PerNormForm, NormFormTest, ::testing::ValuesIn(kNormFormCases),
    [](const ::testing::TestParamInfo<NormFormCase>& info) {
      return info.param.test_name;
    });

}  // namespace
}  // namespace valkey_search::indexes::text
