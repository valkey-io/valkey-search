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
#include "src/indexes/text/unicode_normalizer.h"
#include "src/valkey_search_options.h"
#include "src/version.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {
namespace {

// Tokenizer settings built from the language's own default punctuation and
// stop words, as for an index created without PUNCTUATION or STOPWORDS.
TokenizerConfig DefaultConfig(const Language& language) {
  return language.MakeTokenizerConfig(language.GetDefaultPunctuation(),
                                      language.GetDefaultStopWords());
}

// The registered (production) instance of a language.
const Language& Registered(data_model::Language language) {
  return *LanguageRegistry::Instance().Get(language);
}

class SnowballLanguageTest : public ::testing::Test {
 protected:
  const Language& english_ = Registered(data_model::LANGUAGE_ENGLISH);
  const Language& turkish_ = Registered(data_model::LANGUAGE_TURKISH);
  const Language& arabic_ = Registered(data_model::LANGUAGE_ARABIC);
};

// --- Tokenize: full pipeline ---

TEST_F(SnowballLanguageTest, EmptyString) {
  auto result = english_.Tokenize("", DefaultConfig(english_));
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result->empty());
}

TEST_F(SnowballLanguageTest, OnlyPunctuation) {
  auto result =
      english_.Tokenize("   \t\n!@#$%^&*()   ", DefaultConfig(english_));
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result->empty());
}

TEST_F(SnowballLanguageTest, PunctuationSplitting) {
  auto result =
      english_.Tokenize("hello,world!nice-day.today", DefaultConfig(english_));
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>(
                         {"hello", "world", "nice", "day", "today"}));
}

TEST_F(SnowballLanguageTest, CaseFolding) {
  auto result = english_.Tokenize("HELLO World miXeD", DefaultConfig(english_));
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world", "mixed"}));
}

TEST_F(SnowballLanguageTest, StopWordsFiltered) {
  auto result = english_.Tokenize("the cat and dog", DefaultConfig(english_));
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"cat", "dog"}));
}

TEST_F(SnowballLanguageTest, AllStopWordsProducesEmpty) {
  auto result = english_.Tokenize("the and or is", DefaultConfig(english_));
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result->empty());
}

TEST_F(SnowballLanguageTest, InvalidUtf8ReturnsError) {
  auto result =
      english_.Tokenize("hello \xFF\xFE world", DefaultConfig(english_));
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_F(SnowballLanguageTest, Utf8ContentPreserved) {
  auto result = english_.Tokenize("hello \xe4\xb8\x96\xe7\x95\x8c test",
                                  DefaultConfig(english_));
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>(
                         {"hello", "\xe4\xb8\x96\xe7\x95\x8c", "test"}));
}

TEST_F(SnowballLanguageTest, TabsAndNewlines) {
  auto result =
      english_.Tokenize("hello\tworld\ntest", DefaultConfig(english_));
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world", "test"}));
}

TEST_F(SnowballLanguageTest, NonAsciiNotTreatedAsPunctuation) {
  auto result =
      english_.Tokenize("hello\xf0\x9f\x99\x82world", DefaultConfig(english_));
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello\xf0\x9f\x99\x82world"}));
}

TEST_F(SnowballLanguageTest, LongWord) {
  std::string long_word(1000, 'a');
  auto result = english_.Tokenize(long_word, DefaultConfig(english_));
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({long_word}));
}

// --- Backslash escape handling ---

TEST_F(SnowballLanguageTest, EscapedPunctuationIncluded) {
  auto result = english_.Tokenize("hello\\,world", DefaultConfig(english_));
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello,world"}));
}

TEST_F(SnowballLanguageTest, DoubleBackslash) {
  auto result = english_.Tokenize("hello\\\\world", DefaultConfig(english_));
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello\\world"}));
}

TEST_F(SnowballLanguageTest, EscapedMultiBytePunctuation) {
  auto result =
      arabic_.Tokenize("hello\\\xd8\x8cworld", DefaultConfig(arabic_));
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello\xd8\x8cworld"}));
}

TEST_F(SnowballLanguageTest, BackslashBeforeNonPunctuationBreaksToken) {
  // When backslash IS punctuation (English) and the next char is NOT
  // punctuation, the backslash acts as a word boundary.
  auto result = english_.Tokenize("hello\\world", DefaultConfig(english_));
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world"}));
}

TEST_F(SnowballLanguageTest, TrailingBackslashIgnored) {
  // Backslash at end with no following char — no escape triggered.
  // The backslash itself is punctuation in English, so it just splits.
  auto result = english_.Tokenize("hello\\", DefaultConfig(english_));
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello"}));
}

// --- TokenizeWithStemMap ---

TEST_F(SnowballLanguageTest, TokenizeWithStemMapFiltersStopWords) {
  InProgressStemMap stem_map;
  auto result = english_.TokenizeWithStemMap("the running and jumping",
                                             DefaultConfig(english_), 0,
                                             LengthUnit::kCodePoints, stem_map);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"running", "jumping"}));
  EXPECT_TRUE(stem_map.contains("run"));
  EXPECT_TRUE(stem_map.contains("jump"));
}

// --- NormalizeInPlace ---

TEST_F(SnowballLanguageTest, NormalizeAscii) {
  std::string token = "HELLO";
  english_.NormalizeInPlace(token);
  EXPECT_EQ(token, "hello");
}

// --- Arabic NFKC: normalize-before-split prevents delimiter injection ---

TEST_F(SnowballLanguageTest, ArabicNfkcFullwidthCommaSplitsTokens) {
  // U+FF0C (fullwidth comma, \xef\xbc\x8c) is not listed as Arabic
  // punctuation, but NFKC maps it to U+002C (ASCII comma), so the punctuation
  // set's normalization closure makes it a delimiter.
  auto result = arabic_.Tokenize(
      "abc\xef\xbc\x8c"
      "def",
      DefaultConfig(arabic_));
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
      "world",
      DefaultConfig(arabic_));
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 2);
  EXPECT_EQ((*result)[0], "hello");
  EXPECT_EQ((*result)[1], "world");
}

TEST_F(SnowballLanguageTest, FrenchNfcPreservesFullwidthComma) {
  // Contrast: NFC does NOT decompose U+FF0C, so French keeps it as part of
  // the token (it's not in French punctuation set either).
  const Language& french = Registered(data_model::LANGUAGE_FRENCH);
  auto result = french.Tokenize(
      "abc\xef\xbc\x8c"
      "def",
      DefaultConfig(french));
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 1)
      << "Fullwidth comma should NOT split under NFC (French)";
}

// --- GetStemmer ---

TEST_F(SnowballLanguageTest, StemmerNonNull) {
  EXPECT_NE(english_.GetStemmer(), nullptr);
}

// --- Per-index settings (FT.CREATE PUNCTUATION / STOPWORDS) ---

// An index's own punctuation and stop words replace the language defaults,
// while the shared language still supplies normalization.
struct TokenizerConfigCase {
  std::string test_name;
  std::string punctuation;
  std::vector<std::string> stop_words;
  std::string input;
  std::vector<std::string> expected;
};

class TokenizerConfigTest
    : public ::testing::TestWithParam<TokenizerConfigCase> {};

TEST_P(TokenizerConfigTest, TokenizesWithIndexSettings) {
  const auto& tc = GetParam();
  const Language& english = Registered(data_model::LANGUAGE_ENGLISH);
  auto result = english.Tokenize(
      tc.input, english.MakeTokenizerConfig(tc.punctuation, tc.stop_words));
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, tc.expected);
}

INSTANTIATE_TEST_SUITE_P(
    IndexSettings, TokenizerConfigTest,
    ::testing::ValuesIn(std::vector<TokenizerConfigCase>{
        // Only " ," is punctuation, so "world!this-is_a.test" stays one
        // token; the custom stop word is dropped case-insensitively.
        {"custom_punctuation_and_stop_words",
         " ,",
         {"Foo"},
         "World!this-is_a.test, FOO bar",
         {"world!this-is_a.test", "bar"}},
        // With no stop words, words that are English stop words by default
        // are kept, and digits are ordinary word characters.
        {"no_stop_words_keeps_all",
         kAsciiPunctuation,
         {},
         "and or 2024 v1.2",
         {"and", "or", "2024", "v1", "2"}},
    }),
    [](const ::testing::TestParamInfo<TokenizerConfigCase>& info) {
      return info.param.test_name;
    });

// --- IsStopWord ---

TEST_F(SnowballLanguageTest, EnglishStopWords) {
  EXPECT_TRUE(DefaultConfig(english_).IsStopWord("the"));
  EXPECT_TRUE(DefaultConfig(english_).IsStopWord("and"));
  EXPECT_FALSE(DefaultConfig(english_).IsStopWord("hello"));
}

TEST_F(SnowballLanguageTest, TurkishStopWords) {
  EXPECT_TRUE(DefaultConfig(turkish_).IsStopWord("ve"));
  EXPECT_TRUE(DefaultConfig(turkish_).IsStopWord("bir"));
  EXPECT_FALSE(DefaultConfig(turkish_).IsStopWord("hello"));
}

// --- Version gating ---

TEST_F(SnowballLanguageTest, EnglishHasNoMinimumVersion) {
  EXPECT_EQ(english_.MinRequiredVersion(), vmsdk::ValkeyVersion(0, 0, 0));
}

TEST_F(SnowballLanguageTest, NonEnglishRequiresRelease13) {
  EXPECT_EQ(turkish_.MinRequiredVersion(), vmsdk::ValkeyVersion(1, 3, 0));
}

// --- Cross-language isolation ---

TEST_F(SnowballLanguageTest, FrenchStopWordsKeptByEnglish) {
  auto result = english_.Tokenize("dans la maison", DefaultConfig(english_));
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->size(), 3);
}

TEST_F(SnowballLanguageTest, FrenchApostropheSplitsToken) {
  const Language& french = Registered(data_model::LANGUAGE_FRENCH);
  auto result = french.Tokenize(
      "l'\xc3\xa9"
      "cole",
      DefaultConfig(french));
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
  PunctuationSet ps = BuildPunctuationSet(
      kAsciiPunctuation, NormalizationForm::NFC, DelimiterScope::kUnicode);
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

// With DelimiterScope::kUnicode the set also holds Unicode White_Space and is
// closed under the normalization form (a code point whose normalized form is
// all punctuation is punctuation), so ingest and query can split raw text and
// normalize per token. DelimiterScope::kAscii adds neither (1.2 and Redis).
// Listed characters are honored under either scope.
struct PunctuationClosureCase {
  std::string test_name;
  NormalizationForm form;
  DelimiterScope scope;
  std::string punctuation;
  uint32_t cp;
  bool expected;
};

class PunctuationClosureTest
    : public ::testing::TestWithParam<PunctuationClosureCase> {};

TEST_P(PunctuationClosureTest, ContainsNormalizationEquivalents) {
  const auto& tc = GetParam();
  EXPECT_EQ(
      BuildPunctuationSet(tc.punctuation, tc.form, tc.scope).Contains(tc.cp),
      tc.expected);
}

INSTANTIATE_TEST_SUITE_P(
    Forms, PunctuationClosureTest,
    ::testing::ValuesIn(std::vector<PunctuationClosureCase>{
        // U+037E GREEK QUESTION MARK is canonically ';'.
        {"nfc_greek_question_mark", NormalizationForm::NFC,
         DelimiterScope::kUnicode, kAsciiPunctuation, 0x037E, true},
        // U+FF0C FULLWIDTH COMMA is ',' only under compatibility forms.
        {"nfkc_fullwidth_comma", NormalizationForm::NFKC,
         DelimiterScope::kUnicode, kAsciiPunctuation, 0xFF0C, true},
        {"nfc_fullwidth_comma", NormalizationForm::NFC,
         DelimiterScope::kUnicode, kAsciiPunctuation, 0xFF0C, false},
        // U+2025 TWO DOT LEADER is "..": every code point is punctuation.
        {"nfkc_two_dot_leader", NormalizationForm::NFKC,
         DelimiterScope::kUnicode, kAsciiPunctuation, 0x2025, true},
        // U+2474 PARENTHESIZED DIGIT ONE is "(1)": not all punctuation.
        {"nfkc_parenthesized_digit", NormalizationForm::NFKC,
         DelimiterScope::kUnicode, kAsciiPunctuation, 0x2474, false},
        // U+FB01 LATIN SMALL LIGATURE FI is "fi": letters.
        {"nfkc_fi_ligature", NormalizationForm::NFKC, DelimiterScope::kUnicode,
         kAsciiPunctuation, 0xFB01, false},
        // NBSP is Unicode White_Space: a boundary only under kUnicode.
        {"unicode_scope_nbsp", NormalizationForm::NFC, DelimiterScope::kUnicode,
         kAsciiPunctuation, 0x00A0, true},
        {"ascii_scope_nbsp", NormalizationForm::NFC, DelimiterScope::kAscii,
         kAsciiPunctuation, 0x00A0, false},
        {"ascii_scope_greek_question_mark", NormalizationForm::NFC,
         DelimiterScope::kAscii, kAsciiPunctuation, 0x037E, false},
        // A listed multi-byte character (U+2014 EM DASH) is honored as one code
        // point under either scope, without its lead bytes splitting others.
        {"ascii_scope_listed_em_dash", NormalizationForm::NFC,
         DelimiterScope::kAscii, ",\xe2\x80\x94", 0x2014, true},
        {"ascii_scope_unlisted_left_quote", NormalizationForm::NFC,
         DelimiterScope::kAscii, ",\xe2\x80\x94", 0x201C, false},
    }),
    [](const ::testing::TestParamInfo<PunctuationClosureCase>& info) {
      return info.param.test_name;
    });

TEST_F(SnowballLanguageTest, IdeographicSpaceSplitsOnlyUnicodeScope) {
  // U+3000 IDEOGRAPHIC SPACE (\xe3\x80\x80) is a word boundary for the new
  // languages (DelimiterScope::kUnicode). English keeps 1.2's ASCII-only
  // boundaries, which match Redis, so the word stays whole.
  const Language& french = Registered(data_model::LANGUAGE_FRENCH);
  auto split =
      french.Tokenize("bonjour\xe3\x80\x80monde", DefaultConfig(french));
  ASSERT_TRUE(split.ok());
  EXPECT_EQ(*split, std::vector<std::string>({"bonjour", "monde"}));

  auto whole =
      english_.Tokenize("hello\xe3\x80\x80world", DefaultConfig(english_));
  ASSERT_TRUE(whole.ok());
  EXPECT_EQ(*whole, std::vector<std::string>({"hello\xe3\x80\x80world"}));
}

TEST_F(SnowballLanguageTest, GermanCompoundWordNotDecomposed) {
  const Language& german = Registered(data_model::LANGUAGE_GERMAN);
  auto result = german.Tokenize("Donaudampfschifffahrtsgesellschaft",
                                DefaultConfig(german));
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

// --- Length unit for MINSTEMSIZE and fuzzy distance (compat-gated) ---

struct LengthUnitTestCase {
  std::string test_name;
  data_model::Language language;
  vmsdk::ValkeyVersion emulate_release;
  LengthUnit expected;
};

class LengthUnitTest : public ::testing::TestWithParam<LengthUnitTestCase> {
 protected:
  void SetUp() override {
    saved_emulate_release_ = options::GetEmulateRelease().GetValue();
  }
  void TearDown() override {
    VMSDK_EXPECT_OK(
        options::GetEmulateRelease().SetValue(saved_emulate_release_));
  }

 private:
  vmsdk::ValkeyVersion saved_emulate_release_{0};
};

TEST_P(LengthUnitTest, ResolvesFromLanguageAndEmulateRelease) {
  const auto& tc = GetParam();
  VMSDK_EXPECT_OK(options::GetEmulateRelease().SetValue(tc.emulate_release));
  EXPECT_EQ(LanguageRegistry::Instance().Get(tc.language)->GetLengthUnit(),
            tc.expected);
}

// English existed in 1.2, so it keeps byte counting until emulate-release
// 1.3.0. Languages added in 1.3 always count code points.
INSTANTIATE_TEST_SUITE_P(
    LengthUnits, LengthUnitTest,
    ::testing::ValuesIn(std::vector<LengthUnitTestCase>{
        {"english_legacy", data_model::LANGUAGE_ENGLISH, kRelease12,
         LengthUnit::kBytes},
        {"english_current", data_model::LANGUAGE_ENGLISH, kRelease13,
         LengthUnit::kCodePoints},
        {"unspecified_legacy", data_model::LANGUAGE_UNSPECIFIED, kRelease12,
         LengthUnit::kBytes},
        {"french_legacy", data_model::LANGUAGE_FRENCH, kRelease12,
         LengthUnit::kCodePoints},
    }),
    [](const ::testing::TestParamInfo<LengthUnitTestCase>& info) {
      return info.param.test_name;
    });

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
  std::string input;
  // Code point that is punctuation in some languages but not others.
  uint32_t punct_codepoint;
  bool expect_split;
};

const std::vector<NonAsciiPunctuationCase> kNonAsciiPunctuationCases = {
    // EN DASH U+2013 splits in French but not English
    {"french_en_dash_splits", data_model::LANGUAGE_FRENCH,
     "hello\xe2\x80\x93world", 0x2013, true},
    {"english_en_dash_no_split", data_model::LANGUAGE_ENGLISH,
     "hello\xe2\x80\x93world", 0x2013, false},
    // Arabic comma U+060C splits in Arabic but not English
    {"arabic_comma_splits", data_model::LANGUAGE_ARABIC, "hello\xd8\x8cworld",
     0x060C, true},
    {"english_arabic_comma_no_split", data_model::LANGUAGE_ENGLISH,
     "hello\xd8\x8cworld", 0x060C, false},
    // German low-9 quotation mark U+201E splits in German but not English
    {"german_low_quote_splits", data_model::LANGUAGE_GERMAN,
     "hello\xe2\x80\x9eworld", 0x201E, true},
    {"english_low_quote_no_split", data_model::LANGUAGE_ENGLISH,
     "hello\xe2\x80\x9eworld", 0x201E, false},
    // Spanish inverted question mark U+00BF splits in Spanish
    {"spanish_inverted_question_splits", data_model::LANGUAGE_SPANISH,
     "hello\xc2\xbfworld", 0x00BF, true},
    {"english_inverted_question_no_split", data_model::LANGUAGE_ENGLISH,
     "hello\xc2\xbfworld", 0x00BF, false},
};

class NonAsciiPunctuationTest
    : public ::testing::TestWithParam<NonAsciiPunctuationCase> {};

TEST_P(NonAsciiPunctuationTest, SplitBehavior) {
  const auto& tc = GetParam();
  const Language& lang = Registered(tc.language);
  const TokenizerConfig config = DefaultConfig(lang);

  EXPECT_EQ(config.punct_set.Contains(tc.punct_codepoint), tc.expect_split);

  auto result = lang.Tokenize(tc.input, config);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->size(), tc.expect_split ? 2u : 1u) << tc.test_name;
}

INSTANTIATE_TEST_SUITE_P(
    PerLanguage, NonAsciiPunctuationTest,
    ::testing::ValuesIn(kNonAsciiPunctuationCases),
    [](const ::testing::TestParamInfo<NonAsciiPunctuationCase>& info) {
      return info.param.test_name;
    });

}  // namespace
}  // namespace valkey_search::indexes::text
