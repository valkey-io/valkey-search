/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/snowball_processor.h"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/language_processor.h"
#include "src/indexes/text/punctuation.h"
#include "src/indexes/text/stop_words.h"

namespace valkey_search::indexes::text {

// Helper: create a processor with language defaults
inline std::shared_ptr<LanguageProcessor> CreateProcessor(
    data_model::Language language) {
  return LanguageProcessor::Create(language, GetDefaultPunctuation(language),
                                   GetDefaultStopWords(language));
}

// =============================================================================
// Parameterized test: Per-language Stemming Correctness
// =============================================================================

struct StemmingTestCase {
  std::string test_name;
  data_model::Language language;
  std::string input;
  std::string expected_stem;
};

// Test data table
// Per-language stemming correctness — each row verifies the Snowball algorithm
// produces the correct stem for a given language's morphology.
const std::vector<StemmingTestCase> kStemmingCases = {
    // English
    {"english_running", data_model::LANGUAGE_ENGLISH, "running", "run"},
    {"english_jumps", data_model::LANGUAGE_ENGLISH, "jumps", "jump"},
    {"english_happily", data_model::LANGUAGE_ENGLISH, "happily", "happili"},
    // French
    {"french_mangeons", data_model::LANGUAGE_FRENCH, "mangeons", "mangeon"},
    {"french_continuellement", data_model::LANGUAGE_FRENCH, "continuellement",
     "continuel"},
    // German
    {"german_laufenden", data_model::LANGUAGE_GERMAN, "laufenden", "laufend"},
    {"german_aufmerksamkeit", data_model::LANGUAGE_GERMAN, "aufmerksamkeit",
     "aufmerksam"},
    // Spanish
    {"spanish_corriendo", data_model::LANGUAGE_SPANISH, "corriendo", "corr"},
    {"spanish_bibliotecas", data_model::LANGUAGE_SPANISH, "bibliotecas",
     "bibliotec"},
    // Italian
    {"italian_mangiando", data_model::LANGUAGE_ITALIAN, "mangiando", "mang"},
    {"italian_continuamente", data_model::LANGUAGE_ITALIAN, "continuamente",
     "continu"},
    // Portuguese
    {"portuguese_correndo", data_model::LANGUAGE_PORTUGUESE, "correndo",
     "corr"},
    {"portuguese_bibliotecas", data_model::LANGUAGE_PORTUGUESE, "bibliotecas",
     "bibliotec"},
    // Russian: бегущий -> бегущ
    {"russian_running", data_model::LANGUAGE_RUSSIAN,
     "\xd0\xb1\xd0\xb5\xd0\xb3\xd1\x83\xd1\x89\xd0\xb8\xd0\xb9",
     "\xd0\xb1\xd0\xb5\xd0\xb3\xd1\x83\xd1\x89"},
    // Russian: библиотеки -> библиотек
    {"russian_libraries", data_model::LANGUAGE_RUSSIAN,
     "\xd0\xb1\xd0\xb8\xd0\xb1\xd0\xbb\xd0\xb8\xd0\xbe\xd1\x82\xd0\xb5"
     "\xd0\xba\xd0\xb8",
     "\xd0\xb1\xd0\xb8\xd0\xb1\xd0\xbb\xd0\xb8\xd0\xbe\xd1\x82\xd0\xb5"
     "\xd0\xba"},
    // Swedish
    {"swedish_springande", data_model::LANGUAGE_SWEDISH, "springande",
     "spring"},
    {"swedish_flickorna", data_model::LANGUAGE_SWEDISH, "flickorna", "flick"},
    // Turkish: koşuyorlar -> koşuyor
    {"turkish_kosuyorlar", data_model::LANGUAGE_TURKISH, "ko\xc5\x9fuyorlar",
     "ko\xc5\x9fuyor"},
    {"turkish_evlerden", data_model::LANGUAGE_TURKISH, "evlerden", "ev"},
    // Dutch
    {"dutch_lopende", data_model::LANGUAGE_DUTCH, "lopende", "loop"},
    {"dutch_fietsen", data_model::LANGUAGE_DUTCH, "fietsen", "fiets"},
    // Indonesian
    {"indonesian_berlari", data_model::LANGUAGE_INDONESIAN, "berlari", "lari"},
    {"indonesian_mempermasalahkan", data_model::LANGUAGE_INDONESIAN,
     "mempermasalahkan", "masalah"},
    // Arabic: الكتب -> كتب
    {"arabic_books", data_model::LANGUAGE_ARABIC,
     "\xd8\xa7\xd9\x84\xd9\x83\xd8\xaa\xd8\xa8", "\xd9\x83\xd8\xaa\xd8\xa8"},
    // Arabic: المدارس -> مدارس
    {"arabic_schools", data_model::LANGUAGE_ARABIC,
     "\xd8\xa7\xd9\x84\xd9\x85\xd8\xaf\xd8\xa7\xd8\xb1\xd8\xb3",
     "\xd9\x85\xd8\xaf\xd8\xa7\xd8\xb1\xd8\xb3"},
};

class SnowballStemmingTest : public ::testing::TestWithParam<StemmingTestCase> {
};

TEST_P(SnowballStemmingTest, ProducesExpectedStem) {
  const auto& tc = GetParam();
  auto processor = CreateProcessor(tc.language);
  ASSERT_NE(processor, nullptr);

  std::string word = tc.input;
  processor->StemWordInPlace(word, 3);
  EXPECT_EQ(word, tc.expected_stem);
}

INSTANTIATE_TEST_SUITE_P(
    PerLanguage, SnowballStemmingTest, ::testing::ValuesIn(kStemmingCases),
    [](const ::testing::TestParamInfo<StemmingTestCase>& info) {
      return info.param.test_name;
    });

// =============================================================================
// Parameterized test: BuildStemMap Convergence
// =============================================================================

struct StemMapTestCase {
  std::string test_name;
  data_model::Language language;
  std::vector<std::string> tokens;
  std::string expected_stem;
  size_t expected_count;
};

// Test data table
// Per-language BuildStemMap convergence — verifies morphological variants
// converge to the same stem entry.
const std::vector<StemMapTestCase> kStemMapCases = {
    {"english_run",
     data_model::LANGUAGE_ENGLISH,
     {"running", "runs"},
     "run",
     2},
    {"french_continuel",
     data_model::LANGUAGE_FRENCH,
     {"continuellement", "continuelle"},
     "continuel",
     2},
    {"german_laufend",
     data_model::LANGUAGE_GERMAN,
     {"laufenden", "laufende"},
     "laufend",
     2},
    {"spanish_corr",
     data_model::LANGUAGE_SPANISH,
     {"corriendo", "corremos"},
     "corr",
     2},
    {"italian_mang",
     data_model::LANGUAGE_ITALIAN,
     {"mangiando", "mangiamo"},
     "mang",
     2},
    {"portuguese_corr",
     data_model::LANGUAGE_PORTUGUESE,
     {"correndo", "corremos"},
     "corr",
     2},
    // Russian: библиотеки, библиотека -> библиотек
    {"russian_bibliotek",
     data_model::LANGUAGE_RUSSIAN,
     {"\xd0\xb1\xd0\xb8\xd0\xb1\xd0\xbb\xd0\xb8\xd0\xbe\xd1\x82\xd0\xb5"
      "\xd0\xba\xd0\xb8",
      "\xd0\xb1\xd0\xb8\xd0\xb1\xd0\xbb\xd0\xb8\xd0\xbe\xd1\x82\xd0\xb5"
      "\xd0\xba\xd0\xb0"},
     "\xd0\xb1\xd0\xb8\xd0\xb1\xd0\xbb\xd0\xb8\xd0\xbe\xd1\x82\xd0\xb5"
     "\xd0\xba",
     2},
    {"swedish_spring",
     data_model::LANGUAGE_SWEDISH,
     {"springande", "springer"},
     "spring",
     2},
    {"turkish_ev",
     data_model::LANGUAGE_TURKISH,
     {"evlerden", "evlerde"},
     "ev",
     2},
    {"dutch_loop",
     data_model::LANGUAGE_DUTCH,
     {"lopende", "lopend"},
     "loop",
     2},
    {"indonesian_lari",
     data_model::LANGUAGE_INDONESIAN,
     {"berlari", "pelari"},
     "lari",
     2},
    // Arabic: الكتب and الكتاب stem to different roots (كتب vs كتاب)
    {"arabic_books",
     data_model::LANGUAGE_ARABIC,
     {"\xd8\xa7\xd9\x84\xd9\x83\xd8\xaa\xd8\xa8",
      "\xd8\xa7\xd9\x84\xd9\x83\xd8\xaa\xd8\xa7\xd8\xa8"},
     "\xd9\x83\xd8\xaa\xd8\xa8",
     1},
};

class SnowballStemMapTest : public ::testing::TestWithParam<StemMapTestCase> {};

TEST_P(SnowballStemMapTest, VariantsConvergeToSameStem) {
  const auto& tc = GetParam();
  auto processor = CreateProcessor(tc.language);
  ASSERT_NE(processor, nullptr);

  InProgressStemMap stem_mappings;
  processor->BuildStemMap(tc.tokens, 3, stem_mappings);

  EXPECT_TRUE(stem_mappings.contains(tc.expected_stem));
  EXPECT_EQ(stem_mappings[tc.expected_stem].size(), tc.expected_count);
}

INSTANTIATE_TEST_SUITE_P(
    PerLanguage, SnowballStemMapTest, ::testing::ValuesIn(kStemMapCases),
    [](const ::testing::TestParamInfo<StemMapTestCase>& info) {
      return info.param.test_name;
    });

// =============================================================================
// Factory tests — verify Create() works for all 12 languages + edge cases
// =============================================================================

class SnowballProcessorFactoryTest
    : public ::testing::TestWithParam<data_model::Language> {
 protected:
  void SetUp() override {
    processor_ = CreateProcessor(GetParam());
    ASSERT_NE(processor_, nullptr);
  }
  std::shared_ptr<LanguageProcessor> processor_;
};

TEST_P(SnowballProcessorFactoryTest, ReturnsNonNull) {
  EXPECT_NE(processor_, nullptr);
}

TEST_P(SnowballProcessorFactoryTest, SupportsStemming) {
  EXPECT_TRUE(processor_->SupportsStemming());
}

INSTANTIATE_TEST_SUITE_P(
    AllLanguages, SnowballProcessorFactoryTest,
    ::testing::Values(
        data_model::LANGUAGE_ENGLISH, data_model::LANGUAGE_FRENCH,
        data_model::LANGUAGE_GERMAN, data_model::LANGUAGE_SPANISH,
        data_model::LANGUAGE_ITALIAN, data_model::LANGUAGE_PORTUGUESE,
        data_model::LANGUAGE_RUSSIAN, data_model::LANGUAGE_SWEDISH,
        data_model::LANGUAGE_TURKISH, data_model::LANGUAGE_DUTCH,
        data_model::LANGUAGE_INDONESIAN, data_model::LANGUAGE_ARABIC));

TEST(SnowballProcessorFactoryEdgeTest, UnspecifiedLanguageReturnsNonNull) {
  auto processor = CreateProcessor(data_model::LANGUAGE_UNSPECIFIED);
  EXPECT_NE(processor, nullptr);
}

// =============================================================================
// Shared-mechanism tests (language-agnostic code paths, tested once w/ English)
// =============================================================================

class SnowballProcessorSharedTest : public ::testing::Test {
 protected:
  std::shared_ptr<LanguageProcessor> processor_ =
      CreateProcessor(data_model::LANGUAGE_ENGLISH);
};

// min_stem_size guard: word with fewer codepoints than threshold is not stemmed
TEST_F(SnowballProcessorSharedTest, StemWordInPlace_MinStemSizePrevents) {
  std::string word = "running";
  processor_->StemWordInPlace(word, 10);
  EXPECT_EQ(word, "running");
}

// Idempotency: a word already at its stem form is left unchanged
TEST_F(SnowballProcessorSharedTest, StemWordInPlace_AlreadyStemmed) {
  std::string word = "run";
  processor_->StemWordInPlace(word, 3);
  EXPECT_EQ(word, "run");
}

// Empty string: should not crash, returned unchanged
TEST_F(SnowballProcessorSharedTest, StemWordInPlace_EmptyString) {
  std::string word;
  processor_->StemWordInPlace(word, 3);
  EXPECT_EQ(word, "");
}

// BuildStemMap: large min_stem_size produces empty map
TEST_F(SnowballProcessorSharedTest, BuildStemMap_MinStemSizePreventsAll) {
  std::vector<std::string> tokens = {"running", "jumps", "happily"};
  InProgressStemMap stem_mappings;

  processor_->BuildStemMap(tokens, 100, stem_mappings);

  EXPECT_TRUE(stem_mappings.empty());
}

// BuildStemMap: token that stems to itself is excluded from the map
TEST_F(SnowballProcessorSharedTest, BuildStemMap_SelfStemExcluded) {
  std::vector<std::string> tokens = {"run"};
  InProgressStemMap stem_mappings;

  processor_->BuildStemMap(tokens, 3, stem_mappings);

  EXPECT_TRUE(stem_mappings.empty());
}

// BuildStemMap: duplicate tokens in input produce only one entry per variant
TEST_F(SnowballProcessorSharedTest, BuildStemMap_DuplicateTokensDeduped) {
  std::vector<std::string> tokens = {"running", "running", "running"};
  InProgressStemMap stem_mappings;

  processor_->BuildStemMap(tokens, 3, stem_mappings);

  EXPECT_TRUE(stem_mappings.contains("run"));
  EXPECT_EQ(stem_mappings["run"].size(), 1);
}

// BuildStemMap: multiple distinct tokens mapping to same stem
TEST_F(SnowballProcessorSharedTest, BuildStemMap_MultipleWordsToSameStem) {
  std::vector<std::string> tokens = {"running", "runs"};
  InProgressStemMap stem_mappings;

  processor_->BuildStemMap(tokens, 3, stem_mappings);

  EXPECT_EQ(stem_mappings.size(), 1);
  EXPECT_TRUE(stem_mappings.contains("run"));
  EXPECT_EQ(stem_mappings["run"].size(), 2);
}

// BuildStemMap: multiple tokens producing distinct stems
TEST_F(SnowballProcessorSharedTest, BuildStemMap_MultipleDistinctStems) {
  std::vector<std::string> tokens = {"running", "jumps", "happily"};
  InProgressStemMap stem_mappings;

  processor_->BuildStemMap(tokens, 3, stem_mappings);

  EXPECT_EQ(stem_mappings.size(), 3);
  EXPECT_TRUE(stem_mappings.contains("run"));
  EXPECT_TRUE(stem_mappings.contains("jump"));
  EXPECT_TRUE(stem_mappings.contains("happili"));
}

// =============================================================================
// Multi-byte codepoint edge case (French)
//
// Verifies the AtLeastNCodepoints check counts Unicode codepoints, not bytes.
// "né" is 2 codepoints but 3 bytes; with min_stem_size=3, it must NOT stem.
// =============================================================================

TEST(SnowballProcessorMultiByteTest, FrenchCodePointCounting) {
  auto processor = CreateProcessor(data_model::LANGUAGE_FRENCH);
  // "né" = 'n' (1 byte) + 'é' (2 bytes) = 2 codepoints, 3 bytes
  std::string word = "n\xc3\xa9";
  processor->StemWordInPlace(word, 3);
  EXPECT_EQ(word, "n\xc3\xa9");
}

// =============================================================================
// Tokenize() pipeline tests — coverage from lexer_test.cc
//
// Tests the full 6-step pipeline: validate UTF-8 → punct split → normalize →
// stop words → stem map → emit.
// =============================================================================

class SnowballProcessorTokenizeTest : public ::testing::Test {
 protected:
  std::shared_ptr<LanguageProcessor> processor_ =
      CreateProcessor(data_model::LANGUAGE_ENGLISH);
};

TEST_F(SnowballProcessorTokenizeTest, EmptyStringReturnsNoTokens) {
  auto result = processor_->Tokenize("", true, 3, nullptr);
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result->empty());
}

TEST_F(SnowballProcessorTokenizeTest, OnlyPunctuationReturnsNoTokens) {
  auto result = processor_->Tokenize("   \t\n!@#$%^&*()   ", true, 3, nullptr);
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result->empty());
}

TEST_F(SnowballProcessorTokenizeTest, DefaultPunctuationSplitting) {
  auto result =
      processor_->Tokenize("hello,world!nice-day.today", false, 3, nullptr);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>(
                         {"hello", "world", "nice", "day", "today"}));
}

TEST_F(SnowballProcessorTokenizeTest, CaseConversion) {
  auto result = processor_->Tokenize("HELLO World miXeD", false, 3, nullptr);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world", "mixed"}));
}

TEST_F(SnowballProcessorTokenizeTest, Utf8Support) {
  auto result = processor_->Tokenize("hello \xe4\xb8\x96\xe7\x95\x8c test",
                                     false, 3, nullptr);
  ASSERT_TRUE(result.ok());
  // 世界 is not punctuation — should be preserved as a token
  EXPECT_EQ(*result, std::vector<std::string>(
                         {"hello", "\xe4\xb8\x96\xe7\x95\x8c", "test"}));
}

TEST_F(SnowballProcessorTokenizeTest, TabsAndNewlines) {
  auto result = processor_->Tokenize("hello\tworld\ntest", false, 3, nullptr);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world", "test"}));
}

TEST_F(SnowballProcessorTokenizeTest, InvalidUtf8ReturnsError) {
  auto result = processor_->Tokenize("hello \xFF\xFE world", true, 3, nullptr);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(result.status().message(), "Invalid UTF-8");
}

TEST_F(SnowballProcessorTokenizeTest, StopWordsFiltered) {
  // English default stop words include "the", "and"
  auto processor = LanguageProcessor::Create(
      data_model::LANGUAGE_ENGLISH,
      GetDefaultPunctuation(data_model::LANGUAGE_ENGLISH),
      {"the", "and", "or"});
  auto result = processor->Tokenize("the cat and dog", false, 3, nullptr);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"cat", "dog"}));
}

TEST_F(SnowballProcessorTokenizeTest, StemmingProducesStemMap) {
  InProgressStemMap stem_map;
  auto result = processor_->Tokenize("running jumps", true, 3, &stem_map);
  ASSERT_TRUE(result.ok());
  // Tokens are the original (lowercased) words, not stems
  EXPECT_EQ(*result, std::vector<std::string>({"running", "jumps"}));
  // Stem map records the mappings
  EXPECT_TRUE(stem_map.contains("run"));
  EXPECT_TRUE(stem_map.contains("jump"));
}

TEST_F(SnowballProcessorTokenizeTest, StemmingDisabledNoStemMap) {
  InProgressStemMap stem_map;
  auto result = processor_->Tokenize("running jumps", false, 3, &stem_map);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"running", "jumps"}));
  EXPECT_TRUE(stem_map.empty());
}

TEST_F(SnowballProcessorTokenizeTest, NonAsciiNotTreatedAsPunctuation) {
  // Emoji is not punctuation — should stay as part of the word
  auto result =
      processor_->Tokenize("hello\xf0\x9f\x99\x82world", false, 3, nullptr);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello\xf0\x9f\x99\x82world"}));
}

TEST_F(SnowballProcessorTokenizeTest, CustomPunctuation) {
  // Create processor with minimal punctuation (only space and comma)
  auto processor =
      LanguageProcessor::Create(data_model::LANGUAGE_ENGLISH, " ,", {});
  auto result =
      processor->Tokenize("hello,world!this-is_a.test", false, 3, nullptr);
  ASSERT_TRUE(result.ok());
  // Only space and comma split — everything else stays in the token
  EXPECT_EQ(*result,
            std::vector<std::string>({"hello", "world!this-is_a.test"}));
}

TEST_F(SnowballProcessorTokenizeTest, LongWord) {
  std::string long_word(1000, 'a');
  auto result = processor_->Tokenize(long_word, true, 3, nullptr);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({long_word}));
}

TEST_F(SnowballProcessorTokenizeTest, EmptyStopWordsAllWordsPassThrough) {
  auto processor = LanguageProcessor::Create(
      data_model::LANGUAGE_ENGLISH,
      GetDefaultPunctuation(data_model::LANGUAGE_ENGLISH), {});
  auto result = processor->Tokenize(
      "hello, world! testing 123 with-dashes and/or symbols", true, 3, nullptr);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world", "testing",
                                               "123", "with", "dashes", "and",
                                               "or", "symbols"}));
}

TEST_F(SnowballProcessorTokenizeTest, MultiBytePunctuation) {
  // Arabic comma ، (U+060C) as punctuation must split correctly without
  // corrupting Arabic text (all Arabic letters U+0600..U+06FF share lead byte
  // 0xD8 — byte-level splitting would shred them).
  auto processor = LanguageProcessor::Create(data_model::LANGUAGE_ENGLISH,
                                             "\xd8\x8c",  // ، U+060C only
                                             {});
  auto result = processor->Tokenize("hello\xd8\x8cworld", false, 4, nullptr);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world"}));

  // Arabic text split on whitespace (not on the shared 0xD8 byte)
  // Uses English processor with custom punctuation to isolate the multi-byte
  // handling in BuildPunctuationSet — avoids Arabic stop words/NFKC/stemming
  // confounding the result.
  result = processor->Tokenize(
      "\xd9\x85\xd8\xb1\xd8\xad\xd8\xa8\xd8\xa7 "
      "\xd8\xa8\xd8\xa7\xd9\x84\xd8\xb9\xd8\xa7\xd9\x84\xd9\x85",
      false, 4, nullptr);
  ASSERT_TRUE(result.ok());
  // مرحبا بالعالم → two tokens
  EXPECT_EQ(result->size(), 2);
}

TEST_F(SnowballProcessorTokenizeTest, EscapedMultiBytePunctuation) {
  // Backslash-escaped Arabic comma should be retained in the token
  auto processor = LanguageProcessor::Create(data_model::LANGUAGE_ENGLISH,
                                             "\xd8\x8c",  // ، U+060C only
                                             {});
  auto result = processor->Tokenize("hello\\\xd8\x8cworld", false, 4, nullptr);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello\xd8\x8cworld"}));
}

TEST_F(SnowballProcessorTokenizeTest,
       CanonicallyEquivalentFormsTokenizeIdentically) {
  // Precomposed "café" (é = U+00E9, C3 A9) and decomposed "café"
  // (e + combining acute U+0301, 65 CC 81) must produce the same token
  // after NFC normalization.
  auto processor = LanguageProcessor::Create(
      data_model::LANGUAGE_ENGLISH,
      GetDefaultPunctuation(data_model::LANGUAGE_ENGLISH), {});

  auto precomposed = processor->Tokenize("caf\xc3\xa9", false, 3, nullptr);
  auto decomposed = processor->Tokenize("cafe\xcc\x81", false, 3, nullptr);
  ASSERT_TRUE(precomposed.ok());
  ASSERT_TRUE(decomposed.ok());
  ASSERT_EQ(precomposed->size(), 1u);
  ASSERT_EQ(decomposed->size(), 1u);
  EXPECT_EQ((*precomposed)[0], (*decomposed)[0]);
  EXPECT_EQ((*precomposed)[0], "caf\xc3\xa9");
}

// =============================================================================
// Arabic NFKC normalization — verifies presentation forms collapse to base
// forms during tokenization (exercises the norm_form_ = NFKC code path).
// =============================================================================

TEST(SnowballProcessorArabicNFKCTest, PresentationFormsCollapseToBase) {
  auto processor = LanguageProcessor::Create(
      data_model::LANGUAGE_ARABIC,
      GetDefaultPunctuation(data_model::LANGUAGE_ARABIC), {});

  // Token with presentation form: ﻛﺘﺎﺏ (using presentation forms)
  // After NFKC: كتاب (base forms)
  std::string presentation_form =
      "\xef\xbb\x9b\xef\xba\x98\xef\xba\x8e\xef\xba\x8f";
  auto result = processor->Tokenize(presentation_form, false, 3, nullptr);
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 1);

  // Same word with base characters: كتاب
  std::string base_form = "\xd9\x83\xd8\xaa\xd8\xa7\xd8\xa8";
  auto result2 = processor->Tokenize(base_form, false, 3, nullptr);
  ASSERT_TRUE(result2.ok());
  ASSERT_EQ(result2->size(), 1);

  // Both should produce the same normalized token
  EXPECT_EQ((*result)[0], (*result2)[0]);
}

}  // namespace valkey_search::indexes::text
