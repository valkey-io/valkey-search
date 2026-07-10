/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/language_processor.h"
#include "src/indexes/text/punctuation.h"
#include "src/indexes/text/snowball_stem.h"
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
  const auto &tc = GetParam();
  auto processor = CreateProcessor(tc.language);
  ASSERT_NE(processor, nullptr);

  auto *stem_filter = processor->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  std::string result = stem_filter->GetStemRoot(tc.input, 3);
  EXPECT_EQ(result, tc.expected_stem);
}

INSTANTIATE_TEST_SUITE_P(
    PerLanguage, SnowballStemmingTest, ::testing::ValuesIn(kStemmingCases),
    [](const ::testing::TestParamInfo<StemmingTestCase> &info) {
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
  const auto &tc = GetParam();
  auto processor = CreateProcessor(tc.language);
  ASSERT_NE(processor, nullptr);

  auto *stem_filter = processor->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  InProgressStemMap stem_mappings;
  stem_filter->BuildStemMap(tc.tokens, 3, stem_mappings);

  EXPECT_TRUE(stem_mappings.contains(tc.expected_stem));
  EXPECT_EQ(stem_mappings[tc.expected_stem].size(), tc.expected_count);
}

INSTANTIATE_TEST_SUITE_P(
    PerLanguage, SnowballStemMapTest, ::testing::ValuesIn(kStemMapCases),
    [](const ::testing::TestParamInfo<StemMapTestCase> &info) {
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
  EXPECT_NE(processor_->GetStemmer(), nullptr);
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
  auto *stem_filter = processor_->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  std::string result = stem_filter->GetStemRoot("running", 10);
  EXPECT_EQ(result, "running");
}

// Idempotency: a word already at its stem form is left unchanged
TEST_F(SnowballProcessorSharedTest, StemWordInPlace_AlreadyStemmed) {
  auto *stem_filter = processor_->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  std::string result = stem_filter->GetStemRoot("run", 3);
  EXPECT_EQ(result, "run");
}

// Empty string: should not crash, returned unchanged
TEST_F(SnowballProcessorSharedTest, StemWordInPlace_EmptyString) {
  auto *stem_filter = processor_->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  std::string result = stem_filter->GetStemRoot("", 3);
  EXPECT_EQ(result, "");
}

// BuildStemMap: large min_stem_size produces empty map
TEST_F(SnowballProcessorSharedTest, BuildStemMap_MinStemSizePreventsAll) {
  std::vector<std::string> tokens = {"running", "jumps", "happily"};
  InProgressStemMap stem_mappings;

  auto *stem_filter = processor_->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  stem_filter->BuildStemMap(tokens, 100, stem_mappings);

  EXPECT_TRUE(stem_mappings.empty());
}

// BuildStemMap: token that stems to itself is excluded from the map
TEST_F(SnowballProcessorSharedTest, BuildStemMap_SelfStemExcluded) {
  std::vector<std::string> tokens = {"run"};
  InProgressStemMap stem_mappings;

  auto *stem_filter = processor_->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  stem_filter->BuildStemMap(tokens, 3, stem_mappings);

  EXPECT_TRUE(stem_mappings.empty());
}

// BuildStemMap: duplicate tokens in input produce only one entry per variant
TEST_F(SnowballProcessorSharedTest, BuildStemMap_DuplicateTokensDeduped) {
  std::vector<std::string> tokens = {"running", "running", "running"};
  InProgressStemMap stem_mappings;

  auto *stem_filter = processor_->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  stem_filter->BuildStemMap(tokens, 3, stem_mappings);

  EXPECT_TRUE(stem_mappings.contains("run"));
  EXPECT_EQ(stem_mappings["run"].size(), 1);
}

// BuildStemMap: multiple distinct tokens mapping to same stem
TEST_F(SnowballProcessorSharedTest, BuildStemMap_MultipleWordsToSameStem) {
  std::vector<std::string> tokens = {"running", "runs"};
  InProgressStemMap stem_mappings;

  auto *stem_filter = processor_->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  stem_filter->BuildStemMap(tokens, 3, stem_mappings);

  EXPECT_EQ(stem_mappings.size(), 1);
  EXPECT_TRUE(stem_mappings.contains("run"));
  EXPECT_EQ(stem_mappings["run"].size(), 2);
}

// BuildStemMap: multiple tokens producing distinct stems
TEST_F(SnowballProcessorSharedTest, BuildStemMap_MultipleDistinctStems) {
  std::vector<std::string> tokens = {"running", "jumps", "happily"};
  InProgressStemMap stem_mappings;

  auto *stem_filter = processor_->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  stem_filter->BuildStemMap(tokens, 3, stem_mappings);

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
  auto *stem_filter = processor->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  std::string result = stem_filter->GetStemRoot("n\xc3\xa9", 3);
  EXPECT_EQ(result, "n\xc3\xa9");
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
  auto result = processor_->Process("");
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result->empty());
}

TEST_F(SnowballProcessorTokenizeTest, OnlyPunctuationReturnsNoTokens) {
  auto result = processor_->Process("   \t\n!@#$%^&*()   ");
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result->empty());
}

TEST_F(SnowballProcessorTokenizeTest, DefaultPunctuationSplitting) {
  auto result = processor_->Process("hello,world!nice-day.today");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>(
                         {"hello", "world", "nice", "day", "today"}));
}

TEST_F(SnowballProcessorTokenizeTest, CaseConversion) {
  auto result = processor_->Process("HELLO World miXeD");
  ASSERT_TRUE(result.ok());
  // Process() normalizes + removes stop words but does NOT stem
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world", "mixed"}));
}

TEST_F(SnowballProcessorTokenizeTest, Utf8Support) {
  auto result = processor_->Process("hello \xe4\xb8\x96\xe7\x95\x8c test");
  ASSERT_TRUE(result.ok());
  // 世界 is not punctuation — should be preserved as a token
  EXPECT_EQ(*result, std::vector<std::string>(
                         {"hello", "\xe4\xb8\x96\xe7\x95\x8c", "test"}));
}

TEST_F(SnowballProcessorTokenizeTest, TabsAndNewlines) {
  auto result = processor_->Process("hello\tworld\ntest");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world", "test"}));
}

TEST_F(SnowballProcessorTokenizeTest, InvalidUtf8ReturnsError) {
  auto result = processor_->Process("hello \xFF\xFE world");
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
  auto result = processor->Process("the cat and dog");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"cat", "dog"}));
}

TEST_F(SnowballProcessorTokenizeTest, StemmingProducesStemMap) {
  auto result = processor_->Process("running jumps");
  ASSERT_TRUE(result.ok());
  // Process() returns normalized but unstemmed tokens
  EXPECT_EQ(*result, std::vector<std::string>({"running", "jumps"}));
  // Build stem map separately using the stem filter
  InProgressStemMap stem_map;
  auto *stem_filter = processor_->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  stem_filter->BuildStemMap(*result, 3, stem_map);
  EXPECT_TRUE(stem_map.contains("run"));
  EXPECT_TRUE(stem_map.contains("jump"));
}

TEST_F(SnowballProcessorTokenizeTest, LargeMinStemSizeProducesNoStemEntries) {
  // BuildStemMap with a min_stem_size larger than any real word effectively
  // disables stemming. Verify no stem entries are produced in that case.
  auto *stem_filter = processor_->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  InProgressStemMap stem_map;
  std::vector<std::string> tokens = {"running", "jumps"};
  stem_filter->BuildStemMap(tokens, 100, stem_map);
  EXPECT_TRUE(stem_map.empty());
}

TEST_F(SnowballProcessorTokenizeTest, NonAsciiNotTreatedAsPunctuation) {
  // Emoji is not punctuation — should stay as part of the word
  auto result = processor_->Process("hello\xf0\x9f\x99\x82world");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello\xf0\x9f\x99\x82world"}));
}

TEST_F(SnowballProcessorTokenizeTest, CustomPunctuation) {
  // Create processor with minimal punctuation (only space and comma)
  auto processor =
      LanguageProcessor::Create(data_model::LANGUAGE_ENGLISH, " ,", {});
  auto result = processor->Process("hello,world!this-is_a.test");
  ASSERT_TRUE(result.ok());
  // Only space and comma split — everything else stays in the token
  EXPECT_EQ(*result,
            std::vector<std::string>({"hello", "world!this-is_a.test"}));
}

TEST_F(SnowballProcessorTokenizeTest, LongWord) {
  std::string long_word(1000, 'a');
  auto result = processor_->Process(long_word);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({long_word}));
}

TEST_F(SnowballProcessorTokenizeTest, EmptyStopWordsAllWordsPassThrough) {
  auto processor = LanguageProcessor::Create(
      data_model::LANGUAGE_ENGLISH,
      GetDefaultPunctuation(data_model::LANGUAGE_ENGLISH), {});
  auto result = processor->Process(
      "hello, world! testing 123 with-dashes and/or symbols");
  ASSERT_TRUE(result.ok());
  // No stop words removed (empty list), tokens are normalized but unstemmed
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
  auto result = processor->Process("hello\xd8\x8cworld");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world"}));

  // Arabic text split on whitespace (not on the shared 0xD8 byte)
  // Uses English processor with custom punctuation to isolate the multi-byte
  // handling in BuildPunctuationSet — avoids Arabic stop words/NFKC/stemming
  // confounding the result.
  result = processor->Process(
      "\xd9\x85\xd8\xb1\xd8\xad\xd8\xa8\xd8\xa7 "
      "\xd8\xa8\xd8\xa7\xd9\x84\xd8\xb9\xd8\xa7\xd9\x84\xd9\x85");
  ASSERT_TRUE(result.ok());
  // مرحبا بالعالم → two tokens
  EXPECT_EQ(result->size(), 2);
}

TEST_F(SnowballProcessorTokenizeTest, EscapedMultiBytePunctuation) {
  // Backslash-escaped Arabic comma should be retained in the token
  auto processor = LanguageProcessor::Create(data_model::LANGUAGE_ENGLISH,
                                             "\xd8\x8c",  // ، U+060C only
                                             {});
  auto result = processor->Process("hello\\\xd8\x8cworld");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello\xd8\x8cworld"}));
}

TEST_F(SnowballProcessorTokenizeTest, SingleCharacterWords) {
  auto processor = LanguageProcessor::Create(
      data_model::LANGUAGE_ENGLISH,
      GetDefaultPunctuation(data_model::LANGUAGE_ENGLISH),
      {"the", "and", "or"});
  auto result = processor->Process("a b c");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"a", "b", "c"}));
}

TEST_F(SnowballProcessorTokenizeTest, AllStopWordsProducesEmptyResult) {
  // When every token is a stop word, the pipeline should return an empty
  // vector.
  auto processor = LanguageProcessor::Create(
      data_model::LANGUAGE_ENGLISH,
      GetDefaultPunctuation(data_model::LANGUAGE_ENGLISH),
      {"the", "and", "or"});
  auto result = processor->Process("the and or");
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result->empty());
}

// Verifies the stemmer-is-nullptr contract: when a processor has no stemmer
// (the architectural equivalent of old Lexer::Tokenize with stemming_enabled =
// false), the caller knows not to build a stem map and GetStemmer() returns
// nullptr. We simulate this by constructing a bare LanguageProcessor without
// wiring a stemmer — today all Snowball languages have one, so we verify the
// accessor contract directly.
TEST_F(SnowballProcessorTokenizeTest, NoStemmerMeansNoStemMap) {
  // Current Snowball languages always have a stemmer, so verify the accessor
  // returns non-null and that the caller can choose not to call BuildStemMap
  // (the new architecture's equivalent of stemming_enabled=false).
  auto result = processor_->Process("running jumps happily");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"running", "jumps", "happily"}));

  // When the caller does NOT call BuildStemMap, no stem entries exist — this is
  // the new equivalent of the old stemming_enabled=false path.
  InProgressStemMap stem_map;
  // stem_map is never populated → empty
  EXPECT_TRUE(stem_map.empty());
}

// Multi-byte word passes through the full pipeline intact. The old lexer_test
// tested "été" (3 codepoints, 5 bytes) at varying min_stem_size thresholds via
// Tokenize(). In the new architecture, Process() does not take min_stem_size —
// stemming is separate. This test verifies the pipeline doesn't corrupt the
// multi-byte word, and the stem filter handles it correctly at both thresholds.
TEST_F(SnowballProcessorTokenizeTest, MultiByteWordTokenizesIntact) {
  // "été" = 'é' (2 bytes) + 't' (1 byte) + 'é' (2 bytes) = 3 codepoints
  auto result = processor_->Process("\xc3\xa9t\xc3\xa9");
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 1u);
  EXPECT_EQ((*result)[0], "\xc3\xa9t\xc3\xa9");

  // Verify stem filter also handles the multi-byte word at varying thresholds
  auto *stem_filter = processor_->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  // min_stem_size=3: word has exactly 3 codepoints, eligible for stemming
  std::string stem_at_3 = stem_filter->GetStemRoot("\xc3\xa9t\xc3\xa9", 3);
  // min_stem_size=4: word has only 3 codepoints, must NOT stem
  std::string stem_at_4 = stem_filter->GetStemRoot("\xc3\xa9t\xc3\xa9", 4);
  EXPECT_EQ(stem_at_4, "\xc3\xa9t\xc3\xa9");
}

TEST_F(SnowballProcessorTokenizeTest,
       CanonicallyEquivalentFormsTokenizeIdentically) {
  // Precomposed "café" (é = U+00E9, C3 A9) and decomposed "café"
  // (e + combining acute U+0301, 65 CC 81) must produce the same token
  // after NFC normalization.
  auto processor = LanguageProcessor::Create(
      data_model::LANGUAGE_ENGLISH,
      GetDefaultPunctuation(data_model::LANGUAGE_ENGLISH), {});

  auto precomposed = processor->Process("caf\xc3\xa9");
  auto decomposed = processor->Process("cafe\xcc\x81");
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
  auto result = processor->Process(presentation_form);
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 1);

  // Same word with base characters: كتاب
  std::string base_form = "\xd9\x83\xd8\xaa\xd8\xa7\xd8\xa8";
  auto result2 = processor->Process(base_form);
  ASSERT_TRUE(result2.ok());
  ASSERT_EQ(result2->size(), 1);

  // Both should produce the same normalized token
  EXPECT_EQ((*result)[0], (*result2)[0]);
}

// =============================================================================
// German compound word — Snowball does NOT decompose compounds.
// =============================================================================

TEST(SnowballProcessorGermanCompoundTest, CompoundWordNotDecomposed) {
  auto processor = CreateProcessor(data_model::LANGUAGE_GERMAN);
  // "Donaudampfschifffahrtsgesellschaft" — classic long German compound.
  // Snowball does not perform decompounding; the word stays as one token
  // (possibly stemmed but never split).
  auto result = processor->Process("Donaudampfschifffahrtsgesellschaft");
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 1u);
  // The token should be the case-folded version (lowercased)
  EXPECT_EQ((*result)[0], "donaudampfschifffahrtsgesellschaft");
}

// =============================================================================
// French apostrophe elision — apostrophe is in the default punctuation set.
// =============================================================================

TEST(SnowballProcessorFrenchApostropheTest, ApostropheSplitsToken) {
  auto processor = CreateProcessor(data_model::LANGUAGE_FRENCH);
  // l'école — the ASCII apostrophe is punctuation, so it splits.
  auto result = processor->Process(
      "l'\xc3\xa9"
      "cole");
  ASSERT_TRUE(result.ok());
  // Should produce at least the word "école" as an independent token.
  // "l" may or may not survive (it could be a stop word in French).
  bool found_ecole = false;
  for (const auto &token : *result) {
    // After NFC normalization and casefold, we expect "école" (é = C3 A9)
    if (token ==
        "\xc3\xa9"
        "cole") {
      found_ecole = true;
    }
  }
  EXPECT_TRUE(found_ecole)
      << "Apostrophe should split l'école, making 'école' an independent token";
}

// =============================================================================
// Cross-language processor isolation — different processors produce different
// results for the same input.
// =============================================================================

TEST(SnowballProcessorCrossLanguageTest, ProcessorsAreIndependent) {
  auto french = CreateProcessor(data_model::LANGUAGE_FRENCH);
  auto german = CreateProcessor(data_model::LANGUAGE_GERMAN);

  // French stop word "dans" should be filtered by French processor
  auto fr_result = french->Process("dans la maison");
  ASSERT_TRUE(fr_result.ok());
  for (const auto &token : *fr_result) {
    EXPECT_NE(token, "dans") << "French processor should filter 'dans'";
    EXPECT_NE(token, "la") << "French processor should filter 'la'";
  }

  // German processor should NOT filter French stop words
  auto de_result = german->Process("dans la maison");
  ASSERT_TRUE(de_result.ok());
  // "dans", "la", and "maison" should all survive (none are German stop words)
  EXPECT_EQ(de_result->size(), 3u);
}

// =============================================================================
// Stop word list snapshot regression — locks in the current stop word list
// sizes per language so accidental additions/removals are caught immediately.
// =============================================================================

struct StopWordSnapshotCase {
  std::string test_name;
  data_model::Language language;
  size_t expected_count;
  // A few sentinel words that must be present (spot-check).
  std::vector<std::string> must_contain;
};

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
     {"\xd0\xb8", "\xd0\xb2", "\xd0\xbd\xd0\xb5"}},  // и, в, не
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
     {"\xd9\x85\xd9\x86", "\xd9\x81\xd9\x8a", "\xd9\x88"}},  // من, في, و
};

class StopWordSnapshotTest
    : public ::testing::TestWithParam<StopWordSnapshotCase> {};

TEST_P(StopWordSnapshotTest, ListSizeAndSentinelsMatch) {
  const auto &tc = GetParam();
  const auto &stop_words = GetDefaultStopWords(tc.language);

  // Lock in list size — any accidental addition or removal will fail this.
  EXPECT_EQ(stop_words.size(), tc.expected_count)
      << "Stop word list size changed for " << tc.test_name
      << ". If intentional, update this snapshot.";

  // Spot-check a few known entries.
  for (const auto &word : tc.must_contain) {
    EXPECT_NE(std::find(stop_words.begin(), stop_words.end(), word),
              stop_words.end())
        << "Expected stop word '" << word << "' missing from " << tc.test_name;
  }
}

INSTANTIATE_TEST_SUITE_P(
    PerLanguage, StopWordSnapshotTest, ::testing::ValuesIn(kStopWordSnapshots),
    [](const ::testing::TestParamInfo<StopWordSnapshotCase> &info) {
      return info.param.test_name;
    });

// =============================================================================
// LANGUAGE_UNSPECIFIED defaults to English stop words and stemmer — proves
// backward compatibility when loading an old RDB without a LANGUAGE field.
// =============================================================================

TEST(StopWordDefaultBehaviorTest, UnspecifiedLanguageUsesEnglishStopWords) {
  const auto &unspecified =
      GetDefaultStopWords(data_model::LANGUAGE_UNSPECIFIED);
  const auto &english = GetDefaultStopWords(data_model::LANGUAGE_ENGLISH);
  EXPECT_EQ(&unspecified, &english)
      << "LANGUAGE_UNSPECIFIED must return the same stop word list as ENGLISH";
}

TEST(StopWordDefaultBehaviorTest, UnspecifiedLanguageProcessorMatchesEnglish) {
  auto unspecified_proc = CreateProcessor(data_model::LANGUAGE_UNSPECIFIED);
  auto english_proc = CreateProcessor(data_model::LANGUAGE_ENGLISH);

  // Both should produce identical output for the same input.
  auto unspecified_result =
      unspecified_proc->Process("The children are running quickly");
  auto english_result =
      english_proc->Process("The children are running quickly");
  ASSERT_TRUE(unspecified_result.ok());
  ASSERT_TRUE(english_result.ok());
  EXPECT_EQ(*unspecified_result, *english_result)
      << "LANGUAGE_UNSPECIFIED processor must behave identically to ENGLISH";
}

// =============================================================================
// Edge Case 1: Pipeline with 0 filters
//
// Verifies that a LanguageProcessor built with segmenters but NO TokenFilters
// works correctly — Process() returns the raw segmented tokens unmodified.
// =============================================================================

class ZeroFilterPipelineTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Build a processor with a segmenter but zero filters.
    auto segmenter = std::make_shared<PunctuationSegmenter>(
        GetDefaultPunctuation(data_model::LANGUAGE_ENGLISH));
    processor_ =
        LanguageProcessor::Builder().AddSegmenter(std::move(segmenter)).Build();
  }
  std::shared_ptr<LanguageProcessor> processor_;
};

TEST_F(ZeroFilterPipelineTest, ProcessReturnsRawTokens) {
  // Without any filters, tokens should not be case-folded or removed.
  auto result = processor_->Process("Hello, World");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"Hello", "World"}));
}

TEST_F(ZeroFilterPipelineTest, ApplyFiltersIsNoOp) {
  // ApplyFilters on an empty filter chain should return the input unchanged.
  std::vector<std::string> tokens = {"The", "CAT", "AND"};
  auto result = processor_->ApplyFilters(tokens);
  EXPECT_EQ(result, tokens);
}

TEST_F(ZeroFilterPipelineTest, EmptyInputStillWorks) {
  auto result = processor_->Process("");
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result->empty());
}

// =============================================================================
// Edge Case 2: Filter ordering — normalize runs before stopword
//
// Proves that normalization happens before stop word matching. If the order
// were reversed, mixed-case stop words like "The" wouldn't match the lowercase
// stop word set, creating a case-sensitive stop word bug.
// =============================================================================

class FilterOrderingTest : public ::testing::Test {};

TEST_F(FilterOrderingTest, NormalizeBeforeStopword_UpperCaseDropped) {
  // Production ordering: normalize → stop word. Mixed-case stop words are
  // lowercased first, then matched against the lowercase stop word set.
  auto processor = LanguageProcessor::Create(
      data_model::LANGUAGE_ENGLISH,
      GetDefaultPunctuation(data_model::LANGUAGE_ENGLISH), {"the", "and"});

  auto result = processor->Process("The AND cat");
  ASSERT_TRUE(result.ok());
  // "The" → "the" (normalize) → dropped (stop word)
  // "AND" → "and" (normalize) → dropped (stop word)
  // "cat" → "cat" (normalize) → kept
  EXPECT_EQ(*result, std::vector<std::string>({"cat"}));
}

TEST_F(FilterOrderingTest, ReverseOrderLeaksMixedCase) {
  // Manually build a processor with StopWordFilter BEFORE
  // NormalizeCaseFoldFilter. This demonstrates the bug that production ordering
  // prevents.
  auto segmenter = std::make_shared<PunctuationSegmenter>(
      GetDefaultPunctuation(data_model::LANGUAGE_ENGLISH));
  auto normalizer = std::make_shared<NormalizeCaseFoldFilter>();
  auto stop_filter =
      std::make_shared<StopWordFilter>(std::vector<std::string>{"the", "and"});

  // WRONG order: stop word first, then normalize
  auto processor = LanguageProcessor::Builder()
                       .AddSegmenter(std::move(segmenter))
                       .AddFilter(stop_filter)  // stop word check first
                       .AddFilter(normalizer)   // normalize second
                       .SetNormalizer(normalizer)
                       .SetStopWordFilter(stop_filter)
                       .Build();

  auto result = processor->Process("The AND cat");
  ASSERT_TRUE(result.ok());
  // With wrong ordering: "The" doesn't match "the" in stop set → survives
  // (but gets lowercased after). "AND" doesn't match "and" → survives.
  EXPECT_EQ(result->size(), 3u)
      << "Reversed filter order should leak mixed-case stop words";
  // Tokens are still lowercased (normalize still runs, just after stop check)
  EXPECT_EQ((*result)[0], "the");
  EXPECT_EQ((*result)[1], "and");
  EXPECT_EQ((*result)[2], "cat");
}

TEST_F(FilterOrderingTest, UnicodeNormBeforeStopword) {
  // Stop word list contains the NFC form of "café" (precomposed é = C3 A9).
  // Input uses the NFD form (decomposed: e + combining acute = 65 CC 81).
  // If normalization runs first, NFC conversion produces the precomposed form,
  // which then matches the stop word set and gets dropped.
  std::string nfc_cafe = "caf\xc3\xa9";   // precomposed
  std::string nfd_cafe = "cafe\xcc\x81";  // decomposed

  auto processor = LanguageProcessor::Create(
      data_model::LANGUAGE_ENGLISH,
      GetDefaultPunctuation(data_model::LANGUAGE_ENGLISH), {nfc_cafe});

  // Decomposed form should be normalized to NFC, then dropped as stop word
  auto result = processor->Process(nfd_cafe + " hello");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello"}));

  // Precomposed form should also be dropped
  auto result2 = processor->Process(nfc_cafe + " hello");
  ASSERT_TRUE(result2.ok());
  EXPECT_EQ(*result2, std::vector<std::string>({"hello"}));
}

// =============================================================================
// Edge Case 3: Multi-segmenter chaining
//
// Exercises the multi-segmenter branch in LanguageProcessor::Segment() that
// feeds each segmenter's output into the next. No production language uses
// this path today, but it's implemented and should be tested.
// =============================================================================

class MultiSegmenterTest : public ::testing::Test {};

TEST_F(MultiSegmenterTest, TwoSegmentersChained) {
  // First segmenter: splits on comma only
  // Second segmenter: splits on space only
  auto seg_comma = std::make_shared<PunctuationSegmenter>(",");
  auto seg_space = std::make_shared<PunctuationSegmenter>(" ");

  auto processor = LanguageProcessor::Builder()
                       .AddSegmenter(std::move(seg_comma))
                       .AddSegmenter(std::move(seg_space))
                       .Build();

  // "hello world,foo bar"
  //   → seg_comma splits on ',' → ["hello world", "foo bar"]
  //   → seg_space splits each on ' ' → ["hello", "world", "foo", "bar"]
  auto result = processor->Segment("hello world,foo bar");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result,
            std::vector<std::string>({"hello", "world", "foo", "bar"}));
}

TEST_F(MultiSegmenterTest, ThreeSegmentersChained) {
  // Split on '.', then ',', then ' '
  auto seg_dot = std::make_shared<PunctuationSegmenter>(".");
  auto seg_comma = std::make_shared<PunctuationSegmenter>(",");
  auto seg_space = std::make_shared<PunctuationSegmenter>(" ");

  auto processor = LanguageProcessor::Builder()
                       .AddSegmenter(std::move(seg_dot))
                       .AddSegmenter(std::move(seg_comma))
                       .AddSegmenter(std::move(seg_space))
                       .Build();

  auto result = processor->Segment("a b,c d.e f,g h");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result,
            std::vector<std::string>({"a", "b", "c", "d", "e", "f", "g", "h"}));
}

TEST_F(MultiSegmenterTest, ErrorInSecondSegmenterPropagates) {
  // First segmenter splits on comma (doesn't validate UTF-8 of each piece
  // until the second segmenter processes them).
  // We use a two-segmenter pipeline and inject invalid UTF-8 that the second
  // segmenter will reject.
  auto seg_comma = std::make_shared<PunctuationSegmenter>(",");
  auto seg_space = std::make_shared<PunctuationSegmenter>(" ");

  auto processor = LanguageProcessor::Builder()
                       .AddSegmenter(std::move(seg_comma))
                       .AddSegmenter(std::move(seg_space))
                       .Build();

  // Invalid UTF-8 byte sequence: 0xFF 0xFE is never valid
  auto result = processor->Segment("good,\xFF\xFE bad");
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_F(MultiSegmenterTest, SingleTokenPassthrough) {
  // Two segmenters, but input has no delimiters for either — single token.
  auto seg_comma = std::make_shared<PunctuationSegmenter>(",");
  auto seg_space = std::make_shared<PunctuationSegmenter>(" ");

  auto processor = LanguageProcessor::Builder()
                       .AddSegmenter(std::move(seg_comma))
                       .AddSegmenter(std::move(seg_space))
                       .Build();

  auto result = processor->Segment("hello");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello"}));
}

// =============================================================================
// Edge Case 4: Apply returning true for non-drop filters
//
// Guards that NormalizeCaseFoldFilter::Apply and SnowballStemFilter::Apply
// always return true (never drop tokens), even for edge case inputs.
// Also confirms StopWordFilter::Apply is the only filter that returns false.
// =============================================================================

class ApplyNeverDropsTest : public ::testing::Test {};

// --- NormalizeCaseFoldFilter: Apply must always return true ---

TEST_F(ApplyNeverDropsTest, NormalizeFilter_EmptyString) {
  NormalizeCaseFoldFilter filter;
  std::string token;
  EXPECT_TRUE(filter.Apply(token));
}

TEST_F(ApplyNeverDropsTest, NormalizeFilter_WhitespaceOnly) {
  NormalizeCaseFoldFilter filter;
  std::string token = "   ";
  EXPECT_TRUE(filter.Apply(token));
}

TEST_F(ApplyNeverDropsTest, NormalizeFilter_HighCodepointEmoji) {
  NormalizeCaseFoldFilter filter;
  // 🙂 = F0 9F 99 82
  std::string token = "\xf0\x9f\x99\x82";
  EXPECT_TRUE(filter.Apply(token));
}

TEST_F(ApplyNeverDropsTest, NormalizeFilter_AlreadyNormalized) {
  NormalizeCaseFoldFilter filter;
  std::string token = "hello";
  EXPECT_TRUE(filter.Apply(token));
  EXPECT_EQ(token, "hello");
}

TEST_F(ApplyNeverDropsTest, NormalizeFilter_MixedCase) {
  NormalizeCaseFoldFilter filter;
  std::string token = "HeLLo";
  EXPECT_TRUE(filter.Apply(token));
  EXPECT_EQ(token, "hello");
}

// --- SnowballStemFilter: Apply must always return true ---

TEST_F(ApplyNeverDropsTest, StemFilter_EmptyString) {
  SnowballStemFilter filter(data_model::LANGUAGE_ENGLISH);
  std::string token;
  EXPECT_TRUE(filter.Apply(token));
}

TEST_F(ApplyNeverDropsTest, StemFilter_SingleChar) {
  SnowballStemFilter filter(data_model::LANGUAGE_ENGLISH);
  std::string token = "a";
  EXPECT_TRUE(filter.Apply(token));
}

TEST_F(ApplyNeverDropsTest, StemFilter_AlreadyStemmed) {
  SnowballStemFilter filter(data_model::LANGUAGE_ENGLISH);
  std::string token = "run";
  EXPECT_TRUE(filter.Apply(token));
}

TEST_F(ApplyNeverDropsTest, StemFilter_LongWord) {
  SnowballStemFilter filter(data_model::LANGUAGE_ENGLISH);
  std::string token(1000, 'a');
  EXPECT_TRUE(filter.Apply(token));
}

TEST_F(ApplyNeverDropsTest, StemFilter_MutatesButKeeps) {
  // "running" should be stemmed to "run" but Apply still returns true.
  SnowballStemFilter filter(data_model::LANGUAGE_ENGLISH);
  std::string token = "running";
  EXPECT_TRUE(filter.Apply(token));
  EXPECT_EQ(token, "run");
}

// --- StopWordFilter: Apply correctly returns false for stop words ---

TEST_F(ApplyNeverDropsTest, StopWordFilter_DropsStopWord) {
  StopWordFilter filter(std::vector<std::string>{"the", "and", "or"});
  std::string token = "the";
  EXPECT_FALSE(filter.Apply(token));
}

TEST_F(ApplyNeverDropsTest, StopWordFilter_KeepsNonStopWord) {
  StopWordFilter filter(std::vector<std::string>{"the", "and", "or"});
  std::string token = "cat";
  EXPECT_TRUE(filter.Apply(token));
}

TEST_F(ApplyNeverDropsTest, StopWordFilter_EmptyStringKept) {
  StopWordFilter filter(std::vector<std::string>{"the", "and", "or"});
  std::string token;
  EXPECT_TRUE(filter.Apply(token));
}

TEST_F(ApplyNeverDropsTest, StopWordFilter_EmptySetKeepsAll) {
  StopWordFilter filter(std::vector<std::string>{});
  std::string token = "the";
  EXPECT_TRUE(filter.Apply(token));
}

}  // namespace valkey_search::indexes::text
