/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/snowball_stem.h"

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

class SnowballStemSharedTest : public ::testing::Test {
 protected:
  std::shared_ptr<LanguageProcessor> processor_ =
      CreateProcessor(data_model::LANGUAGE_ENGLISH);
};

// min_stem_size guard: word with fewer codepoints than threshold is not stemmed
TEST_F(SnowballStemSharedTest, GetStemRoot_MinStemSizePrevents) {
  auto *stem_filter = processor_->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  std::string result = stem_filter->GetStemRoot("running", 10);
  EXPECT_EQ(result, "running");
}

// Idempotency: a word already at its stem form is left unchanged
TEST_F(SnowballStemSharedTest, GetStemRoot_AlreadyStemmed) {
  auto *stem_filter = processor_->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  std::string result = stem_filter->GetStemRoot("run", 3);
  EXPECT_EQ(result, "run");
}

// Empty string: should not crash, returned unchanged
TEST_F(SnowballStemSharedTest, GetStemRoot_EmptyString) {
  auto *stem_filter = processor_->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  std::string result = stem_filter->GetStemRoot("", 3);
  EXPECT_EQ(result, "");
}

// BuildStemMap: large min_stem_size produces empty map
TEST_F(SnowballStemSharedTest, BuildStemMap_MinStemSizePreventsAll) {
  std::vector<std::string> tokens = {"running", "jumps", "happily"};
  InProgressStemMap stem_mappings;

  auto *stem_filter = processor_->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  stem_filter->BuildStemMap(tokens, 100, stem_mappings);

  EXPECT_TRUE(stem_mappings.empty());
}

// BuildStemMap: token that stems to itself is excluded from the map
TEST_F(SnowballStemSharedTest, BuildStemMap_SelfStemExcluded) {
  std::vector<std::string> tokens = {"run"};
  InProgressStemMap stem_mappings;

  auto *stem_filter = processor_->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  stem_filter->BuildStemMap(tokens, 3, stem_mappings);

  EXPECT_TRUE(stem_mappings.empty());
}

// BuildStemMap: duplicate tokens in input produce only one entry per variant
TEST_F(SnowballStemSharedTest, BuildStemMap_DuplicateTokensDeduped) {
  std::vector<std::string> tokens = {"running", "running", "running"};
  InProgressStemMap stem_mappings;

  auto *stem_filter = processor_->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  stem_filter->BuildStemMap(tokens, 3, stem_mappings);

  EXPECT_TRUE(stem_mappings.contains("run"));
  EXPECT_EQ(stem_mappings["run"].size(), 1);
}

// BuildStemMap: multiple distinct tokens mapping to same stem
TEST_F(SnowballStemSharedTest, BuildStemMap_MultipleWordsToSameStem) {
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
TEST_F(SnowballStemSharedTest, BuildStemMap_MultipleDistinctStems) {
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

TEST(SnowballStemMultiByteTest, FrenchCodePointCounting) {
  auto processor = CreateProcessor(data_model::LANGUAGE_FRENCH);
  // "né" = 'n' (1 byte) + 'é' (2 bytes) = 2 codepoints, 3 bytes
  auto *stem_filter = processor->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  std::string result = stem_filter->GetStemRoot("n\xc3\xa9", 3);
  EXPECT_EQ(result, "n\xc3\xa9");
}

// =============================================================================
// SnowballStemFilter::Apply — always returns true (never drops tokens)
// =============================================================================

TEST(SnowballStemApplyTest, EmptyString) {
  SnowballStemFilter filter(data_model::LANGUAGE_ENGLISH);
  std::string token;
  EXPECT_TRUE(filter.Apply(token));
}

TEST(SnowballStemApplyTest, SingleChar) {
  SnowballStemFilter filter(data_model::LANGUAGE_ENGLISH);
  std::string token = "a";
  EXPECT_TRUE(filter.Apply(token));
}

TEST(SnowballStemApplyTest, AlreadyStemmed) {
  SnowballStemFilter filter(data_model::LANGUAGE_ENGLISH);
  std::string token = "run";
  EXPECT_TRUE(filter.Apply(token));
}

TEST(SnowballStemApplyTest, LongWord) {
  SnowballStemFilter filter(data_model::LANGUAGE_ENGLISH);
  std::string token(1000, 'a');
  EXPECT_TRUE(filter.Apply(token));
}

TEST(SnowballStemApplyTest, MutatesButKeeps) {
  // "running" should be stemmed to "run" but Apply still returns true.
  SnowballStemFilter filter(data_model::LANGUAGE_ENGLISH);
  std::string token = "running";
  EXPECT_TRUE(filter.Apply(token));
  EXPECT_EQ(token, "run");
}

}  // namespace valkey_search::indexes::text
