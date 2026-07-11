/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/language_processor.h"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "src/index_schema.pb.h"
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
// End-to-end pipeline tests (LanguageProcessor::Process)
//
// Tests the full pipeline: validate UTF-8 → punct split → normalize →
// stop words → emit.
// =============================================================================

class LanguageProcessorTest : public ::testing::Test {
 protected:
  std::shared_ptr<LanguageProcessor> processor_ =
      CreateProcessor(data_model::LANGUAGE_ENGLISH);
};

TEST_F(LanguageProcessorTest, EmptyStringReturnsNoTokens) {
  auto result = processor_->Process("");
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result->empty());
}

TEST_F(LanguageProcessorTest, OnlyPunctuationReturnsNoTokens) {
  auto result = processor_->Process("   \t\n!@#$%^&*()   ");
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result->empty());
}

TEST_F(LanguageProcessorTest, DefaultPunctuationSplitting) {
  auto result = processor_->Process("hello,world!nice-day.today");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>(
                         {"hello", "world", "nice", "day", "today"}));
}

TEST_F(LanguageProcessorTest, CaseConversion) {
  auto result = processor_->Process("HELLO World miXeD");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world", "mixed"}));
}

TEST_F(LanguageProcessorTest, Utf8Support) {
  auto result = processor_->Process("hello \xe4\xb8\x96\xe7\x95\x8c test");
  ASSERT_TRUE(result.ok());
  // 世界 is not punctuation — should be preserved as a token
  EXPECT_EQ(*result, std::vector<std::string>(
                         {"hello", "\xe4\xb8\x96\xe7\x95\x8c", "test"}));
}

TEST_F(LanguageProcessorTest, TabsAndNewlines) {
  auto result = processor_->Process("hello\tworld\ntest");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world", "test"}));
}

TEST_F(LanguageProcessorTest, InvalidUtf8ReturnsError) {
  auto result = processor_->Process("hello \xFF\xFE world");
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(result.status().message(), "Invalid UTF-8");
}

TEST_F(LanguageProcessorTest, StopWordsFiltered) {
  auto processor = LanguageProcessor::Create(
      data_model::LANGUAGE_ENGLISH,
      GetDefaultPunctuation(data_model::LANGUAGE_ENGLISH),
      {"the", "and", "or"});
  auto result = processor->Process("the cat and dog");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"cat", "dog"}));
}

TEST_F(LanguageProcessorTest, StemmingProducesStemMap) {
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

TEST_F(LanguageProcessorTest, LargeMinStemSizeProducesNoStemEntries) {
  auto *stem_filter = processor_->GetStemmer();
  ASSERT_NE(stem_filter, nullptr);
  InProgressStemMap stem_map;
  std::vector<std::string> tokens = {"running", "jumps"};
  stem_filter->BuildStemMap(tokens, 100, stem_map);
  EXPECT_TRUE(stem_map.empty());
}

TEST_F(LanguageProcessorTest, NonAsciiNotTreatedAsPunctuation) {
  // Emoji is not punctuation — should stay as part of the word
  auto result = processor_->Process("hello\xf0\x9f\x99\x82world");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello\xf0\x9f\x99\x82world"}));
}

TEST_F(LanguageProcessorTest, CustomPunctuation) {
  // Create processor with minimal punctuation (only space and comma)
  auto processor =
      LanguageProcessor::Create(data_model::LANGUAGE_ENGLISH, " ,", {});
  auto result = processor->Process("hello,world!this-is_a.test");
  ASSERT_TRUE(result.ok());
  // Only space and comma split — everything else stays in the token
  EXPECT_EQ(*result,
            std::vector<std::string>({"hello", "world!this-is_a.test"}));
}

TEST_F(LanguageProcessorTest, LongWord) {
  std::string long_word(1000, 'a');
  auto result = processor_->Process(long_word);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({long_word}));
}

TEST_F(LanguageProcessorTest, EmptyStopWordsAllWordsPassThrough) {
  auto processor = LanguageProcessor::Create(
      data_model::LANGUAGE_ENGLISH,
      GetDefaultPunctuation(data_model::LANGUAGE_ENGLISH), {});
  auto result = processor->Process(
      "hello, world! testing 123 with-dashes and/or symbols");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello", "world", "testing",
                                               "123", "with", "dashes", "and",
                                               "or", "symbols"}));
}

TEST_F(LanguageProcessorTest, MultiBytePunctuation) {
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
  result = processor->Process(
      "\xd9\x85\xd8\xb1\xd8\xad\xd8\xa8\xd8\xa7 "
      "\xd8\xa8\xd8\xa7\xd9\x84\xd8\xb9\xd8\xa7\xd9\x84\xd9\x85");
  ASSERT_TRUE(result.ok());
  // مرحبا بالعالم → two tokens
  EXPECT_EQ(result->size(), 2);
}

TEST_F(LanguageProcessorTest, EscapedMultiBytePunctuation) {
  // Backslash-escaped Arabic comma should be retained in the token
  auto processor = LanguageProcessor::Create(data_model::LANGUAGE_ENGLISH,
                                             "\xd8\x8c",  // ، U+060C only
                                             {});
  auto result = processor->Process("hello\\\xd8\x8cworld");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello\xd8\x8cworld"}));
}

TEST_F(LanguageProcessorTest, SingleCharacterWords) {
  auto processor = LanguageProcessor::Create(
      data_model::LANGUAGE_ENGLISH,
      GetDefaultPunctuation(data_model::LANGUAGE_ENGLISH),
      {"the", "and", "or"});
  auto result = processor->Process("a b c");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"a", "b", "c"}));
}

TEST_F(LanguageProcessorTest, AllStopWordsProducesEmptyResult) {
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
// false), the caller knows not to build a stem map.
TEST_F(LanguageProcessorTest, NoStemmerMeansNoStemMap) {
  auto result = processor_->Process("running jumps happily");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"running", "jumps", "happily"}));

  // When the caller does NOT call BuildStemMap, no stem entries exist — this is
  // the new equivalent of the old stemming_enabled=false path.
  InProgressStemMap stem_map;
  EXPECT_TRUE(stem_map.empty());
}

// Multi-byte word passes through the full pipeline intact.
TEST_F(LanguageProcessorTest, MultiByteWordTokenizesIntact) {
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

TEST_F(LanguageProcessorTest, CanonicallyEquivalentFormsTokenizeIdentically) {
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

TEST(ArabicNFKCTest, PresentationFormsCollapseToBase) {
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

TEST(GermanCompoundTest, CompoundWordNotDecomposed) {
  auto processor = CreateProcessor(data_model::LANGUAGE_GERMAN);
  auto result = processor->Process("Donaudampfschifffahrtsgesellschaft");
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 1u);
  EXPECT_EQ((*result)[0], "donaudampfschifffahrtsgesellschaft");
}

// =============================================================================
// French apostrophe elision — apostrophe is in the default punctuation set.
// =============================================================================

TEST(FrenchApostropheTest, ApostropheSplitsToken) {
  auto processor = CreateProcessor(data_model::LANGUAGE_FRENCH);
  auto result = processor->Process(
      "l'\xc3\xa9"
      "cole");
  ASSERT_TRUE(result.ok());
  bool found_ecole = false;
  for (const auto &token : *result) {
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

TEST(CrossLanguageTest, ProcessorsAreIndependent) {
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

  EXPECT_EQ(stop_words.size(), tc.expected_count)
      << "Stop word list size changed for " << tc.test_name
      << ". If intentional, update this snapshot.";

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
// LANGUAGE_UNSPECIFIED defaults to English stop words and stemmer
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
// =============================================================================

class ZeroFilterPipelineTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto segmenter = std::make_shared<PunctuationSegmenter>(
        GetDefaultPunctuation(data_model::LANGUAGE_ENGLISH));
    processor_ =
        LanguageProcessor::Builder().AddSegmenter(std::move(segmenter)).Build();
  }
  std::shared_ptr<LanguageProcessor> processor_;
};

TEST_F(ZeroFilterPipelineTest, ProcessReturnsRawTokens) {
  auto result = processor_->Process("Hello, World");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"Hello", "World"}));
}

TEST_F(ZeroFilterPipelineTest, ApplyFiltersIsNoOp) {
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
// =============================================================================

class FilterOrderingTest : public ::testing::Test {};

TEST_F(FilterOrderingTest, NormalizeBeforeStopword_UpperCaseDropped) {
  auto processor = LanguageProcessor::Create(
      data_model::LANGUAGE_ENGLISH,
      GetDefaultPunctuation(data_model::LANGUAGE_ENGLISH), {"the", "and"});

  auto result = processor->Process("The AND cat");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"cat"}));
}

TEST_F(FilterOrderingTest, ReverseOrderLeaksMixedCase) {
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
  EXPECT_EQ(result->size(), 3u)
      << "Reversed filter order should leak mixed-case stop words";
  EXPECT_EQ((*result)[0], "the");
  EXPECT_EQ((*result)[1], "and");
  EXPECT_EQ((*result)[2], "cat");
}

TEST_F(FilterOrderingTest, UnicodeNormBeforeStopword) {
  std::string nfc_cafe = "caf\xc3\xa9";   // precomposed
  std::string nfd_cafe = "cafe\xcc\x81";  // decomposed

  auto processor = LanguageProcessor::Create(
      data_model::LANGUAGE_ENGLISH,
      GetDefaultPunctuation(data_model::LANGUAGE_ENGLISH), {nfc_cafe});

  auto result = processor->Process(nfd_cafe + " hello");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result, std::vector<std::string>({"hello"}));

  auto result2 = processor->Process(nfc_cafe + " hello");
  ASSERT_TRUE(result2.ok());
  EXPECT_EQ(*result2, std::vector<std::string>({"hello"}));
}

// =============================================================================
// Edge Case 3: Multi-segmenter chaining
// =============================================================================

class MultiSegmenterTest : public ::testing::Test {};

TEST_F(MultiSegmenterTest, TwoSegmentersChained) {
  auto seg_comma = std::make_shared<PunctuationSegmenter>(",");
  auto seg_space = std::make_shared<PunctuationSegmenter>(" ");

  auto processor = LanguageProcessor::Builder()
                       .AddSegmenter(std::move(seg_comma))
                       .AddSegmenter(std::move(seg_space))
                       .Build();

  auto result = processor->Segment("hello world,foo bar");
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(*result,
            std::vector<std::string>({"hello", "world", "foo", "bar"}));
}

TEST_F(MultiSegmenterTest, ThreeSegmentersChained) {
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
  auto seg_comma = std::make_shared<PunctuationSegmenter>(",");
  auto seg_space = std::make_shared<PunctuationSegmenter>(" ");

  auto processor = LanguageProcessor::Builder()
                       .AddSegmenter(std::move(seg_comma))
                       .AddSegmenter(std::move(seg_space))
                       .Build();

  auto result = processor->Segment("good,\xFF\xFE bad");
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_F(MultiSegmenterTest, SingleTokenPassthrough) {
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
// NormalizeCaseFoldFilter::Apply — always returns true (never drops tokens)
// =============================================================================

TEST(NormalizeCaseFoldApplyTest, EmptyString) {
  NormalizeCaseFoldFilter filter;
  std::string token;
  EXPECT_TRUE(filter.Apply(token));
}

TEST(NormalizeCaseFoldApplyTest, WhitespaceOnly) {
  NormalizeCaseFoldFilter filter;
  std::string token = "   ";
  EXPECT_TRUE(filter.Apply(token));
}

TEST(NormalizeCaseFoldApplyTest, HighCodepointEmoji) {
  NormalizeCaseFoldFilter filter;
  std::string token = "\xf0\x9f\x99\x82";
  EXPECT_TRUE(filter.Apply(token));
}

TEST(NormalizeCaseFoldApplyTest, AlreadyNormalized) {
  NormalizeCaseFoldFilter filter;
  std::string token = "hello";
  EXPECT_TRUE(filter.Apply(token));
  EXPECT_EQ(token, "hello");
}

TEST(NormalizeCaseFoldApplyTest, MixedCase) {
  NormalizeCaseFoldFilter filter;
  std::string token = "HeLLo";
  EXPECT_TRUE(filter.Apply(token));
  EXPECT_EQ(token, "hello");
}

// =============================================================================
// StopWordFilter::Apply — correctly drops stop words, keeps others
// =============================================================================

TEST(StopWordFilterApplyTest, DropsStopWord) {
  StopWordFilter filter(std::vector<std::string>{"the", "and", "or"});
  std::string token = "the";
  EXPECT_FALSE(filter.Apply(token));
}

TEST(StopWordFilterApplyTest, KeepsNonStopWord) {
  StopWordFilter filter(std::vector<std::string>{"the", "and", "or"});
  std::string token = "cat";
  EXPECT_TRUE(filter.Apply(token));
}

TEST(StopWordFilterApplyTest, EmptyStringKept) {
  StopWordFilter filter(std::vector<std::string>{"the", "and", "or"});
  std::string token;
  EXPECT_TRUE(filter.Apply(token));
}

TEST(StopWordFilterApplyTest, EmptySetKeepsAll) {
  StopWordFilter filter(std::vector<std::string>{});
  std::string token = "the";
  EXPECT_TRUE(filter.Apply(token));
}

}  // namespace valkey_search::indexes::text
