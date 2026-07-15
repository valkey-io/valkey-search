/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/language_processor.h"

#include <utility>

#include "absl/algorithm/container.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "src/indexes/text/snowball_stem.h"
#include "src/indexes/text/stop_words.h"
#include "src/utils/scanner.h"
#include "vmsdk/src/status/status_macros.h"

namespace valkey_search::indexes::text {

// ============================================================================
// PunctuationSegmenter implementation
// ============================================================================

PunctuationSegmenter::PunctuationSegmenter(const std::string &punctuation)
    : punct_set_(BuildPunctuationSet(punctuation)) {}

PunctuationSegmenter::PunctuationSegmenter(PunctuationSet punct_set)
    : punct_set_(std::move(punct_set)) {}

absl::StatusOr<std::vector<std::string>> PunctuationSegmenter::Segment(
    absl::string_view text) const {
  if (!utils::Scanner::IsValidUtf8(text)) {
    return absl::InvalidArgumentError("Invalid UTF-8");
  }

  std::vector<std::string> tokens;
  std::string word;
  word.reserve(64);
  size_t pos = 0;

  while (pos < text.size()) {
    // Skip leading punctuation (code-point aware)
    while (pos < text.size()) {
      if (text[pos] == '\\' && pos + 1 < text.size()) {
        break;
      }
      uint8_t lead = static_cast<uint8_t>(text[pos]);
      if (lead < 0x80) {
        // ASCII fast path: direct bitset lookup, no Scanner needed
        if (!punct_set_.ascii[lead]) {
          break;
        }
        pos++;
      } else {
        // Multi-byte: use Scanner for proper UTF-8 decode
        utils::Scanner s(text.substr(pos));
        auto cp = s.NextUtf8();
        CHECK(cp != utils::Scanner::kInvalidCp)
            << "Segment decoded invalid UTF-8 after IsValidUtf8 passed";
        if (!IsPunctuation(cp)) {
          break;
        }
        pos += s.LastUtf8ByteLen();
      }
    }

    word.clear();

    // Build word until next punctuation boundary
    while (pos < text.size()) {
      // Handle backslash escape
      if (text[pos] == '\\' && pos + 1 < text.size()) {
        pos++;
        uint8_t esc_lead = static_cast<uint8_t>(text[pos]);
        if (esc_lead < 0x80) {
          // ASCII escaped char: fast path
          bool esc_is_delim = punct_set_.ascii[esc_lead];
          if (esc_lead != '\\' && !esc_is_delim && punct_set_.ascii['\\']) {
            break;
          }
          word.push_back(text[pos]);
          pos++;
        } else {
          // Multi-byte escaped char
          utils::Scanner s(text.substr(pos));
          auto esc_cp = s.NextUtf8();
          CHECK(esc_cp != utils::Scanner::kInvalidCp)
              << "Segment decoded invalid UTF-8 after IsValidUtf8 passed";
          uint8_t esc_len = s.LastUtf8ByteLen();
          if (esc_cp != '\\' && !IsPunctuation(esc_cp) && IsPunctuation('\\')) {
            break;
          }
          word.append(text.data() + pos, esc_len);
          pos += esc_len;
        }
        continue;
      }

      uint8_t lead = static_cast<uint8_t>(text[pos]);
      if (lead < 0x80) {
        // ASCII fast path: direct bitset lookup, no Scanner needed
        if (punct_set_.ascii[lead]) {
          break;
        }
        word.push_back(text[pos]);
        pos++;
      } else {
        // Multi-byte: use Scanner for proper UTF-8 decode
        utils::Scanner s(text.substr(pos));
        auto cp = s.NextUtf8();
        CHECK(cp != utils::Scanner::kInvalidCp)
            << "Segment decoded invalid UTF-8 after IsValidUtf8 passed";
        if (IsPunctuation(cp)) {
          break;
        }
        uint8_t len = s.LastUtf8ByteLen();
        word.append(text.data() + pos, len);
        pos += len;
      }
    }

    if (!word.empty()) {
      tokens.push_back(std::move(word));
      word.clear();
    }
  }
  return tokens;
}

bool PunctuationSegmenter::IsPunctuation(uint32_t cp) const {
  return punct_set_.Contains(cp);
}

// ============================================================================
// NormalizeCaseFoldFilter implementation
// ============================================================================

NormalizeCaseFoldFilter::NormalizeCaseFoldFilter(NormalizationForm form,
                                                 const std::string &locale)
    : norm_form_(form), locale_(locale) {}

bool NormalizeCaseFoldFilter::Apply(std::string &token) const {
  NormalizeInPlace(token);
  return true;
}

void NormalizeCaseFoldFilter::NormalizeInPlace(std::string &token) const {
  if (!locale_.empty()) {
    // Locale-aware path: required for Turkish (and Azerbaijani) where the
    // standard Unicode case folding produces incorrect results.
    // Turkish I→ı (not I→i) and İ→i require locale-specific toLower rules.
    token = UnicodeNormalizer::Normalize(token, norm_form_);
    token = UnicodeNormalizer::LocaleAwareCaseFold(token, locale_);
  } else if (absl::c_all_of(token, absl::ascii_isascii)) {
    absl::AsciiStrToLower(&token);
  } else {
    token = UnicodeNormalizer::Normalize(token, norm_form_);
    UnicodeNormalizer::CaseFoldInPlace(token);
  }
}

// ============================================================================
// StopWordFilter implementation
// ============================================================================

StopWordFilter::StopWordFilter(const std::vector<std::string> &stop_words)
    : stop_words_set_(BuildStopWordsSet(stop_words)) {}

StopWordFilter::StopWordFilter(absl::flat_hash_set<std::string> stop_words_set)
    : stop_words_set_(std::move(stop_words_set)) {}

bool StopWordFilter::Apply(std::string &token) const {
  return !stop_words_set_.contains(token);
}

bool StopWordFilter::IsStopWord(absl::string_view word) const {
  return stop_words_set_.contains(word);
}

// ============================================================================
// PunctuationQueryTokenizer implementation
// ============================================================================

absl::StatusOr<std::optional<QueryTokenizer::Token>>
PunctuationQueryTokenizer::NextQuotedToken(absl::string_view text,
                                           size_t pos) const {
  if (pos >= text.size()) {
    return std::nullopt;
  }

  // Check if we're at a quote -- don't consume it
  {
    utils::Scanner s(text.substr(pos));
    auto cp = s.NextUtf8();
    if (cp == '"') {
      return std::nullopt;
    }
  }

  std::string content;
  size_t cursor = pos;
  bool building_token = false;

  while (cursor < text.size()) {
    // Check for quote boundary
    if (text[cursor] == '"') {
      break;
    }

    // Check for backslash escape
    if (text[cursor] == '\\') {
      if (!building_token) {
        building_token = true;
      }
      VMSDK_ASSIGN_OR_RETURN(auto esc_result,
                             HandleEscape(text, cursor, content));
      if (esc_result == EscapeResult::kBreakToken) {
        break;
      }
      continue;
    }

    utils::Scanner s(text.substr(cursor));
    auto cp = s.NextUtf8();
    if (cp == utils::Scanner::kEOF) {
      break;
    }
    if (cp == utils::Scanner::kInvalidCp) {
      // Invalid UTF-8 -- replace with U+FFFD and treat as content
      utils::Scanner::PushBackUtf8(content, 0xFFFD);
      cursor += s.LastUtf8ByteLen();
      building_token = true;
      continue;
    }
    if (segmenter_.IsPunctuation(cp)) {
      if (building_token) {
        break;
      }
      // Still skipping leading punctuation
      cursor += s.LastUtf8ByteLen();
      continue;
    }
    // Non-punctuation character — append to token
    building_token = true;
    uint8_t len = s.LastUtf8ByteLen();
    content.append(text.data() + cursor, len);
    cursor += len;
  }

  size_t bytes_consumed = cursor - pos;
  if (bytes_consumed == 0) {
    return std::nullopt;
  }
  return std::make_optional(Token{std::move(content), bytes_consumed});
}

absl::StatusOr<std::optional<QueryTokenizer::Token>>
PunctuationQueryTokenizer::NextUnquotedToken(
    absl::string_view text, size_t pos, bool &hit_query_syntax,
    absl::FunctionRef<bool(uint32_t cp)> is_query_syntax) const {
  hit_query_syntax = false;
  if (pos >= text.size()) {
    return std::nullopt;
  }

  // Check if we're already at a query syntax char
  {
    utils::Scanner s(text.substr(pos));
    auto cp = s.NextUtf8();
    if (cp != utils::Scanner::kEOF && cp != utils::Scanner::kInvalidCp &&
        is_query_syntax(cp)) {
      hit_query_syntax = true;
      return std::nullopt;
    }
  }

  std::string content;
  size_t cursor = pos;
  bool building_token = false;

  while (cursor < text.size()) {
    // Check for backslash escape
    if (text[cursor] == '\\') {
      if (!building_token) {
        building_token = true;
      }
      VMSDK_ASSIGN_OR_RETURN(auto esc_result,
                             HandleEscape(text, cursor, content));
      if (esc_result == EscapeResult::kBreakToken) {
        break;
      }
      continue;
    }

    utils::Scanner s(text.substr(cursor));
    auto cp = s.NextUtf8();
    if (cp == utils::Scanner::kEOF) {
      break;
    }
    if (cp == utils::Scanner::kInvalidCp) {
      // Invalid UTF-8 -- replace with U+FFFD and treat as content
      utils::Scanner::PushBackUtf8(content, 0xFFFD);
      cursor += s.LastUtf8ByteLen();
      building_token = true;
      continue;
    }
    if (is_query_syntax(cp)) {
      hit_query_syntax = true;
      break;
    }
    if (segmenter_.IsPunctuation(cp)) {
      if (building_token) {
        break;
      }
      // Still skipping leading punctuation
      cursor += s.LastUtf8ByteLen();
      continue;
    }
    // Non-punctuation character — append to token
    building_token = true;
    uint8_t len = s.LastUtf8ByteLen();
    content.append(text.data() + cursor, len);
    cursor += len;
  }

  if (!building_token && hit_query_syntax) {
    size_t bytes_consumed = cursor - pos;
    if (bytes_consumed == 0) {
      return std::nullopt;
    }
    return std::make_optional(Token{"", bytes_consumed});
  }

  size_t bytes_consumed = cursor - pos;
  if (bytes_consumed == 0) {
    return std::nullopt;
  }
  return std::make_optional(Token{std::move(content), bytes_consumed});
}

absl::StatusOr<PunctuationQueryTokenizer::EscapeResult>
PunctuationQueryTokenizer::HandleEscape(absl::string_view text, size_t &cursor,
                                        std::string &content) const {
  cursor++;  // consume backslash
  if (cursor >= text.size()) {
    return absl::InvalidArgumentError(
        "Invalid escape sequence: backslash at end of input");
  }
  utils::Scanner s(text.substr(cursor));
  auto esc_cp = s.NextUtf8();
  uint8_t esc_len = s.LastUtf8ByteLen();

  if (esc_cp == utils::Scanner::kInvalidCp) {
    utils::Scanner::PushBackUtf8(content, 0xFFFD);
    cursor += esc_len;
    return EscapeResult::kContinue;
  }
  if (esc_cp == '\\' || segmenter_.IsPunctuation(esc_cp)) {
    // Double backslash or escaped punctuation -> keep literally, continue
    content.append(text.data() + cursor, esc_len);
    cursor += esc_len;
    return EscapeResult::kContinue;
  }
  // Backslash before non-punctuation character
  if (segmenter_.IsPunctuation('\\')) {
    // Backslash is itself punctuation -> token break.
    // Don't consume the escaped char -- it starts the next token.
    return EscapeResult::kBreakToken;
  }
  // Backslash is NOT punctuation -> keep the non-punctuation char, continue
  content.append(text.data() + cursor, esc_len);
  cursor += esc_len;
  return EscapeResult::kContinue;
}

// ============================================================================
// LanguageProcessor::Builder implementation
// ============================================================================

LanguageProcessor::Builder &LanguageProcessor::Builder::AddSegmenter(
    std::shared_ptr<Segmenter> segmenter) {
  processor_->segmenters_.push_back(std::move(segmenter));
  return *this;
}

LanguageProcessor::Builder &LanguageProcessor::Builder::AddFilter(
    std::shared_ptr<TokenFilter> filter) {
  processor_->filters_.push_back(std::move(filter));
  return *this;
}

LanguageProcessor::Builder &LanguageProcessor::Builder::SetQueryTokenizer(
    std::shared_ptr<QueryTokenizer> tokenizer) {
  processor_->query_tokenizer_ = std::move(tokenizer);
  return *this;
}

LanguageProcessor::Builder &LanguageProcessor::Builder::SetNormalizer(
    std::shared_ptr<Normalizer> normalizer) {
  processor_->normalizer_ = std::move(normalizer);
  return *this;
}

LanguageProcessor::Builder &LanguageProcessor::Builder::SetStopWordFilter(
    std::shared_ptr<StopWordFilter> filter) {
  processor_->stop_word_filter_ = std::move(filter);
  return *this;
}

LanguageProcessor::Builder &LanguageProcessor::Builder::SetStemmer(
    std::shared_ptr<Stemmer> stemmer) {
  processor_->stemmer_ = std::move(stemmer);
  return *this;
}

std::shared_ptr<LanguageProcessor> LanguageProcessor::Builder::Build() {
  return std::move(processor_);
}

// ============================================================================
// LanguageProcessor implementation
// ============================================================================

absl::StatusOr<std::vector<std::string>> LanguageProcessor::Segment(
    absl::string_view text) const {
  // Fast path: single segmenter (all Snowball languages). Avoids copying
  // the input into an intermediate vector just to iterate once.
  if (segmenters_.size() == 1) {
    return segmenters_[0]->Segment(text);
  }

  // Multi-segmenter chaining: feed each segmenter's output into the next.
  std::vector<std::string> current = {std::string(text)};

  for (const auto &segmenter : segmenters_) {
    std::vector<std::string> next;
    for (const auto &input : current) {
      auto result = segmenter->Segment(input);
      if (!result.ok()) {
        return result.status();
      }
      next.insert(next.end(), std::move_iterator(result->begin()),
                  std::move_iterator(result->end()));
    }
    current = std::move(next);
  }

  return current;
}

bool LanguageProcessor::ProcessWord(std::string &token) const {
  for (const auto &filter : filters_) {
    if (!filter->Apply(token)) {
      return false;
    }
  }
  return true;
}

std::vector<std::string> LanguageProcessor::ApplyFilters(
    std::vector<std::string> tokens) const {
  std::vector<std::string> result;
  result.reserve(tokens.size());
  for (auto &token : tokens) {
    if (ProcessWord(token)) {
      result.push_back(std::move(token));
    }
  }
  return result;
}

absl::StatusOr<std::vector<std::string>> LanguageProcessor::Process(
    absl::string_view text) const {
  auto tokens = Segment(text);
  if (!tokens.ok()) {
    return tokens.status();
  }
  return ApplyFilters(std::move(*tokens));
}

// ============================================================================
// CreateSnowballProcessor — factory for Snowball-family languages
// ============================================================================

namespace {

std::shared_ptr<LanguageProcessor> CreateSnowballProcessor(
    data_model::Language language, const std::string &punctuation,
    const std::vector<std::string> &stop_words) {
  // Segmenter: punctuation-based splitting for all Snowball languages
  auto punct_segmenter = std::make_shared<PunctuationSegmenter>(punctuation);

  // Query tokenizer: punctuation-based (uses the same punctuation segmenter)
  auto query_tokenizer =
      std::make_shared<PunctuationQueryTokenizer>(*punct_segmenter);

  // Filter 1: Unicode normalization + case folding
  // Turkish requires locale-aware case folding for correct İ/I handling:
  //   I (U+0049) → ı (U+0131)  — dotless lowercase i
  //   İ (U+0130) → i (U+0069)  — standard lowercase i
  // Generic Unicode case folding maps both to 'i', which is incorrect for
  // Turkish and causes retrieval failures on words containing dotless-ı.
  NormalizationForm norm_form = (language == data_model::LANGUAGE_ARABIC)
                                    ? NormalizationForm::NFKC
                                    : NormalizationForm::NFC;
  std::string locale = (language == data_model::LANGUAGE_TURKISH) ? "tr" : "";
  auto normalizer =
      std::make_shared<NormalizeCaseFoldFilter>(norm_form, locale);

  // Filter 2: Stop word removal
  auto stop_filter = std::make_shared<StopWordFilter>(stop_words);

  // Stemming is NOT part of the main pipeline. Tokens are indexed in their
  // normalized (unstemmed) form. The stem filter is stored separately for
  // callers that need it (stem map building, query expansion, delete path).
  auto stemmer = std::make_shared<SnowballStemFilter>(language);

  return LanguageProcessor::Builder()
      .AddSegmenter(std::move(punct_segmenter))
      .SetQueryTokenizer(std::move(query_tokenizer))
      .SetNormalizer(normalizer)
      .AddFilter(std::move(normalizer))
      .SetStopWordFilter(stop_filter)
      .AddFilter(std::move(stop_filter))
      .SetStemmer(std::move(stemmer))
      .Build();
}

}  // namespace

std::shared_ptr<LanguageProcessor> LanguageProcessor::Create(
    data_model::Language language, const std::string &punctuation,
    const std::vector<std::string> &stop_words) {
  switch (language) {
      // TODO: Add ICU processor cases here when implemented
      // case data_model::LANGUAGE_CHINESE:
      // case data_model::LANGUAGE_JAPANESE:
      // case data_model::LANGUAGE_KOREAN:
      //   return CreateICUProcessor(language, stop_words);

    default:
      return CreateSnowballProcessor(language, punctuation, stop_words);
  }
}

}  // namespace valkey_search::indexes::text
