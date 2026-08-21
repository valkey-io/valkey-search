/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/snowball_language.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/indexes/text/language.h"
#include "src/indexes/text/snowball_stem.h"
#include "src/utils/scanner.h"
#include "src/version.h"

namespace valkey_search::indexes::text {

SnowballLanguage::~SnowballLanguage() = default;

SnowballLanguage::SnowballLanguage(data_model::Language id,
                                   const std::string& punctuation,
                                   const std::vector<std::string>& stop_words,
                                   NormalizationForm norm_form,
                                   absl::string_view locale,
                                   absl::string_view stemmer_algorithm)
    : id_(id),
      stemmer_algorithm_(stemmer_algorithm),
      punct_set_(BuildPunctuationSet(punctuation)),
      normalizer_(norm_form, std::string(locale)),
      stemmer_(std::make_unique<SnowballStemFilter>(id, stemmer_algorithm)) {
  // Build stop words set by normalizing each word through the same normalizer
  // used for tokens, ensuring consistent matching.
  for (const auto& word : stop_words) {
    std::string normalized = word;
    normalizer_.NormalizeInPlace(normalized);
    stop_words_set_.insert(std::move(normalized));
  }
}

template <typename TokenCallback>
void SnowballLanguage::SegmentInternal(absl::string_view text,
                                       bool handle_escapes,
                                       bool filter_stop_words,
                                       TokenCallback on_token) const {
  std::string word;
  word.reserve(64);
  size_t pos = 0;

  while (pos < text.size()) {
    // Skip leading punctuation/whitespace (codepoint-aware).
    while (pos < text.size()) {
      if (handle_escapes && text[pos] == '\\' && pos + 1 < text.size()) {
        break;
      }
      uint8_t lead = static_cast<uint8_t>(text[pos]);
      if (lead < 0x80) {
        if (!punct_set_.Contains(lead)) break;
        pos++;
      } else {
        utils::Scanner s(text.substr(pos));
        auto cp = s.NextUtf8();
        if (cp == utils::Scanner::kInvalidCp) {
          pos += s.LastUtf8ByteLen();
          continue;
        }
        if (!punct_set_.Contains(cp)) break;
        pos += s.LastUtf8ByteLen();
      }
    }

    word.clear();

    // Build word until next punctuation boundary.
    while (pos < text.size()) {
      // Handle backslash escape (ingestion path only).
      if (handle_escapes && text[pos] == '\\' && pos + 1 < text.size()) {
        pos++;
        uint8_t esc_lead = static_cast<uint8_t>(text[pos]);
        if (esc_lead < 0x80) {
          bool esc_is_delim = punct_set_.Contains(esc_lead);
          if (esc_lead != '\\' && !esc_is_delim && punct_set_.Contains('\\')) {
            break;
          }
          word.push_back(text[pos]);
          pos++;
        } else {
          utils::Scanner s(text.substr(pos));
          auto esc_cp = s.NextUtf8();
          uint8_t esc_len = s.LastUtf8ByteLen();
          if (esc_cp != '\\' && !punct_set_.Contains(esc_cp) &&
              punct_set_.Contains('\\')) {
            break;
          }
          word.append(text.data() + pos, esc_len);
          pos += esc_len;
        }
        continue;
      }

      uint8_t lead = static_cast<uint8_t>(text[pos]);
      if (lead < 0x80) {
        if (punct_set_.Contains(lead)) break;
        word.push_back(text[pos]);
        pos++;
      } else {
        utils::Scanner s(text.substr(pos));
        auto cp = s.NextUtf8();
        if (cp == utils::Scanner::kInvalidCp) {
          pos += s.LastUtf8ByteLen();
          break;
        }
        if (punct_set_.Contains(cp)) break;
        uint8_t len = s.LastUtf8ByteLen();
        word.append(text.data() + pos, len);
        pos += len;
      }
    }

    if (!word.empty()) {
      normalizer_.NormalizeInPlace(word);
      if (!filter_stop_words || !stop_words_set_.contains(word)) {
        on_token(std::move(word));
        word.clear();
      }
    }
  }
}

absl::StatusOr<std::vector<std::string>> SnowballLanguage::Tokenize(
    absl::string_view text) const {
  if (!utils::Scanner::IsValidUtf8(text)) {
    return absl::InvalidArgumentError("Invalid UTF-8");
  }
  std::vector<std::string> tokens;
  SegmentInternal(
      text, /*handle_escapes=*/true, /*filter_stop_words=*/true,
      [&tokens](std::string&& token) { tokens.push_back(std::move(token)); });
  return tokens;
}

absl::StatusOr<std::vector<std::string>> SnowballLanguage::TokenizeWithStemMap(
    absl::string_view text, uint32_t min_stem_size,
    InProgressStemMap& stem_mappings) const {
  if (!utils::Scanner::IsValidUtf8(text)) {
    return absl::InvalidArgumentError("Invalid UTF-8");
  }
  std::vector<std::string> tokens;
  SegmentInternal(
      text, /*handle_escapes=*/true, /*filter_stop_words=*/true,
      [&tokens](std::string&& token) { tokens.push_back(std::move(token)); });
  stemmer_->BuildStemMap(tokens, min_stem_size, stem_mappings);
  return tokens;
}

bool SnowballLanguage::IsQueryDelimiter(uint32_t codepoint) const {
  return punct_set_.Contains(codepoint);
}

const PunctuationSet& SnowballLanguage::GetPunctuationSet() const {
  return punct_set_;
}

void SnowballLanguage::NormalizeInPlace(std::string& token) const {
  normalizer_.NormalizeInPlace(token);
}

bool SnowballLanguage::IsStopWord(absl::string_view word) const {
  return stop_words_set_.contains(word);
}

Stemmer* SnowballLanguage::GetStemmer() const { return stemmer_.get(); }

absl::StatusOr<std::vector<std::string>> SnowballLanguage::QueryTokenize(
    absl::string_view text_span) const {
  if (!utils::Scanner::IsValidUtf8(text_span)) {
    return absl::InvalidArgumentError("Invalid UTF-8");
  }
  std::vector<std::string> tokens;
  SegmentInternal(text_span, /*handle_escapes=*/false,
                  /*filter_stop_words=*/false, [&tokens](std::string&& token) {
                    tokens.push_back(std::move(token));
                  });
  return tokens;
}

bool SnowballLanguage::IsSupported() const {
  return kModuleVersion >= MinRequiredVersion();
}

}  // namespace valkey_search::indexes::text
