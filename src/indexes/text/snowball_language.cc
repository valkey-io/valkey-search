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
#include "src/valkey_search_options.h"
#include "src/version.h"
#include "vmsdk/src/status/status_macros.h"

namespace valkey_search::indexes::text {

SnowballLanguage::~SnowballLanguage() = default;

SnowballLanguage::SnowballLanguage(data_model::Language id,
                                   const std::string& punctuation,
                                   const std::vector<std::string>& stop_words,
                                   NormalizationForm norm_form,
                                   absl::string_view locale,
                                   absl::string_view stemmer_algorithm,
                                   DelimiterScope delimiter_scope)
    : id_(id),
      punctuation_(punctuation),
      stop_words_(stop_words),
      delimiter_scope_(delimiter_scope),
      normalizer_(norm_form, std::string(locale)),
      stemmer_(std::make_unique<SnowballStemFilter>(id, stemmer_algorithm)) {}

TokenizerConfig SnowballLanguage::MakeTokenizerConfig(
    const std::string& punctuation,
    const std::vector<std::string>& stop_words) const {
  TokenizerConfig config;
  config.punct_set = BuildPunctuationSet(
      punctuation, normalizer_.GetNormalizationForm(), delimiter_scope_);
  // Normalize each stop word through the same normalizer used for tokens, so
  // they match consistently.
  for (const auto& word : stop_words) {
    std::string normalized = word;
    normalizer_.NormalizeInPlace(normalized);
    config.stop_words.insert(std::move(normalized));
  }
  return config;
}

bool SnowballLanguage::Segment(absl::string_view text,
                               const TokenizerConfig& config,
                               std::vector<std::string>& tokens) const {
  // Split the raw text, then normalize each token: the same pipeline as the
  // query parser, so ingest and query produce the same terms. The punctuation
  // set is closed under the normalization form (see BuildPunctuationSet), so a
  // compatibility form of a delimiter (e.g. NFKC U+FF0C) still splits here.
  // UTF-8 is validated as the text is decoded, so it is read only once.
  absl::string_view input(text);
  const PunctuationSet& punct = config.punct_set;

  std::string word;
  word.reserve(64);
  size_t pos = 0;

  while (pos < input.size()) {
    // Skip leading punctuation/whitespace (codepoint-aware).
    while (pos < input.size()) {
      if (input[pos] == '\\' && pos + 1 < input.size()) {
        break;
      }
      uint8_t lead = static_cast<uint8_t>(input[pos]);
      if (lead < 0x80) {
        if (!punct.Contains(lead)) break;
        pos++;
      } else {
        utils::Scanner s(input.substr(pos));
        auto cp = s.NextUtf8();
        if (cp == utils::Scanner::kInvalidCp) return false;
        if (!punct.Contains(cp)) break;
        pos += s.LastUtf8ByteLen();
      }
    }

    word.clear();

    // Build word until next punctuation boundary.
    while (pos < input.size()) {
      if (input[pos] == '\\' && pos + 1 < input.size()) {
        pos++;  // skip backslash
        uint8_t n = ResolveBackslashEscape(input.substr(pos), punct);
        if (n == 0) break;
        // A valid non-ASCII code point is at least 2 bytes, so a 1-byte
        // non-ASCII escape is a malformed sequence.
        if (n == 1 && static_cast<uint8_t>(input[pos]) >= 0x80) return false;
        word.append(input.data() + pos, n);
        pos += n;
        continue;
      }

      uint8_t lead = static_cast<uint8_t>(input[pos]);
      if (lead < 0x80) {
        if (punct.Contains(lead)) break;
        word.push_back(input[pos]);
        pos++;
      } else {
        utils::Scanner s(input.substr(pos));
        auto cp = s.NextUtf8();
        if (cp == utils::Scanner::kInvalidCp) return false;
        if (punct.Contains(cp)) break;
        uint8_t len = s.LastUtf8ByteLen();
        word.append(input.data() + pos, len);
        pos += len;
      }
    }

    if (!word.empty()) {
      normalizer_.NormalizeInPlace(word);
      if (!config.IsStopWord(word)) {
        tokens.push_back(std::move(word));
        word.clear();
      }
    }
  }
  return true;
}

absl::StatusOr<std::vector<std::string>> SnowballLanguage::Tokenize(
    absl::string_view text, const TokenizerConfig& config) const {
  std::vector<std::string> tokens;
  if (!Segment(text, config, tokens)) {
    return absl::InvalidArgumentError("Invalid UTF-8");
  }
  return tokens;
}

absl::StatusOr<std::vector<std::string>> SnowballLanguage::TokenizeWithStemMap(
    absl::string_view text, const TokenizerConfig& config,
    uint32_t min_stem_size, LengthUnit unit,
    InProgressStemMap& stem_mappings) const {
  VMSDK_ASSIGN_OR_RETURN(auto tokens, Tokenize(text, config));
  stemmer_->BuildStemMap(tokens, min_stem_size, unit, stem_mappings);
  return tokens;
}

data_model::Language SnowballLanguage::Id() const { return id_; }

const std::string& SnowballLanguage::GetDefaultPunctuation() const {
  return punctuation_;
}

const std::vector<std::string>& SnowballLanguage::GetDefaultStopWords() const {
  return stop_words_;
}

NormalizationForm SnowballLanguage::GetNormalizationForm() const {
  return normalizer_.GetNormalizationForm();
}

absl::string_view SnowballLanguage::CaseFoldLocale() const {
  return normalizer_.GetLocale();
}

void SnowballLanguage::NormalizeInPlace(std::string& token) const {
  normalizer_.NormalizeInPlace(token);
}

Stemmer* SnowballLanguage::GetStemmer() const { return stemmer_.get(); }

LengthUnit SnowballLanguage::GetLengthUnit() const {
  // 1.2 measured lengths in bytes. Languages available before 1.3 keep that
  // unless emulate-release >= 1.3.0; languages added in 1.3 have no prior
  // behavior to preserve and always count code points.
  if (MinRequiredVersion() < kRelease13 &&
      !options::EnabledInVersion(kRelease13)) {
    return LengthUnit::kBytes;
  }
  return LengthUnit::kCodePoints;
}

}  // namespace valkey_search::indexes::text
