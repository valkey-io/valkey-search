/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_SNOWBALL_LANGUAGE_H_
#define VALKEY_SEARCH_INDEXES_TEXT_SNOWBALL_LANGUAGE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/language.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {

class SnowballStemFilter;

/// Base class for European languages using punctuation-based segmentation
/// and Snowball stemming. Concrete subclasses provide language-specific data.
class SnowballLanguage : public Language {
 public:
  ~SnowballLanguage() override;

  absl::StatusOr<std::vector<std::string>> Tokenize(
      absl::string_view text) const override;

  absl::StatusOr<std::vector<std::string>> TokenizeWithStemMap(
      absl::string_view text, uint32_t min_stem_size,
      InProgressStemMap& stem_mappings) const override;

  bool IsQueryDelimiter(uint32_t codepoint) const override;
  const PunctuationSet& GetPunctuationSet() const override;
  void NormalizeInPlace(std::string& token) const override;
  bool IsStopWord(absl::string_view word) const override;
  Stemmer* GetStemmer() const override;

  bool IsSupported() const override;

  /// Returns the libstemmer algorithm name (e.g., "english", "french").
  absl::string_view GetStemmerAlgorithm() const { return stemmer_algorithm_; }

 protected:
  SnowballLanguage(data_model::Language id, const std::string& punctuation,
                   const std::vector<std::string>& stop_words,
                   NormalizationForm norm_form, absl::string_view locale,
                   absl::string_view stemmer_algorithm);

 private:
  /// Core segmentation loop shared by Tokenize and TokenizeWithStemMap.
  /// Calls `on_token` for each segmented+normalized token that passes optional
  /// stop-word filtering.
  template <typename TokenCallback>
  void SegmentInternal(absl::string_view text, bool handle_escapes,
                       bool filter_stop_words, TokenCallback on_token) const;

  data_model::Language id_;
  std::string stemmer_algorithm_;
  PunctuationSet punct_set_;
  NormalizeCaseFoldFilter normalizer_;
  absl::flat_hash_set<std::string> stop_words_set_;
  std::unique_ptr<SnowballStemFilter> stemmer_;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_SNOWBALL_LANGUAGE_H_
