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

#include "absl/base/call_once.h"
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
/// and Snowball stemming. Concrete subclasses supply their language-specific
/// data through the constructor; this class is its single owner.
class SnowballLanguage : public Language {
 public:
  ~SnowballLanguage() override;

  data_model::Language Id() const override;
  const std::string& GetDefaultPunctuation() const override;
  const std::vector<std::string>& GetDefaultStopWords() const override;
  NormalizationForm GetNormalizationForm() const override;
  absl::string_view CaseFoldLocale() const override;

  std::shared_ptr<const TokenizerConfig> TokenizerConfigFor(
      const std::string& punctuation,
      const std::vector<std::string>& stop_words) const override;

  absl::StatusOr<std::vector<std::string>> Tokenize(
      absl::string_view text, const TokenizerConfig& config) const override;

  absl::StatusOr<std::vector<std::string>> TokenizeWithStemMap(
      absl::string_view text, const TokenizerConfig& config,
      uint32_t min_stem_size, LengthUnit unit,
      InProgressStemMap& stem_mappings) const override;

  void NormalizeInPlace(std::string& token) const override;
  Stemmer* GetStemmer() const override;

  LengthUnit GetLengthUnit() const override;

 protected:
  SnowballLanguage(data_model::Language id, const std::string& punctuation,
                   const std::vector<std::string>& stop_words,
                   NormalizationForm norm_form, absl::string_view locale,
                   absl::string_view stemmer_algorithm,
                   DelimiterScope delimiter_scope);

 private:
  TokenizerConfig MakeTokenizerConfig(
      const std::string& punctuation,
      const std::vector<std::string>& stop_words) const;

  /// Splits `text` into normalized tokens, dropping stop words, and appends
  /// them to `tokens`. Returns false if `text` is not valid UTF-8.
  bool Segment(absl::string_view text, const TokenizerConfig& config,
               std::vector<std::string>& tokens) const;

  data_model::Language id_;
  std::string punctuation_;
  std::vector<std::string> stop_words_;
  DelimiterScope delimiter_scope_;
  NormalizeCaseFoldFilter normalizer_;
  std::unique_ptr<SnowballStemFilter> stemmer_;
  mutable absl::once_flag default_config_once_;
  mutable std::shared_ptr<const TokenizerConfig> default_config_;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_SNOWBALL_LANGUAGE_H_
