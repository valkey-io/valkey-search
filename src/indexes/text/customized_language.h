/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_CUSTOMIZED_LANGUAGE_H_
#define VALKEY_SEARCH_INDEXES_TEXT_CUSTOMIZED_LANGUAGE_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/language.h"
#include "src/indexes/text/snowball_language.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {

/// Decorator that wraps a base Language with custom punctuation and/or stop
/// words from FT.CREATE. Normalization form, locale, version gating, and
/// stemming always delegate to the base language.
class CustomizedLanguage final : public SnowballLanguage {
 public:
  CustomizedLanguage(std::shared_ptr<const SnowballLanguage> base,
                     const std::string& punctuation,
                     const std::vector<std::string>& stop_words);

  data_model::Language Id() const override;
  absl::string_view Name() const override;
  const std::string& GetDefaultPunctuation() const override;
  const std::vector<std::string>& GetDefaultStopWords() const override;
  NormalizationForm GetNormalizationForm() const override;
  absl::string_view CaseFoldLocale() const override;
  vmsdk::ValkeyVersion MinRequiredVersion() const override;

 private:
  std::shared_ptr<const SnowballLanguage> base_;
  std::string punctuation_;
  std::vector<std::string> stop_words_;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_CUSTOMIZED_LANGUAGE_H_
