/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/customized_language.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/snowball_language.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {

CustomizedLanguage::CustomizedLanguage(
    std::shared_ptr<const SnowballLanguage> base,
    const std::string& punctuation, const std::vector<std::string>& stop_words)
    : SnowballLanguage(base->Id(), punctuation, stop_words,
                       base->GetNormalizationForm(), base->CaseFoldLocale(),
                       base->GetStemmerAlgorithm()),
      base_(std::move(base)),
      punctuation_(punctuation),
      stop_words_(stop_words) {}

data_model::Language CustomizedLanguage::Id() const { return base_->Id(); }

absl::string_view CustomizedLanguage::Name() const { return base_->Name(); }

const std::string& CustomizedLanguage::GetDefaultPunctuation() const {
  return punctuation_;
}

const std::vector<std::string>& CustomizedLanguage::GetDefaultStopWords()
    const {
  return stop_words_;
}

NormalizationForm CustomizedLanguage::GetNormalizationForm() const {
  return base_->GetNormalizationForm();
}

absl::string_view CustomizedLanguage::CaseFoldLocale() const {
  return base_->CaseFoldLocale();
}

vmsdk::ValkeyVersion CustomizedLanguage::MinRequiredVersion() const {
  return base_->MinRequiredVersion();
}

}  // namespace valkey_search::indexes::text
