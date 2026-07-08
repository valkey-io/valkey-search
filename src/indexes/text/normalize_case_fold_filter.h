/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_NORMALIZE_CASE_FOLD_FILTER_H_
#define VALKEY_SEARCH_INDEXES_TEXT_NORMALIZE_CASE_FOLD_FILTER_H_

#include <optional>
#include <string>

#include "absl/strings/string_view.h"
#include "src/indexes/text/normalizer.h"
#include "src/indexes/text/unicode_normalizer.h"

namespace valkey_search::indexes::text {

/// Concrete Normalizer that performs Unicode normalization and case folding.
///
/// For ASCII-only tokens, applies simple ASCII lowercasing.
/// For tokens containing non-ASCII characters, applies the configured
/// Unicode normalization form (NFC or NFKC) followed by Unicode case folding.
class NormalizeCaseFoldFilter : public Normalizer {
 public:
  explicit NormalizeCaseFoldFilter(
      NormalizationForm form = NormalizationForm::NFC);

  std::optional<std::string> Apply(absl::string_view token) const override;
  void NormalizeInPlace(std::string &token) const override;

  /// Get the normalization form used by this filter.
  NormalizationForm GetNormForm() const { return norm_form_; }

 private:
  NormalizationForm norm_form_;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_NORMALIZE_CASE_FOLD_FILTER_H_
