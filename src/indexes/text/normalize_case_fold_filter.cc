/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/normalize_case_fold_filter.h"

#include "absl/algorithm/container.h"
#include "absl/strings/ascii.h"

namespace valkey_search::indexes::text {

NormalizeCaseFoldFilter::NormalizeCaseFoldFilter(NormalizationForm form)
    : norm_form_(form) {}

std::optional<std::string> NormalizeCaseFoldFilter::Apply(
    absl::string_view token) const {
  std::string result(token);
  NormalizeInPlace(result);
  return result;
}

void NormalizeCaseFoldFilter::NormalizeInPlace(std::string &token) const {
  if (absl::c_all_of(token, absl::ascii_isascii)) {
    absl::AsciiStrToLower(&token);
  } else {
    token = UnicodeNormalizer::Normalize(token, norm_form_);
    UnicodeNormalizer::CaseFoldInPlace(token);
  }
}

}  // namespace valkey_search::indexes::text
