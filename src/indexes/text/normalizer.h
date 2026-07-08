/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_NORMALIZER_H_
#define VALKEY_SEARCH_INDEXES_TEXT_NORMALIZER_H_

#include <optional>
#include <string>

#include "absl/strings/string_view.h"
#include "src/indexes/text/token_filter.h"

namespace valkey_search::indexes::text {

/// Abstract interface for text normalization, extending TokenFilter.
///
/// A Normalizer is a TokenFilter that performs Unicode normalization and
/// case folding. It is exposed via LanguageProcessor::GetNormalizer() for
/// O(1) access by callers that need normalization independent of the full
/// pipeline (e.g., wildcard/fuzzy predicates).
///
/// Concrete implementations: NormalizeCaseFoldFilter (NFC/NFKC + case fold).
class Normalizer : public TokenFilter {
 public:
  ~Normalizer() override = default;

  /// Normalize a token in place. Convenience method that avoids the
  /// optional<string> allocation when the caller knows normalization
  /// never eliminates tokens.
  virtual void NormalizeInPlace(std::string &token) const = 0;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_NORMALIZER_H_
