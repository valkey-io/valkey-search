/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_TOKEN_FILTER_H_
#define VALKEY_SEARCH_INDEXES_TEXT_TOKEN_FILTER_H_

#include <optional>
#include <string>

#include "absl/strings/string_view.h"

namespace valkey_search::indexes::text {

/// Abstract interface for a single post-segmentation processing step.
///
/// A TokenFilter converts a single token into a nullable token (1→1 or 1→0).
/// It operates on individual tokens in isolation — it has no access to the
/// surrounding token list.
///
/// TokenFilters are stateless and idempotent.
///
/// Examples:
///   - NormalizeCaseFoldFilter: normalizes and lowercases a token
///   - StopWordFilter: returns nullopt for stop words
///   - SnowballStemFilter: applies Snowball stemming
class TokenFilter {
 public:
  virtual ~TokenFilter() = default;

  /// Apply this filter to a single token.
  /// Returns the transformed token, or nullopt if the token should be removed.
  virtual std::optional<std::string> Apply(absl::string_view token) const = 0;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_TOKEN_FILTER_H_
