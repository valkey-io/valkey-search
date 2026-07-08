/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_SEGMENTER_H_
#define VALKEY_SEARCH_INDEXES_TEXT_SEGMENTER_H_

#include <cstdint>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace valkey_search::indexes::text {

/// Abstract interface for word boundary detection.
///
/// A Segmenter converts a chunk of text into multiple tokens (1→many).
/// It is the "cut" step — it splits raw text into tokens without applying
/// any normalization, filtering, or stop-word removal.
///
/// Escape contract: Segmenters handle `\<char>` in the input text as "include
/// <char> literally, do not treat it as a word boundary." This supports the
/// query language escape convention.
class Segmenter {
 public:
  virtual ~Segmenter() = default;

  /// Segment a single UTF-8 input string into tokens.
  /// Returns an error if the input contains invalid UTF-8.
  virtual absl::StatusOr<std::vector<std::string>> Segment(
      absl::string_view text) const = 0;

  /// Returns true if this segmenter treats the given code point as a word
  /// boundary (delimiter). Used by the query parser for escape handling.
  virtual bool IsDelimiter(uint32_t cp) const = 0;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_SEGMENTER_H_
