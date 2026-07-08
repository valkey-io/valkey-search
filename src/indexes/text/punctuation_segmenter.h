/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_PUNCTUATION_SEGMENTER_H_
#define VALKEY_SEARCH_INDEXES_TEXT_PUNCTUATION_SEGMENTER_H_

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/indexes/text/punctuation.h"
#include "src/indexes/text/segmenter.h"

namespace valkey_search::indexes::text {

/// Segmenter that splits text on punctuation/whitespace boundaries.
///
/// Used by Snowball languages (English, French, German, etc.) where word
/// boundaries are defined by punctuation and whitespace characters.
///
/// Handles backslash escapes: `\<char>` means "include <char> literally,
/// don't treat it as a delimiter."
class PunctuationSegmenter : public Segmenter {
 public:
  explicit PunctuationSegmenter(const std::string &punctuation);
  PunctuationSegmenter(PunctuationSet punct_set);

  absl::StatusOr<std::vector<std::string>> Segment(
      absl::string_view text) const override;

  bool IsDelimiter(uint32_t cp) const override;

 private:
  PunctuationSet punct_set_;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_PUNCTUATION_SEGMENTER_H_
