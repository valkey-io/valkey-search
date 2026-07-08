/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_STOP_WORD_FILTER_H_
#define VALKEY_SEARCH_INDEXES_TEXT_STOP_WORD_FILTER_H_

#include <optional>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/strings/string_view.h"
#include "src/indexes/text/token_filter.h"

namespace valkey_search::indexes::text {

/// TokenFilter that removes stop words.
///
/// Returns nullopt for tokens that are in the configured stop word set,
/// effectively removing them from the token stream.
class StopWordFilter : public TokenFilter {
 public:
  explicit StopWordFilter(const std::vector<std::string> &stop_words);
  explicit StopWordFilter(absl::flat_hash_set<std::string> stop_words_set);

  std::optional<std::string> Apply(absl::string_view token) const override;

  /// Returns true if the given word is a stop word.
  bool IsStopWord(absl::string_view word) const;

 private:
  absl::flat_hash_set<std::string> stop_words_set_;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_STOP_WORD_FILTER_H_
