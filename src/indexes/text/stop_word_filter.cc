/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/stop_word_filter.h"

#include "src/indexes/text/stop_words.h"

namespace valkey_search::indexes::text {

StopWordFilter::StopWordFilter(const std::vector<std::string> &stop_words)
    : stop_words_set_(BuildStopWordsSet(stop_words)) {}

StopWordFilter::StopWordFilter(absl::flat_hash_set<std::string> stop_words_set)
    : stop_words_set_(std::move(stop_words_set)) {}

std::optional<std::string> StopWordFilter::Apply(
    absl::string_view token) const {
  if (stop_words_set_.contains(token)) {
    return std::nullopt;
  }
  return std::string(token);
}

bool StopWordFilter::IsStopWord(absl::string_view word) const {
  return stop_words_set_.contains(word);
}

}  // namespace valkey_search::indexes::text
