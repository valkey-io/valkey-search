/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/language_processor.h"

#include <utility>

#include "src/indexes/text/snowball_processor.h"

namespace valkey_search::indexes::text {

absl::StatusOr<std::vector<std::string>> LanguageProcessor::Segment(
    absl::string_view text) const {
  std::vector<std::string> current = {std::string(text)};

  for (const auto &segmenter : segmenters_) {
    std::vector<std::string> next;
    for (const auto &input : current) {
      auto result = segmenter->Segment(input);
      if (!result.ok()) return result.status();
      next.insert(next.end(), std::move_iterator(result->begin()),
                  std::move_iterator(result->end()));
    }
    current = std::move(next);
  }

  return current;
}

std::vector<std::string> LanguageProcessor::ApplyFilters(
    std::vector<std::string> tokens) const {
  for (const auto &filter : filters_) {
    std::vector<std::string> filtered;
    filtered.reserve(tokens.size());
    for (const auto &token : tokens) {
      auto result = filter->Apply(token);
      if (result.has_value()) {
        filtered.push_back(std::move(*result));
      }
    }
    tokens = std::move(filtered);
  }
  return tokens;
}

absl::StatusOr<std::vector<std::string>> LanguageProcessor::Process(
    absl::string_view text) const {
  auto tokens = Segment(text);
  if (!tokens.ok()) return tokens.status();
  return ApplyFilters(std::move(*tokens));
}

std::shared_ptr<LanguageProcessor> LanguageProcessor::Create(
    data_model::Language language, const std::string &punctuation,
    const std::vector<std::string> &stop_words) {
  switch (language) {
      // TODO: Add ICU processor cases here when implemented
      // case data_model::LANGUAGE_CHINESE:
      // case data_model::LANGUAGE_JAPANESE:
      // case data_model::LANGUAGE_KOREAN:
      //   return CreateICUProcessor(language, stop_words);

    default:
      return CreateSnowballProcessor(language, punctuation, stop_words);
  }
}

}  // namespace valkey_search::indexes::text
