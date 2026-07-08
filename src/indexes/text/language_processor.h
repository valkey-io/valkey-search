/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_LANGUAGE_PROCESSOR_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_LANGUAGE_PROCESSOR_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/normalizer.h"
#include "src/indexes/text/segmenter.h"
#include "src/indexes/text/stemmer.h"
#include "src/indexes/text/stop_word_filter.h"
#include "src/indexes/text/token_filter.h"

namespace valkey_search::indexes::text {

/// Composed pipeline of Segmenters and TokenFilters.
///
/// The LanguageProcessor is a stateless, idempotent composition that owns
/// its segmenters and token filters. It provides:
///   - Process(): full pipeline execution (segment then filter)
///   - Segment(): segmentation only
///   - ApplyFilters(): apply only the filter chain
///   - O(1) accessors for components needed outside the pipeline
///
/// Adding a new language means composing different segmenters/filters via
/// the Create() factory — no interface changes required.
class LanguageProcessor {
 public:
  virtual ~LanguageProcessor() = default;

  /// Full pipeline: segment then filter. Stateless and idempotent.
  absl::StatusOr<std::vector<std::string>> Process(
      absl::string_view text) const;

  /// Apply segmenters sequentially to text.
  absl::StatusOr<std::vector<std::string>> Segment(
      absl::string_view text) const;

  /// Apply the filter chain to a list of tokens.
  /// Tokens that any filter eliminates are removed from the result.
  std::vector<std::string> ApplyFilters(std::vector<std::string> tokens) const;

  /// Get the normalizer. Always non-null — every language has normalization.
  /// O(1) access.
  Normalizer* GetNormalizer() const { return normalizer_.get(); }

  /// Get the primary segmenter. Used by the query parser to check word
  /// boundaries (IsDelimiter). If there are multiple segmenters in the
  /// pipeline, this returns the first one. O(1) access.
  const Segmenter* GetSegmenter() const {
    return segmenters_.empty() ? nullptr : segmenters_[0].get();
  }

  /// Get the stop word filter, or nullptr if this processor has no stop words.
  /// O(1) access. Use for direct stop-word checks on already-normalized
  /// tokens (avoids redundant normalization from ApplyFilters).
  StopWordFilter* GetStopWordFilter() const { return stop_word_filter_.get(); }

  /// Get the stemmer, or nullptr if this processor doesn't support stemming.
  /// O(1) access.
  Stemmer* GetStemmer() const { return stemmer_.get(); }

  /// Factory: create a LanguageProcessor for the given language with
  /// the appropriate segmenter and filter composition.
  static std::shared_ptr<LanguageProcessor> Create(
      data_model::Language language, const std::string& punctuation,
      const std::vector<std::string>& stop_words);

 private:
  // Factory functions that compose language-specific pipelines
  friend std::shared_ptr<LanguageProcessor> CreateSnowballProcessor(
      data_model::Language language, const std::string& punctuation,
      const std::vector<std::string>& stop_words);

  std::vector<std::shared_ptr<Segmenter>> segmenters_;
  std::vector<std::shared_ptr<TokenFilter>> filters_;

  // O(1) accessors for components used outside the pipeline.
  // Normalizer is also in the filter chain but exposed for direct use
  // (e.g., wildcard/fuzzy normalization without stopword removal).
  std::shared_ptr<Normalizer> normalizer_;
  // Stop word filter is also in the filter chain but exposed for direct
  // IsStopWord() checks on already-normalized tokens in query parser.
  std::shared_ptr<StopWordFilter> stop_word_filter_;
  // Stemmer is not part of the main filter chain — tokens are indexed
  // unstemmed. Nullptr for languages without stemming support.
  std::shared_ptr<Stemmer> stemmer_;
};

}  // namespace valkey_search::indexes::text

#endif
