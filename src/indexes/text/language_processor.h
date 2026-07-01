/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_LANGUAGE_PROCESSOR_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_LANGUAGE_PROCESSOR_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"

namespace valkey_search::indexes::text {

// Inline capacity for per-document stem mapping
constexpr size_t kInProgressStemVariantsInlineCapacity = 4;

// Per-document stem mappings: stemmed_word -> list of original words that stem
// to it
using InProgressStemMap = absl::flat_hash_map<
    std::string,
    absl::InlinedVector<std::string, kInProgressStemVariantsInlineCapacity>>;

class LanguageProcessor {
 public:
  virtual ~LanguageProcessor() = default;

  static std::shared_ptr<LanguageProcessor> Create(
      data_model::Language language, const std::string& punctuation,
      const std::vector<std::string>& stop_words);

  // Full tokenization pipeline: text → normalized tokens + optional stem map.
  virtual absl::StatusOr<std::vector<std::string>> Tokenize(
      absl::string_view text, bool stemming_enabled, uint32_t min_stem_size,
      InProgressStemMap* stem_map) const = 0;

  // Build the stem map for a set of tokens. Called during indexing.
  // No-op for ICU languages (they don't support stemming).
  virtual void BuildStemMap(const std::vector<std::string>& tokens,
                            uint32_t min_stem_size,
                            InProgressStemMap& stem_mappings) const {}

  // Stem a single word in place. Used during key deletion and query expansion.
  // No-op for ICU languages.
  virtual void StemWordInPlace(std::string& word,
                               uint32_t min_stem_size = 0) const {}

  // Whether this processor produces stems (controls stem tree construction).
  virtual bool SupportsStemming() const { return false; }

  // Returns true if cp is a configured word-boundary character.
  virtual bool IsPunctuation(uint32_t cp) const = 0;

  // Normalize and lowercase a word in place (NFC/NFKC + case fold).
  virtual void NormalizeLowerCaseInPlace(std::string& word) const = 0;

  // Returns true if the word is a configured stop word.
  virtual bool IsStopWord(absl::string_view word) const = 0;
};

}  // namespace valkey_search::indexes::text

#endif
