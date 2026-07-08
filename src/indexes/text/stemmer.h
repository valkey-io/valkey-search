/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_STEMMER_H_
#define VALKEY_SEARCH_INDEXES_TEXT_STEMMER_H_

#include <cstdint>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "absl/strings/string_view.h"
#include "src/indexes/text/token_filter.h"

namespace valkey_search::indexes::text {

// Inline capacity for per-document stem mapping
constexpr size_t kInProgressStemVariantsInlineCapacity = 4;

// Per-document stem mappings: stemmed_word -> list of original words that stem
// to it
using InProgressStemMap = absl::flat_hash_map<
    std::string,
    absl::InlinedVector<std::string, kInProgressStemVariantsInlineCapacity>>;

/// Abstract interface for stemming, extending TokenFilter.
///
/// A Stemmer is a TokenFilter that also provides direct access to stemming
/// operations for use outside the pipeline (delete path, query expansion,
/// stem map building).
///
/// Concrete implementations: SnowballStemFilter (Snowball algorithm for
/// European languages).
class Stemmer : public TokenFilter {
 public:
  ~Stemmer() override = default;

  /// Compute the stem root of a token.
  /// Used by delete path and query expansion for stem tree lookups.
  /// Returns the input unchanged if the word is too short to stem.
  virtual std::string GetStemRoot(absl::string_view token,
                                  uint32_t min_stem_size = 0) const = 0;

  /// Build stem map from already-processed tokens.
  /// Ingestion-specific: maps stem roots to their original surface forms.
  /// For each token, if its stem differs from the original, adds the mapping
  /// stem_root -> original_token.
  virtual void BuildStemMap(const std::vector<std::string> &tokens,
                            uint32_t min_stem_size,
                            InProgressStemMap &stem_mappings) const = 0;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_STEMMER_H_
