/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_SNOWBALL_STEM_H_
#define VALKEY_SEARCH_INDEXES_TEXT_SNOWBALL_STEM_H_

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/language.h"

struct sb_stemmer;

namespace valkey_search::indexes::text {

/// Concrete Stemmer that applies the Snowball algorithm.
///
/// Supports: English, French, German, Spanish, Italian, Portuguese,
///           Russian, Swedish, Turkish, Dutch, Indonesian, Arabic.
class SnowballStemFilter : public Stemmer {
 public:
  /// Construct with language enum and algorithm name string.
  /// The enum is used as the thread-local cache key (O(1) integer hash).
  /// The algorithm name is passed to libstemmer's sb_stemmer_new().
  SnowballStemFilter(data_model::Language language,
                     absl::string_view algorithm_name,
                     uint32_t default_min_stem_size = 0);

  /// Stemmer interface
  std::string GetStemRoot(absl::string_view token,
                          uint32_t min_stem_size = 0) const override;

  void BuildStemMap(const std::vector<std::string>& tokens,
                    uint32_t min_stem_size,
                    InProgressStemMap& stem_mappings) const override;

  /// Get the algorithm name this filter was configured with.
  const std::string& GetAlgorithmName() const { return algorithm_name_; }

 private:
  data_model::Language language_;
  std::string algorithm_name_;
  uint32_t default_min_stem_size_;

  sb_stemmer* GetStemmer() const;
  std::string_view DoStemming(absl::string_view word, sb_stemmer* stemmer,
                              uint32_t min_stem_size) const;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_SNOWBALL_STEM_H_
