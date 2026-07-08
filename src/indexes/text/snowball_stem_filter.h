/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_SNOWBALL_STEM_FILTER_H_
#define VALKEY_SEARCH_INDEXES_TEXT_SNOWBALL_STEM_FILTER_H_

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/stemmer.h"

struct sb_stemmer;

namespace valkey_search::indexes::text {

/// Concrete Stemmer that applies the Snowball algorithm.
///
/// Supports: English, French, German, Spanish, Italian, Portuguese,
///           Russian, Swedish, Turkish, Dutch, Indonesian, Arabic.
class SnowballStemFilter : public Stemmer {
 public:
  explicit SnowballStemFilter(data_model::Language language,
                              uint32_t default_min_stem_size = 0);

  /// TokenFilter interface — applies stemming as part of the pipeline.
  /// Returns the stemmed form of the token.
  std::optional<std::string> Apply(absl::string_view token) const override;

  /// Stemmer interface
  std::string GetStemRoot(absl::string_view token,
                          uint32_t min_stem_size = 0) const override;

  void BuildStemMap(const std::vector<std::string> &tokens,
                    uint32_t min_stem_size,
                    InProgressStemMap &stem_mappings) const override;

  /// Get the language this filter was configured for.
  data_model::Language GetLanguage() const { return language_; }

 private:
  data_model::Language language_;
  uint32_t default_min_stem_size_;

  sb_stemmer *GetStemmer() const;
  std::string_view DoStemming(absl::string_view word, sb_stemmer *stemmer,
                              uint32_t min_stem_size) const;
  static const char *GetLanguageString(data_model::Language language);
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_SNOWBALL_STEM_FILTER_H_
