/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_SNOWBALL_PROCESSOR_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_SNOWBALL_PROCESSOR_H_

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/language_processor.h"

struct sb_stemmer;

namespace valkey_search::indexes::text {

class SnowballProcessor : public LanguageProcessor {
  friend class LanguageProcessor;

 public:
  ~SnowballProcessor() override = default;

  std::vector<std::string> Tokenize(absl::string_view text) const override;

  void BuildStemMap(const std::vector<std::string>& tokens,
                    uint32_t min_stem_size,
                    InProgressStemMap& stem_mappings) const override;

  void StemWordInPlace(std::string& word,
                       uint32_t min_stem_size = 0) const override;

  const std::string& DefaultPunctuation() const override;

  bool SupportsStemming() const override { return true; }

 private:
  explicit SnowballProcessor(data_model::Language language);

  data_model::Language language_;
  std::string default_punctuation_;

  sb_stemmer* GetStemmer() const;
  std::string_view DoStemming(absl::string_view word, sb_stemmer* stemmer,
                              uint32_t min_stem_size) const;
  static const char* GetLanguageString(data_model::Language language);
};

}  // namespace valkey_search::indexes::text

#endif
