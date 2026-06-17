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

#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/lexer.h"

namespace valkey_search::indexes::text {

class LanguageProcessor {
 public:
  virtual ~LanguageProcessor() = default;

  static std::shared_ptr<LanguageProcessor> Create(
      data_model::Language language);

  // Segment text into tokens. For Snowball languages: punct/space split +
  // normalize + lowercase. For ICU languages: BreakIterator word boundaries.
  virtual std::vector<std::string> Tokenize(absl::string_view text) const = 0;

  // Build the stem map for a set of tokens. Called during indexing.
  // No-op for ICU languages (they don't support stemming).
  virtual void BuildStemMap(const std::vector<std::string>& tokens,
                            uint32_t min_stem_size,
                            InProgressStemMap& stem_mappings) const {}

  // Stem a single word in place. Used during key deletion and query expansion.
  // No-op for ICU languages.
  virtual void StemWordInPlace(std::string& word,
                               uint32_t min_stem_size = 0) const {}

  // Default punctuation characters for this language.
  // Arabic extends ASCII defaults with Arabic-specific punctuation.
  // ICU languages return empty — BreakIterator handles punctuation natively.
  virtual const std::string& DefaultPunctuation() const = 0;

  // Whether this processor produces stems (controls stem tree construction).
  virtual bool SupportsStemming() const { return false; }
};

}  // namespace valkey_search::indexes::text

#endif
