/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/text_index.h"

#include "libstemmer.h"
#include "absl/strings/ascii.h"

namespace valkey_search::indexes::text {

TextIndexSchema::~TextIndexSchema() {
  if (stemmer_) {
    sb_stemmer_delete(stemmer_);
  }
}

sb_stemmer* TextIndexSchema::GetStemmer() const {
  if (!stemmer_) {
    stemmer_ = sb_stemmer_new(GetLanguageString().c_str(), "UTF_8");
  }
  return stemmer_;
}

void TextIndexSchema::BuildStopWordsSet(const std::vector<std::string>& stop_words) {
  stop_words_set_.clear();
  
  // Convert all stop words to lowercase for case-insensitive matching
  for (const auto& word : stop_words) {
    stop_words_set_.insert(absl::AsciiStrToLower(word));
  }
}


}  // namespace valkey_search::indexes::text
