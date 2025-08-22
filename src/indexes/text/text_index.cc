/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/text_index.h"

#include "libstemmer.h"

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

}  // namespace valkey_search::indexes::text
