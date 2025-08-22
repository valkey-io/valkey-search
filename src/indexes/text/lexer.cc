/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/indexes/text/lexer.h"

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "libstemmer.h"

namespace valkey_search::indexes::text {

// TODO : Update Constructor and Deconstructor below after rest of lexical operations are added
Lexer::Lexer() : stemmer_(nullptr) {}

Lexer::~Lexer() {
  if (stemmer_) {
    sb_stemmer_delete(stemmer_);
  }
}

absl::Status Lexer::Initialize(const std::string& language) {
  if (stemmer_) {
    sb_stemmer_delete(stemmer_);
  }
  
  stemmer_ = sb_stemmer_new(language.c_str(), "UTF_8");
  CHECK(stemmer_) << "Failed to initialize stemmer for language: " + language;
  
  return absl::OkStatus();
}

// TODO : Add test for stemming after rest of lexical operations are added
absl::StatusOr<std::string> Lexer::StemWord(const std::string& word) const {
  CHECK(stemmer_) << "Stemmer not initialized";
  
  if (word.empty()) {
    return word;
  }
  
  const sb_symbol* stemmed = sb_stemmer_stem(
    stemmer_, 
    reinterpret_cast<const sb_symbol*>(word.c_str()),
    word.length()
  );
  
  DCHECK(stemmed) << "Stemming failed for word: " + word;
  
  int stemmed_length = sb_stemmer_length(stemmer_);
  return std::string(reinterpret_cast<const char*>(stemmed), stemmed_length);
}

}  // namespace valkey_search::indexes::text
