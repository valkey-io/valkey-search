/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/indexes/stemmer.h"

#include <algorithm>
#include <iostream>
#include "absl/status/status.h"
#include "libstemmer.h"

namespace valkey_search::indexes {

Stemmer::Stemmer() : stemmer_(nullptr) {}

Stemmer::~Stemmer() {
  if (stemmer_) {
    sb_stemmer_delete(stemmer_);
  }
}

absl::Status Stemmer::Initialize(const std::string& language) {
  if (stemmer_) {
    sb_stemmer_delete(stemmer_);
  }
  
  stemmer_ = sb_stemmer_new(language.c_str(), "UTF_8");
  if (!stemmer_) {
    return absl::InvalidArgumentError("Failed to initialize stemmer for language: " + language);
  }
  
  return absl::OkStatus();
}

absl::StatusOr<std::string> Stemmer::StemWord(const std::string& word) const {
  if (!stemmer_) {
    return absl::FailedPreconditionError("Stemmer not initialized");
  }
  
  if (word.empty()) {
    return word;
  }
  
  // Convert to lowercase for stemming
  std::string lowercase_word = word;
  std::transform(lowercase_word.begin(), lowercase_word.end(), 
                lowercase_word.begin(), ::tolower);
  
  const sb_symbol* stemmed = sb_stemmer_stem(
    stemmer_, 
    reinterpret_cast<const sb_symbol*>(lowercase_word.c_str()),
    lowercase_word.length()
  );
  
  if (!stemmed) {
    return absl::InternalError("Stemming failed for word: " + word);
  }
  
  int stemmed_length = sb_stemmer_length(stemmer_);
  return std::string(reinterpret_cast<const char*>(stemmed), stemmed_length);
}

}  // namespace valkey_search::indexes
