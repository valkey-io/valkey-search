/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEY_SEARCH_INDEXES_STEMMER_H_
#define VALKEY_SEARCH_INDEXES_STEMMER_H_

#include <string>
#include "absl/status/status.h"
#include "absl/status/statusor.h"

// Forward declaration for Snowball stemmer
struct sb_stemmer;

namespace valkey_search::indexes {

/**
 * Minimalistic stemmer using Snowball algorithm.
 */
class Stemmer {
 public:
  Stemmer();
  ~Stemmer();
  
  // Disable copy constructor and assignment operator
  Stemmer(const Stemmer&) = delete;
  Stemmer& operator=(const Stemmer&) = delete;
  
  /**
   * Initialize stemmer for a specific language.
   * @param language Language code (e.g., "english", "french")
   */
  absl::Status Initialize(const std::string& language);
  
  /**
   * Stem a word to its root form.
   * @param word The word to stem (UTF-8 encoded)
   */
  absl::StatusOr<std::string> StemWord(const std::string& word) const;
  
 private:
  struct sb_stemmer* stemmer_;
};

}  // namespace valkey_search::indexes

#endif  // VALKEY_SEARCH_INDEXES_STEMMER_H_
