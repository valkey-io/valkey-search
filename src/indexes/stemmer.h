// TODO: Remove this after rest of tokenization is ready, currently just for testing stemming functionality



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
 * Stemmer provides text stemming functionality using the Snowball algorithm library.
 * 
 * This class encapsulates the Snowball stemming engine to reduce words to their
 * morphological root form. It supports multiple languages via UTF-8 encoding.
 * 
 * Example usage:
 *   Stemmer stemmer;
 *   auto status = stemmer.Initialize("english");
 *   if (status.ok()) {
 *     auto result = stemmer.StemWord("running");
 *     // result.value() == "run"
 *   }
 */
class Stemmer {
 public:
  /**
   * Constructs an uninitialized Stemmer.
   * Call Initialize() before using stemming functionality.
   */
  Stemmer();
  
  /**
   * Destructor - automatically cleans up Snowball resources.
   */
  ~Stemmer();
  
  // Disable copy constructor and assignment operator
  Stemmer(const Stemmer&) = delete;
  Stemmer& operator=(const Stemmer&) = delete;
  
  /**
   * Initializes the stemmer for a specific language.
   * 
   * @param language Language code (e.g., "english", "french", "german")
   * @return OK status on success, error status with details on failure
   * 
   * Supported languages depend on what's included in the Snowball library.
   * Currently supported: "english" (more can be added via scripts).
   */
  absl::Status Initialize(const std::string& language);
  
  /**
   * Stems a single word to its root form.
   * 
   * @param word The word to stem (UTF-8 encoded)
   * @return Stemmed word on success, error status on failure
   * 
   * Requires Initialize() to be called successfully first.
   * Automatically converts input to lowercase before stemming.
   */
  absl::StatusOr<std::string> StemWord(const std::string& word) const;
  
  /**
   * Checks if the stemmer has been successfully initialized.
   * 
   * @return true if Initialize() was called successfully, false otherwise
   */
  bool IsInitialized() const;
  
  /**
   * Gets the currently initialized language.
   * 
   * @return Language code, or empty string if not initialized
   * 
   * Note: This is a placeholder implementation as Snowball doesn't
   * provide language introspection. In production, track language internally.
   */
  std::string GetLanguage() const;
  
  /**
   * Runs a self-test to verify stemmer integration.
   * 
   * This static method tests basic stemming functionality and prints
   * results to stdout. Useful for integration testing and debugging.
   */
  static void RunSelfTest();
  
 private:
  struct sb_stemmer* stemmer_;  ///< Snowball stemmer instance
};

}  // namespace valkey_search::indexes

#endif  // VALKEY_SEARCH_INDEXES_STEMMER_H_
