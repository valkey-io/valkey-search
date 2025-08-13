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

void Stemmer::RunSelfTest() {
  std::cout << "=== Stemmer Self-Test ===" << std::endl;
  
  // English test
  std::cout << "\n--- English Test ---" << std::endl;
  Stemmer english_stemmer;
  auto status = english_stemmer.Initialize("english");
  if (!status.ok()) {
    std::cout << "Failed to initialize English stemmer: " << status.message() << std::endl;
  } else {
    std::vector<std::pair<std::string, std::string>> english_tests = {
      {"running", "run"},
      {"flies", "fli"},
      {"dogs", "dog"},
      {"programming", "program"}
    };
    
    for (const auto& [word, expected] : english_tests) {
      auto result = english_stemmer.StemWord(word);
      if (result.ok()) {
        std::cout << word << " -> " << *result << std::endl;
      } else {
        std::cout << word << " -> ERROR: " << result.status().message() << std::endl;
      }
    }
  }
  
  // French test
  std::cout << "\n--- French Test ---" << std::endl;
  Stemmer french_stemmer;
  auto french_status = french_stemmer.Initialize("french");
  if (!french_status.ok()) {
    std::cout << "Failed to initialize French stemmer: " << french_status.message() << std::endl;
  } else {
    std::vector<std::pair<std::string, std::string>> french_tests = {
      {"chevaux", "cheval"},
      {"ordinateurs", "ordin"},
      {"développement", "développ"}
    };
    
    for (const auto& [word, expected] : french_tests) {
      auto result = french_stemmer.StemWord(word);
      if (result.ok()) {
        std::cout << word << " -> " << *result << std::endl;
      } else {
        std::cout << word << " -> ERROR: " << result.status().message() << std::endl;
      }
    }
  }
}

}  // namespace valkey_search::indexes

// Self-test main function (only compiled if STEMMER_SELF_TEST is defined)
#ifdef STEMMER_SELF_TEST
int main() {
  valkey_search::indexes::Stemmer::RunSelfTest();
  return 0;
}
#endif
