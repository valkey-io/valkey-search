// TODO: Remove this after rest of tokenization is ready, currently just for testing stemming functionality



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
#include "vmsdk/src/log.h"

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
  
  VMSDK_LOG(NOTICE, nullptr) << "Initialized Snowball stemmer for language: " << language;
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

bool Stemmer::IsInitialized() const {
  return stemmer_ != nullptr;
}

std::string Stemmer::GetLanguage() const {
  if (!stemmer_) {
    return "";
  }
  // Note: Snowball doesn't provide a way to get language from stemmer
  // In practice, you'd track this in the class
  return "unknown"; // Placeholder
}

// Self-test function to verify stemmer integration
void Stemmer::RunSelfTest() {
  std::cout << "=== Valkey-Search Stemmer Self-Test ===" << std::endl;
  bool overall_success = true;
  
  // English Language Test
  std::cout << "\n--- English Language Test ---" << std::endl;
  Stemmer english_stemmer;
  auto status = english_stemmer.Initialize("english");
  if (!status.ok()) {
    std::cout << "❌ Failed to initialize English stemmer: " << status.message() << std::endl;
    overall_success = false;
  } else {
    std::cout << "✅ English stemmer initialized successfully" << std::endl;
    
    // English test words
    std::vector<std::pair<std::string, std::string>> english_tests = {
      {"running", "run"},
      {"flies", "fli"},
      {"dogs", "dog"},
      {"programming", "program"},
      {"development", "develop"}
    };
    
    bool english_passed = true;
    for (const auto& [word, expected] : english_tests) {
      auto result = english_stemmer.StemWord(word);
      if (result.ok()) {
        std::string stemmed = *result;
        bool passed = (stemmed == expected);
        std::cout << "  " << word << " → " << stemmed;
        if (passed) {
          std::cout << " ✅" << std::endl;
        } else {
          std::cout << " ❌ (expected: " << expected << ")" << std::endl;
          english_passed = false;
        }
      } else {
        std::cout << "  " << word << " → ERROR: " << result.status().message() << " ❌" << std::endl;
        english_passed = false;
      }
    }
    
    if (!english_passed) {
      overall_success = false;
    }
  }
  
  // French Language Test
  std::cout << "\n--- French Language Test ---" << std::endl;
  Stemmer french_stemmer;
  auto french_status = french_stemmer.Initialize("french");
  if (!french_status.ok()) {
    std::cout << "❌ Failed to initialize French stemmer: " << french_status.message() << std::endl;
    std::cout << "   This is expected if French language support hasn't been added yet." << std::endl;
    std::cout << "   Use './add_language.sh french' to add French support." << std::endl;
    overall_success = false;
  } else {
    std::cout << "✅ French stemmer initialized successfully" << std::endl;
    
    // French test words (word -> expected stem)
    std::vector<std::pair<std::string, std::string>> french_tests = {
      {"chevaux", "cheval"},     // horses -> horse
      {"journaux", "journal"},   // newspapers -> newspaper  
      {"ordinateurs", "ordin"},  // computers -> comput (French stemmer result)
      {"développement", "développ"}, // development -> develop
      {"programmation", "program"}   // programming -> program
    };
    
    bool french_passed = true;
    for (const auto& [word, expected] : french_tests) {
      auto result = french_stemmer.StemWord(word);
      if (result.ok()) {
        std::string stemmed = *result;
        bool passed = (stemmed == expected);
        std::cout << "  " << word << " → " << stemmed;
        if (passed) {
          std::cout << " ✅" << std::endl;
        } else {
          std::cout << " ❌ (expected: " << expected << ")" << std::endl;
          french_passed = false;
        }
      } else {
        std::cout << "  " << word << " → ERROR: " << result.status().message() << " ❌" << std::endl;
        french_passed = false;
      }
    }
    
    if (!french_passed) {
      overall_success = false;
    }
  }
  
  std::cout << "\n--- Overall Test Summary ---" << std::endl;
  if (overall_success) {
    std::cout << "🎉 All stemming tests passed!" << std::endl;
  } else {
    std::cout << "❌ Some stemming tests failed or languages not supported" << std::endl;
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
