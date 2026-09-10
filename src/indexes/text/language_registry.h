/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_LANGUAGE_REGISTRY_H_
#define VALKEY_SEARCH_INDEXES_TEXT_LANGUAGE_REGISTRY_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/language.h"

namespace valkey_search::indexes::text {

/// Singleton registry of Language instances keyed by protobuf enum.
///
/// All Language objects are immutable, stateless, and shared across indexes.
/// The registry is populated once at static init time with all supported
/// Snowball languages. Returns English for LANGUAGE_UNSPECIFIED.
class LanguageRegistry {
 public:
  static LanguageRegistry& Instance();

  /// Returns a shared Language instance for the given enum (default config).
  /// Returns English for LANGUAGE_UNSPECIFIED.
  /// Returns nullptr for unknown/unregistered language values.
  std::shared_ptr<const Language> Get(data_model::Language language) const;

  // Non-copyable, non-movable
  LanguageRegistry(const LanguageRegistry&) = delete;
  LanguageRegistry& operator=(const LanguageRegistry&) = delete;

 private:
  LanguageRegistry();

  absl::flat_hash_map<data_model::Language, std::shared_ptr<const Language>>
      languages_;
};

/// Create a Language with custom punctuation/stop words (for FT.CREATE
/// overrides and tests). When no overrides are needed, prefer
/// LanguageRegistry::Instance().Get().
std::shared_ptr<const Language> CreateLanguage(
    data_model::Language language, const std::string& punctuation,
    const std::vector<std::string>& stop_words);

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_LANGUAGE_REGISTRY_H_
