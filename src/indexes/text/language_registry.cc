/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/language_registry.h"

#include <memory>
#include <string>
#include <vector>

#include "absl/log/check.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/customized_language.h"
#include "src/indexes/text/languages/arabic.h"
#include "src/indexes/text/languages/dutch.h"
#include "src/indexes/text/languages/english.h"
#include "src/indexes/text/languages/french.h"
#include "src/indexes/text/languages/german.h"
#include "src/indexes/text/languages/indonesian.h"
#include "src/indexes/text/languages/italian.h"
#include "src/indexes/text/languages/portuguese.h"
#include "src/indexes/text/languages/russian.h"
#include "src/indexes/text/languages/spanish.h"
#include "src/indexes/text/languages/swedish.h"
#include "src/indexes/text/languages/turkish.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {

LanguageRegistry& LanguageRegistry::Instance() {
  static LanguageRegistry instance;
  return instance;
}

LanguageRegistry::LanguageRegistry() {
  using L = data_model::Language;

  languages_[L::LANGUAGE_ENGLISH] = std::make_shared<EnglishLanguage>();
  languages_[L::LANGUAGE_FRENCH] = std::make_shared<FrenchLanguage>();
  languages_[L::LANGUAGE_GERMAN] = std::make_shared<GermanLanguage>();
  languages_[L::LANGUAGE_SPANISH] = std::make_shared<SpanishLanguage>();
  languages_[L::LANGUAGE_ITALIAN] = std::make_shared<ItalianLanguage>();
  languages_[L::LANGUAGE_PORTUGUESE] = std::make_shared<PortugueseLanguage>();
  languages_[L::LANGUAGE_RUSSIAN] = std::make_shared<RussianLanguage>();
  languages_[L::LANGUAGE_SWEDISH] = std::make_shared<SwedishLanguage>();
  languages_[L::LANGUAGE_TURKISH] = std::make_shared<TurkishLanguage>();
  languages_[L::LANGUAGE_DUTCH] = std::make_shared<DutchLanguage>();
  languages_[L::LANGUAGE_INDONESIAN] = std::make_shared<IndonesianLanguage>();
  languages_[L::LANGUAGE_ARABIC] = std::make_shared<ArabicLanguage>();

  // LANGUAGE_UNSPECIFIED maps to English — capture before inserting to avoid
  // use-after-free if the insert triggers a rehash.
  auto english = languages_[L::LANGUAGE_ENGLISH];
  languages_[L::LANGUAGE_UNSPECIFIED] = english;
}

std::shared_ptr<const Language> LanguageRegistry::Get(
    data_model::Language language) const {
  auto it = languages_.find(language);
  if (it != languages_.end()) {
    return it->second;
  }
  return nullptr;
}

std::shared_ptr<const Language> CreateLanguage(
    data_model::Language language, const std::string& punctuation,
    const std::vector<std::string>& stop_words) {
  auto lang = LanguageRegistry::Instance().Get(language);
  CHECK(lang != nullptr) << "CreateLanguage called with unregistered language: "
                         << data_model::Language_Name(language);
  auto base = std::static_pointer_cast<const SnowballLanguage>(lang);
  return std::make_shared<CustomizedLanguage>(std::move(base), punctuation,
                                              stop_words);
}

}  // namespace valkey_search::indexes::text
