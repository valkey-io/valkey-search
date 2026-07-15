/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_MULTI_LANGUAGE_H_
#define VALKEYSEARCH_SRC_MULTI_LANGUAGE_H_

#include "src/index_schema.pb.h"
#include "src/version.h"

namespace valkey_search {

/// Returns true if the language is non-English and non-unspecified.
inline bool IsNonEnglishLanguage(data_model::Language language) {
  return language != data_model::LANGUAGE_UNSPECIFIED &&
         language != data_model::LANGUAGE_ENGLISH;
}

/// Returns true if multi-language support is enabled (version >= 1.4).
inline bool IsMultiLanguageSupported() { return kModuleVersion >= kRelease14; }

/// Returns true if the given language can be used right now.
/// English/unspecified are always allowed; non-English requires
/// version >= 1.4.
inline bool IsLanguageSupported(data_model::Language language) {
  if (!IsNonEnglishLanguage(language)) {
    return true;
  }
  return IsMultiLanguageSupported();
}

}  // namespace valkey_search

#endif
