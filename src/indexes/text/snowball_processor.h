/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_SNOWBALL_PROCESSOR_H_
#define VALKEY_SEARCH_INDEXES_TEXT_SNOWBALL_PROCESSOR_H_

#include <memory>
#include <string>
#include <vector>

#include "src/index_schema.pb.h"
#include "src/indexes/text/language_processor.h"

namespace valkey_search::indexes::text {

/// Create a LanguageProcessor composed for Snowball languages.
///
/// Composition:
///   Segmenter:  PunctuationSegmenter(punctuation)
///   Filters:    NormalizeCaseFoldFilter(NFC/NFKC) → StopWordFilter(stop_words)
///   Stem:       SnowballStemFilter(language)
///
/// Handles: English, French, German, Spanish, Italian, Portuguese,
///          Russian, Swedish, Turkish, Dutch, Indonesian, Arabic.
std::shared_ptr<LanguageProcessor> CreateSnowballProcessor(
    data_model::Language language, const std::string &punctuation,
    const std::vector<std::string> &stop_words);

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_SNOWBALL_PROCESSOR_H_
