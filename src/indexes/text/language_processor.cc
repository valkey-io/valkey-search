/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/language_processor.h"

#include "src/indexes/text/snowball_processor.h"

namespace valkey_search::indexes::text {

std::shared_ptr<LanguageProcessor> LanguageProcessor::Create(
    data_model::Language language) {
  switch (language) {
    // TODO: ICUSegmenter class to be implemented
    case data_model::LANGUAGE_ENGLISH:
    case data_model::LANGUAGE_FRENCH:
    case data_model::LANGUAGE_GERMAN:
    case data_model::LANGUAGE_SPANISH:
    case data_model::LANGUAGE_ITALIAN:
    case data_model::LANGUAGE_PORTUGUESE:
    case data_model::LANGUAGE_RUSSIAN:
    case data_model::LANGUAGE_SWEDISH:
    case data_model::LANGUAGE_TURKISH:
    case data_model::LANGUAGE_DUTCH:
    case data_model::LANGUAGE_INDONESIAN:
    case data_model::LANGUAGE_ARABIC:
      return std::shared_ptr<SnowballProcessor>(
          new SnowballProcessor(language));

    default:
      return std::shared_ptr<SnowballProcessor>(
          new SnowballProcessor(data_model::LANGUAGE_ENGLISH));
  }
}

}  // namespace valkey_search::indexes::text
