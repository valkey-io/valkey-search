/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/snowball_processor.h"

#include "src/indexes/text/delimiter_query_tokenizer.h"
#include "src/indexes/text/normalize_case_fold_filter.h"
#include "src/indexes/text/punctuation_segmenter.h"
#include "src/indexes/text/snowball_stem_filter.h"
#include "src/indexes/text/stop_word_filter.h"
#include "src/indexes/text/unicode_normalizer.h"

namespace valkey_search::indexes::text {

std::shared_ptr<LanguageProcessor> CreateSnowballProcessor(
    data_model::Language language, const std::string& punctuation,
    const std::vector<std::string>& stop_words) {
  auto processor = std::make_shared<LanguageProcessor>();

  // Segmenter: punctuation-based splitting for all Snowball languages
  auto punct_segmenter = std::make_shared<PunctuationSegmenter>(punctuation);
  processor->segmenters_.push_back(punct_segmenter);

  // Query tokenizer: delimiter-based (uses the same punctuation segmenter)
  processor->query_tokenizer_ =
      std::make_shared<DelimiterQueryTokenizer>(*punct_segmenter);

  // Filter 1: Unicode normalization + case folding
  NormalizationForm norm_form = (language == data_model::LANGUAGE_ARABIC)
                                    ? NormalizationForm::NFKC
                                    : NormalizationForm::NFC;
  auto normalizer = std::make_shared<NormalizeCaseFoldFilter>(norm_form);
  processor->normalizer_ = normalizer;
  processor->filters_.push_back(std::move(normalizer));

  // Filter 2: Stop word removal
  auto stop_filter = std::make_shared<StopWordFilter>(stop_words);
  processor->stop_word_filter_ = stop_filter;
  processor->filters_.push_back(std::move(stop_filter));

  // Stemming is NOT part of the main pipeline. Tokens are indexed in their
  // normalized (unstemmed) form. The stem filter is stored separately for
  // callers that need it (stem map building, query expansion, delete path).
  processor->stemmer_ = std::make_shared<SnowballStemFilter>(language);

  return processor;
}

}  // namespace valkey_search::indexes::text
