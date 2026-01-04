/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_NEGATION_ENTRIES_FETCHER_H_
#define VALKEY_SEARCH_INDEXES_TEXT_NEGATION_ENTRIES_FETCHER_H_

#include <memory>

#include "src/indexes/text.h"
#include "src/indexes/text/text_fetcher.h"
#include "src/indexes/text/text_iterator.h"

namespace valkey_search::indexes::text {

class NegationEntriesFetcher : public Text::EntriesFetcher {
 public:
  NegationEntriesFetcher(std::unique_ptr<TextIterator> iter, size_t size,
                         const std::shared_ptr<TextIndex>& text_index,
                         FieldMaskPredicate field_mask)
      : EntriesFetcher(size, text_index, nullptr, field_mask),
        iter_(std::move(iter)) {}

  std::unique_ptr<EntriesFetcherIteratorBase> Begin() override {
    return std::make_unique<TextFetcher>(std::move(iter_));
  }

 private:
  std::unique_ptr<TextIterator> iter_;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_NEGATION_ENTRIES_FETCHER_H_
