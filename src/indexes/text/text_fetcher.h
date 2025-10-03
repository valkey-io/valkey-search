/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_FETCHER_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_FETCHER_H_

#include "src/indexes/index_base.h"
#include "src/indexes/text/text_iterator.h"

namespace valkey_search::indexes::text {

class TextFetcher : public indexes::EntriesFetcherIteratorBase {
 public:
  TextFetcher(std::unique_ptr<TextIterator> iter);

  bool Done() const override;
  const Key& operator*() const override;
  void Next() override;

 private:
  std::unique_ptr<TextIterator> iter_;
};
}  // namespace valkey_search::indexes::text

#endif
