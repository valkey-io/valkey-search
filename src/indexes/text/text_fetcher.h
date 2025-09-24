/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_FETCHER_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_FETCHER_H_

#include <cstddef>
#include <vector>

#include "src/indexes/index_base.h"
#include "src/indexes/text/posting.h"
#include "src/indexes/text/radix_tree.h"
#include "src/indexes/text/text_iterator.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes::text {

using FieldMaskPredicate = uint64_t;
using WordIterator = RadixTree<std::shared_ptr<Postings>, false>::WordIterator;

class TextFetcher : public indexes::EntriesFetcherIteratorBase {
 public:
  TextFetcher(std::unique_ptr<TextIterator> iter);

  bool Done() const override;
  void Next() override;
  const InternedStringPtr& operator*() const override;

 private:
  std::unique_ptr<TextIterator>
      iter_;  // List of all the Text Predicates contained in the Proximity AND.
};
}  // namespace valkey_search::indexes::text

#endif
