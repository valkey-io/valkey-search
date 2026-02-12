/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_UNIVERSAL_SET_FETCHER_H_
#define VALKEYSEARCH_SRC_INDEXES_UNIVERSAL_SET_FETCHER_H_

#include <memory>

#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes {

class UniversalSetFetcher : public EntriesFetcherBase {
 public:
  explicit UniversalSetFetcher(const IndexSchema* index_schema);

  size_t Size() const override { return size_; }
  std::unique_ptr<EntriesFetcherIteratorBase> Begin() override;

 private:
  class Iterator : public EntriesFetcherIteratorBase {
   public:
    explicit Iterator(const IndexSchema* index_schema);

    bool Done() const override;
    void Next() override;
    const InternedStringPtr& operator*() const override;

   private:
    IndexSchema::IndexKeyInfoMap::const_iterator current_it_;
    IndexSchema::IndexKeyInfoMap::const_iterator end_it_;
  };

  const IndexSchema* index_schema_;
  size_t size_;
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_UNIVERSAL_SET_FETCHER_H_
