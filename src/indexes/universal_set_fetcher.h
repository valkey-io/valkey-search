/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_UNIVERSAL_SET_FETCHER_H_
#define VALKEYSEARCH_SRC_INDEXES_UNIVERSAL_SET_FETCHER_H_

#include <memory>

#include "src/indexes/index_base.h"
#include "src/utils/string_interning.h"

namespace valkey_search {
class IndexSchema;
}

namespace valkey_search::indexes {

// UniversalSetFetcher returns all keys from IndexSchema
// Used for negation with text queries: U (all keys)
class UniversalSetFetcher : public EntriesFetcherBase {
 public:
  explicit UniversalSetFetcher(const IndexSchema* index_schema);

  size_t Size() const override { return size_; }
  std::unique_ptr<EntriesFetcherIteratorBase> Begin() override;

 private:
  class Iterator : public EntriesFetcherIteratorBase {
   public:
    explicit Iterator(const IndexSchema* index_schema);
    ~Iterator() override;

    bool Done() const override;
    void Next() override;
    const InternedStringPtr& operator*() const override;

   private:
    const IndexSchema* index_schema_;
    void* db_key_iter_;  // Type-erased iterator
    bool done_;
  };

  const IndexSchema* index_schema_;
  size_t size_;
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_UNIVERSAL_SET_FETCHER_H_
