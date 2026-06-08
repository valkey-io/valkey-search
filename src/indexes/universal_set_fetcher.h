/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_UNIVERSAL_SET_FETCHER_H_
#define VALKEYSEARCH_SRC_INDEXES_UNIVERSAL_SET_FETCHER_H_

#include <memory>
#include <vector>

#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes {

class UniversalSetFetcher : public EntriesFetcherBase {
 public:
  explicit UniversalSetFetcher(const IndexSchema* index_schema);

  size_t Size() const override { return keys_.size(); }
  std::unique_ptr<EntriesFetcherIteratorBase> Begin() override;

 private:
  class Iterator : public EntriesFetcherIteratorBase {
   public:
    explicit Iterator(const std::vector<InternedStringPtr>* keys);

    bool Done() const override;
    void Next() override;
    const InternedStringPtr& operator*() const override;

   private:
    const std::vector<InternedStringPtr>* keys_;
    size_t i_{0};
  };

  // Snapshot of keys taken at construction under schema_mutex_'s reader lock.
  // We keep refs (InternedStringPtr is shared-ptr-like) so subsequent
  // mutations on the schema don't affect iteration.
  std::vector<InternedStringPtr> keys_;
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_UNIVERSAL_SET_FETCHER_H_
