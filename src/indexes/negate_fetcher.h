/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_NEGATE_FETCHER_H_
#define VALKEYSEARCH_SRC_INDEXES_NEGATE_FETCHER_H_

#include <memory>
#include <queue>

#include "absl/container/flat_hash_set.h"
#include "src/indexes/index_base.h"
#include "src/utils/string_interning.h"

namespace valkey_search {
class IndexSchema;
}

namespace valkey_search::indexes {

// NegateEntriesFetcher returns all keys from IndexSchema EXCEPT those matched
// by the inner fetchers. Implements: U \ M(P) where U = all keys, M(P) =
// matched keys
class NegateEntriesFetcher : public EntriesFetcherBase {
 public:
  NegateEntriesFetcher(
      std::queue<std::unique_ptr<EntriesFetcherBase>> inner_fetchers,
      const IndexSchema* index_schema);

  size_t Size() const override { return size_; }
  std::unique_ptr<EntriesFetcherIteratorBase> Begin() override;

 private:
  class Iterator : public EntriesFetcherIteratorBase {
   public:
    Iterator(const absl::flat_hash_set<const char*>& matched_keys,
             const IndexSchema* index_schema);

    bool Done() const override;
    void Next() override;
    const InternedStringPtr& operator*() const override;

   private:
    void AdvanceToNextUnmatched();

    const absl::flat_hash_set<const char*>& matched_keys_;
    const IndexSchema* index_schema_;
    // Iterator over db_key_info_ map
    void* db_key_iter_;  // Type-erased iterator
    bool done_;
  };

  absl::flat_hash_set<const char*> matched_keys_;
  const IndexSchema* index_schema_;
  size_t size_;
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_NEGATE_FETCHER_H_
