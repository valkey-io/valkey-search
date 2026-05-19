/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_COMPOSED_ITERATORS_H_
#define VALKEYSEARCH_SRC_INDEXES_COMPOSED_ITERATORS_H_

#include <memory>
#include <vector>

#include "src/indexes/index_base.h"
#include "src/utils/inlined_priority_queue.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes {

// AND Iterator: emits keys present in ALL children, in sorted pointer order.
// Negated children act as exclusion filters.
class AndIterator : public EntriesFetcherIteratorBase {
 public:
  AndIterator(
      std::vector<std::unique_ptr<EntriesFetcherIteratorBase>> children,
      std::vector<std::unique_ptr<EntriesFetcherIteratorBase>> excluded);

  bool Done() const override;
  void Next() override;
  const InternedStringPtr& operator*() const override;
  bool SeekForwardKey(const InternedStringPtr& target) override;

 private:
  std::vector<std::unique_ptr<EntriesFetcherIteratorBase>> children_;
  std::vector<std::unique_ptr<EntriesFetcherIteratorBase>> excluded_;
  InternedStringPtr current_key_;

  bool FindCommonKey();
  bool IsExcluded(const InternedStringPtr& key);
};

// OR Iterator: emits keys present in ANY child, in sorted pointer order,
// deduplicated.
class OrIterator : public EntriesFetcherIteratorBase {
 public:
  explicit OrIterator(
      std::vector<std::unique_ptr<EntriesFetcherIteratorBase>> children);

  bool Done() const override;
  void Next() override;
  const InternedStringPtr& operator*() const override;
  bool SeekForwardKey(const InternedStringPtr& target) override;

 private:
  struct HeapEntry {
    size_t idx;
    const std::vector<std::unique_ptr<EntriesFetcherIteratorBase>>* children;
    bool operator>(const HeapEntry& other) const {
      return **(*children)[idx] > **(*other.children)[other.idx];
    }
  };

  std::vector<std::unique_ptr<EntriesFetcherIteratorBase>> children_;
  InlinedPriorityQueue<HeapEntry, 8> heap_;
  InternedStringPtr current_key_;

  void Advance();
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_COMPOSED_ITERATORS_H_
