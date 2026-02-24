/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_UTILS_INLINED_MULTISET_H_
#define VALKEYSEARCH_SRC_UTILS_INLINED_MULTISET_H_

#include <algorithm>

#include "absl/container/inlined_vector.h"

namespace valkey_search {

template <typename T, size_t N>
class InlinedMultiset {
 public:
  using Storage = absl::InlinedVector<T, N>;
  using iterator = typename Storage::iterator;
  using const_iterator = typename Storage::const_iterator;

  template <typename... Args>
  void emplace(Args &&...args) {
    T value(std::forward<Args>(args)...);
    auto it = std::lower_bound(storage_.begin(), storage_.end(), value);
    storage_.insert(it, std::move(value));
  }

  iterator erase(const_iterator pos) { return storage_.erase(pos); }

  const_iterator begin() const { return storage_.begin(); }
  const_iterator end() const { return storage_.end(); }
  bool empty() const { return storage_.empty(); }
  void clear() { storage_.clear(); }

 private:
  Storage storage_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_UTILS_INLINED_MULTISET_H_
