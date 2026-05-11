/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_UTILS_LRU_H_
#define VALKEYSEARCH_SRC_UTILS_LRU_H_
#include <cstddef>

#include "absl/log/check.h"
#include "src/utils/intrusive_list.h"

namespace valkey_search {
template <typename T>
class LRU {
 public:
  explicit LRU(size_t capacity) : capacity_(capacity) {};
  T *InsertAtTop(T *node);
  void Promote(T *node);
  void Remove(T *node);
  size_t Size() const { return list_.Size(); }

 private:
  size_t capacity_{0};
  IntrusiveList<T> list_;
};

template <typename T>
T *LRU<T>::InsertAtTop(T *node) {
  CHECK(node->next == nullptr && node->prev == nullptr);
  if (list_.Size() < capacity_) {
    list_.PushBack(node);
    return nullptr;
  }
  auto *last = list_.Front();
  list_.Remove(last);
  list_.PushBack(node);
  return last;
}

template <typename T>
void LRU<T>::Promote(T *node) {
  Remove(node);
  CHECK(!InsertAtTop(node));
}

template <typename T>
void LRU<T>::Remove(T *node) {
  list_.Remove(node);
}

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_UTILS_LRU_H_
