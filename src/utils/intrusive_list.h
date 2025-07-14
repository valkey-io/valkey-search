/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_UTILS_INTRUSIVE_LIST_H_
#define VALKEYSEARCH_SRC_UTILS_INTRUSIVE_LIST_H_

#include <cstddef>

#include "absl/log/check.h"

namespace valkey_search {
template <typename T>
class IntrusiveList {
 public:
  IntrusiveList() : head_(nullptr), tail_(nullptr) {}
  void PushBack(T *node);
  void Remove(T *node);
  T *Front() const { return head_; }
  bool Empty() const { return head_ == nullptr; }
  size_t Size() const { return size_; }

 private:
  T *head_;
  T *tail_;
  size_t size_{0};
};

template <typename T>
void IntrusiveList<T>::PushBack(T *node) {
  CHECK(node->next == nullptr);
  CHECK(node->prev == nullptr);
  if (tail_) {
    tail_->next = node;
    node->prev = tail_;
    tail_ = node;
  } else {
    head_ = tail_ = node;
  }
  node->next = nullptr;
  ++size_;
}

template <typename T>
void IntrusiveList<T>::Remove(T *node) {
  if (node->next == nullptr && node->prev == nullptr && tail_ != node &&
      head_ != node) {
    return;
  }
  if (node->prev) {
    node->prev->next = node->next;
  } else {
    head_ = node->next;
  }
  if (node->next) {
    node->next->prev = node->prev;
  } else {
    tail_ = node->prev;
  }
  node->next = node->prev = nullptr;
  --size_;
}

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_UTILS_INTRUSIVE_LIST_H_
