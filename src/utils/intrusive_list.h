/*
 * Copyright (c) 2025, ValkeySearch contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
