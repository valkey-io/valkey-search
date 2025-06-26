/*
 * Copyright (c) 2025, valkey-search contributors
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
