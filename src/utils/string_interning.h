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

#ifndef VALKEYSEARCH_SRC_UTILS_STRING_INTERNING_H_
#define VALKEYSEARCH_SRC_UTILS_STRING_INTERNING_H_

#include <cstddef>
#include <memory>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/utils/allocator.h"

namespace valkey_search {

class InternedString;

class StringInternStore {
 public:
  friend class InternedString;
  friend class InternedString;
  static StringInternStore &Instance() {
    static StringInternStore *instance = new StringInternStore();
    return *instance;
  }
  static std::shared_ptr<InternedString> Intern(absl::string_view str,
                                                Allocator *allocator = nullptr);

  size_t Size() const {
    absl::MutexLock lock(&mutex_);
    return str_to_interned_.size();
  }

 private:
  StringInternStore() = default;
  std::shared_ptr<InternedString> InternImpl(absl::string_view str,
                                             Allocator *allocator);
  void Release(InternedString *str);
  absl::flat_hash_map<absl::string_view, std::weak_ptr<InternedString>>
      str_to_interned_ ABSL_GUARDED_BY(mutex_);
  mutable absl::Mutex mutex_;
};

class InternedString {
 public:
  friend class StringInternStore;
  InternedString() = delete;
  InternedString(const InternedString &) = delete;
  InternedString &operator=(const InternedString &) = delete;
  InternedString(InternedString &&) = delete;
  InternedString &operator=(InternedString &&) = delete;
  // Note: The constructor below does not perform actual string interning.
  // It is intended for cases where an API requires a `StringIntern` object
  // but interning is unnecessary or inefficient. For example, this applies
  // when fetching data from remote nodes.
  InternedString(absl::string_view str) : InternedString(str, false){};

  ~InternedString();

  absl::string_view Str() const { return {data_, length_}; }
  operator absl::string_view() const { return Str(); }
  absl::string_view operator*() const { return Str(); }

 private:
  InternedString(absl::string_view str, bool shared);
  InternedString(char *data, size_t length);

  char *data_;
  size_t length_;
  bool is_shared_;
  bool is_data_owner_;
};

using InternedStringPtr = std::shared_ptr<InternedString>;

struct InternedStringPtrHash {
  template <typename T>
  std::size_t operator()(const T &sp) const {
    return absl::HashOf(sp->Str());
  }
};

struct InternedStringPtrEqual {
  template <typename T, typename U>
  bool operator()(const T &lhs, const U &rhs) const {
    return lhs->Str() == rhs->Str();
  }
};

template <typename T>
using InternedStringMap =
    absl::flat_hash_map<InternedStringPtr, T, InternedStringPtrHash,
                        InternedStringPtrEqual>;
using InternedStringSet =
    absl::flat_hash_set<InternedStringPtr, InternedStringPtrHash,
                        InternedStringPtrEqual>;

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_UTILS_STRING_INTERNING_H_
