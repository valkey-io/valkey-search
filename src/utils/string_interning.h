/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
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
#include "gtest/gtest_prod.h"
#include "src/utils/allocator.h"
#include "vmsdk/src/memory_tracker.h"

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

  static int64_t GetMemoryUsage();

  size_t Size() const {
    absl::MutexLock lock(&mutex_);
    return str_to_interned_.size();
  }

 private:
  static MemoryPool memory_pool_;

  StringInternStore() = default;
  std::shared_ptr<InternedString> InternImpl(absl::string_view str,
                                             Allocator *allocator);
  void Release(InternedString *str);
  absl::flat_hash_map<absl::string_view, std::weak_ptr<InternedString>>
      str_to_interned_ ABSL_GUARDED_BY(mutex_);
  mutable absl::Mutex mutex_;

  // Used for testing.
  static void SetMemoryUsage(int64_t value) {
    memory_pool_.Reset();
    memory_pool_.Add(value);
  }

  FRIEND_TEST(ValkeySearchTest, Info);
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
