/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_UTILS_STRING_INTERNING_H_
#define VALKEYSEARCH_SRC_UTILS_STRING_INTERNING_H_

#include <absl/container/btree_map.h>

#include <cstddef>

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

class InternedStringImpl;
class InternedStringPtr;
class InternedStringPtrEqual;
class InternedStringPtrHash;

//
// An interned string. This is a reference-counted object of variable size.
// Either the string data is stored inline OR for externally allocated strings
// a pointer to the allocation is stored.
//
class InternedString {
 public:
  friend class StringInternStore;
  absl::string_view Str() const;
  operator absl::string_view() const { return Str(); }
  absl::string_view operator*() const { return Str(); }

  // Private except for specialized out-of-line construction.
 protected:
  InternedString() = default;
  InternedString(const InternedString &) = delete;
  InternedString &operator=(const InternedString &) = delete;
  InternedString(InternedString &&) = delete;
  InternedString &operator=(InternedString &&) = delete;
  ~InternedString() = default;

  friend class InternedStringPtr;
  static InternedString *Constructor(absl::string_view str,
                                     Allocator *allocator);
  void Destructor();
  void IncrementRefCount() {
    auto old_value = ref_count_.fetch_add(1, std::memory_order_relaxed);
  }
  void DecrementRefCount() {
    if (ref_count_.fetch_sub(1, std::memory_order_seq_cst) == 1) {
      Destructor();
    }
  }

  bool IsInline() const { return is_inline_; }
  //
  // Data layout. there's an 8 byte header followed by either the inline
  // string data or a pointer to externally allocated string data.
  //
  // The declaration below only provides a placeholder for the variable-length
  // data_ member. In the allocated case there's[0] actually a pointer stored
  // here. Future optimizations could further shrink the header size by using a
  // variable size for the length_ field.
  //
  std::atomic<uint32_t> ref_count_;
  uint32_t length_ : 31;
  uint32_t is_inline_ : 1;
  char data_[0];
};

static_assert(sizeof(InternedString) == 8,
              "InternedString has unexpected padding");

//
// A smart pointer for InternedString that manages reference counting.
//
class InternedStringPtr {
 public:
  InternedStringPtr() = default;
  InternedStringPtr(const InternedStringPtr &other) : impl_(other.impl_) {
    if (impl_) {
      impl_->IncrementRefCount();
    }
  }
  InternedStringPtr(InternedStringPtr &&other) noexcept : impl_(other.impl_) {
    other.impl_ = nullptr;
  }
  InternedStringPtr &operator=(const InternedStringPtr &other) {
    if (this != &other) {
      if (impl_) {
        impl_->DecrementRefCount();
      }
      impl_ = other.impl_;
      if (impl_) {
        impl_->IncrementRefCount();
      }
    }
    return *this;
  }
  InternedStringPtr &operator=(InternedStringPtr &&other) noexcept {
    if (this != &other) {
      if (impl_) {
        impl_->DecrementRefCount();
      }
      impl_ = other.impl_;
      other.impl_ = nullptr;
    }
    return *this;
  }

  bool operator==(const InternedStringPtr &other) const {
    return impl_ == other.impl_;
  }
  bool operator!=(const InternedStringPtr &other) const {
    return impl_ != other.impl_;
  }

  InternedString &operator*() { return *impl_; }
  InternedString *operator->() { return impl_; }
  operator bool() const { return impl_ != nullptr; }
  const InternedString &operator*() const { return *impl_; }
  const InternedString *operator->() const { return impl_; }
  ~InternedStringPtr() {
    if (impl_) {
      impl_->DecrementRefCount();
    }
  }

  size_t RefCount() const {
    return impl_ ? impl_->ref_count_.load(std::memory_order_relaxed) : 0;
  }

 private:
  InternedStringPtr(InternedString *impl) : impl_(impl) {}
  InternedString *impl_{nullptr};
  friend class StringInternStore;
  friend class InternedStringPtrHash;
  friend class InternedStringPtrEqual;
};

inline std::ostream &operator<<(std::ostream &os,
                                const InternedStringPtr &str) {
  return str ? os << str->Str() : os << "<null>";
}

//
// Helper classes to allow using InternedStringPtr as keys in hash maps and
// sets.
//
struct InternedStringPtrHash {
  std::size_t operator()(const InternedStringPtr &sp) const {
    return absl::HashOf(sp.impl_);
  }
};

struct InternedStringPtrEqual {
  bool operator()(const InternedStringPtr &lhs,
                  const InternedStringPtr &rhs) const {
    return lhs.impl_ == rhs.impl_;
  }
};

template <typename T>
using InternedStringMap =
    absl::flat_hash_map<InternedStringPtr, T, InternedStringPtrHash,
                        InternedStringPtrEqual>;
using InternedStringSet =
    absl::flat_hash_set<InternedStringPtr, InternedStringPtrHash,
                        InternedStringPtrEqual>;

class StringInternStore {
 public:
  friend class InternedString;
  static StringInternStore &Instance();
  //
  // Interns the given string. If an identical string has already been
  // interned, returns a pointer to the existing interned string. Otherwise,
  // creates a new interned string.
  //
  static InternedStringPtr Intern(absl::string_view str,
                                  Allocator *allocator = nullptr);

  static int64_t GetMemoryUsage();

  size_t UniqueStrings() const {
    absl::MutexLock lock(&mutex_);
    return str_to_interned_.size();
  }

  struct Stats {
    struct BucketStats {
      size_t count_{0};
      size_t bytes_{0};
    };
    absl::btree_map<int, BucketStats> by_ref_stats_;
    absl::btree_map<int, BucketStats> by_size_stats_;
    BucketStats inline_total_stats_;
    BucketStats out_of_line_total_stats_;
  };
  Stats GetStats() const;

 private:
  static MemoryPool memory_pool_;

  StringInternStore() = default;
  bool Release(InternedString *str);
  InternedStringPtr InternImpl(absl::string_view str,
                               Allocator *allocator = nullptr);
  struct InternedStringPtrFullHash {
    std::size_t operator()(const InternedStringPtr &sp) const {
      return absl::HashOf(sp->Str());
    }
  };

  struct InternedStringPtrFullEqual {
    bool operator()(const InternedStringPtr &lhs,
                    const InternedStringPtr &rhs) const {
      return lhs->Str() == rhs->Str();
    }
  };
  absl::flat_hash_set<InternedStringPtr, InternedStringPtrFullHash,
                      InternedStringPtrFullEqual>
      str_to_interned_ ABSL_GUARDED_BY(mutex_);
  mutable absl::Mutex mutex_;

  // Used for testing.
  static void SetMemoryUsage(int64_t value) {
    memory_pool_.Reset();
    memory_pool_.Add(value);
  }

  static StringInternStore *MakeInstance();

  FRIEND_TEST(ValkeySearchTest, Info);
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_UTILS_STRING_INTERNING_H_
