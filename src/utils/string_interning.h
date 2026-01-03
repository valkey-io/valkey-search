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
#include <thread>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_map.h"
#include "absl/hash/hash.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "gtest/gtest_prod.h"
#include "src/utils/allocator.h"
#include "vmsdk/src/memory_tracker.h"

namespace valkey_search {

class InternedStringImpl;
class InternedStringPtr;

extern absl::Mutex cout_mutex_;
#define SYNCOUT(x)                                                         \
  if (0) {                                                                 \
    std::ostringstream oss;                                                \
    auto now = std::chrono::steady_clock::now();                           \
    auto ms = std::chrono::duration_cast<std::chrono::microseconds>(       \
                  now.time_since_epoch())                                  \
                  .count();                                                \
    oss << std::dec << std::this_thread::get_id() << " : " << (ms % 10000) \
        << " : " << x;                                                     \
    absl::MutexLock lock(&cout_mutex_);                                    \
    std::cout << oss.str() << std::endl;                                   \
  }  // namespace valkey_search

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
  void IncrementRefCount();
  void DecrementRefCount();

  uint32_t RefCount() const {
    return ref_count_.load(std::memory_order_seq_cst);
  }

  //
  // For stats.
  //
  bool IsInline() const { return is_inline_; }
  size_t Allocated() const {
    size_t bytes = 0;
#ifndef SAN_BUILD
    // Crashes with sanitizer builds.
    bytes += ValkeyModule_MallocUsableSize(const_cast<InternedString *>(this));
#endif
    if (!is_inline_) {
      // Can't actually do a UsableSize of allocated blobs, so use the size.
      bytes += length_;
    }
    return bytes;
  }
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

  InternedStringPtr &operator=(void *other) noexcept {
    CHECK(!other);  // Only nullptr is allowed
    if (impl_) {
      impl_->DecrementRefCount();
    }
    impl_ = nullptr;
    return *this;
  }

  auto operator<=>(const InternedStringPtr &other) const = default;

  size_t Hash() const { return absl::HashOf(impl_); }

  InternedString &operator*() { return *impl_; }
  InternedString *operator->() { return impl_; }
  operator bool() const { return impl_ != nullptr; }
  const InternedString &operator*() const { return *impl_; }
  const InternedString *operator->() const { return impl_; }
  ~InternedStringPtr() {
    if (impl_) {
      impl_->DecrementRefCount();
    }
    impl_ = nullptr;
  }

  size_t RefCount() const { return impl_ ? impl_->RefCount() : 0; }

 private:
  InternedStringPtr(InternedString *impl) : impl_(impl) {}
  InternedString *impl_{nullptr};
  friend class StringInternStore;
};

inline std::ostream &operator<<(std::ostream &os,
                                const InternedStringPtr &str) {
  return str ? os << str->Str() : os << "<null>";
}

template <typename T>
using InternedStringHashMap = absl::flat_hash_map<InternedStringPtr, T>;
using InternedStringSet = absl::flat_hash_set<InternedStringPtr>;

template <typename T>
using InternedStringNodeHashMap = absl::node_hash_map<InternedStringPtr, T>;

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
      size_t count_{0};      // Number of entries in this bucket
      size_t bytes_{0};      // Total bytes of string data (no overhead)
      size_t allocated_{0};  // Total bytes of allocated data
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

//
// Helper classes to allow using InternedStringPtr as keys in hash maps and
// sets.
//
template <>
struct std::hash<valkey_search::InternedStringPtr> {
  std::size_t operator()(const valkey_search::InternedStringPtr &sp) const {
    return sp.Hash();
  }
};

#endif  // VALKEYSEARCH_SRC_UTILS_STRING_INTERNING_H_
