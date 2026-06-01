/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_UTILS_STRING_INTERNING_H_
#define VALKEYSEARCH_SRC_UTILS_STRING_INTERNING_H_

#include <absl/container/btree_map.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <utility>

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
    ref_count_.fetch_add(1, std::memory_order_seq_cst);
  }
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

}  // namespace valkey_search

// std::hash specialization for InternedStringPtr. Defined here, before any
// flat_hash_set<InternedStringPtr> instantiation, so the
// BagOfInternedStringPtrs definition below (which references InternedStringSet)
// can compile.
template <>
struct std::hash<valkey_search::InternedStringPtr> {
  std::size_t operator()(const valkey_search::InternedStringPtr &sp) const {
    return sp.Hash();
  }
};

namespace valkey_search {

template <typename T>
using InternedStringHashMap = absl::flat_hash_map<InternedStringPtr, T>;
using InternedStringSet = absl::flat_hash_set<InternedStringPtr>;

template <typename T>
using InternedStringNodeHashMap = absl::node_hash_map<InternedStringPtr, T>;

//
// BagOfInternedStringPtrs is a space-optimized drop-in replacement for
// InternedStringSet (i.e. absl::flat_hash_set<InternedStringPtr>) tuned for
// small sets. The whole object is exactly 8 bytes; it picks one of four
// representations based on the bottom two bits of storage_:
//
//   storage_ == 0                            -> empty
//   storage_ != 0, low 2 bits == 00          -> Single: the slot IS a clean
//                                               InternedStringPtr (8 bytes)
//   storage_ low 2 bits == 01                -> Array4: (storage_ & ~0x3) is
//                                               a heap std::array<Ptr, 4>;
//                                               holds 2-4 elements packed at
//                                               the front, nulls at the tail
//   storage_ low 2 bits == 10                -> Array8: (storage_ & ~0x3) is
//                                               a heap std::array<Ptr, 8>;
//                                               holds 5-8 elements packed at
//                                               the front, nulls at the tail
//   storage_ low 2 bits == 11                -> Set: (storage_ & ~0x3) is an
//                                               InternedStringSet*; holds 9+
//                                               elements
//
// Tag location: bits 0-1 are free because (a) InternedStringPtr wraps a
// pointer to InternedString whose first member is std::atomic<uint32_t>
// (alignment 4 → low 2 bits 0), and (b) std::array of InternedStringPtr and
// absl::flat_hash_set allocated via `new` have alignment 8 → low 3 bits 0.
// The tag must be stripped before dereferencing any heap-pointer mode (a
// tagged value is misaligned and would not point at the real object). Single
// mode stores a clean InternedStringPtr directly, so the bag hands out a
// stable reference to it without masking.
//
// AddressSanitizer note: the tagged values point at most three bytes into the
// heap allocation they represent. ASAN's leak scanner uses interior-pointer
// detection and recognizes such pointers as references to the surrounding
// allocation, so the owning pointer is not hidden from the leak checker.
//
// Mode invariants in steady state:
//   Single -> 1 element
//   Array4 -> 2..4 elements
//   Array8 -> 5..8 elements
//   Set    -> 9+ elements
// Insert promotes one mode up when capacity is exceeded; erase demotes one
// mode down when count drops below the lower bound (single demotion step per
// erase, never cascades because erase removes only one element).
//
// In any array mode, elements occupy contiguous slots [0, count) with nulls
// in [count, capacity). size() and find() linear-scan the array. The user
// pays O(N) for N elements in the array, which is fine because N <= 8.
//
// This class works exclusively through the public InternedStringPtr API. It
// is not a friend of InternedStringPtr or its underlying string type. All
// ref-count traffic flows through InternedStringPtr's public copy/move
// constructors and destructor, invoked via placement new and explicit
// destruction on the storage slot (or, for the array/set modes, normal
// member-by-member construction/destruction inside the heap object).
//
// The bag is intentionally non-copyable and non-equality-comparable; only
// move construction and move assignment are provided.
//
class BagOfInternedStringPtrs {
 public:
  using value_type = InternedStringPtr;
  using key_type = InternedStringPtr;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;

  class const_iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = InternedStringPtr;
    using reference = const InternedStringPtr &;
    using pointer = const InternedStringPtr *;
    using difference_type = std::ptrdiff_t;

    const_iterator() = default;

    reference operator*() const {
      if (tag_ == Tag::kSingle) {
        return bag_->SingleRef();
      }
      if (tag_ == Tag::kArray) {
        return array_data_[array_idx_];
      }
      return *set_iter_;
    }
    pointer operator->() const {
      if (tag_ == Tag::kSingle) {
        return &bag_->SingleRef();
      }
      if (tag_ == Tag::kArray) {
        return &array_data_[array_idx_];
      }
      return set_iter_.operator->();
    }
    const_iterator &operator++() {
      if (tag_ == Tag::kSingle) {
        tag_ = Tag::kEnd;
      } else if (tag_ == Tag::kArray) {
        ++array_idx_;
        if (array_idx_ >= array_count_) {
          tag_ = Tag::kEnd;
        }
      } else if (tag_ == Tag::kSet) {
        ++set_iter_;
      }
      return *this;
    }
    const_iterator operator++(int) {
      const_iterator tmp(*this);
      ++(*this);
      return tmp;
    }
    bool operator==(const const_iterator &other) const {
      if (tag_ != other.tag_) {
        return false;
      }
      switch (tag_) {
        case Tag::kEnd:
          return true;
        case Tag::kSingle:
          return bag_ == other.bag_;
        case Tag::kArray:
          return bag_ == other.bag_ && array_idx_ == other.array_idx_;
        case Tag::kSet:
          return set_iter_ == other.set_iter_;
      }
      return false;
    }
    bool operator!=(const const_iterator &other) const {
      return !(*this == other);
    }

   private:
    friend class BagOfInternedStringPtrs;
    enum class Tag { kEnd, kSingle, kArray, kSet };

    // Single-mode constructor.
    const_iterator(Tag tag, const BagOfInternedStringPtrs *bag)
        : tag_(tag), bag_(bag) {}
    // Array-mode constructor.
    const_iterator(const BagOfInternedStringPtrs *bag,
                   const InternedStringPtr *data, std::size_t count,
                   std::size_t idx)
        : tag_(Tag::kArray),
          bag_(bag),
          array_data_(data),
          array_count_(count),
          array_idx_(idx) {}
    // Set-mode constructor.
    const_iterator(const BagOfInternedStringPtrs *bag,
                   InternedStringSet::const_iterator it)
        : tag_(Tag::kSet), bag_(bag), set_iter_(it) {}

    Tag tag_ = Tag::kEnd;
    const BagOfInternedStringPtrs *bag_ = nullptr;
    const InternedStringPtr *array_data_ = nullptr;
    std::size_t array_count_ = 0;
    std::size_t array_idx_ = 0;
    InternedStringSet::const_iterator set_iter_;
  };
  using iterator = const_iterator;
  using pointer = const InternedStringPtr *;
  using const_pointer = const InternedStringPtr *;
  using reference = const InternedStringPtr &;
  using const_reference = const InternedStringPtr &;

  BagOfInternedStringPtrs() = default;
  BagOfInternedStringPtrs(const BagOfInternedStringPtrs &) = delete;
  BagOfInternedStringPtrs &operator=(const BagOfInternedStringPtrs &) = delete;
  BagOfInternedStringPtrs(BagOfInternedStringPtrs &&other) noexcept
      : storage_(other.storage_) {
    other.storage_ = 0;
  }
  BagOfInternedStringPtrs &operator=(BagOfInternedStringPtrs &&other) noexcept {
    if (this != &other) {
      clear();
      storage_ = other.storage_;
      other.storage_ = 0;
    }
    return *this;
  }
  ~BagOfInternedStringPtrs() { clear(); }

  size_type size() const {
    switch (storage_ & kTagMask) {
      case kSingleTag:
        return storage_ == 0 ? 0 : 1;
      case kArray4Tag:
        return ArrayCount(*GetArray4());
      case kArray8Tag:
        return ArrayCount(*GetArray8());
      case kSetTag:
        return GetSet()->size();
    }
    return 0;
  }
  bool empty() const { return storage_ == 0; }

  void clear() {
    switch (storage_ & kTagMask) {
      case kSingleTag:
        if (storage_ != 0) {
          reinterpret_cast<InternedStringPtr *>(&storage_)
              ->~InternedStringPtr();
        }
        break;
      case kArray4Tag:
        delete GetArray4();
        break;
      case kArray8Tag:
        delete GetArray8();
        break;
      case kSetTag:
        delete GetSet();
        break;
    }
    storage_ = 0;
  }

  bool contains(const InternedStringPtr &key) const {
    switch (storage_ & kTagMask) {
      case kSingleTag:
        return storage_ != 0 && SingleRef() == key;
      case kArray4Tag:
        return ArrayFind(*GetArray4(), key) != kArray4Cap;
      case kArray8Tag:
        return ArrayFind(*GetArray8(), key) != kArray8Cap;
      case kSetTag:
        return GetSet()->contains(key);
    }
    return false;
  }

  const_iterator find(const InternedStringPtr &key) const {
    switch (storage_ & kTagMask) {
      case kSingleTag:
        if (storage_ != 0 && SingleRef() == key) {
          return const_iterator(const_iterator::Tag::kSingle, this);
        }
        return end();
      case kArray4Tag: {
        auto *arr = GetArray4();
        std::size_t idx = ArrayFind(*arr, key);
        if (idx == arr->size()) {
          return end();
        }
        return const_iterator(this, arr->data(), ArrayCount(*arr), idx);
      }
      case kArray8Tag: {
        auto *arr = GetArray8();
        std::size_t idx = ArrayFind(*arr, key);
        if (idx == arr->size()) {
          return end();
        }
        return const_iterator(this, arr->data(), ArrayCount(*arr), idx);
      }
      case kSetTag: {
        auto *set = GetSet();
        auto it = set->find(key);
        if (it == set->end()) {
          return end();
        }
        return const_iterator(this, it);
      }
    }
    return end();
  }

  std::pair<const_iterator, bool> insert(const InternedStringPtr &key) {
    return InsertImpl(key, /*is_rvalue=*/false);
  }
  std::pair<const_iterator, bool> insert(InternedStringPtr &&key) {
    return InsertImpl(std::move(key), /*is_rvalue=*/true);
  }

  size_type erase(const InternedStringPtr &key) {
    switch (storage_ & kTagMask) {
      case kSingleTag:
        if (storage_ != 0 && SingleRef() == key) {
          reinterpret_cast<InternedStringPtr *>(&storage_)
              ->~InternedStringPtr();
          storage_ = 0;
          return 1;
        }
        return 0;
      case kArray4Tag:
        return EraseFromArray4(key);
      case kArray8Tag:
        return EraseFromArray8(key);
      case kSetTag: {
        size_type n = GetSet()->erase(key);
        if (n > 0) {
          DemoteSetIfPossible();
        }
        return n;
      }
    }
    return 0;
  }

  // erase(iterator) follows flat_hash_set semantics: any outstanding iterator
  // (including pos) is invalidated. absl::flat_hash_set::erase(it) returns
  // void (it does not provide a "next" iterator like std::unordered_set), so
  // we mirror that here and always return end(). Callers that need to
  // continue iterating after an erase must restart from begin().
  const_iterator erase(const_iterator pos) {
    switch (pos.tag_) {
      case const_iterator::Tag::kSingle:
        reinterpret_cast<InternedStringPtr *>(&storage_)->~InternedStringPtr();
        storage_ = 0;
        return end();
      case const_iterator::Tag::kArray:
        if (IsArray4()) {
          ArrayEraseAt(*GetArray4(), pos.array_idx_);
          DemoteArray4IfNeeded();
        } else if (IsArray8()) {
          ArrayEraseAt(*GetArray8(), pos.array_idx_);
          DemoteArray8IfNeeded();
        }
        return end();
      case const_iterator::Tag::kSet:
        GetSet()->erase(pos.set_iter_);
        DemoteSetIfPossible();
        return end();
      case const_iterator::Tag::kEnd:
        return end();
    }
    return end();
  }

  const_iterator begin() const {
    switch (storage_ & kTagMask) {
      case kSingleTag:
        if (storage_ == 0) {
          return end();
        }
        return const_iterator(const_iterator::Tag::kSingle, this);
      case kArray4Tag: {
        auto *arr = GetArray4();
        std::size_t cnt = ArrayCount(*arr);
        if (cnt == 0) {
          return end();
        }
        return const_iterator(this, arr->data(), cnt, 0);
      }
      case kArray8Tag: {
        auto *arr = GetArray8();
        std::size_t cnt = ArrayCount(*arr);
        if (cnt == 0) {
          return end();
        }
        return const_iterator(this, arr->data(), cnt, 0);
      }
      case kSetTag:
        return const_iterator(this, GetSet()->begin());
    }
    return end();
  }
  const_iterator end() const {
    if (IsSet()) {
      return const_iterator(this, GetSet()->end());
    }
    return const_iterator();
  }
  const_iterator cbegin() const { return begin(); }
  const_iterator cend() const { return end(); }

  void swap(BagOfInternedStringPtrs &other) noexcept {
    std::swap(storage_, other.storage_);
  }

 private:
  static constexpr std::size_t kArray4Cap = 4;
  static constexpr std::size_t kArray8Cap = 8;
  using Array4 = std::array<InternedStringPtr, kArray4Cap>;
  using Array8 = std::array<InternedStringPtr, kArray8Cap>;

  static constexpr uintptr_t kTagMask = 0x3;
  static constexpr uintptr_t kSingleTag = 0;  // also used for empty
  static constexpr uintptr_t kArray4Tag = 1;
  static constexpr uintptr_t kArray8Tag = 2;
  static constexpr uintptr_t kSetTag = 3;

  uintptr_t storage_ = 0;

  bool IsEmpty() const { return storage_ == 0; }
  bool IsSingle() const {
    return storage_ != 0 && (storage_ & kTagMask) == kSingleTag;
  }
  bool IsArray4() const { return (storage_ & kTagMask) == kArray4Tag; }
  bool IsArray8() const { return (storage_ & kTagMask) == kArray8Tag; }
  bool IsSet() const { return (storage_ & kTagMask) == kSetTag; }

  // Stable reference to the bag's single-mode slot, viewed as an
  // InternedStringPtr. Precondition: IsSingle().
  const InternedStringPtr &SingleRef() const {
    return *reinterpret_cast<const InternedStringPtr *>(&storage_);
  }
  InternedStringPtr &SingleRefMut() {
    return *reinterpret_cast<InternedStringPtr *>(&storage_);
  }
  Array4 *GetArray4() const {
    return reinterpret_cast<Array4 *>(storage_ & ~kTagMask);
  }
  Array8 *GetArray8() const {
    return reinterpret_cast<Array8 *>(storage_ & ~kTagMask);
  }
  InternedStringSet *GetSet() const {
    return reinterpret_cast<InternedStringSet *>(storage_ & ~kTagMask);
  }
  void SetArray4(Array4 *p) {
    storage_ = reinterpret_cast<uintptr_t>(p) | kArray4Tag;
  }
  void SetArray8(Array8 *p) {
    storage_ = reinterpret_cast<uintptr_t>(p) | kArray8Tag;
  }
  void SetSet(InternedStringSet *p) {
    storage_ = reinterpret_cast<uintptr_t>(p) | kSetTag;
  }

  // Count non-null prefix of an array (since elements are collapsed to front).
  template <std::size_t N>
  static std::size_t ArrayCount(const std::array<InternedStringPtr, N> &arr) {
    std::size_t i = 0;
    while (i < N && arr[i]) {
      ++i;
    }
    return i;
  }
  // Linear-search an array for a key; returns N if not found.
  template <std::size_t N>
  static std::size_t ArrayFind(const std::array<InternedStringPtr, N> &arr,
                               const InternedStringPtr &key) {
    for (std::size_t i = 0; i < N && arr[i]; ++i) {
      if (arr[i] == key) {
        return i;
      }
    }
    return N;
  }
  // Erase the element at `idx`, shifting subsequent elements left to keep the
  // packed-from-zero invariant.
  template <std::size_t N>
  static void ArrayEraseAt(std::array<InternedStringPtr, N> &arr,
                           std::size_t idx) {
    std::size_t i = idx;
    while (i + 1 < N && arr[i + 1]) {
      arr[i] = std::move(arr[i + 1]);
      ++i;
    }
    arr[i] = nullptr;
  }

  template <typename Key>
  std::pair<const_iterator, bool> InsertImpl(Key &&key, bool is_rvalue) {
    switch (storage_ & kTagMask) {
      case kSingleTag:
        if (storage_ == 0) {
          if (is_rvalue) {
            new (&storage_) InternedStringPtr(std::move(key));
          } else {
            new (&storage_) InternedStringPtr(key);
          }
          return {const_iterator(const_iterator::Tag::kSingle, this), true};
        }
        if (SingleRef() == key) {
          return {const_iterator(const_iterator::Tag::kSingle, this), false};
        }
        // Single -> Array4: move the lone element to slot 0, key to slot 1.
        {
          auto *arr = new Array4();
          (*arr)[0] = std::move(SingleRefMut());
          if (is_rvalue) {
            (*arr)[1] = std::move(key);
          } else {
            (*arr)[1] = key;
          }
          SetArray4(arr);
          return {const_iterator(this, arr->data(), 2, 1), true};
        }
      case kArray4Tag:
        return InsertIntoArray4(std::forward<Key>(key), is_rvalue);
      case kArray8Tag:
        return InsertIntoArray8(std::forward<Key>(key), is_rvalue);
      case kSetTag: {
        auto *set = GetSet();
        if (is_rvalue) {
          auto [it, inserted] = set->insert(std::move(key));
          return {const_iterator(this, it), inserted};
        }
        auto [it, inserted] = set->insert(key);
        return {const_iterator(this, it), inserted};
      }
    }
    return {end(), false};
  }

  template <typename Key>
  std::pair<const_iterator, bool> InsertIntoArray4(Key &&key, bool is_rvalue) {
    auto *arr = GetArray4();
    for (std::size_t i = 0; i < kArray4Cap; ++i) {
      if (!(*arr)[i]) {
        if (is_rvalue) {
          (*arr)[i] = std::move(key);
        } else {
          (*arr)[i] = key;
        }
        return {const_iterator(this, arr->data(), i + 1, i), true};
      }
      if ((*arr)[i] == key) {
        return {const_iterator(this, arr->data(), ArrayCount(*arr), i), false};
      }
    }
    // Full and key not present: promote to Array8.
    auto *arr8 = new Array8();
    for (std::size_t i = 0; i < kArray4Cap; ++i) {
      (*arr8)[i] = std::move((*arr)[i]);
    }
    delete arr;
    if (is_rvalue) {
      (*arr8)[4] = std::move(key);
    } else {
      (*arr8)[4] = key;
    }
    SetArray8(arr8);
    return {const_iterator(this, arr8->data(), 5, 4), true};
  }

  template <typename Key>
  std::pair<const_iterator, bool> InsertIntoArray8(Key &&key, bool is_rvalue) {
    auto *arr = GetArray8();
    for (std::size_t i = 0; i < kArray8Cap; ++i) {
      if (!(*arr)[i]) {
        if (is_rvalue) {
          (*arr)[i] = std::move(key);
        } else {
          (*arr)[i] = key;
        }
        return {const_iterator(this, arr->data(), i + 1, i), true};
      }
      if ((*arr)[i] == key) {
        return {const_iterator(this, arr->data(), ArrayCount(*arr), i), false};
      }
    }
    // Full and key not present: promote to Set.
    auto *set = new InternedStringSet();
    for (std::size_t i = 0; i < kArray8Cap; ++i) {
      set->insert(std::move((*arr)[i]));
    }
    delete arr;
    InternedStringSet::const_iterator it;
    bool inserted;
    if (is_rvalue) {
      auto pr = set->insert(std::move(key));
      it = pr.first;
      inserted = pr.second;
    } else {
      auto pr = set->insert(key);
      it = pr.first;
      inserted = pr.second;
    }
    SetSet(set);
    return {const_iterator(this, it), inserted};
  }

  size_type EraseFromArray4(const InternedStringPtr &key) {
    auto *arr = GetArray4();
    std::size_t idx = ArrayFind(*arr, key);
    if (idx == kArray4Cap) {
      return 0;
    }
    ArrayEraseAt(*arr, idx);
    DemoteArray4IfNeeded();
    return 1;
  }
  size_type EraseFromArray8(const InternedStringPtr &key) {
    auto *arr = GetArray8();
    std::size_t idx = ArrayFind(*arr, key);
    if (idx == kArray8Cap) {
      return 0;
    }
    ArrayEraseAt(*arr, idx);
    DemoteArray8IfNeeded();
    return 1;
  }

  // Demote Array4 -> Single when count drops to 1, or -> Empty when 0.
  void DemoteArray4IfNeeded() {
    auto *arr = GetArray4();
    if (!(*arr)[0]) {
      delete arr;
      storage_ = 0;
      return;
    }
    if (!(*arr)[1]) {
      InternedStringPtr lone = std::move((*arr)[0]);
      delete arr;
      new (&storage_) InternedStringPtr(std::move(lone));
    }
  }
  // Demote Array8 -> Array4 when count drops to 4.
  void DemoteArray8IfNeeded() {
    auto *arr = GetArray8();
    if ((*arr)[4]) {
      return;  // still 5+ elements
    }
    auto *arr4 = new Array4();
    for (std::size_t i = 0; i < kArray4Cap && (*arr)[i]; ++i) {
      (*arr4)[i] = std::move((*arr)[i]);
    }
    delete arr;
    SetArray4(arr4);
  }
  // Demote Set -> Array8 when set size drops to 8.
  void DemoteSetIfPossible() {
    auto *set = GetSet();
    if (set->size() > kArray8Cap) {
      return;
    }
    auto *arr8 = new Array8();
    std::size_t i = 0;
    for (const auto &elem : *set) {
      (*arr8)[i++] = elem;  // copy bumps ref count
    }
    delete set;  // dec ref count for each element
    SetArray8(arr8);
  }
};
static_assert(sizeof(BagOfInternedStringPtrs) == 8,
              "BagOfInternedStringPtrs must fit in 8 bytes");

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

#endif  // VALKEYSEARCH_SRC_UTILS_STRING_INTERNING_H_
