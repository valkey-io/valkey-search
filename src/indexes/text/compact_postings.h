/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_COMPACT_POSTINGS_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_COMPACT_POSTINGS_H_

#include <cstddef>
#include <cstdint>
#include <utility>

#include "absl/container/btree_map.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes::text {

//
// CompactPostings is a space-optimized ordered map from InternedStringPtr to a
// trivially-copyable value, tuned for the many-small / few-large distribution
// of a text index's per-term document lists. Following the tagged-pointer idea
// of BagOfInternedStringPtrs (PR #1026), the whole object is exactly 8 bytes
// and picks one of three heap representations from the bottom two bits of
// storage_:
//
//   storage_ == 0                -> empty
//   storage_ low 2 bits == 00    -> Single: heap Entry, one key/value pair
//   storage_ low 2 bits == 01    -> SmallVec: heap array of up to 4 entries,
//                                   sorted by key for binary search
//   storage_ low 2 bits == 11    -> Map: heap btree_map for 5+ entries
//
// Keys are ordered by interned-pointer identity (InternedStringPtrLess), the
// same order the btree mode uses, so iteration and SkipForward are consistent
// across modes and callers can probe with a borrowed key transparently.
//
// The tag bits are free because every heap mode is allocated via `new` with
// alignment 8; the tag must be stripped before dereferencing. Values are held
// by value and are not owned beyond their storage -- the caller owns any
// resources a value points at (e.g. FlatPositionMap) and must reclaim them on
// erase/destruction. The container is move-only, matching
// BagOfInternedStringPtrs.
//
template <typename Value>
class CompactPostings {
 public:
  using Key = InternedStringPtr;
  using MapType = absl::btree_map<Key, Value, InternedStringPtrLess>;

 private:
  struct Entry;

 public:
  CompactPostings() = default;
  ~CompactPostings() { Clear(); }

  CompactPostings(const CompactPostings&) = delete;
  CompactPostings& operator=(const CompactPostings&) = delete;
  CompactPostings(CompactPostings&& other) noexcept : storage_(other.storage_) {
    other.storage_ = 0;
  }
  CompactPostings& operator=(CompactPostings&& other) noexcept {
    if (this != &other) {
      Clear();
      storage_ = other.storage_;
      other.storage_ = 0;
    }
    return *this;
  }

  bool empty() const { return storage_ == 0; }
  size_t size() const;

  // Insert value under key. Returns false without modifying an existing entry,
  // matching btree_map::emplace semantics.
  bool Insert(const Key& key, const Value& value);

  // Copy the value for key into *out (if non-null) and remove it. Returns false
  // if the key is absent.
  bool Erase(const Key& key, Value* out);

  // Return a pointer to the value for key, or nullptr. Accepts an owning or a
  // borrowed key for transparent lookup.
  const Value* Find(const Key& key) const { return FindImpl(key); }
  const Value* Find(const BorrowedInternedStringPtr& key) const {
    return FindImpl(key);
  }

  // Ordered forward iterator over key/value pairs.
  class Iterator {
   public:
    Iterator() = default;
    bool IsValid() const;
    void Next();
    // Advance to the first key >= target; returns true if it equals target.
    bool SkipForward(const Key& key);
    const Key& GetKey() const;
    const Value& GetValue() const;

   private:
    friend class CompactPostings;
    enum class Mode { kEmpty, kSingle, kSmallVec, kMap };

    Mode mode_{Mode::kEmpty};
    const Entry* single_{nullptr};
    bool single_done_{false};
    const Entry* vec_data_{nullptr};
    size_t vec_count_{0};
    size_t vec_idx_{0};
    const MapType* map_{nullptr};
    typename MapType::const_iterator map_iter_;
    typename MapType::const_iterator map_end_;
  };

  Iterator GetIterator() const;

  // Test-only: the current representation, for white-box mode-transition tests.
  enum class TestMode { kEmpty, kSingle, kSmallVec, kMap };
  TestMode TestModeForTesting() const {
    if (storage_ == 0) return TestMode::kEmpty;
    switch (storage_ & kTagMask) {
      case kSmallVecTag:
        return TestMode::kSmallVec;
      case kMapTag:
        return TestMode::kMap;
      default:
        return TestMode::kSingle;
    }
  }

 private:
  struct Entry {
    Key key;
    Value value;
  };

  static constexpr size_t kSmallVecCap = 4;
  struct SmallVec {
    size_t count{0};
    Entry entries[kSmallVecCap];
  };

  static constexpr uintptr_t kTagMask = 0x3;
  static constexpr uintptr_t kSingleTag = 0;
  static constexpr uintptr_t kSmallVecTag = 1;
  static constexpr uintptr_t kMapTag = 3;

  bool IsSingle() const {
    return storage_ != 0 && (storage_ & kTagMask) == kSingleTag;
  }
  bool IsSmallVec() const { return (storage_ & kTagMask) == kSmallVecTag; }
  bool IsMap() const { return (storage_ & kTagMask) == kMapTag; }

  Entry* GetSingle() const {
    return reinterpret_cast<Entry*>(storage_ & ~kTagMask);
  }
  SmallVec* GetSmallVec() const {
    return reinterpret_cast<SmallVec*>(storage_ & ~kTagMask);
  }
  MapType* GetMap() const {
    return reinterpret_cast<MapType*>(storage_ & ~kTagMask);
  }
  void SetSingle(Entry* p) {
    storage_ = reinterpret_cast<uintptr_t>(p) | kSingleTag;
  }
  void SetSmallVec(SmallVec* p) {
    storage_ = reinterpret_cast<uintptr_t>(p) | kSmallVecTag;
  }
  void SetMap(MapType* p) {
    storage_ = reinterpret_cast<uintptr_t>(p) | kMapTag;
  }

  template <typename K>
  const Value* FindImpl(const K& key) const;

  // Index of the first SmallVec entry with key >= target.
  static size_t SmallVecLowerBound(const SmallVec* vec, const Key& key);
  void PromoteToMap(const Key& key, const Value& value);
  void DemoteToSmallVec();
  void Clear();

  uintptr_t storage_ = 0;
};

template <typename Value>
size_t CompactPostings<Value>::size() const {
  if (storage_ == 0) return 0;
  switch (storage_ & kTagMask) {
    case kSmallVecTag:
      return GetSmallVec()->count;
    case kMapTag:
      return GetMap()->size();
    default:
      return 1;
  }
}

template <typename Value>
bool CompactPostings<Value>::Insert(const Key& key, const Value& value) {
  InternedStringPtrLess less;
  if (storage_ == 0) {
    auto* single = new Entry{key, value};
    SetSingle(single);
    return true;
  }

  switch (storage_ & kTagMask) {
    case kSingleTag: {
      auto* single = GetSingle();
      if (RawInternedPtr(single->key) == RawInternedPtr(key)) return false;
      auto* vec = new SmallVec();
      if (less(key, single->key)) {
        vec->entries[0] = Entry{key, value};
        vec->entries[1] = std::move(*single);
      } else {
        vec->entries[0] = std::move(*single);
        vec->entries[1] = Entry{key, value};
      }
      vec->count = 2;
      delete single;
      SetSmallVec(vec);
      return true;
    }
    case kSmallVecTag: {
      auto* vec = GetSmallVec();
      size_t pos = SmallVecLowerBound(vec, key);
      if (pos < vec->count &&
          RawInternedPtr(vec->entries[pos].key) == RawInternedPtr(key)) {
        return false;
      }
      if (vec->count < kSmallVecCap) {
        for (size_t i = vec->count; i > pos; --i) {
          vec->entries[i] = std::move(vec->entries[i - 1]);
        }
        vec->entries[pos] = Entry{key, value};
        ++vec->count;
        return true;
      }
      PromoteToMap(key, value);
      return true;
    }
    case kMapTag:
      return GetMap()->emplace(key, value).second;
    default:
      return false;
  }
}

template <typename Value>
bool CompactPostings<Value>::Erase(const Key& key, Value* out) {
  if (storage_ == 0) return false;

  switch (storage_ & kTagMask) {
    case kSingleTag: {
      auto* single = GetSingle();
      if (RawInternedPtr(single->key) != RawInternedPtr(key)) return false;
      if (out) *out = single->value;
      delete single;
      storage_ = 0;
      return true;
    }
    case kSmallVecTag: {
      auto* vec = GetSmallVec();
      size_t pos = SmallVecLowerBound(vec, key);
      if (pos >= vec->count ||
          RawInternedPtr(vec->entries[pos].key) != RawInternedPtr(key)) {
        return false;
      }
      if (out) *out = vec->entries[pos].value;
      for (size_t i = pos; i + 1 < vec->count; ++i) {
        vec->entries[i] = std::move(vec->entries[i + 1]);
      }
      vec->entries[vec->count - 1] = Entry{};
      --vec->count;
      if (vec->count == 1) {
        auto* single = new Entry{std::move(vec->entries[0])};
        delete vec;
        SetSingle(single);
      }
      return true;
    }
    case kMapTag: {
      auto* map = GetMap();
      auto it = map->find(key);
      if (it == map->end()) return false;
      if (out) *out = it->second;
      map->erase(it);
      if (map->size() <= kSmallVecCap) DemoteToSmallVec();
      return true;
    }
    default:
      return false;
  }
}

template <typename Value>
template <typename K>
const Value* CompactPostings<Value>::FindImpl(const K& key) const {
  if (storage_ == 0) return nullptr;

  switch (storage_ & kTagMask) {
    case kSingleTag: {
      auto* single = GetSingle();
      return RawInternedPtr(single->key) == RawInternedPtr(key) ? &single->value
                                                                : nullptr;
    }
    case kSmallVecTag: {
      auto* vec = GetSmallVec();
      InternedStringPtrLess less;
      size_t lo = 0, hi = vec->count;
      while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (less(vec->entries[mid].key, key)) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      if (lo < vec->count &&
          RawInternedPtr(vec->entries[lo].key) == RawInternedPtr(key)) {
        return &vec->entries[lo].value;
      }
      return nullptr;
    }
    case kMapTag: {
      auto* map = GetMap();
      auto it = map->find(key);
      return it != map->end() ? &it->second : nullptr;
    }
    default:
      return nullptr;
  }
}

template <typename Value>
size_t CompactPostings<Value>::SmallVecLowerBound(const SmallVec* vec,
                                                  const Key& key) {
  InternedStringPtrLess less;
  size_t lo = 0, hi = vec->count;
  while (lo < hi) {
    size_t mid = lo + (hi - lo) / 2;
    if (less(vec->entries[mid].key, key)) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

template <typename Value>
void CompactPostings<Value>::PromoteToMap(const Key& key, const Value& value) {
  auto* vec = GetSmallVec();
  auto* map = new MapType();
  for (size_t i = 0; i < vec->count; ++i) {
    map->emplace(std::move(vec->entries[i].key), vec->entries[i].value);
  }
  map->emplace(key, value);
  delete vec;
  SetMap(map);
}

template <typename Value>
void CompactPostings<Value>::DemoteToSmallVec() {
  auto* map = GetMap();
  auto* vec = new SmallVec();
  for (auto& [k, v] : *map) {
    vec->entries[vec->count] = Entry{k, v};
    ++vec->count;
  }
  delete map;
  SetSmallVec(vec);
}

template <typename Value>
void CompactPostings<Value>::Clear() {
  if (storage_ == 0) return;
  switch (storage_ & kTagMask) {
    case kSmallVecTag:
      delete GetSmallVec();
      break;
    case kMapTag:
      delete GetMap();
      break;
    default:
      delete GetSingle();
      break;
  }
  storage_ = 0;
}

template <typename Value>
typename CompactPostings<Value>::Iterator CompactPostings<Value>::GetIterator()
    const {
  Iterator it;
  if (storage_ == 0) return it;

  switch (storage_ & kTagMask) {
    case kSmallVecTag: {
      auto* vec = GetSmallVec();
      it.mode_ = Iterator::Mode::kSmallVec;
      it.vec_data_ = vec->entries;
      it.vec_count_ = vec->count;
      break;
    }
    case kMapTag: {
      auto* map = GetMap();
      it.mode_ = Iterator::Mode::kMap;
      it.map_ = map;
      it.map_iter_ = map->begin();
      it.map_end_ = map->end();
      break;
    }
    default:
      it.mode_ = Iterator::Mode::kSingle;
      it.single_ = GetSingle();
      break;
  }
  return it;
}

template <typename Value>
bool CompactPostings<Value>::Iterator::IsValid() const {
  switch (mode_) {
    case Mode::kSingle:
      return !single_done_;
    case Mode::kSmallVec:
      return vec_idx_ < vec_count_;
    case Mode::kMap:
      return map_iter_ != map_end_;
    default:
      return false;
  }
}

template <typename Value>
void CompactPostings<Value>::Iterator::Next() {
  switch (mode_) {
    case Mode::kSingle:
      single_done_ = true;
      break;
    case Mode::kSmallVec:
      if (vec_idx_ < vec_count_) ++vec_idx_;
      break;
    case Mode::kMap:
      if (map_iter_ != map_end_) ++map_iter_;
      break;
    default:
      break;
  }
}

// Absolute lower_bound over the whole collection, matching the btree behaviour
// callers relied on: a fresh iterator can probe any key (membership test), and
// a merge join can gallop forward.
template <typename Value>
bool CompactPostings<Value>::Iterator::SkipForward(const Key& key) {
  InternedStringPtrLess less;
  switch (mode_) {
    case Mode::kSingle:
      single_done_ = less(single_->key, key);
      return !single_done_ &&
             RawInternedPtr(single_->key) == RawInternedPtr(key);
    case Mode::kSmallVec: {
      size_t lo = 0, hi = vec_count_;
      while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (less(vec_data_[mid].key, key)) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      vec_idx_ = lo;
      return vec_idx_ < vec_count_ &&
             RawInternedPtr(vec_data_[vec_idx_].key) == RawInternedPtr(key);
    }
    case Mode::kMap:
      map_iter_ = map_->lower_bound(key);
      return map_iter_ != map_end_ &&
             RawInternedPtr(map_iter_->first) == RawInternedPtr(key);
    default:
      return false;
  }
}

template <typename Value>
const typename CompactPostings<Value>::Key&
CompactPostings<Value>::Iterator::GetKey() const {
  switch (mode_) {
    case Mode::kSmallVec:
      return vec_data_[vec_idx_].key;
    case Mode::kMap:
      return map_iter_->first;
    default:
      return single_->key;
  }
}

template <typename Value>
const Value& CompactPostings<Value>::Iterator::GetValue() const {
  switch (mode_) {
    case Mode::kSmallVec:
      return vec_data_[vec_idx_].value;
    case Mode::kMap:
      return map_iter_->second;
    default:
      return single_->value;
  }
}

}  // namespace valkey_search::indexes::text

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_COMPACT_POSTINGS_H_
