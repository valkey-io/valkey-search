/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_HUGE_FLAT_HASH_MAP_H_
#define VMSDK_SRC_HUGE_FLAT_HASH_MAP_H_

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "vmsdk/src/huge/allocator.h"

namespace vmsdk {
namespace config {
class Number;
}  // namespace config

namespace internal {

// Entries migrated from the background to the foreground map per insertion or
// erase. Written by the "huge-hash-map-migrate-batch" config; read without
// synchronization since it is only a tuning knob.
extern size_t g_huge_map_migrate_batch;

// Bumps the "huge_hash_map_rehashes" developer info field.
void CountHugeMapRehash();
long long GetHugeMapRehashCount();

// absl's load-factor rules, used to predict when absl would rehash on its own.
inline size_t HugeMapGrowth(size_t capacity) {
  return capacity == 0 ? 0
                       : absl::container_internal::CapacityToGrowth(capacity);
}

// The capacity absl picks for reserve(n) on an empty table.
inline size_t HugeMapCapacityFor(size_t n) {
  return absl::container_internal::NormalizeCapacity(
      absl::container_internal::GrowthToLowerboundCapacity(n));
}

}  // namespace internal

config::Number &GetHugeFlatHashMapMigrateBatch();

struct HugeFlatHashMapTestPeer;

// A hash map for very large tables that never stalls on a full rehash.
//
// Two absl::flat_hash_maps are kept. New entries always go to the foreground.
// When the foreground would have to grow (or has become mostly empty), it
// becomes the background and a new, correctly sized foreground is created.
// Each later insertion or erase then moves at least
// "huge-hash-map-migrate-batch" entries from the background to the
// foreground, so the cost of the rehash is spread across operations. Lookups
// check the foreground, then the background, and never move entries.
//
// Sizing follows absl's power-of-2 capacities: a new foreground is reserved
// for twice the live entries. A shrink starts once the live entries would fit
// in a table a quarter of the current size, which leaves 2x hysteresis.
//
// absl itself never rehashes: its private growth_left is mirrored
// conservatively (erases are treated as permanent tombstones) and the
// background is always drained before that budget runs out.
//
// As with absl::flat_hash_map, lookups (find, contains, count, at) preserve
// iterators, pointers and references. Unlike it, every insertion (including
// operator[] on an existing key) and every erase may move elements and so
// invalidates all of them. A background lingers until enough insertions or
// erases have drained it.
template <typename K, typename V,
          typename Hash = typename absl::flat_hash_map<K, V>::hasher,
          typename Eq = typename absl::flat_hash_map<K, V>::key_equal,
          typename Alloc = HugeAllocator<std::pair<const K, V>>>
class HugeFlatHashMap {
  using Map = absl::flat_hash_map<K, V, Hash, Eq, Alloc>;

 public:
  using key_type = K;
  using mapped_type = V;
  using value_type = typename Map::value_type;
  using size_type = size_t;
  using hasher = Hash;
  using key_equal = Eq;
  using allocator_type = Alloc;

  template <bool Const>
  class Iterator {
    using Owner =
        std::conditional_t<Const, const HugeFlatHashMap *, HugeFlatHashMap *>;
    using Inner = std::conditional_t<Const, typename Map::const_iterator,
                                     typename Map::iterator>;

   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = typename Map::value_type;
    using difference_type = std::ptrdiff_t;
    using reference =
        std::conditional_t<Const, const value_type &, value_type &>;
    using pointer = std::conditional_t<Const, const value_type *, value_type *>;

    Iterator() = default;
    Iterator(const Iterator &) = default;
    Iterator &operator=(const Iterator &) = default;
    Iterator(
        const Iterator<false> &other)  // NOLINT: iterator -> const_iterator.
      requires Const
        : owner_(other.owner_), in_bg_(other.in_bg_), it_(other.it_) {}

    reference operator*() const { return *it_; }
    pointer operator->() const { return &*it_; }
    Iterator &operator++() {
      ++it_;
      SkipToBackground();
      return *this;
    }
    Iterator operator++(int) {
      Iterator tmp = *this;
      ++*this;
      return tmp;
    }
    // Iterators into different absl maps must not be compared.
    friend bool operator==(const Iterator &a, const Iterator &b) {
      return a.in_bg_ == b.in_bg_ && a.it_ == b.it_;
    }

   private:
    friend class HugeFlatHashMap;
    friend class Iterator<!Const>;

    Iterator(Owner owner, bool in_bg, Inner it)
        : owner_(owner), in_bg_(in_bg), it_(it) {
      SkipToBackground();
    }
    void SkipToBackground() {
      if (!in_bg_ && it_ == owner_->fg_.end()) {
        in_bg_ = true;
        it_ = owner_->bg_.begin();
      }
    }

    Owner owner_ = nullptr;
    bool in_bg_ = true;
    Inner it_;
  };

  using iterator = Iterator<false>;
  using const_iterator = Iterator<true>;

  HugeFlatHashMap() = default;
  // The migration cursor points into bg_, so copying and moving are not
  // supported.
  HugeFlatHashMap(const HugeFlatHashMap &) = delete;
  HugeFlatHashMap &operator=(const HugeFlatHashMap &) = delete;

  size_t size() const { return fg_.size() + bg_.size(); }
  bool empty() const { return size() == 0; }

  void clear() {
    fg_ = Map();
    fg_erased_ = 0;
    ResetBackground();
  }

  iterator begin() { return iterator(this, false, fg_.begin()); }
  iterator end() { return iterator(this, true, bg_.end()); }
  const_iterator begin() const {
    return const_iterator(this, false, fg_.begin());
  }
  const_iterator end() const { return const_iterator(this, true, bg_.end()); }

  iterator find(const K &key) {
    if (auto it = fg_.find(key); it != fg_.end()) {
      return iterator(this, false, it);
    }
    if (!bg_.empty()) {
      if (auto it = bg_.find(key); it != bg_.end()) {
        return iterator(this, true, it);
      }
    }
    return end();
  }
  const_iterator find(const K &key) const {
    if (auto it = fg_.find(key); it != fg_.end()) {
      return const_iterator(this, false, it);
    }
    if (!bg_.empty()) {
      if (auto it = bg_.find(key); it != bg_.end()) {
        return const_iterator(this, true, it);
      }
    }
    return end();
  }

  bool contains(const K &key) { return find(key) != end(); }
  bool contains(const K &key) const { return find(key) != end(); }
  size_t count(const K &key) { return contains(key) ? 1 : 0; }
  size_t count(const K &key) const { return contains(key) ? 1 : 0; }

  V &at(const K &key) {
    auto it = find(key);
    if (it == end()) {
      throw std::out_of_range("vmsdk::HugeFlatHashMap::at");
    }
    return it->second;
  }
  const V &at(const K &key) const {
    auto it = find(key);
    if (it == end()) {
      throw std::out_of_range("vmsdk::HugeFlatHashMap::at");
    }
    return it->second;
  }

  template <typename... Args>
  std::pair<iterator, bool> try_emplace(const K &key, Args &&...args) {
    PrepareInsert(key);
    return Wrap(fg_.try_emplace(key, std::forward<Args>(args)...));
  }
  template <typename... Args>
  std::pair<iterator, bool> try_emplace(K &&key, Args &&...args) {
    PrepareInsert(key);
    return Wrap(fg_.try_emplace(std::move(key), std::forward<Args>(args)...));
  }

  std::pair<iterator, bool> insert(const value_type &value) {
    PrepareInsert(value.first);
    return Wrap(fg_.insert(value));
  }
  std::pair<iterator, bool> insert(value_type &&value) {
    PrepareInsert(value.first);
    return Wrap(fg_.insert(std::move(value)));
  }

  template <typename... Args>
  std::pair<iterator, bool> emplace(Args &&...args) {
    return insert(value_type(std::forward<Args>(args)...));
  }

  template <typename M>
  std::pair<iterator, bool> insert_or_assign(const K &key, M &&obj) {
    PrepareInsert(key);
    return Wrap(fg_.insert_or_assign(key, std::forward<M>(obj)));
  }

  V &operator[](const K &key) { return try_emplace(key).first->second; }
  V &operator[](K &&key) { return try_emplace(std::move(key)).first->second; }

  size_t erase(const K &key) {
    size_t erased = EraseKey(key);
    Migrate(internal::g_huge_map_migrate_batch);
    return erased;
  }
  void erase(iterator pos) {
    if (pos.in_bg_) {
      EraseFromBackground(pos.it_);
    } else {
      fg_.erase(pos.it_);
      ErasedFromForeground();
    }
    Migrate(internal::g_huge_map_migrate_batch);
  }

 private:
  friend struct HugeFlatHashMapTestPeer;

  size_t EraseKey(const K &key) {
    if (fg_.erase(key) != 0) {
      ErasedFromForeground();
      return 1;
    }
    if (bg_.empty()) {
      return 0;
    }
    auto it = bg_.find(key);
    if (it == bg_.end()) {
      return 0;
    }
    EraseFromBackground(it);
    return 1;
  }

  std::pair<iterator, bool> Wrap(std::pair<typename Map::iterator, bool> r) {
    return {iterator(this, false, r.first), r.second};
  }

  // Inserts still allowed into fg_ before absl would rehash it, after every
  // background entry has been migrated.
  size_t Budget() const {
    size_t used = fg_.size() + fg_erased_ + bg_.size();
    size_t growth = internal::HugeMapGrowth(fg_.capacity());
    CHECK_LE(used, growth);
    return growth - used;
  }

  // Called before an insertion of `key` into fg_.
  void PrepareInsert(const K &key) {
    if (bg_.empty()) {
      if (Budget() > 0 || fg_.contains(key)) {
        return;
      }
      StartCycle(std::max<size_t>(2 * size(), 1));
      if (bg_.empty()) {
        return;
      }
    } else if (auto it = bg_.find(key); it != bg_.end()) {
      MoveToForeground(it);
    }
    // A cycle reserves fg_ for twice the live entries, so bg_ starts no larger
    // than the budget. Each insert, including the one that starts the cycle,
    // spends at most one unit of budget and migrates at least one entry, so
    // bg_ drains before the budget runs out.
    CHECK(bg_.size() <= Budget())
        << "bg=" << bg_.size() << " budget=" << Budget()
        << " capacity=" << fg_.capacity();
    Migrate(internal::g_huge_map_migrate_batch);
  }

  // Makes fg_ the background and replaces it with a table reserved for
  // `reserve` entries. A failed allocation leaves the map unchanged.
  void StartCycle(size_t reserve) {
    CHECK(bg_.empty());
    Map next;
    next.reserve(reserve);
    bg_ = std::move(fg_);
    fg_ = std::move(next);
    fg_erased_ = 0;
    ++cycles_;
    internal::CountHugeMapRehash();
    if (bg_.empty()) {
      ResetBackground();
    } else {
      bg_it_ = bg_.begin();
    }
  }

  void Migrate(size_t n) {
    if (bg_.empty()) {
      return;
    }
    while (n-- > 0 && bg_it_ != bg_.end()) {
      fg_.insert(bg_.extract(bg_it_++));
    }
    if (bg_it_ == bg_.end()) {
      CHECK(bg_.empty());
      ResetBackground();
    }
  }

  void MoveToForeground(typename Map::iterator it) {
    if (it == bg_it_) {
      ++bg_it_;
    }
    fg_.insert(bg_.extract(it));
    if (bg_.empty()) {
      ResetBackground();
    }
  }

  void EraseFromBackground(typename Map::iterator it) {
    if (it == bg_it_) {
      ++bg_it_;
    }
    bg_.erase(it);
    if (bg_.empty()) {
      ResetBackground();
    }
  }

  // Shrinks once the live entries would fit in a quarter-size table.
  void ErasedFromForeground() {
    ++fg_erased_;
    if (bg_.empty() && internal::HugeMapCapacityFor(
                           std::max<size_t>(4 * size(), 1)) < fg_.capacity()) {
      StartCycle(std::max<size_t>(2 * size(), 1));
    }
  }

  // Frees the drained background table.
  void ResetBackground() {
    bg_ = Map();
    bg_it_ = bg_.end();
  }

  Map fg_;
  Map bg_;
  typename Map::iterator bg_it_ = bg_.end();
  size_t fg_erased_ = 0;
  size_t cycles_ = 0;
};

}  // namespace vmsdk

#endif  // VMSDK_SRC_HUGE_FLAT_HASH_MAP_H_
