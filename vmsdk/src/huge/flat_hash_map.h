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
#include "absl/container/flat_hash_set.h"
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

// Percent of a foreground's growth budget that its next growth cycle may start
// early. Written by the "huge-hash-map-start-jitter-percent" config; read
// without synchronization since it is only a tuning knob.
extern size_t g_huge_map_start_jitter_percent;

// Bumps the "huge_hash_map_rehashes" developer info field.
void CountHugeMapRehash();
long long GetHugeMapRehashCount();

// absl's load-factor rules, used to predict when absl would rehash on its own.
inline size_t HugeMapGrowth(size_t capacity) {
  return capacity == 0 ? 0
                       : absl::container_internal::CapacityToGrowth(capacity);
}

// How much budget to leave unused before a foreground of `growth` starts its
// next growth cycle. Successive calls walk a golden-ratio sequence over
// [0, jitter% of growth), so tables that fill in lockstep start their cycles,
// and finish draining them, at different sizes.
size_t HugeMapEarlyStart(size_t growth);

// The capacity absl picks for reserve(n) on an empty table.
inline size_t HugeMapCapacityFor(size_t n) {
  return absl::container_internal::NormalizeCapacity(
      absl::container_internal::GrowthToLowerboundCapacity(n));
}

}  // namespace internal

config::Number &GetHugeFlatHashMapMigrateBatch();
config::Number &GetHugeFlatHashMapStartJitterPercent();

struct HugeFlatHashMapTestPeer;

namespace internal {

// The shared implementation of HugeFlatHashMap and HugeFlatHashSet. `Table` is
// the absl::flat_hash_map or absl::flat_hash_set holding the entries.
//
// Two tables are kept. New entries always go to the foreground. When the
// foreground would have to grow (or has become mostly empty), it becomes the
// background and a new, correctly sized foreground is created. Each later
// insertion or erase then moves at least "huge-hash-map-migrate-batch"
// entries from the background to the foreground, so the cost of the rehash is
// spread across operations. Lookups check the foreground, then the
// background, and never move entries.
//
// Sizing follows absl's power-of-2 capacities: a new foreground is reserved
// for twice the live entries. A shrink starts once the live entries would fit
// in a table a quarter of the current size, which leaves 2x hysteresis.
// A growth cycle starts up to "huge-hash-map-start-jitter-percent" of the
// budget early, by a different amount for each cycle, so that many tables
// growing together don't all allocate (and later free) at the same moment.
//
// absl itself never rehashes: its private growth_left is mirrored
// conservatively (erases are treated as permanent tombstones) and the
// background is always drained before that budget runs out.
//
// As with absl, lookups (find, contains, count, at) preserve iterators,
// pointers and references. Unlike it, every insertion (including operator[]
// on an existing key) and every erase may move elements and so invalidates
// all of them. A background lingers until enough insertions or erases have
// drained it. Moving the container also invalidates its iterators; it moves
// no entries.
template <typename Table>
class HugeHashTable {
 public:
  using key_type = typename Table::key_type;
  using value_type = typename Table::value_type;
  using size_type = size_t;
  using hasher = typename Table::hasher;
  using key_equal = typename Table::key_equal;
  using allocator_type = typename Table::allocator_type;

  template <bool Const>
  class Iterator {
    using Owner =
        std::conditional_t<Const, const HugeHashTable *, HugeHashTable *>;
    using Inner = std::conditional_t<Const, typename Table::const_iterator,
                                     typename Table::iterator>;

   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = typename Table::value_type;
    using difference_type = std::ptrdiff_t;
    using reference = decltype(*std::declval<const Inner &>());
    using pointer = std::add_pointer_t<std::remove_reference_t<reference>>;

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
    // Iterators into different absl tables must not be compared.
    friend bool operator==(const Iterator &a, const Iterator &b) {
      return a.in_bg_ == b.in_bg_ && a.it_ == b.it_;
    }

   private:
    friend class HugeHashTable;
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

  HugeHashTable() = default;
  HugeHashTable(const HugeHashTable &) = delete;
  HugeHashTable &operator=(const HugeHashTable &) = delete;
  // absl does not promise that iterators survive a move, so the migration
  // cursor restarts at bg_.begin(). That is exact: migrated entries were
  // extracted, so everything left in bg_ is still to be migrated.
  HugeHashTable(HugeHashTable &&other) noexcept(
      std::is_nothrow_move_constructible_v<Table>)
      : fg_(std::move(other.fg_)),
        bg_(std::move(other.bg_)),
        fg_erased_(other.fg_erased_),
        early_start_(other.early_start_),
        cycles_(other.cycles_) {
    RestartCursor();
    other.clear();
  }
  HugeHashTable &operator=(HugeHashTable &&other) noexcept(
      std::is_nothrow_move_assignable_v<Table>) {
    if (this != &other) {
      fg_ = std::move(other.fg_);
      bg_ = std::move(other.bg_);
      fg_erased_ = other.fg_erased_;
      early_start_ = other.early_start_;
      cycles_ = other.cycles_;
      RestartCursor();
      other.clear();
    }
    return *this;
  }
  void swap(HugeHashTable &other) noexcept(
      std::is_nothrow_move_constructible_v<Table> &&
      std::is_nothrow_move_assignable_v<Table>) {
    HugeHashTable tmp(std::move(other));
    other = std::move(*this);
    *this = std::move(tmp);
  }
  friend void swap(HugeHashTable &a,
                   HugeHashTable &b) noexcept(noexcept(a.swap(b))) {
    a.swap(b);
  }

  size_t size() const { return fg_.size() + bg_.size(); }
  bool empty() const { return size() == 0; }

  void clear() {
    fg_ = Table();
    fg_erased_ = 0;
    early_start_ = 0;
    ResetBackground();
  }

  iterator begin() { return iterator(this, false, fg_.begin()); }
  iterator end() { return iterator(this, true, bg_.end()); }
  const_iterator begin() const {
    return const_iterator(this, false, fg_.begin());
  }
  const_iterator end() const { return const_iterator(this, true, bg_.end()); }

  // Like absl, these accept any type the hasher and key_equal are transparent
  // for.
  template <typename Q = key_type>
  iterator find(const Q &key) {
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
  template <typename Q = key_type>
  const_iterator find(const Q &key) const {
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

  template <typename Q = key_type>
  bool contains(const Q &key) const {
    return find(key) != end();
  }
  template <typename Q = key_type>
  size_t count(const Q &key) const {
    return contains(key) ? 1 : 0;
  }

  size_t erase(const key_type &key) {
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

 protected:
  std::pair<iterator, bool> Wrap(std::pair<typename Table::iterator, bool> r) {
    return {iterator(this, false, r.first), r.second};
  }

  // Called before an insertion of `key` into fg_.
  void PrepareInsert(const key_type &key) {
    if (bg_.empty()) {
      if (Budget() > early_start_ || fg_.contains(key)) {
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

  Table fg_;

 private:
  friend struct vmsdk::HugeFlatHashMapTestPeer;

  size_t EraseKey(const key_type &key) {
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

  // Inserts still allowed into fg_ before absl would rehash it, after every
  // background entry has been migrated.
  size_t Budget() const {
    size_t used = fg_.size() + fg_erased_ + bg_.size();
    size_t growth = internal::HugeMapGrowth(fg_.capacity());
    CHECK_LE(used, growth);
    return growth - used;
  }

  // Makes fg_ the background and replaces it with a table reserved for
  // `reserve` entries. A failed allocation leaves the table unchanged.
  void StartCycle(size_t reserve) {
    CHECK(bg_.empty());
    Table next;
    next.reserve(reserve);
    bg_ = std::move(fg_);
    fg_ = std::move(next);
    fg_erased_ = 0;
    early_start_ =
        internal::HugeMapEarlyStart(internal::HugeMapGrowth(fg_.capacity()));
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

  void MoveToForeground(typename Table::iterator it) {
    if (it == bg_it_) {
      ++bg_it_;
    }
    fg_.insert(bg_.extract(it));
    if (bg_.empty()) {
      ResetBackground();
    }
  }

  void EraseFromBackground(typename Table::iterator it) {
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
    bg_ = Table();
    bg_it_ = bg_.end();
  }

  void RestartCursor() { bg_it_ = bg_.empty() ? bg_.end() : bg_.begin(); }

  Table bg_;
  typename Table::iterator bg_it_ = bg_.end();
  size_t fg_erased_ = 0;
  // Budget left unused when the next growth cycle starts.
  size_t early_start_ = 0;
  size_t cycles_ = 0;
};

}  // namespace internal

// A hash map for very large tables that never stalls on a full rehash. See
// internal::HugeHashTable for how rehashing is spread out and which
// operations invalidate iterators.
template <typename K, typename V,
          typename Hash = typename absl::flat_hash_map<K, V>::hasher,
          typename Eq = typename absl::flat_hash_map<K, V>::key_equal,
          typename Alloc = HugeAllocator<std::pair<const K, V>>>
class HugeFlatHashMap : public internal::HugeHashTable<
                            absl::flat_hash_map<K, V, Hash, Eq, Alloc>> {
  using Base =
      internal::HugeHashTable<absl::flat_hash_map<K, V, Hash, Eq, Alloc>>;

 public:
  using mapped_type = V;
  using typename Base::iterator;
  using typename Base::value_type;

  V &at(const K &key) {
    auto it = this->find(key);
    if (it == this->end()) {
      throw std::out_of_range("vmsdk::HugeFlatHashMap::at");
    }
    return it->second;
  }
  const V &at(const K &key) const {
    auto it = this->find(key);
    if (it == this->end()) {
      throw std::out_of_range("vmsdk::HugeFlatHashMap::at");
    }
    return it->second;
  }

  template <typename... Args>
  std::pair<iterator, bool> try_emplace(const K &key, Args &&...args) {
    this->PrepareInsert(key);
    return this->Wrap(this->fg_.try_emplace(key, std::forward<Args>(args)...));
  }
  template <typename... Args>
  std::pair<iterator, bool> try_emplace(K &&key, Args &&...args) {
    this->PrepareInsert(key);
    return this->Wrap(
        this->fg_.try_emplace(std::move(key), std::forward<Args>(args)...));
  }

  std::pair<iterator, bool> insert(const value_type &value) {
    this->PrepareInsert(value.first);
    return this->Wrap(this->fg_.insert(value));
  }
  std::pair<iterator, bool> insert(value_type &&value) {
    this->PrepareInsert(value.first);
    return this->Wrap(this->fg_.insert(std::move(value)));
  }

  template <typename... Args>
  std::pair<iterator, bool> emplace(Args &&...args) {
    return insert(value_type(std::forward<Args>(args)...));
  }

  template <typename M>
  std::pair<iterator, bool> insert_or_assign(const K &key, M &&obj) {
    this->PrepareInsert(key);
    return this->Wrap(this->fg_.insert_or_assign(key, std::forward<M>(obj)));
  }

  V &operator[](const K &key) { return try_emplace(key).first->second; }
  V &operator[](K &&key) { return try_emplace(std::move(key)).first->second; }
};

// The set counterpart of HugeFlatHashMap, with the same rehash behavior.
template <typename T, typename Hash = typename absl::flat_hash_set<T>::hasher,
          typename Eq = typename absl::flat_hash_set<T>::key_equal,
          typename Alloc = HugeAllocator<T>>
class HugeFlatHashSet
    : public internal::HugeHashTable<absl::flat_hash_set<T, Hash, Eq, Alloc>> {
  using Base = internal::HugeHashTable<absl::flat_hash_set<T, Hash, Eq, Alloc>>;

 public:
  using typename Base::iterator;

  std::pair<iterator, bool> insert(const T &value) {
    this->PrepareInsert(value);
    return this->Wrap(this->fg_.insert(value));
  }
  std::pair<iterator, bool> insert(T &&value) {
    this->PrepareInsert(value);
    return this->Wrap(this->fg_.insert(std::move(value)));
  }
};

}  // namespace vmsdk

#endif  // VMSDK_SRC_HUGE_FLAT_HASH_MAP_H_
