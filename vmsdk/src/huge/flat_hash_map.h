/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_HUGE_FLAT_HASH_MAP_H_
#define VMSDK_SRC_HUGE_FLAT_HASH_MAP_H_

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <iterator>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/strings/str_cat.h"
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

// Entries the advisory MigrateSome() callers move per call: on the ingest and
// query paths, for an IndexSchema's db_key_info_ on those paths, in the writer
// tasks that ingest and queries schedule, and in server cron. Written by
// the "huge-hash-map-advisory-migrate-batch",
// "huge-hash-map-key-info-migrate-batch", "huge-hash-map-ingest-migrate-batch",
// "huge-hash-map-query-migrate-batch" and "huge-hash-map-cron-migrate-batch"
// configs; read without synchronization since they are only tuning knobs.
extern size_t g_huge_map_advisory_migrate_batch;
extern size_t g_huge_map_key_info_migrate_batch;
extern size_t g_huge_map_ingest_migrate_batch;
extern size_t g_huge_map_query_migrate_batch;
extern size_t g_huge_map_cron_migrate_batch;

// Bumps the "huge_hash_map_rehashes" developer info field.
void CountHugeMapRehash();
// Tracks the "huge_hash_maps_migrating" developer info field: the number of
// tables whose NeedsMigration() is true.
void CountHugeMapMigrating(bool started);
long long GetHugeMapsMigrating();
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
config::Number &GetHugeFlatHashMapAdvisoryMigrateBatch();
config::Number &GetHugeFlatHashMapCronMigrateBatch();
config::Number &GetHugeFlatHashMapKeyInfoMigrateBatch();
config::Number &GetHugeFlatHashMapIngestMigrateBatch();
config::Number &GetHugeFlatHashMapQueryMigrateBatch();

// The number of entries an advisory MigrateSome() call on the ingest or query
// path should move.
inline size_t HugeMapAdvisoryMigrateBatch() {
  return internal::g_huge_map_advisory_migrate_batch;
}

// The number of entries an advisory MigrateSome() call on an IndexSchema's
// db_key_info_ should move.
inline size_t HugeMapKeyInfoMigrateBatch() {
  return internal::g_huge_map_key_info_migrate_batch;
}

// The number of entries per map the writer task that ingest schedules should
// move.
inline size_t HugeMapIngestMigrateBatch() {
  return internal::g_huge_map_ingest_migrate_batch;
}

// Whether the advisory MigrateSome() callers (ingest, queries, server cron)
// should run at all: the HugeMapAdvisoryMigration controlled variable.
bool HugeMapAdvisoryMigrationEnabled();

// The number of entries per map the writer task that a query schedules should
// move.
inline size_t HugeMapQueryMigrateBatch() {
  return internal::g_huge_map_query_migrate_batch;
}

// The number of entries each server cron tick's advisory MigrateSome() call
// should move.
inline size_t HugeMapCronMigrateBatch() {
  return internal::g_huge_map_cron_migrate_batch;
}

struct HugeFlatHashMapTestPeer;

namespace internal {

// A key with its hash already computed, so an operation that probes both
// tables, or probes before inserting, hashes the caller's key only once.
template <typename Q>
struct Prehashed {
  const Q &key;
  size_t hash;
};

template <typename T>
struct IsPrehashed : std::false_type {};
template <typename Q>
struct IsPrehashed<Prehashed<Q>> : std::true_type {};

// The tables' hasher: the stored hash for a Prehashed key, the user's Hash for
// anything else (such as entries being migrated). Being transparent makes absl
// accept Prehashed keys in find, erase and lazy_emplace.
template <typename Hash>
struct PrehashedHash : Hash {
  using is_transparent = void;
  using Inner = Hash;

  template <typename Q>
  size_t operator()(const Prehashed<Q> &p) const {
    return p.hash;
  }
  template <typename T>
    requires(!IsPrehashed<T>::value)
  size_t operator()(const T &t) const {
    return static_cast<const Hash &>(*this)(t);
  }
};

// The tables' key_equal: the user's Eq, with Prehashed keys unwrapped.
template <typename Eq>
struct PrehashedEq : Eq {
  using is_transparent = void;
  using Inner = Eq;

  template <typename A, typename B>
  bool operator()(const A &a, const B &b) const {
    return static_cast<const Eq &>(*this)(Unwrap(a), Unwrap(b));
  }

 private:
  template <typename T>
  static const T &Unwrap(const T &t) {
    return t;
  }
  template <typename Q>
  static const Q &Unwrap(const Prehashed<Q> &p) {
    return p.key;
  }
};

// The shared implementation of HugeFlatHashMap and HugeFlatHashSet. `Table` is
// the absl::flat_hash_map or absl::flat_hash_set holding the entries, with the
// user's hasher and key_equal wrapped in PrehashedHash and PrehashedEq.
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
// background is always drained before that budget runs out. Every insert into
// the foreground uses CHECK to verify that absl has room for it.
//
// Owners may drain a background sooner, for example while traffic is
// read-only: NeedsMigration() is a lock-free test, safe from any thread, and
// MigrateSome(n) moves up to n entries.
//
// As with absl, lookups (find, contains, count, at) preserve iterators,
// pointers and references, and erase(iterator) invalidates only the erased
// element, so `m.erase(it++)` is safe. Unlike absl, every insertion
// (including operator[] on an existing key), erase(key) and MigrateSome() may
// move elements and so invalidate all of them. Moving the container also
// invalidates its iterators; it moves no entries.
//
// Each operation hashes the caller's key once and probes both tables, and the
// insert that follows, with that hash.
template <typename Table>
class HugeHashTable {
 public:
  using key_type = typename Table::key_type;
  using value_type = typename Table::value_type;
  using size_type = size_t;
  using hasher = typename Table::hasher::Inner;
  using key_equal = typename Table::key_equal::Inner;
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
  ~HugeHashTable() {
    if (needs_migration_.load(std::memory_order_relaxed)) {
      internal::CountHugeMapMigrating(false);
    }
  }
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
        cycles_(other.cycles_),
        bg_drained_(other.bg_drained_),
        shrink_pending_(other.shrink_pending_) {
    RestartCursor();
    UpdateNeedsMigration();
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
      bg_drained_ = other.bg_drained_;
      shrink_pending_ = other.shrink_pending_;
      RestartCursor();
      UpdateNeedsMigration();
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
    shrink_pending_ = false;
    ResetBackground();
  }

  // True while a background holds entries, or a drained background or a
  // shrink is waiting for MigrateSome(). Lock-free: may be called from any
  // thread while another thread mutates the container.
  bool NeedsMigration() const {
    return needs_migration_.load(std::memory_order_relaxed);
  }
  // Moves up to `n` background entries to the foreground, then frees a
  // drained background or starts a pending shrink. Invalidates iterators.
  void MigrateSome(size_t n) {
    Migrate(n);
    FinishDeferred();
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
    return Find(Prehash<LookupKey<Q>>(key));
  }
  template <typename Q = key_type>
  const_iterator find(const Q &key) const {
    return Find(Prehash<LookupKey<Q>>(key));
  }

  // For several operations on one key that should hash it only once:
  // Prehash(key) hashes it, and find() and insert() take the result without
  // hashing again. The result refers to `key`, which must outlive it.
  using prehashed_key = internal::Prehashed<key_type>;
  template <typename Q>
  internal::Prehashed<Q> Prehash(const Q &key) const {
    return {key, fg_.hash_function()(key)};
  }
  iterator find(const prehashed_key &key) { return Find(key); }
  const_iterator find(const prehashed_key &key) const { return Find(key); }

  template <typename Q = key_type>
  bool contains(const Q &key) const {
    return find(key) != end();
  }
  template <typename Q = key_type>
  size_t count(const Q &key) const {
    return contains(key) ? 1 : 0;
  }

  size_t erase(const key_type &key) {
    size_t erased = EraseKey(Prehash(key));
    Migrate(internal::g_huge_map_migrate_batch);
    FinishDeferred();
    return erased;
  }
  // Erases only the element at `pos`, as absl does: nothing migrates, no cycle
  // starts and no table is freed, so other iterators stay valid. A shrink or a
  // drained background this leaves behind waits for the next insert,
  // erase(key) or MigrateSome().
  void erase(iterator pos) {
    if (pos.in_bg_) {
      if (pos.it_ == bg_it_) {
        ++bg_it_;
      }
      bg_.erase(pos.it_);
      bg_drained_ = bg_.empty();
    } else {
      fg_.erase(pos.it_);
      ++fg_erased_;
      shrink_pending_ = shrink_pending_ || ShouldShrink();
    }
    UpdateNeedsMigration();
  }

 protected:
  // Inserts the entry for `key` into fg_ unless it is already present,
  // calling construct(ctor) to build it in place, where ctor is absl's
  // lazy_emplace constructor.
  template <typename F>
  std::pair<iterator, bool> Emplace(const internal::Prehashed<key_type> &key,
                                    F &&construct) {
    PrepareInsert(key);
    // A key already in fg_ adds nothing, so only a new key needs room.
    if (!ForegroundHasRoom()) {
      CHECK(fg_.contains(key)) << RoomReport();
    }
    bool inserted = false;
    auto it = fg_.lazy_emplace(key, [&](const auto &ctor) {
      construct(ctor);
      inserted = true;
    });
    return {iterator(this, false, it), inserted};
  }

  Table fg_;

 private:
  friend struct vmsdk::HugeFlatHashMapTestPeer;

  // Lookups with a key of another type convert it to key_type first unless
  // the user's hasher and key_equal are transparent, as absl does.
  static constexpr bool kTransparent =
      absl::container_internal::IsTransparent<hasher>::value &&
      absl::container_internal::IsTransparent<key_equal>::value;
  template <typename Q>
  using LookupKey = std::conditional_t<kTransparent, Q, key_type>;

  template <typename Q>
  iterator Find(const internal::Prehashed<Q> &key) {
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
  template <typename Q>
  const_iterator Find(const internal::Prehashed<Q> &key) const {
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

  // Called before an insertion of `key` into fg_.
  void PrepareInsert(const internal::Prehashed<key_type> &key) {
    if (bg_.empty()) {
      if (bg_drained_) {
        ResetBackground();
      }
      if (Budget() > early_start_ || fg_.contains(key)) {
        return;
      }
      StartCycle(std::max<size_t>(2 * size(), 1));
      if (bg_.empty()) {
        return;
      }
    } else if (auto it = bg_.find(key); it != bg_.end()) {
      MoveToForeground(it, key);
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

  size_t EraseKey(const internal::Prehashed<key_type> &key) {
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
    bg_drained_ = false;
    shrink_pending_ = false;
    early_start_ =
        internal::HugeMapEarlyStart(internal::HugeMapGrowth(fg_.capacity()));
    ++cycles_;
    internal::CountHugeMapRehash();
    if (bg_.empty()) {
      ResetBackground();
    } else {
      bg_it_ = bg_.begin();
      UpdateNeedsMigration();
    }
  }

  void Migrate(size_t n) {
    if (bg_.empty()) {
      return;
    }
    while (n-- > 0 && bg_it_ != bg_.end()) {
      CHECK(ForegroundHasRoom()) << RoomReport();
      fg_.insert(bg_.extract(bg_it_++));
    }
    if (bg_it_ == bg_.end()) {
      CHECK(bg_.empty());
      ResetBackground();
    }
  }

  // Moves the bg_ entry for `key` to fg_ without hashing it again.
  void MoveToForeground(typename Table::iterator it,
                        const internal::Prehashed<key_type> &key) {
    if (it == bg_it_) {
      ++bg_it_;
    }
    auto node = bg_.extract(it);
    CHECK(ForegroundHasRoom()) << RoomReport();
    fg_.lazy_emplace(key, [&](const auto &ctor) {
      if constexpr (requires { node.mapped(); }) {
        ctor(std::move(node.key()), std::move(node.mapped()));
      } else {
        ctor(std::move(node.value()));
      }
    });
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
  bool ShouldShrink() const {
    return bg_.empty() && internal::HugeMapCapacityFor(
                              std::max<size_t>(4 * size(), 1)) < fg_.capacity();
  }
  void ErasedFromForeground() {
    ++fg_erased_;
    if (ShouldShrink()) {
      StartCycle(std::max<size_t>(2 * size(), 1));
    }
  }

  // Completes what erase(iterator) deferred: frees a drained background, and
  // starts a shrink if one is still due.
  void FinishDeferred() {
    if (bg_drained_) {
      ResetBackground();
    }
    if (shrink_pending_ && bg_.empty()) {
      shrink_pending_ = false;
      if (ShouldShrink()) {
        StartCycle(std::max<size_t>(2 * size(), 1));
      }
    }
    UpdateNeedsMigration();
  }

  // Whether absl can add one more element to fg_ without rehashing. absl's
  // growth_left is CapacityToGrowth(capacity) - size - deleted, and
  // fg_erased_ is at least the number of deleted slots.
  bool ForegroundHasRoom() const {
    return fg_.size() + fg_erased_ < internal::HugeMapGrowth(fg_.capacity());
  }
  std::string RoomReport() const {
    return absl::StrCat("insert would make absl rehash: size=", fg_.size(),
                        " erased=", fg_erased_, " capacity=", fg_.capacity(),
                        " bg=", bg_.size());
  }

  // Frees the drained background table.
  void ResetBackground() {
    bg_ = Table();
    bg_it_ = bg_.end();
    bg_drained_ = false;
    UpdateNeedsMigration();
  }

  void UpdateNeedsMigration() {
    bool needs = !bg_.empty() || bg_drained_ || shrink_pending_;
    if (needs != needs_migration_.load(std::memory_order_relaxed)) {
      needs_migration_.store(needs, std::memory_order_relaxed);
      internal::CountHugeMapMigrating(needs);
    }
  }

  void RestartCursor() { bg_it_ = bg_.empty() ? bg_.end() : bg_.begin(); }

  Table bg_;
  typename Table::iterator bg_it_ = bg_.end();
  size_t fg_erased_ = 0;
  // Budget left unused when the next growth cycle starts.
  size_t early_start_ = 0;
  size_t cycles_ = 0;
  // erase(iterator) emptied bg_ but left it allocated.
  bool bg_drained_ = false;
  // erase(iterator) made the table eligible to shrink.
  bool shrink_pending_ = false;
  // Mirrors !bg_.empty() || bg_drained_ || shrink_pending_ for other threads.
  std::atomic<bool> needs_migration_{false};
};

}  // namespace internal

// A hash map for very large tables that never stalls on a full rehash. See
// internal::HugeHashTable for how rehashing is spread out and which
// operations invalidate iterators.
template <typename K, typename V,
          typename Hash = typename absl::flat_hash_map<K, V>::hasher,
          typename Eq = typename absl::flat_hash_map<K, V>::key_equal,
          typename Alloc = HugeAllocator<std::pair<const K, V>>>
class HugeFlatHashMap
    : public internal::HugeHashTable<
          absl::flat_hash_map<K, V, internal::PrehashedHash<Hash>,
                              internal::PrehashedEq<Eq>, Alloc>> {
  using Base = internal::HugeHashTable<absl::flat_hash_map<
      K, V, internal::PrehashedHash<Hash>, internal::PrehashedEq<Eq>, Alloc>>;

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
    return this->Emplace(this->Prehash(key), [&](const auto &ctor) {
      ctor(std::piecewise_construct, std::forward_as_tuple(key),
           std::forward_as_tuple(std::forward<Args>(args)...));
    });
  }
  template <typename... Args>
  std::pair<iterator, bool> try_emplace(K &&key, Args &&...args) {
    return this->Emplace(this->Prehash(key), [&](const auto &ctor) {
      ctor(std::piecewise_construct, std::forward_as_tuple(std::move(key)),
           std::forward_as_tuple(std::forward<Args>(args)...));
    });
  }

  std::pair<iterator, bool> insert(const value_type &value) {
    return this->Emplace(this->Prehash(value.first),
                         [&](const auto &ctor) { ctor(value); });
  }
  std::pair<iterator, bool> insert(value_type &&value) {
    return this->Emplace(this->Prehash(value.first),
                         [&](const auto &ctor) { ctor(std::move(value)); });
  }

  template <typename... Args>
  std::pair<iterator, bool> emplace(Args &&...args) {
    return insert(value_type(std::forward<Args>(args)...));
  }

  template <typename M>
  std::pair<iterator, bool> insert_or_assign(const K &key, M &&obj) {
    auto result = this->Emplace(this->Prehash(key), [&](const auto &ctor) {
      ctor(key, std::forward<M>(obj));
    });
    if (!result.second) {
      result.first->second = std::forward<M>(obj);
    }
    return result;
  }

  V &operator[](const K &key) { return try_emplace(key).first->second; }
  V &operator[](K &&key) { return try_emplace(std::move(key)).first->second; }
};

// The set counterpart of HugeFlatHashMap, with the same rehash behavior.
template <typename T, typename Hash = typename absl::flat_hash_set<T>::hasher,
          typename Eq = typename absl::flat_hash_set<T>::key_equal,
          typename Alloc = HugeAllocator<T>>
class HugeFlatHashSet
    : public internal::HugeHashTable<absl::flat_hash_set<
          T, internal::PrehashedHash<Hash>, internal::PrehashedEq<Eq>, Alloc>> {
  using Base = internal::HugeHashTable<absl::flat_hash_set<
      T, internal::PrehashedHash<Hash>, internal::PrehashedEq<Eq>, Alloc>>;

 public:
  using typename Base::iterator;

  std::pair<iterator, bool> insert(const T &value) {
    return this->Emplace(this->Prehash(value),
                         [&](const auto &ctor) { ctor(value); });
  }
  std::pair<iterator, bool> insert(T &&value) {
    return this->Emplace(this->Prehash(value),
                         [&](const auto &ctor) { ctor(std::move(value)); });
  }
  // Inserts `value`, equal to the key `key` was made from, without hashing.
  std::pair<iterator, bool> insert(const typename Base::prehashed_key &key,
                                   T &&value) {
    DCHECK(this->fg_.key_eq()(key.key, value));
    return this->Emplace(key,
                         [&](const auto &ctor) { ctor(std::move(value)); });
  }
};

}  // namespace vmsdk

#endif  // VMSDK_SRC_HUGE_FLAT_HASH_MAP_H_
