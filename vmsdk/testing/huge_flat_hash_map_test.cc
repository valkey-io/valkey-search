/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <chrono>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/huge/flat_hash_map.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

struct HugeFlatHashMapTestPeer {
  template <typename M>
  static auto &Fg(M &m) {
    return m.fg_;
  }
  template <typename M>
  static auto &Bg(M &m) {
    return m.bg_;
  }
  template <typename M>
  static auto &BgCursor(M &m) {
    return m.bg_it_;
  }
  template <typename M>
  static size_t Cycles(const M &m) {
    return m.cycles_;
  }
  template <typename M>
  static size_t &FgErased(M &m) {
    return m.fg_erased_;
  }
};

namespace {

using Peer = HugeFlatHashMapTestPeer;

// The smallest absl capacity that spans more than one probing group (16 slots
// with SSE2, 8 on ARM). The tests below size their tables so that each event
// they check happens at least twice on tables at least this large.
constexpr size_t kMultiGroup = 31;

// Counts backing-array allocations, so an absl-initiated grow is visible.
size_t g_allocations = 0;

template <typename T>
struct CountingAllocator : HugeAllocator<T> {
  using value_type = T;
  CountingAllocator() = default;
  template <typename U>
  CountingAllocator(const CountingAllocator<U> &) {}  // NOLINT
  T *allocate(size_t n) {
    ++g_allocations;
    return HugeAllocator<T>::allocate(n);
  }
};
template <typename T, typename U>
bool operator==(const CountingAllocator<T> &, const CountingAllocator<U> &) {
  return true;
}

using Map = HugeFlatHashMap<int64_t, int64_t>;
using CountedMap =
    HugeFlatHashMap<int64_t, int64_t,
                    absl::flat_hash_map<int64_t, int64_t>::hasher,
                    absl::flat_hash_map<int64_t, int64_t>::key_equal,
                    CountingAllocator<std::pair<const int64_t, int64_t>>>;
using Set = HugeFlatHashSet<int64_t>;
using CountedSet =
    HugeFlatHashSet<int64_t, absl::flat_hash_set<int64_t>::hasher,
                    absl::flat_hash_set<int64_t>::key_equal,
                    CountingAllocator<int64_t>>;

class FlatHashMapTest : public ValkeyTest {
 protected:
  void SetUp() override {
    ValkeyTest::SetUp();
    external_baseline_ = test_external_memory_bytes.load();
    // The tuning configs are Dev() configs.
    SetDebugMode(true);
  }
  void TearDown() override {
    SetBatch(GetHugeFlatHashMapMigrateBatch().GetDefaultValue());
    SetJitter(GetHugeFlatHashMapStartJitterPercent().GetDefaultValue());
    SetDebugMode(false);
    ValkeyTest::TearDown();
  }
  static void SetDebugMode(bool enabled) {
    auto args =
        ToValkeyStringVector(enabled ? "--debug-mode yes" : "--debug-mode no");
    VMSDK_EXPECT_OK(config::ModuleConfigManager::Instance().ParseAndLoadArgv(
        nullptr, args.data(), args.size()));
    for (auto *arg : args) {
      TestValkeyModule_FreeString(nullptr, arg);
    }
  }
  static void SetBatch(long long k) {
    VMSDK_EXPECT_OK(GetHugeFlatHashMapMigrateBatch().SetValue(k));
  }
  static void SetJitter(long long percent) {
    VMSDK_EXPECT_OK(GetHugeFlatHashMapStartJitterPercent().SetValue(percent));
  }

  // Bytes reported through Incr/DecrExternalMemory since SetUp.
  int64_t external() const {
    return test_external_memory_bytes.load() - external_baseline_;
  }
  int64_t external_baseline_{0};
};

// Inserts until a cycle leaves a non-empty background.
template <typename M>
int64_t FillUntilBackground(M &m, int64_t next = 0) {
  while (Peer::Bg(m).empty()) {
    m[next] = next;
    ++next;
  }
  return next;
}

TEST_F(FlatHashMapTest, BasicOperations) {
  Map m;
  EXPECT_TRUE(m.empty());
  EXPECT_TRUE(m.insert({1, 10}).second);
  EXPECT_FALSE(m.insert({1, 11}).second);
  EXPECT_EQ(m.at(1), 10);
  EXPECT_TRUE(m.try_emplace(2, 20).second);
  EXPECT_FALSE(m.insert_or_assign(2, 21).second);
  EXPECT_EQ(m[2], 21);
  m[3] = 30;
  EXPECT_TRUE(m.emplace(4, 40).second);
  EXPECT_EQ(m.size(), 4);
  EXPECT_TRUE(m.contains(3));
  EXPECT_EQ(m.count(5), 0);
  EXPECT_THROW(m.at(5), std::out_of_range);
  EXPECT_EQ(m.erase(3), 1);
  EXPECT_EQ(m.erase(3), 0);
  m.erase(m.find(4));
  EXPECT_EQ(m.size(), 2);
  m.clear();
  EXPECT_TRUE(m.empty());
  EXPECT_EQ(m.begin(), m.end());
}

TEST_F(FlatHashMapTest, InsertDoesNotOverwriteBackgroundKey) {
  SetBatch(1);
  Map m;
  FillUntilBackground(m);
  int64_t key = Peer::Bg(m).begin()->first;
  auto [it, inserted] = m.insert({key, -1});
  EXPECT_FALSE(inserted);
  EXPECT_EQ(it->second, key);
  EXPECT_TRUE(Peer::Fg(m).contains(key));
  EXPECT_FALSE(Peer::Bg(m).contains(key));
}

TEST_F(FlatHashMapTest, EraseElementUnderCursor) {
  SetBatch(1);
  Map m;
  int64_t n = FillUntilBackground(m);
  int64_t key = Peer::BgCursor(m)->first;
  EXPECT_EQ(m.erase(key), 1);
  // Draining must still visit every remaining entry. An erase migrates even
  // when the key is absent.
  while (!Peer::Bg(m).empty()) {
    m.erase(-1);
  }
  EXPECT_EQ(m.size(), n - 1);
  for (int64_t i = 0; i < n; ++i) {
    EXPECT_EQ(m.contains(i), i != key) << i;
  }
}

TEST_F(FlatHashMapTest, IterationCoversBothMaps) {
  SetBatch(1);
  Map m;
  int64_t n = FillUntilBackground(m);
  ASSERT_FALSE(Peer::Fg(m).empty());
  absl::flat_hash_set<int64_t> seen;
  for (const auto &[k, v] : m) {
    EXPECT_EQ(k, v);
    EXPECT_TRUE(seen.insert(k).second) << k;
  }
  EXPECT_EQ(seen.size(), n);
  const Map &cm = m;
  EXPECT_EQ(std::distance(cm.begin(), cm.end()), n);
}

TEST_F(FlatHashMapTest, LookupsDoNotMigrate) {
  SetBatch(1);
  Map m;
  int64_t n = FillUntilBackground(m);
  size_t bg = Peer::Bg(m).size();
  int64_t key = Peer::Bg(m).begin()->first;
  auto first = m.begin();
  auto found = m.find(key);
  const Map &cm = m;
  for (int64_t i = 0; i < n; ++i) {
    EXPECT_NE(m.find(i), m.end());
    EXPECT_NE(cm.find(i), cm.end());
    EXPECT_TRUE(m.contains(i));
    EXPECT_TRUE(cm.contains(i));
    EXPECT_EQ(m.count(i), 1);
    EXPECT_EQ(cm.count(i), 1);
    EXPECT_EQ(m.at(i), i);
    EXPECT_EQ(cm.at(i), i);
  }
  EXPECT_EQ(m.find(n), m.end());
  EXPECT_EQ(Peer::Bg(m).size(), bg);
  // Iterators taken before the lookups are still valid.
  EXPECT_EQ(found->first, key);
  EXPECT_EQ(std::distance(first, m.end()), n);
  // An insertion migrates.
  m[n] = n;
  EXPECT_LT(Peer::Bg(m).size(), bg);
}

TEST_F(FlatHashMapTest, EraseMigrates) {
  SetBatch(1);
  Map m;
  int64_t n = FillUntilBackground(m);
  size_t bg = Peer::Bg(m).size();
  EXPECT_EQ(m.erase(-1), 0);
  EXPECT_EQ(Peer::Bg(m).size(), bg - 1);
  // erase(iterator) only erases, as absl's does.
  m.erase(m.find(Peer::Fg(m).begin()->first));
  EXPECT_EQ(Peer::Bg(m).size(), bg - 1);
  EXPECT_EQ(m.size(), n - 1);
}

// Fails if absl rehashes fg_ on its own. A grow allocates outside a cycle; an
// in-place rehash moves elements, which `sampled` pointers detect.
int64_t KeyOf(const std::pair<const int64_t, int64_t> &entry) {
  return entry.first;
}
int64_t KeyOf(int64_t entry) { return entry; }

template <typename M>
class NoRehashChecker {
 public:
  explicit NoRehashChecker(M &m, size_t samples) : m_(m), samples_(samples) {
    Reset();
  }

  // Backgrounds of more than one probing group that this checker saw
  // non-empty and later saw drained.
  size_t Drains() const { return drains_; }

  void Check() {
    auto &fg = Peer::Fg(m_);
    auto &bg = Peer::Bg(m_);
    if (bg.empty()) {
      drains_ += draining_;
      draining_ = false;
    } else if (bg.capacity() >= kMultiGroup) {
      draining_ = true;
    }
    size_t cycles = Peer::Cycles(m_);
    if (cycles != cycles_) {
      ASSERT_EQ(cycles, cycles_ + 1);
      ASSERT_LE(g_allocations - allocations_, 1);
      Reset();
      return;
    }
    ASSERT_EQ(g_allocations, allocations_) << "absl grew the table";
    ASSERT_EQ(fg.capacity(), capacity_);
    for (auto it = sampled_.begin(); it != sampled_.end();) {
      auto found = fg.find(it->first);
      if (found == fg.end()) {
        sampled_.erase(it++);  // Erased by the test.
        continue;
      }
      ASSERT_EQ(&*found, it->second) << "absl rehashed in place";
      ++it;
    }
    for (auto &entry : fg) {
      if (sampled_.size() >= samples_) {
        break;
      }
      sampled_.try_emplace(KeyOf(entry), &entry);
    }
  }

 private:
  void Reset() {
    cycles_ = Peer::Cycles(m_);
    allocations_ = g_allocations;
    capacity_ = Peer::Fg(m_).capacity();
    sampled_.clear();
  }

  M &m_;
  size_t samples_;
  size_t drains_ = 0;
  bool draining_ = false;
  size_t cycles_;
  size_t allocations_;
  size_t capacity_;
  absl::flat_hash_map<int64_t, const void *> sampled_;
};

class FlatHashMapNoRehashTest : public FlatHashMapTest,
                                public ::testing::WithParamInterface<int> {
 protected:
  void SetUp() override {
    FlatHashMapTest::SetUp();
    if (absl::container_internal::SwisstableGenerationsEnabled()) {
      GTEST_SKIP() << "absl rehashes randomly in sanitizer builds";
    }
    SetBatch(GetParam());
  }
};

TEST_P(FlatHashMapNoRehashTest, GrowWithFullAddressCheck) {
  CountedMap m;
  NoRehashChecker checker(m, SIZE_MAX);
  for (int64_t i = 0; checker.Drains() < 2; ++i) {
    ASSERT_LT(i, 100000);
    m[i] = i;
    ASSERT_NO_FATAL_FAILURE(checker.Check()) << i;
    m.find(i / 2);
    ASSERT_NO_FATAL_FAILURE(checker.Check()) << i;
  }
}

// The capacity a table must grow to so that two shrinks, each to about a
// quarter, still leave a multi-group background of more than one batch.
constexpr size_t kTwoShrinks = 1023;

TEST_P(FlatHashMapNoRehashTest, GrowAndShrink) {
  CountedMap m;
  NoRehashChecker checker(m, 64);
  long long rehashes = internal::GetHugeMapRehashCount();

  int64_t peak = 0;
  for (; Peer::Fg(m).capacity() < kTwoShrinks || !Peer::Bg(m).empty(); ++peak) {
    m[peak] = peak;
    ASSERT_NO_FATAL_FAILURE(checker.Check()) << peak;
  }
  EXPECT_GE(checker.Drains(), 2);
  size_t grow_cycles = Peer::Cycles(m);
  EXPECT_EQ(internal::GetHugeMapRehashCount() - rehashes, grow_cycles);
  size_t peak_capacity = Peer::Fg(m).capacity();

  size_t drains = checker.Drains();
  int64_t erased = 0;
  while (Peer::Cycles(m) < grow_cycles + 2 || !Peer::Bg(m).empty()) {
    ASSERT_LT(erased, peak);
    EXPECT_EQ(m.erase(erased++), 1);
    ASSERT_NO_FATAL_FAILURE(checker.Check()) << erased;
  }
  EXPECT_GE(checker.Drains() - drains, 2);
  EXPECT_LE(Peer::Fg(m).capacity(), peak_capacity / 16);

  // Churn: insert/erase near a steady size accumulates tombstones.
  std::mt19937_64 rng(1);
  const int64_t range = 2 * static_cast<int64_t>(m.size());
  const size_t churn_cycles = Peer::Cycles(m);
  for (int64_t i = 0;
       Peer::Cycles(m) < churn_cycles + 2 || !Peer::Bg(m).empty(); ++i) {
    ASSERT_LT(i, 100000);
    int64_t key = static_cast<int64_t>(rng() % range);
    if (rng() % 2) {
      m[key] = key;
    } else {
      m.erase(key);
    }
    ASSERT_NO_FATAL_FAILURE(checker.Check()) << i;
  }
  EXPECT_EQ(internal::GetHugeMapRehashCount() - rehashes, Peer::Cycles(m));
}

// A sliding window of keys at steady size: every erase is followed by an
// insert of a new key, which leaves tombstones absl would eventually clean up
// with a rehash of the 127-slot table the window fills: in place at 93 (absl
// rehashes in place up to 25/32 full), by growing at 108.
TEST_P(FlatHashMapNoRehashTest, TombstoneChurn) {
  for (int64_t window : {93, 108}) {
    CountedMap m;
    NoRehashChecker checker(m, 64);
    size_t steady_cycles = 0;
    for (int64_t i = 0; i <= window || Peer::Cycles(m) < steady_cycles + 2 ||
                        !Peer::Bg(m).empty();
         ++i) {
      ASSERT_LT(i, 100000);
      if (i == window) {
        steady_cycles = Peer::Cycles(m);
      }
      m[i] = i;
      ASSERT_NO_FATAL_FAILURE(checker.Check()) << window << " " << i;
      if (i >= window) {
        EXPECT_EQ(m.erase(i - window), 1);
        ASSERT_NO_FATAL_FAILURE(checker.Check()) << window << " " << i;
      }
    }
    EXPECT_EQ(m.size(), window);
  }
}

// Deletes during a growth cycle leave the table nearly empty, then stop.
// Whatever finishes the drain -- server cron's MigrateSome() or inserts alone
// -- still shrinks the table, though no later erase tests for it.
TEST_P(FlatHashMapNoRehashTest, ShrinksWhenGrowthDrainsNearlyEmpty) {
  for (bool by_inserts : {false, true}) {
    SCOPED_TRACE(by_inserts ? "drained by inserts" : "drained by MigrateSome");
    CountedMap m;
    int64_t next = 0;
    // Large enough that the next growth cycle's background keeps several of
    // the 1 in 64 entries the erases below leave.
    for (; next < 256; ++next) {
      m[next] = next;
    }
    NoRehashChecker checker(m, 64);
    // Insert until the next growth cycle starts, so bg_ is nearly full.
    size_t cycles = Peer::Cycles(m);
    while (Peer::Cycles(m) == cycles) {
      ASSERT_LT(next, 1000000);
      m[next] = next;
      ++next;
      ASSERT_NO_FATAL_FAILURE(checker.Check()) << next;
    }
    ASSERT_FALSE(Peer::Bg(m).empty());
    const size_t grown_capacity = Peer::Fg(m).capacity();
    // Keep 1 entry in 64. erase(iterator) migrates nothing, so the growth
    // cycle is still draining.
    const int64_t ingested = next;
    for (auto it = m.begin(); it != m.end();) {
      if (it->first % 64 != 0) {
        m.erase(it++);
      } else {
        ++it;
      }
    }
    ASSERT_FALSE(Peer::Bg(m).empty());
    ASSERT_NO_FATAL_FAILURE(checker.Check());
    const size_t kept = m.size();

    // No erase from here on.
    while (m.NeedsMigration()) {
      ASSERT_LT(next, ingested + 100000);
      if (by_inserts) {
        m[next] = next;
        ++next;
      } else {
        m.MigrateSome(1);
      }
      ASSERT_NO_FATAL_FAILURE(checker.Check());
    }
    // The growth cycle, then a shrink.
    EXPECT_EQ(Peer::Cycles(m), cycles + 2);
    EXPECT_LT(Peer::Fg(m).capacity(), grown_capacity / 4);
    EXPECT_EQ(m.size(), kept + static_cast<size_t>(next - ingested));
    for (int64_t i = 0; i < next; ++i) {
      ASSERT_EQ(m.contains(i), i >= ingested || i % 64 == 0) << i;
    }
  }
}

// The reverse: deletes start a shrink, then ingest resumes. The shrink's
// fg_ is reserved for twice the live entries, so inserts drain bg_ before
// absl would have to rehash, and then grow the table again.
TEST_P(FlatHashMapNoRehashTest, IngestDuringShrink) {
  CountedMap m;
  // The shrink's background spans several groups and batches, and the ingest
  // below grows the shrunken table twice.
  constexpr int64_t kPeak = 256;
  for (int64_t i = 0; i < kPeak; ++i) {
    m[i] = i;
  }
  m.MigrateSome(SIZE_MAX);
  NoRehashChecker checker(m, 64);
  const size_t peak_capacity = Peer::Fg(m).capacity();
  const size_t cycles = Peer::Cycles(m);
  int64_t erased = 0;
  while (Peer::Cycles(m) == cycles) {
    ASSERT_LT(erased, kPeak);
    EXPECT_EQ(m.erase(erased++), 1);
    ASSERT_NO_FATAL_FAILURE(checker.Check()) << erased;
  }
  // A shrink started, and bg_ holds nearly every live entry.
  ASSERT_FALSE(Peer::Bg(m).empty());
  const size_t shrunk_capacity = Peer::Fg(m).capacity();
  ASSERT_LT(shrunk_capacity, peak_capacity / 2);

  // Ingest only, well past the shrunken table's capacity.
  for (int64_t i = kPeak; i < 2 * kPeak; ++i) {
    m[i] = i;
    ASSERT_NO_FATAL_FAILURE(checker.Check()) << i;
  }
  EXPECT_GE(Peer::Cycles(m), cycles + 3);  // The shrink, then two growths.
  EXPECT_GE(checker.Drains(), 2);
  EXPECT_GT(Peer::Fg(m).capacity(), shrunk_capacity);
  EXPECT_EQ(m.size(), static_cast<size_t>(2 * kPeak - erased));
  for (int64_t i = 0; i < 2 * kPeak; ++i) {
    ASSERT_EQ(m.contains(i), i >= erased) << i;
  }
}

// erase_and_migrate() is erase(key) without the second lookup: each call
// migrates a batch, so deletes alone drain a growth cycle and then shrink the
// table, with no erase(key), insert or MigrateSome().
TEST_P(FlatHashMapNoRehashTest, EraseAndMigrateDrainsAndShrinks) {
  CountedMap m;
  int64_t next = 0;
  for (; next < 256; ++next) {
    m[next] = next;
  }
  NoRehashChecker checker(m, 64);
  const size_t cycles = Peer::Cycles(m);
  while (Peer::Cycles(m) == cycles) {
    ASSERT_LT(next, 1000000);
    m[next] = next;
    ++next;
    ASSERT_NO_FATAL_FAILURE(checker.Check()) << next;
  }
  ASSERT_FALSE(Peer::Bg(m).empty());
  const size_t grown_capacity = Peer::Fg(m).capacity();
  // The insert that started the cycle put its key in fg_; erasing it
  // migrates one batch.
  const int64_t last = next - 1;
  size_t bg = Peer::Bg(m).size();
  m.erase_and_migrate(m.find(last));
  EXPECT_EQ(Peer::Bg(m).size(), bg - GetParam());
  ASSERT_NO_FATAL_FAILURE(checker.Check());

  // Keep 1 entry in 64.
  for (int64_t i = 0; i < last; ++i) {
    if (i % 64 == 0) {
      continue;
    }
    auto it = m.find(i);
    ASSERT_NE(it, m.end()) << i;
    m.erase_and_migrate(it);
    ASSERT_NO_FATAL_FAILURE(checker.Check()) << i;
  }
  // A later cycle can only start once the growth cycle's background drained.
  EXPECT_GE(Peer::Cycles(m), cycles + 2);
  EXPECT_LT(Peer::Fg(m).capacity(), grown_capacity / 4);
  for (int64_t i = 0; i < next; ++i) {
    ASSERT_EQ(m.contains(i), i < last && i % 64 == 0) << i;
  }
}

// Moves the map during two multi-group drains, then checks the drain it was
// moved in and the next one.
TEST_P(FlatHashMapNoRehashTest, MoveMidCycle) {
  CountedMap a;
  int64_t i = 0;
  size_t moved_cycles = 0;
  size_t last_moved_cycle = 0;
  for (; moved_cycles < 2; ++i) {
    ASSERT_LT(i, 100000);
    a[i] = i;
    if (!Peer::Bg(a).empty() && Peer::Bg(a).size() % 2 == 0) {
      if (Peer::Bg(a).capacity() >= kMultiGroup &&
          Peer::Cycles(a) != last_moved_cycle) {
        ++moved_cycles;
        last_moved_cycle = Peer::Cycles(a);
      }
      CountedMap b(std::move(a));
      a = std::move(b);
    }
  }
  ASSERT_FALSE(Peer::Bg(a).empty());
  NoRehashChecker checker(a, 64);
  while (checker.Drains() < 2) {
    ASSERT_LT(i, 200000);
    a[i] = i;
    ++i;
    ASSERT_NO_FATAL_FAILURE(checker.Check()) << i;
  }
  EXPECT_EQ(a.size(), i);
}

TEST_P(FlatHashMapNoRehashTest, SetGrowAndShrink) {
  CountedSet s;
  NoRehashChecker checker(s, 64);
  int64_t peak = 0;
  for (; Peer::Fg(s).capacity() < kTwoShrinks || !Peer::Bg(s).empty(); ++peak) {
    s.insert(peak);
    ASSERT_NO_FATAL_FAILURE(checker.Check()) << peak;
  }
  EXPECT_GE(checker.Drains(), 2);
  const size_t grow_cycles = Peer::Cycles(s);
  const size_t drains = checker.Drains();
  int64_t erased = 0;
  while (Peer::Cycles(s) < grow_cycles + 2 || !Peer::Bg(s).empty()) {
    ASSERT_LT(erased, peak);
    EXPECT_EQ(s.erase(erased++), 1);
    ASSERT_NO_FATAL_FAILURE(checker.Check()) << erased;
  }
  EXPECT_GE(checker.Drains() - drains, 2);
  EXPECT_EQ(s.size(), static_cast<size_t>(peak - erased));
}

INSTANTIATE_TEST_SUITE_P(MigrateBatch, FlatHashMapNoRehashTest,
                         ::testing::Values(1, 8));

// Fills two maps in lockstep and returns, for each, the size at which each
// growth cycle started.
std::pair<std::vector<size_t>, std::vector<size_t>> LockstepCycleStarts() {
  Map a;
  Map b;
  std::vector<size_t> starts[2];
  // Through the cycles of the 31- and 63-slot tables, the smallest whose
  // budget is large enough for the two maps' start offsets to differ.
  for (int64_t i = 0; i < 64; ++i) {
    int which = 0;
    for (Map *m : {&a, &b}) {
      size_t cycles = Peer::Cycles(*m);
      size_t growth = internal::HugeMapGrowth(Peer::Fg(*m).capacity());
      size_t capacity = Peer::Fg(*m).capacity();
      size_t size = m->size();
      (*m)[i] = i;
      if (Peer::Cycles(*m) != cycles && size > 0) {
        EXPECT_EQ(Peer::Fg(*m).capacity(), 2 * capacity + 1);
        EXPECT_LE(size, growth);
        EXPECT_GE(size, growth - growth / 4);
        starts[which].push_back(size);
      }
      ++which;
    }
  }
  return {starts[0], starts[1]};
}

TEST_F(FlatHashMapTest, StartJitterSpreadsCycles) {
  SetJitter(0);
  auto [a0, b0] = LockstepCycleStarts();
  EXPECT_EQ(a0, b0);

  SetJitter(25);
  auto [a, b] = LockstepCycleStarts();
  ASSERT_EQ(a.size(), a0.size());
  ASSERT_EQ(b.size(), a0.size());
  size_t differ = 0;
  for (size_t i = 0; i < a.size(); ++i) {
    EXPECT_LE(a[i], a0[i]);
    EXPECT_LE(b[i], a0[i]);
    differ += a[i] != b[i];
  }
  // Only the smallest tables have too little budget to jitter.
  EXPECT_GE(differ, a.size() - 4);
}

TEST_F(FlatHashMapTest, MatchesAbslUnderRandomOperations) {
  Map m;
  absl::flat_hash_map<int64_t, int64_t> ref;
  std::mt19937_64 rng(42);
  constexpr int kOps = 100000;
  for (int i = 0; i < kOps; ++i) {
    if (i % (kOps / 4) == 0) {
      SetBatch(std::vector<long long>{1, 8, 64, 2}[i / (kOps / 4)]);
    }
    // Key range shifts so the map both grows and shrinks.
    int64_t range = i < kOps / 2 ? 20000 : 500;
    int64_t key = static_cast<int64_t>(rng() % range);
    switch (rng() % 6) {
      case 0:
        ASSERT_EQ(m.insert({key, i}).second, ref.insert({key, i}).second);
        break;
      case 1:
        m[key] = i;
        ref[key] = i;
        break;
      case 2:
        ASSERT_EQ(m.insert_or_assign(key, i).second,
                  ref.insert_or_assign(key, i).second);
        break;
      case 3:
      case 4:
        ASSERT_EQ(m.erase(key), ref.erase(key));
        break;
      case 5: {
        auto it = m.find(key);
        auto rit = ref.find(key);
        ASSERT_EQ(it == m.end(), rit == ref.end());
        if (rit != ref.end()) {
          ASSERT_EQ(it->second, rit->second);
        }
        break;
      }
    }
    ASSERT_EQ(m.size(), ref.size());
  }
  size_t visited = 0;
  for (const auto &[k, v] : m) {
    ASSERT_EQ(ref.at(k), v);
    ++visited;
  }
  EXPECT_EQ(visited, ref.size());
}

// absl's default string hasher is transparent, so lookups take a string_view.
TEST_F(FlatHashMapTest, HeterogeneousLookup) {
  SetBatch(1);
  HugeFlatHashMap<std::string, int> m;
  int n = 0;
  while (Peer::Bg(m).empty()) {
    m[std::to_string(n)] = n;
    ++n;
  }
  for (int i = 0; i < n; ++i) {
    std::string key = std::to_string(i);
    absl::string_view view = key;
    ASSERT_NE(m.find(view), m.end()) << i;
    EXPECT_EQ(m.find(view)->second, i);
    EXPECT_TRUE(std::as_const(m).contains(view));
    EXPECT_EQ(m.count(view), 1);
  }
  EXPECT_FALSE(m.contains(absl::string_view("missing")));
}

// Counts how often each key is hashed. Not transparent, like most hashers.
absl::flat_hash_map<int64_t, int> g_hashes;
struct CountingHash {
  size_t operator()(int64_t key) const {
    ++g_hashes[key];
    return absl::Hash<int64_t>()(key);
  }
};
using HashCountedMap =
    HugeFlatHashMap<int64_t, int64_t, CountingHash, std::equal_to<int64_t>>;
using HashCountedSet =
    HugeFlatHashSet<int64_t, CountingHash, std::equal_to<int64_t>>;

// Runs `op` and returns how often it hashed `key`.
template <typename F>
int HashesOf(int64_t key, F op) {
  g_hashes.clear();
  op();
  return g_hashes[key];
}

// Fills until a cycle leaves a background of more than one probing group.
template <typename M>
int64_t FillPastSmallUntilBackground(M &m) {
  int64_t next = 0;
  for (; Peer::Bg(m).empty() || Peer::Bg(m).capacity() < kMultiGroup; ++next) {
    if constexpr (requires { m[next]; }) {
      m[next] = next;
    } else {
      m.insert(next);
    }
  }
  return next;
}

TEST_F(FlatHashMapTest, HashesInputOncePerOperation) {
  SetBatch(1);
  HashCountedMap m;
  int64_t next = FillPastSmallUntilBackground(m);
  auto bg_key = [&] { return Peer::Bg(m).begin()->first; };
  auto fg_key = [&] { return Peer::Fg(m).begin()->first; };

  int64_t k = fg_key();
  EXPECT_EQ(HashesOf(k, [&] { EXPECT_NE(m.find(k), m.end()); }), 1);
  k = bg_key();
  EXPECT_EQ(HashesOf(k, [&] { EXPECT_NE(m.find(k), m.end()); }), 1);
  EXPECT_EQ(HashesOf(-1, [&] { EXPECT_EQ(m.find(-1), m.end()); }), 1);
  EXPECT_EQ(HashesOf(-1, [&] { EXPECT_FALSE(m.contains(-1)); }), 1);
  k = bg_key();
  EXPECT_EQ(HashesOf(k, [&] { EXPECT_EQ(m.at(k), k); }), 1);

  k = fg_key();
  EXPECT_EQ(HashesOf(k, [&] { EXPECT_EQ(m.erase(k), 1); }), 1);
  k = bg_key();
  EXPECT_EQ(HashesOf(k, [&] { EXPECT_EQ(m.erase(k), 1); }), 1);
  EXPECT_EQ(HashesOf(-1, [&] { EXPECT_EQ(m.erase(-1), 0); }), 1);

  k = next++;
  EXPECT_EQ(HashesOf(k, [&] { EXPECT_TRUE(m.try_emplace(k, k).second); }), 1);
  k = bg_key();
  EXPECT_EQ(HashesOf(k, [&] { EXPECT_FALSE(m.try_emplace(k, 0).second); }), 1);
  EXPECT_TRUE(Peer::Fg(m).contains(k));
  EXPECT_EQ(m.at(k), k);
  k = next++;
  EXPECT_EQ(HashesOf(k, [&] { EXPECT_TRUE(m.insert({k, k}).second); }), 1);
  k = next++;
  EXPECT_EQ(HashesOf(k, [&] { EXPECT_TRUE(m.insert_or_assign(k, k).second); }),
            1);
  k = bg_key();
  EXPECT_EQ(
      HashesOf(k, [&] { EXPECT_FALSE(m.insert_or_assign(k, -k).second); }), 1);
  EXPECT_EQ(m.at(k), -k);
  k = bg_key();
  EXPECT_EQ(HashesOf(k, [&] { EXPECT_EQ(m[k], k); }), 1);
  ASSERT_FALSE(Peer::Bg(m).empty());

  HashCountedSet s;
  next = FillPastSmallUntilBackground(s);
  k = *Peer::Bg(s).begin();
  EXPECT_EQ(HashesOf(k, [&] { EXPECT_TRUE(s.contains(k)); }), 1);
  EXPECT_EQ(HashesOf(k, [&] { EXPECT_FALSE(s.insert(k).second); }), 1);
  EXPECT_TRUE(Peer::Fg(s).contains(k));
  k = next++;
  EXPECT_EQ(HashesOf(k, [&] { EXPECT_TRUE(s.insert(k).second); }), 1);

  // A prehashed key is hashed once for a find that misses and the insert.
  int64_t fresh = next;
  EXPECT_EQ(HashesOf(fresh,
                     [&] {
                       auto key = s.Prehash(fresh);
                       EXPECT_EQ(s.find(key), s.end());
                       EXPECT_TRUE(s.insert(key, int64_t{fresh}).second);
                       EXPECT_EQ(s.find(key), s.find(fresh));
                     }),
            2);  // Prehash, and the plain find(fresh).
  EXPECT_TRUE(s.contains(fresh));
  k = *Peer::Bg(s).begin();
  EXPECT_EQ(HashesOf(k, [&] { EXPECT_EQ(s.erase(k), 1); }), 1);
  ASSERT_FALSE(Peer::Bg(s).empty());
}

// Covers the inserts that start a cycle, where fg_ is probed before the
// insert, and the ones during a drain.
TEST_F(FlatHashMapTest, EveryInsertHashesOnce) {
  HashCountedMap m;
  // Through the cycles, and drains, of the 31- and 63-slot tables.
  for (int64_t k = 0; k < 64; ++k) {
    ASSERT_EQ(HashesOf(k, [&] { m[k] = k; }), 1) << k;
  }
  EXPECT_GT(Peer::Cycles(m), 5);
}

// A cycle start hashes nothing beyond the inserted key, and draining hashes
// each migrated entry once, for its slot in the new table.
TEST_F(FlatHashMapTest, MigrationHashesEachEntryOnce) {
  if (absl::container_internal::SwisstableGenerationsEnabled()) {
    GTEST_SKIP() << "absl rehashes randomly in sanitizer builds";
  }
  SetBatch(8);
  HashCountedMap m;
  int64_t next = 0;
  for (; next < 32; ++next) {
    m[next] = next;
  }
  // The insert that starts a cycle: its own key, plus the first batch.
  size_t cycles = Peer::Cycles(m);
  while (true) {
    size_t before = m.size();
    g_hashes.clear();
    m[next] = next;
    ++next;
    if (Peer::Cycles(m) != cycles) {
      ASSERT_EQ(m.size(), before + 1);
      break;
    }
  }
  ASSERT_FALSE(Peer::Bg(m).empty());
  EXPECT_EQ(g_hashes.size(), 1 + 8);
  for (const auto &[key, n] : g_hashes) {
    EXPECT_EQ(n, 1) << key;
  }
  // The rest of the drain.
  size_t migrated = Peer::Bg(m).size();
  g_hashes.clear();
  size_t inserts = 0;
  while (!Peer::Bg(m).empty()) {
    m[next] = next;
    ++next;
    ++inserts;
  }
  size_t total = 0;
  for (const auto &[key, n] : g_hashes) {
    EXPECT_EQ(n, 1) << key;
    total += n;
  }
  EXPECT_EQ(total, inserts + migrated);
}

TEST_F(FlatHashMapTest, SetBasicOperations) {
  Set s;
  EXPECT_TRUE(s.empty());
  EXPECT_TRUE(s.insert(1).second);
  EXPECT_FALSE(s.insert(1).second);
  int64_t two = 2;
  EXPECT_TRUE(s.insert(two).second);
  EXPECT_EQ(*s.insert(3).first, 3);
  EXPECT_EQ(s.size(), 3);
  EXPECT_TRUE(s.contains(2));
  EXPECT_EQ(s.count(4), 0);
  EXPECT_EQ(*s.find(3), 3);
  EXPECT_EQ(s.erase(2), 1);
  EXPECT_EQ(s.erase(2), 0);
  s.erase(s.find(3));
  EXPECT_EQ(s.size(), 1);
  s.clear();
  EXPECT_TRUE(s.empty());
  EXPECT_EQ(s.begin(), s.end());
}

TEST_F(FlatHashMapTest, SetMatchesAbslUnderRandomOperations) {
  Set s;
  absl::flat_hash_set<int64_t> ref;
  std::mt19937_64 rng(7);
  constexpr int kOps = 100000;
  for (int i = 0; i < kOps; ++i) {
    if (i % (kOps / 4) == 0) {
      SetBatch(std::vector<long long>{1, 8, 64, 2}[i / (kOps / 4)]);
    }
    int64_t range = i < kOps / 2 ? 20000 : 500;
    int64_t key = static_cast<int64_t>(rng() % range);
    switch (rng() % 4) {
      case 0:
      case 1:
        ASSERT_EQ(s.insert(key).second, ref.insert(key).second);
        break;
      case 2:
        ASSERT_EQ(s.erase(key), ref.erase(key));
        break;
      case 3:
        ASSERT_EQ(s.contains(key), ref.contains(key));
        break;
    }
    ASSERT_EQ(s.size(), ref.size());
  }
  size_t visited = 0;
  for (int64_t k : s) {
    ASSERT_TRUE(ref.contains(k));
    ++visited;
  }
  EXPECT_EQ(visited, ref.size());
}

// Moves a map in the middle of a drain, then finishes the drain.
TEST_F(FlatHashMapTest, MoveDuringDrain) {
  SetBatch(1);
  Map a;
  int64_t n = FillUntilBackground(a);
  a[n] = n;
  ++n;
  ASSERT_FALSE(Peer::Bg(a).empty());

  Map b(std::move(a));
  EXPECT_TRUE(a.empty());
  EXPECT_TRUE(Peer::Bg(a).empty());
  EXPECT_EQ(b.size(), n);
  ASSERT_FALSE(Peer::Bg(b).empty());

  Map c;
  c[-1] = -1;
  c = std::move(b);
  EXPECT_TRUE(b.empty());
  EXPECT_EQ(c.size(), n);

  // The moved-from maps are usable.
  a[1] = 1;
  b[2] = 2;
  EXPECT_EQ(a.size(), 1);
  EXPECT_EQ(b.size(), 1);

  while (!Peer::Bg(c).empty()) {
    c[n] = n;
    ++n;
  }
  EXPECT_EQ(c.size(), n);
  for (int64_t i = 0; i < n; ++i) {
    ASSERT_EQ(c.at(i), i);
  }
  EXPECT_EQ(std::distance(c.begin(), c.end()), n);
}

TEST_F(FlatHashMapTest, SwapWithActiveBackgrounds) {
  SetBatch(1);
  Map a;
  Map b;
  int64_t na = FillUntilBackground(a);
  int64_t nb = FillUntilBackground(b, 1000000) - 1000000;
  ASSERT_FALSE(Peer::Bg(a).empty());
  ASSERT_FALSE(Peer::Bg(b).empty());
  std::swap(a, b);
  EXPECT_EQ(a.size(), nb);
  EXPECT_EQ(b.size(), na);
  using std::swap;
  swap(a, b);
  EXPECT_EQ(a.size(), na);
  EXPECT_EQ(b.size(), nb);
  while (!Peer::Bg(a).empty()) {
    a.erase(-1);
  }
  while (!Peer::Bg(b).empty()) {
    b.erase(-1);
  }
  for (int64_t i = 0; i < na; ++i) {
    ASSERT_TRUE(a.contains(i));
  }
  for (int64_t i = 0; i < nb; ++i) {
    ASSERT_TRUE(b.contains(1000000 + i));
  }
}

TEST_F(FlatHashMapTest, SetMoveDuringDrain) {
  SetBatch(1);
  Set a;
  int64_t n = 0;
  while (Peer::Bg(a).empty()) {
    a.insert(n++);
  }
  Set b(std::move(a));
  EXPECT_TRUE(a.empty());
  while (!Peer::Bg(b).empty()) {
    b.insert(n++);
  }
  EXPECT_EQ(b.size(), n);
  for (int64_t i = 0; i < n; ++i) {
    ASSERT_TRUE(b.contains(i));
  }
}

TEST_F(FlatHashMapTest, CapacityHelpersMatchAbsl) {
  std::vector<size_t> sizes;
  for (size_t n = 1; n <= 5000; ++n) {
    sizes.push_back(n);
  }
  for (size_t p = 8192; p <= (size_t{1} << 20); p *= 2) {
    for (size_t n : {p * 7 / 8 - 1, p * 7 / 8, p * 7 / 8 + 1}) {
      sizes.push_back(n);
    }
  }
  for (size_t n : sizes) {
    absl::flat_hash_map<int64_t, int64_t> m;
    m.reserve(n);
    ASSERT_EQ(internal::HugeMapCapacityFor(n), m.capacity()) << n;
    ASSERT_GE(internal::HugeMapGrowth(m.capacity()), n) << n;
  }
  // Reserving twice a full table's entries is exactly absl's doubling.
  for (size_t c = 15; c < (size_t{1} << 30); c = 2 * c + 1) {
    ASSERT_EQ(internal::HugeMapGrowth(2 * c + 1),
              2 * internal::HugeMapGrowth(c))
        << c;
    ASSERT_EQ(internal::HugeMapCapacityFor(2 * internal::HugeMapGrowth(c)),
              2 * c + 1)
        << c;
  }
}

TEST_F(FlatHashMapTest, LargeMapUsesHugePages) {
  {
    Map m;
    // Past the drain of the first table of at least 2MB (131071 slots) into
    // the second, so huge-page regions are allocated twice and freed once.
    for (int64_t i = 0; i < (1 << 17); ++i) {
      m[i] = i;
    }
    EXPECT_GT(external(), 0);
  }
  EXPECT_EQ(external(), 0);
}

TEST_F(FlatHashMapTest, NeedsMigrationLifecycle) {
  SetBatch(1);
  const long long migrating = internal::GetHugeMapsMigrating();
  Map m;
  EXPECT_FALSE(m.NeedsMigration());
  int64_t next = FillPastSmallUntilBackground(m);
  EXPECT_TRUE(m.NeedsMigration());
  EXPECT_EQ(internal::GetHugeMapsMigrating(), migrating + 1);

  Map moved(std::move(m));
  EXPECT_TRUE(moved.NeedsMigration());
  EXPECT_FALSE(m.NeedsMigration());  // NOLINT: moved-from is cleared.
  EXPECT_EQ(internal::GetHugeMapsMigrating(), migrating + 1);
  Map other;
  other[-1] = -1;
  swap(moved, other);
  EXPECT_FALSE(moved.NeedsMigration());
  EXPECT_TRUE(other.NeedsMigration());
  EXPECT_EQ(internal::GetHugeMapsMigrating(), migrating + 1);

  size_t size = other.size();
  other.MigrateSome(SIZE_MAX);
  EXPECT_FALSE(other.NeedsMigration());
  EXPECT_EQ(internal::GetHugeMapsMigrating(), migrating);
  EXPECT_TRUE(Peer::Bg(other).empty());
  EXPECT_EQ(other.size(), size);
  for (int64_t i = 0; i < next; ++i) {
    ASSERT_TRUE(other.contains(i)) << i;
  }

  FillUntilBackground(other, next);
  EXPECT_TRUE(other.NeedsMigration());
  other.clear();
  EXPECT_FALSE(other.NeedsMigration());
  EXPECT_EQ(internal::GetHugeMapsMigrating(), migrating);

  // A table destroyed mid-migration no longer counts.
  {
    Map doomed;
    FillPastSmallUntilBackground(doomed);
    EXPECT_EQ(internal::GetHugeMapsMigrating(), migrating + 1);
  }
  EXPECT_EQ(internal::GetHugeMapsMigrating(), migrating);
}

TEST_F(FlatHashMapTest, MigrateSomeMovesAndHashesEachEntryOnce) {
  SetBatch(1);
  HashCountedMap m;
  FillPastSmallUntilBackground(m);
  size_t bg = Peer::Bg(m).size();
  ASSERT_GT(bg, 5);
  g_hashes.clear();
  m.MigrateSome(5);
  EXPECT_EQ(Peer::Bg(m).size(), bg - 5);
  EXPECT_EQ(g_hashes.size(), 5);
  for (const auto &[key, n] : g_hashes) {
    EXPECT_EQ(n, 1) << key;
    EXPECT_TRUE(Peer::Fg(m).contains(key)) << key;
  }
  EXPECT_TRUE(m.NeedsMigration());
  m.MigrateSome(bg);
  EXPECT_TRUE(Peer::Bg(m).empty());
  EXPECT_FALSE(m.NeedsMigration());
}

// erase(it++) must visit every element once and keep the rest intact, from
// a table with no background, mid-drain, and across a shrink.
TEST_F(FlatHashMapTest, EraseWhileIterating) {
  enum class Start { kNoBackground, kMidDrain };
  for (Start start : {Start::kNoBackground, Start::kMidDrain}) {
    for (bool erase_all : {false, true}) {
      SCOPED_TRACE(absl::StrCat("mid_drain=", start == Start::kMidDrain,
                                " erase_all=", erase_all));
      SetBatch(1);
      Map m;
      int64_t n = 0;
      if (start == Start::kMidDrain) {
        n = FillPastSmallUntilBackground(m);
        ASSERT_FALSE(Peer::Bg(m).empty());
      } else {
        for (; n < 64; ++n) {
          m[n] = n;
        }
        m.MigrateSome(SIZE_MAX);
        ASSERT_TRUE(Peer::Bg(m).empty());
      }
      size_t cycles = Peer::Cycles(m);
      absl::flat_hash_map<int64_t, int> visits;
      for (auto it = m.begin(); it != m.end();) {
        int64_t key = it->first;
        ++visits[key];
        if (erase_all || key % 2 == 0) {
          m.erase(it++);
        } else {
          ++it;
        }
      }
      EXPECT_EQ(Peer::Cycles(m), cycles) << "erase(iterator) started a cycle";
      ASSERT_EQ(visits.size(), n);
      for (const auto &[key, count] : visits) {
        ASSERT_EQ(count, 1) << key;
      }
      for (int64_t i = 0; i < n; ++i) {
        ASSERT_EQ(m.contains(i), !erase_all && i % 2 != 0) << i;
      }
      EXPECT_EQ(m.size(), erase_all ? 0 : n / 2);
      // Erasing everything leaves a shrink or a drained background pending.
      if (erase_all) {
        EXPECT_TRUE(m.NeedsMigration());
      }
      m.MigrateSome(SIZE_MAX);
      if (erase_all) {
        EXPECT_FALSE(m.NeedsMigration());
        EXPECT_TRUE(Peer::Bg(m).empty());
      }
      for (int64_t i = 0; i < n; ++i) {
        ASSERT_EQ(m.contains(i), !erase_all && i % 2 != 0) << i;
      }
    }
  }
}

TEST_F(FlatHashMapTest, EraseIteratorDefersShrinkToMigrateSome) {
  Map m;
  // A 1023-slot table: a sixteenth of it still exceeds the shrink's.
  for (int64_t i = 0; i < 512; ++i) {
    m[i] = i;
  }
  m.MigrateSome(SIZE_MAX);
  size_t capacity = Peer::Fg(m).capacity();
  size_t cycles = Peer::Cycles(m);
  while (m.size() > 10) {
    m.erase(m.begin());
  }
  EXPECT_EQ(Peer::Cycles(m), cycles);
  EXPECT_EQ(Peer::Fg(m).capacity(), capacity);
  EXPECT_TRUE(m.NeedsMigration());
  m.MigrateSome(SIZE_MAX);
  EXPECT_EQ(Peer::Cycles(m), cycles + 1);
  EXPECT_LT(Peer::Fg(m).capacity(), capacity / 16);
  m.MigrateSome(SIZE_MAX);
  EXPECT_FALSE(m.NeedsMigration());
  EXPECT_EQ(m.size(), 10);
}

// An insert, too, starts the shrink that erase(iterator) deferred, so the
// table stops reporting NeedsMigration() without any MigrateSome() call.
TEST_F(FlatHashMapTest, EraseIteratorDefersShrinkToInsert) {
  Map m;
  // A 1023-slot table: a sixteenth of it still exceeds the shrink's.
  for (int64_t i = 0; i < 512; ++i) {
    m[i] = i;
  }
  m.MigrateSome(SIZE_MAX);
  size_t capacity = Peer::Fg(m).capacity();
  size_t cycles = Peer::Cycles(m);
  while (m.size() > 10) {
    m.erase(m.begin());
  }
  ASSERT_TRUE(m.NeedsMigration());
  ASSERT_EQ(Peer::Cycles(m), cycles);
  m[-1] = -1;
  EXPECT_EQ(Peer::Cycles(m), cycles + 1);
  EXPECT_LT(Peer::Fg(m).capacity(), capacity / 16);
  int64_t next = -2;
  while (m.NeedsMigration()) {
    ASSERT_GT(next, -100) << "inserts never finished the shrink";
    m[next] = next;
    --next;
  }
  EXPECT_EQ(m.size(), static_cast<size_t>(10 - next - 1));
}

// extract() moves an element out of either table into a node that owns it,
// and migrates a batch like erase_and_migrate(). The nodes outlive every later
// migration, shrink and free of the tables the elements came from.
TEST_F(FlatHashMapTest, ExtractMovesTheElementOutAndMigrates) {
  SetBatch(8);
  Set s;
  int64_t next = 0;
  for (; next < 32; ++next) {
    s.insert(next);
  }
  const size_t cycles = Peer::Cycles(s);
  while (Peer::Cycles(s) == cycles) {
    ASSERT_LT(next, 1000000);
    s.insert(next++);
  }
  // Room for the three extracts below, each with its batch of 8.
  ASSERT_GT(Peer::Bg(s).size(), 3 * (1 + 8));

  // From bg_: the migration cursor's element, then another.
  for (int i = 0; i < 2; ++i) {
    auto bg_it = Peer::Bg(s).begin();
    std::advance(bg_it, i);
    const int64_t key = *bg_it;
    const size_t bg = Peer::Bg(s).size();
    auto node = s.extract(s.find(key));
    EXPECT_EQ(node.value(), key);
    EXPECT_FALSE(s.contains(key));
    EXPECT_EQ(Peer::Bg(s).size(), bg - 1 - 8);
  }
  // From fg_: the key whose insert started the cycle.
  const size_t bg = Peer::Bg(s).size();
  auto node = s.extract(s.find(next - 1));
  EXPECT_EQ(node.value(), next - 1);
  EXPECT_EQ(Peer::Bg(s).size(), bg - 8);

  std::vector<Set::node_type> nodes;
  const size_t remaining = s.size();
  while (!s.empty()) {
    nodes.push_back(s.extract(s.begin()));
  }
  ASSERT_EQ(nodes.size(), remaining);
  EXPECT_GT(Peer::Cycles(s), cycles + 1) << "extracting everything shrank it";
  std::set<int64_t> values;
  for (const auto &n : nodes) {
    values.insert(n.value());
  }
  EXPECT_EQ(values.size(), remaining);
  EXPECT_EQ(values.count(next - 1), 0);
}

// The direct check before every insert into fg_: a migration into a full
// foreground fails, while re-inserting an existing key into it is fine.
TEST_F(FlatHashMapTest, InsertIntoFullForegroundChecks) {
  SetBatch(1);
  Map m;
  FillPastSmallUntilBackground(m);
  ASSERT_FALSE(Peer::Bg(m).empty());
  size_t growth = internal::HugeMapGrowth(Peer::Fg(m).capacity());
  Peer::FgErased(m) = growth - Peer::Fg(m).size();
  EXPECT_DEATH(m.MigrateSome(1), "insert would make absl rehash");

  Map full;
  for (int64_t i = 0; i < 64; ++i) {
    full[i] = i;
  }
  full.MigrateSome(SIZE_MAX);
  ASSERT_TRUE(Peer::Bg(full).empty());
  Peer::FgErased(full) = internal::HugeMapGrowth(Peer::Fg(full).capacity()) -
                         Peer::Fg(full).size();
  int64_t existing = Peer::Fg(full).begin()->first;
  full[existing] = -existing;
  EXPECT_EQ(full.at(existing), -existing);
}

// Not a correctness test: reports what an advisory MigrateSome() costs per
// entry on a table well past the CPU caches. Disabled, so not run by ctest;
// run it with --gtest_also_run_disabled_tests.
TEST_F(FlatHashMapTest, DISABLED_MigrateSomeCost) {
  SetBatch(1);
  Map m;
  int64_t next = 0;
  for (; next < (int64_t{1} << 23); ++next) {
    m[next] = next;
  }
  FillUntilBackground(m, next);
  size_t bg = Peer::Bg(m).size();
  constexpr size_t kBatch = 32;
  size_t calls = std::min<size_t>(bg / kBatch, 100000);
  auto start = std::chrono::steady_clock::now();
  for (size_t i = 0; i < calls; ++i) {
    m.MigrateSome(kBatch);
  }
  double ns = std::chrono::duration<double, std::nano>(
                  std::chrono::steady_clock::now() - start)
                  .count();
  std::cout << "MigrateSome(" << kBatch << ") on a " << m.size()
            << "-entry map (bg " << bg << "): " << ns / calls
            << " ns per call, " << ns / (calls * kBatch) << " ns per entry\n";
  EXPECT_EQ(Peer::Bg(m).size(), bg - calls * kBatch);
}

}  // namespace
}  // namespace vmsdk
