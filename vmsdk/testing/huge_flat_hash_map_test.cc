/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <cstdint>
#include <random>
#include <stdexcept>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
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
};

namespace {

using ::testing::_;
using Peer = HugeFlatHashMapTestPeer;

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

class FlatHashMapTest : public ValkeyTest {
 protected:
  void SetUp() override {
    ValkeyTest::SetUp();
    ON_CALL(*kMockValkeyModule, IncrExternalMemory(_))
        .WillByDefault([this](size_t bytes) {
          external_ += bytes;
          return VALKEYMODULE_OK;
        });
    ON_CALL(*kMockValkeyModule, DecrExternalMemory(_))
        .WillByDefault([this](size_t bytes) {
          external_ -= bytes;
          return VALKEYMODULE_OK;
        });
  }
  void TearDown() override {
    SetBatch(GetHugeFlatHashMapMigrateBatch().GetDefaultValue());
    ValkeyTest::TearDown();
  }
  static void SetBatch(long long k) {
    VMSDK_EXPECT_OK(GetHugeFlatHashMapMigrateBatch().SetValue(k));
  }

  int64_t external_{0};
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
  m.erase(m.find(Peer::Fg(m).begin()->first));
  EXPECT_EQ(Peer::Bg(m).size(), bg - 2);
  EXPECT_EQ(m.size(), n - 1);
}

// Fails if absl rehashes fg_ on its own. A grow allocates outside a cycle; an
// in-place rehash moves elements, which `sampled` pointers detect.
class NoRehashChecker {
 public:
  explicit NoRehashChecker(CountedMap &m, size_t samples)
      : m_(m), samples_(samples) {
    Reset();
  }

  void Check() {
    auto &fg = Peer::Fg(m_);
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
      sampled_.try_emplace(entry.first, &entry);
    }
  }

 private:
  void Reset() {
    cycles_ = Peer::Cycles(m_);
    allocations_ = g_allocations;
    capacity_ = Peer::Fg(m_).capacity();
    sampled_.clear();
  }

  CountedMap &m_;
  size_t samples_;
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
  for (int64_t i = 0; i < 5000; ++i) {
    m[i] = i;
    ASSERT_NO_FATAL_FAILURE(checker.Check()) << i;
    m.find(i / 2);
    ASSERT_NO_FATAL_FAILURE(checker.Check()) << i;
  }
}

TEST_P(FlatHashMapNoRehashTest, GrowAndShrink) {
  CountedMap m;
  NoRehashChecker checker(m, 64);
  long long rehashes = internal::GetHugeMapRehashCount();

  constexpr int64_t kPeak = 200000;
  for (int64_t i = 0; i < kPeak; ++i) {
    m[i] = i;
    ASSERT_NO_FATAL_FAILURE(checker.Check()) << i;
  }
  size_t grow_cycles = Peer::Cycles(m);
  EXPECT_GT(grow_cycles, 0);
  EXPECT_EQ(internal::GetHugeMapRehashCount() - rehashes, grow_cycles);
  size_t peak_capacity = Peer::Fg(m).capacity();

  for (int64_t i = 0; i < kPeak - 1000; ++i) {
    EXPECT_EQ(m.erase(i), 1);
    ASSERT_NO_FATAL_FAILURE(checker.Check()) << i;
  }
  size_t shrink_cycles = Peer::Cycles(m) - grow_cycles;
  EXPECT_GT(shrink_cycles, 0);
  EXPECT_LT(Peer::Fg(m).capacity(), peak_capacity / 16);

  // Churn: insert/erase near a steady size accumulates tombstones.
  std::mt19937_64 rng(1);
  for (int64_t i = 0; i < 100000; ++i) {
    int64_t key = static_cast<int64_t>(rng() % 4000);
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
// with a rehash: in place at 3000, by growing at 3500.
TEST_P(FlatHashMapNoRehashTest, TombstoneChurn) {
  for (int64_t window : {3000, 3500}) {
    CountedMap m;
    NoRehashChecker checker(m, 64);
    for (int64_t i = 0; i < 300000; ++i) {
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

INSTANTIATE_TEST_SUITE_P(MigrateBatch, FlatHashMapNoRehashTest,
                         ::testing::Values(1, 8));

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
    for (int64_t i = 0; i < 300000; ++i) {
      m[i] = i;
    }
    EXPECT_GT(external_, 0);
  }
  EXPECT_EQ(external_, 0);
}

}  // namespace
}  // namespace vmsdk
