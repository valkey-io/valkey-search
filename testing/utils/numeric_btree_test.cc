/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/numeric_btree.h"

#include <algorithm>
#include <cstdint>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search::utils {
namespace {

class NumericBTreeTest : public vmsdk::ValkeyTest {};

InternedStringPtr Key(int i) {
  return StringInternStore::Intern("k" + std::to_string(i));
}

// Walks the tree and returns the postings in order as (value, key) pairs.
std::vector<std::pair<double, std::string>> ToVector(
    NumericBTree::Iterator it, NumericBTree::Iterator end) {
  std::vector<std::pair<double, std::string>> out;
  while (it != end) {
    out.push_back({it.Value(), std::string((*it)->Str())});
    ++it;
  }
  return out;
}

// Brute-force reference: stores (value, key-name) pairs in a multiset
// ordered by value. Within a value, key order is unspecified (the bag is
// unordered), so tests compare unordered collections of keys per value.
struct Reference {
  std::set<std::pair<double, std::string>> data;

  void Insert(double v, const std::string& k) { data.insert({v, k}); }
  bool Erase(double v, const std::string& k) { return data.erase({v, k}) > 0; }
  uint64_t Count(double s, double e, bool si, bool ei) const {
    uint64_t c = 0;
    for (auto& [val, key] : data) {
      bool ge_s = si ? val >= s : val > s;
      bool le_e = ei ? val <= e : val < e;
      if (ge_s && le_e) {
        ++c;
      }
    }
    return c;
  }
};

TEST_F(NumericBTreeTest, Empty) {
  NumericBTree t;
  EXPECT_EQ(t.TotalPostings(), 0u);
  EXPECT_EQ(t.UniqueValues(), 0u);
  EXPECT_TRUE(t.Empty());
  EXPECT_EQ(t.Count(-1e9, 1e9, true, true), 0u);
  EXPECT_TRUE(t.Begin() == t.End());
}

TEST_F(NumericBTreeTest, SingleInsertEraseRoundtrip) {
  NumericBTree t;
  auto k = Key(42);
  EXPECT_TRUE(t.Insert(1.5, k));
  EXPECT_EQ(t.TotalPostings(), 1u);
  EXPECT_EQ(t.UniqueValues(), 1u);
  EXPECT_EQ(t.Count(0.0, 2.0, true, true), 1u);
  EXPECT_EQ(t.Count(1.5, 1.5, true, true), 1u);
  EXPECT_EQ(t.Count(1.5, 1.5, false, true), 0u);
  EXPECT_EQ(t.Count(1.5, 1.5, true, false), 0u);
  EXPECT_FALSE(t.Insert(1.5, k));  // duplicate posting
  EXPECT_EQ(t.TotalPostings(), 1u);
  EXPECT_TRUE(t.Erase(1.5, k));
  EXPECT_EQ(t.TotalPostings(), 0u);
  EXPECT_EQ(t.UniqueValues(), 0u);
  EXPECT_FALSE(t.Erase(1.5, k));
}

TEST_F(NumericBTreeTest, ManyDistinctValuesSequential) {
  NumericBTree t;
  constexpr int N = 1000;
  for (int i = 0; i < N; ++i) {
    EXPECT_TRUE(t.Insert(static_cast<double>(i), Key(i)));
  }
  EXPECT_EQ(t.TotalPostings(), N);
  EXPECT_EQ(t.UniqueValues(), N);
  EXPECT_EQ(t.Count(0.0, N - 1, true, true), N);
  EXPECT_EQ(t.Count(100.0, 200.0, true, true), 101u);
  EXPECT_EQ(t.Count(100.0, 200.0, false, false), 99u);

  auto v = ToVector(t.Begin(), t.End());
  ASSERT_EQ(v.size(), N);
  for (int i = 0; i < N; ++i) {
    EXPECT_EQ(v[i].first, static_cast<double>(i));
  }

  for (int i = N - 1; i >= 0; i -= 2) {
    EXPECT_TRUE(t.Erase(static_cast<double>(i), Key(i)));
  }
  EXPECT_EQ(t.TotalPostings(), N / 2);
  EXPECT_EQ(t.UniqueValues(), N / 2);
  EXPECT_EQ(t.Count(0.0, N - 1, true, true), N / 2);
}

TEST_F(NumericBTreeTest, ManyKeysPerSingleValue) {
  // All postings share value 7.0 -> single leaf entry whose bag grows
  // through Single -> Array4 -> Array8 -> Set.
  NumericBTree t;
  constexpr int N = 500;
  for (int i = 0; i < N; ++i) {
    EXPECT_TRUE(t.Insert(7.0, Key(i)));
  }
  EXPECT_EQ(t.TotalPostings(), N);
  EXPECT_EQ(t.UniqueValues(), 1u);
  EXPECT_EQ(t.Count(7.0, 7.0, true, true), N);
  EXPECT_EQ(t.Count(7.0, 7.0, false, true), 0u);
  EXPECT_EQ(t.Count(0.0, 7.0, true, false), 0u);
  EXPECT_EQ(t.Count(0.0, 7.0, true, true), N);

  // Iterate -- should yield N postings, all at value 7.0.
  auto v = ToVector(t.Begin(), t.End());
  ASSERT_EQ(v.size(), N);
  for (auto& [val, _] : v) {
    EXPECT_EQ(val, 7.0);
  }

  // Erase a few and watch the bag demote.
  for (int i = 0; i < 100; ++i) {
    EXPECT_TRUE(t.Erase(7.0, Key(i)));
  }
  EXPECT_EQ(t.TotalPostings(), 400u);
  EXPECT_EQ(t.UniqueValues(), 1u);
  EXPECT_EQ(t.Count(7.0, 7.0, true, true), 400u);

  // Erase the rest down to one, then to zero.
  for (int i = 100; i < N - 1; ++i) {
    EXPECT_TRUE(t.Erase(7.0, Key(i)));
  }
  EXPECT_EQ(t.TotalPostings(), 1u);
  EXPECT_EQ(t.UniqueValues(), 1u);
  EXPECT_TRUE(t.Erase(7.0, Key(N - 1)));
  EXPECT_EQ(t.TotalPostings(), 0u);
  EXPECT_EQ(t.UniqueValues(), 0u);
  EXPECT_TRUE(t.Empty());
}

TEST_F(NumericBTreeTest, RangeBoundaries) {
  NumericBTree t;
  for (int i = 1; i <= 10; ++i) {
    t.Insert(static_cast<double>(i), Key(i));
  }
  // [3, 7] inclusive.
  auto v = ToVector(t.LowerBoundByValue(3.0), t.UpperBoundByValue(7.0));
  ASSERT_EQ(v.size(), 5u);
  EXPECT_EQ(v.front().first, 3.0);
  EXPECT_EQ(v.back().first, 7.0);

  // (3, 7) exclusive.
  v = ToVector(t.UpperBoundByValue(3.0), t.LowerBoundByValue(7.0));
  ASSERT_EQ(v.size(), 3u);
  EXPECT_EQ(v.front().first, 4.0);
  EXPECT_EQ(v.back().first, 6.0);

  // Out of range to the right.
  v = ToVector(t.LowerBoundByValue(100.0), t.End());
  EXPECT_TRUE(v.empty());
  // Out of range to the left.
  v = ToVector(t.LowerBoundByValue(-1.0), t.UpperBoundByValue(0.5));
  EXPECT_TRUE(v.empty());
}

TEST_F(NumericBTreeTest, RangeBoundariesAcrossSharedValue) {
  // Many keys at value 5.0; queries that bracket 5.0 must include or
  // exclude all of them coherently.
  NumericBTree t;
  for (int i = 0; i < 20; ++i) {
    t.Insert(5.0, Key(i));
  }
  for (int i = 100; i < 105; ++i) {
    t.Insert(7.0, Key(i));
  }
  EXPECT_EQ(t.Count(4.9, 5.1, true, true), 20u);
  EXPECT_EQ(t.Count(4.9, 5.0, true, true), 20u);
  EXPECT_EQ(t.Count(4.9, 5.0, true, false), 0u);  // (4.9, 5.0) excludes 5
  EXPECT_EQ(t.Count(5.0, 7.0, true, true), 25u);
  EXPECT_EQ(t.Count(5.0, 7.0, false, true), 5u);
}

TEST_F(NumericBTreeTest, RandomCrossCheck) {
  NumericBTree t;
  Reference ref;
  std::mt19937 rng(0xC0FFEE);
  std::uniform_real_distribution<double> dval(-50.0, 50.0);
  std::uniform_int_distribution<int> dop(0, 99);
  std::uniform_int_distribution<int> dkey(0, 9999);

  // 4k mixed ops; the 0.25 quantization forces many keys per value.
  for (int op = 0; op < 4000; ++op) {
    int choice = dop(rng);
    if (choice < 70 || ref.data.size() < 50) {
      double v = std::round(dval(rng) * 4.0) / 4.0;
      int k = dkey(rng);
      std::string ks = "k" + std::to_string(k);
      bool tree_ins = t.Insert(v, Key(k));
      auto [_, ref_ins] = ref.data.insert({v, ks});
      EXPECT_EQ(tree_ins, ref_ins);
    } else {
      auto it = ref.data.begin();
      std::advance(it, std::uniform_int_distribution<size_t>(
                           0, ref.data.size() - 1)(rng));
      auto [v, ks] = *it;
      int k = std::stoi(ks.substr(1));
      EXPECT_TRUE(t.Erase(v, Key(k)));
      ref.data.erase(it);
    }
    EXPECT_EQ(t.TotalPostings(), ref.data.size()) << "op=" << op;
  }

  // Exhaustive iteration: same multiset of (value, key) entries.
  auto v = ToVector(t.Begin(), t.End());
  ASSERT_EQ(v.size(), ref.data.size());
  std::set<std::pair<double, std::string>> tree_set(v.begin(), v.end());
  EXPECT_EQ(tree_set, ref.data);

  // Random range count checks.
  std::uniform_real_distribution<double> dq(-60.0, 60.0);
  for (int q = 0; q < 200; ++q) {
    double a = dq(rng), b = dq(rng);
    if (a > b) {
      std::swap(a, b);
    }
    bool si = (q & 1) != 0;
    bool ei = (q & 2) != 0;
    EXPECT_EQ(t.Count(a, b, si, ei), ref.Count(a, b, si, ei))
        << "a=" << a << " b=" << b << " si=" << si << " ei=" << ei;
  }
}

TEST_F(NumericBTreeTest, WholeRangeCountFastPath) {
  NumericBTree t;
  for (int i = 0; i < 1000; ++i) {
    t.Insert(static_cast<double>(i), Key(i));
  }
  // Whole-range inclusive over the actual extents.
  EXPECT_EQ(t.Count(0.0, 999.0, true, true), 1000u);
  // Wider-than-needed bounds also trigger the fast path.
  EXPECT_EQ(t.Count(-1e9, 1e9, true, true), 1000u);
  // Exclusive bounds outside the extents still count everything.
  EXPECT_EQ(t.Count(-1.0, 1000.0, false, false), 1000u);
  // Exclusive at the extent excludes the boundary value.
  EXPECT_EQ(t.Count(0.0, 999.0, false, true), 999u);
  EXPECT_EQ(t.Count(0.0, 999.0, true, false), 999u);

  // Loose bounds: erase the extreme values. The cached bounds may now be
  // wider than the actual data, but Count must still be correct because
  // the fast-path test is intentionally conservative.
  EXPECT_TRUE(t.Erase(0.0, Key(0)));
  EXPECT_TRUE(t.Erase(999.0, Key(999)));
  // Whole-tree query still returns the correct count even though
  // cached_min_=0.0 / cached_max_=999.0 no longer match the real extents.
  EXPECT_EQ(t.Count(-1e9, 1e9, true, true), 998u);
  EXPECT_EQ(t.Count(1.0, 998.0, true, true), 998u);
  // A query whose end coincides with the (loose) cached_max but is below
  // the real max (because real max is now 998) still returns 998 -- the
  // fast path declines to fire (end >= cached_max_=999 is false), and the
  // walk gives the right answer.
  EXPECT_EQ(t.Count(1.0, 998.5, true, true), 998u);

  // Empty tree -> 0 even for full-range queries.
  for (int i = 1; i < 999; ++i) {
    EXPECT_TRUE(t.Erase(static_cast<double>(i), Key(i)));
  }
  EXPECT_EQ(t.Count(-1e9, 1e9, true, true), 0u);
  // Re-insert -- bounds reseed correctly.
  t.Insert(42.0, Key(42));
  EXPECT_EQ(t.Count(-1e9, 1e9, true, true), 1u);
  EXPECT_EQ(t.Count(42.0, 42.0, true, true), 1u);
}

TEST_F(NumericBTreeTest, EraseAllDownToEmpty) {
  NumericBTree t;
  Reference ref;
  for (int i = 0; i < 600; ++i) {
    double v = static_cast<double>(i % 50);
    t.Insert(v, Key(i));
    ref.data.insert({v, "k" + std::to_string(i)});
  }
  EXPECT_EQ(t.TotalPostings(), 600u);
  EXPECT_EQ(t.UniqueValues(), 50u);

  std::vector<std::pair<double, std::string>> all(ref.data.begin(),
                                                  ref.data.end());
  std::mt19937 rng(123);
  std::shuffle(all.begin(), all.end(), rng);
  for (auto& [v, ks] : all) {
    int k = std::stoi(ks.substr(1));
    EXPECT_TRUE(t.Erase(v, Key(k)));
  }
  EXPECT_EQ(t.TotalPostings(), 0u);
  EXPECT_EQ(t.UniqueValues(), 0u);
  EXPECT_TRUE(t.Empty());
  EXPECT_TRUE(t.Begin() == t.End());
}

}  // namespace
}  // namespace valkey_search::utils
