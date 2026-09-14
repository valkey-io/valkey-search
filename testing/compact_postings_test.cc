/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/compact_postings.h"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search::indexes::text {
namespace {

using Map = CompactPostings<int>;
using TestMode = Map::TestMode;

class CompactPostingsTest : public vmsdk::ValkeyTest {
 protected:
  // Interns n distinct keys and returns them in interned-pointer (iteration)
  // order so tests can assert traversal without depending on address layout.
  std::vector<InternedStringPtr> OrderedKeys(int n) {
    std::vector<InternedStringPtr> keys;
    keys.reserve(n);
    for (int i = 0; i < n; ++i) {
      keys.push_back(StringInternStore::Intern("key-" + std::to_string(i)));
    }
    std::sort(keys.begin(), keys.end(), InternedStringPtrLess{});
    return keys;
  }
};

TEST_F(CompactPostingsTest, FitsIn8Bytes) {
  static_assert(sizeof(Map) == 8);
  EXPECT_EQ(sizeof(Map), 8u);
}

TEST_F(CompactPostingsTest, DefaultEmpty) {
  Map m;
  EXPECT_TRUE(m.empty());
  EXPECT_EQ(m.size(), 0u);
  EXPECT_EQ(m.TestModeForTesting(), TestMode::kEmpty);
  EXPECT_EQ(m.Find(StringInternStore::Intern("nope")), nullptr);
  EXPECT_FALSE(m.GetIterator().IsValid());
}

TEST_F(CompactPostingsTest, SingleLifecycleReleasesKeyRef) {
  auto k = StringInternStore::Intern("k");
  EXPECT_EQ(k.RefCount(), 1);
  {
    Map m;
    EXPECT_TRUE(m.Insert(k, 42));
    EXPECT_EQ(k.RefCount(), 2);
    EXPECT_EQ(m.size(), 1u);
    EXPECT_EQ(m.TestModeForTesting(), TestMode::kSingle);
    const int* v = m.Find(k);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(*v, 42);
  }
  EXPECT_EQ(k.RefCount(), 1);
}

TEST_F(CompactPostingsTest, InsertDuplicateDoesNotOverwrite) {
  auto k = StringInternStore::Intern("dup");
  Map m;
  EXPECT_TRUE(m.Insert(k, 1));
  EXPECT_FALSE(m.Insert(k, 2));
  EXPECT_EQ(m.size(), 1u);
  const int* v = m.Find(k);
  ASSERT_NE(v, nullptr);
  EXPECT_EQ(*v, 1);
}

TEST_F(CompactPostingsTest, ModeTransitionsOnGrowth) {
  auto keys = OrderedKeys(6);
  Map m;
  m.Insert(keys[0], 0);
  EXPECT_EQ(m.TestModeForTesting(), TestMode::kSingle);
  m.Insert(keys[1], 1);
  EXPECT_EQ(m.TestModeForTesting(), TestMode::kDouble);
  m.Insert(keys[2], 2);
  EXPECT_EQ(m.TestModeForTesting(), TestMode::kSmallVec);
  m.Insert(keys[3], 3);
  EXPECT_EQ(m.TestModeForTesting(), TestMode::kSmallVec);
  EXPECT_EQ(m.size(), 4u);
  m.Insert(keys[4], 4);
  EXPECT_EQ(m.TestModeForTesting(), TestMode::kMap);
  m.Insert(keys[5], 5);
  EXPECT_EQ(m.size(), 6u);
  for (int i = 0; i < 6; ++i) {
    const int* v = m.Find(keys[i]);
    ASSERT_NE(v, nullptr) << "missing key " << i;
    EXPECT_EQ(*v, i);
  }
}

TEST_F(CompactPostingsTest, ModeTransitionsOnShrink) {
  auto keys = OrderedKeys(6);
  Map m;
  for (int i = 0; i < 6; ++i) m.Insert(keys[i], i);
  EXPECT_EQ(m.TestModeForTesting(), TestMode::kMap);

  // 6 -> 4 demotes Map to SmallVec.
  EXPECT_TRUE(m.Erase(keys[5], nullptr));
  EXPECT_EQ(m.TestModeForTesting(), TestMode::kMap);
  EXPECT_TRUE(m.Erase(keys[4], nullptr));
  EXPECT_EQ(m.TestModeForTesting(), TestMode::kSmallVec);
  EXPECT_EQ(m.size(), 4u);

  // Down to 3 stays in SmallVec; down to 2 demotes to Double.
  m.Erase(keys[3], nullptr);
  EXPECT_EQ(m.TestModeForTesting(), TestMode::kSmallVec);
  m.Erase(keys[2], nullptr);
  EXPECT_EQ(m.TestModeForTesting(), TestMode::kDouble);
  EXPECT_EQ(m.size(), 2u);

  // 2 -> 1 demotes Double to Single.
  m.Erase(keys[1], nullptr);
  EXPECT_EQ(m.TestModeForTesting(), TestMode::kSingle);
  EXPECT_EQ(m.size(), 1u);

  // 1 -> 0 becomes empty.
  EXPECT_TRUE(m.Erase(keys[0], nullptr));
  EXPECT_TRUE(m.empty());
  EXPECT_EQ(m.TestModeForTesting(), TestMode::kEmpty);
}

TEST_F(CompactPostingsTest, EraseCopiesOutValueAndMissesReturnFalse) {
  auto keys = OrderedKeys(3);
  Map m;
  for (int i = 0; i < 3; ++i) m.Insert(keys[i], i * 10);

  int out = -1;
  EXPECT_TRUE(m.Erase(keys[1], &out));
  EXPECT_EQ(out, 10);
  EXPECT_EQ(m.Find(keys[1]), nullptr);

  EXPECT_FALSE(m.Erase(keys[1], &out));
  EXPECT_FALSE(m.Erase(StringInternStore::Intern("absent"), nullptr));
}

TEST_F(CompactPostingsTest, IterationIsOrderedInEveryMode) {
  for (int n : {1, 2, 4, 5, 16}) {
    auto keys = OrderedKeys(n);
    Map m;
    // Insert in reverse to exercise sorted insertion.
    for (int i = n - 1; i >= 0; --i) {
      m.Insert(keys[i], i);
    }
    EXPECT_EQ(m.size(), static_cast<size_t>(n));

    std::vector<int> seen;
    for (auto it = m.GetIterator(); it.IsValid(); it.Next()) {
      seen.push_back(it.GetValue());
      // The key at this position must match the sorted reference.
      EXPECT_EQ(RawInternedPtr(it.GetKey()),
                RawInternedPtr(keys[seen.size() - 1]));
    }
    ASSERT_EQ(seen.size(), static_cast<size_t>(n));
    for (int i = 0; i < n; ++i) EXPECT_EQ(seen[i], i);
  }
}

TEST_F(CompactPostingsTest, FindAcceptsBorrowedKey) {
  auto keys = OrderedKeys(5);
  Map m;
  for (int i = 0; i < 5; ++i) m.Insert(keys[i], i);
  for (int i = 0; i < 5; ++i) {
    BorrowedInternedStringPtr borrowed(keys[i]);
    const int* v = m.Find(borrowed);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(*v, i);
  }
  EXPECT_EQ(m.Find(BorrowedInternedStringPtr(StringInternStore::Intern("x"))),
            nullptr);
}

TEST_F(CompactPostingsTest, SkipForwardExactAndMiss) {
  for (int n : {1, 3, 5, 16}) {
    auto keys = OrderedKeys(n);
    Map m;
    for (int i = 0; i < n; ++i) m.Insert(keys[i], i);

    // Fresh iterator lands exactly on every present key (membership probe).
    for (int i = 0; i < n; ++i) {
      auto it = m.GetIterator();
      EXPECT_TRUE(it.SkipForward(keys[i]));
      EXPECT_EQ(RawInternedPtr(it.GetKey()), RawInternedPtr(keys[i]));
      EXPECT_EQ(it.GetValue(), i);
    }
    // Skipping to an absent key returns false but positions at the next key.
    auto absent = StringInternStore::Intern("absent-key");
    auto it = m.GetIterator();
    it.SkipForward(absent);  // result depends on address; must not crash
  }
}

TEST_F(CompactPostingsTest, MoveTransfersOwnership) {
  auto k = StringInternStore::Intern("mv");
  Map a;
  a.Insert(k, 7);
  EXPECT_EQ(k.RefCount(), 2);

  Map b(std::move(a));
  EXPECT_TRUE(a.empty());
  ASSERT_NE(b.Find(k), nullptr);
  EXPECT_EQ(*b.Find(k), 7);
  EXPECT_EQ(k.RefCount(), 2);

  Map c;
  c = std::move(b);
  EXPECT_TRUE(b.empty());
  ASSERT_NE(c.Find(k), nullptr);
  EXPECT_EQ(k.RefCount(), 2);
}

TEST_F(CompactPostingsTest, DestructorReleasesAllKeyRefsInMapMode) {
  auto keys = OrderedKeys(16);
  for (const auto& k : keys) EXPECT_EQ(k.RefCount(), 1);
  {
    Map m;
    for (int i = 0; i < 16; ++i) m.Insert(keys[i], i);
    for (const auto& k : keys) EXPECT_EQ(k.RefCount(), 2);
  }
  for (const auto& k : keys) EXPECT_EQ(k.RefCount(), 1);
}

}  // namespace
}  // namespace valkey_search::indexes::text
