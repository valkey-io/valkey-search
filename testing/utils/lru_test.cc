/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/lru.h"

#include <cstddef>
#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"

namespace valkey_search {

namespace {
struct TestNode {
  std::string key;
  TestNode *prev{nullptr};
  TestNode *next{nullptr};
};

constexpr int kLruCapacity = 10;

std::vector<TestNode> InitializeLRU(LRU<TestNode> &lru, int size) {
  EXPECT_EQ(lru.Size(), 0L);
  std::vector<TestNode> nodes;
  nodes.reserve(20);
  for (int i = 0; i < 20; ++i) {
    nodes.push_back(TestNode{.key = absl::StrCat(i)});
  }
  for (size_t i = 0; i < nodes.size(); ++i) {
    TestNode *removed = lru.InsertAtTop(&nodes[i]);
    if (i < kLruCapacity) {
      EXPECT_EQ(removed, nullptr);
      EXPECT_EQ(lru.Size(), i + 1);
    } else {
      EXPECT_EQ(removed, &nodes[i - kLruCapacity]);
      EXPECT_EQ(lru.Size(), kLruCapacity);
    }
  }
  EXPECT_EQ(lru.Size(), kLruCapacity);
  return nodes;
}

TEST(LRUTest, SimpleAddRemove) {
  LRU<TestNode> lru(kLruCapacity);
  auto nodes = InitializeLRU(lru, 20);
  for (size_t i = 0; i < nodes.size(); ++i) {
    lru.Remove(&nodes[i]);
    if (i < kLruCapacity) {
      EXPECT_EQ(lru.Size(), kLruCapacity);
    } else {
      EXPECT_EQ(lru.Size(), nodes.size() - i - 1);
    }
  }
}

TEST(LRUTest, Promote) {
  LRU<TestNode> lru(kLruCapacity);
  auto nodes = InitializeLRU(lru, 15);
  for (size_t i = nodes.size() - 1; i >= nodes.size() - kLruCapacity; --i) {
    lru.Promote(&nodes[i]);
  }
  for (size_t i = 0; i < nodes.size() - kLruCapacity; ++i) {
    TestNode *removed = lru.InsertAtTop(&nodes[i]);
    EXPECT_EQ(removed, &nodes[nodes.size() - i - 1]);
  }
}
}  // namespace
}  // namespace valkey_search
