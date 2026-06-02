/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/intrusive_list.h"

#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace valkey_search {

namespace {
struct TestNode {
  std::string key;
  TestNode *prev{nullptr};
  TestNode *next{nullptr};
};

TEST(IntrusiveListTest, SimpleAddRemove) {
  IntrusiveList<TestNode> list;
  EXPECT_EQ(list.Size(), 0);
  std::vector<TestNode> nodes;
  nodes.push_back(TestNode{.key = "a"});
  nodes.push_back(TestNode{.key = "b"});
  nodes.push_back(TestNode{.key = "c"});
  for (auto &node : nodes) {
    list.PushBack(&node);
  }
  EXPECT_EQ(list.Size(), 3);
  auto i = 0;
  while (!list.Empty()) {
    auto node = list.Front();
    list.Remove(node);
    EXPECT_EQ(node->key, nodes[i].key);
    ++i;
  }
  EXPECT_EQ(i, 3);
  EXPECT_EQ(list.Size(), 0L);
}

}  // namespace
}  // namespace valkey_search
