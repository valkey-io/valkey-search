/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
