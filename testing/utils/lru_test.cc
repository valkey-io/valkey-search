/*
 * Copyright (c) 2025, ValkeySearch contributors
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
