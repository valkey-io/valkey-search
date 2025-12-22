/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/pooled_memory.h"

#include <string>
#include <unordered_set>
#include <vector>

#include "vmsdk/src/type_conversions.h"
#include "gtest/gtest.h"

namespace vmsdk {

TEST(MemoryPoolTest, Vector) {
  for (auto push : {10, 20, 30}) {
    vmsdk::PooledMemory pool(17);
    {
      ASSERT_EQ(pool.GetInUse(), 0);
      std::pmr::vector<char> buffer(&pool);
      for (int i = 0; i < push; ++i) {
        buffer.push_back('a');
      }
      ASSERT_GE(pool.GetInUse(), buffer.capacity());
      for (int i = 0; i < push; ++i) {
        ASSERT_EQ(buffer[i], 'a');
      }
      ASSERT_GT(pool.GetInUse(), 0);
    }
    ASSERT_EQ(pool.GetInUse(), 0);

  }
}

TEST(MemoryPoolTest, String) {
  for (auto push : {100, 200, 300}) {
    vmsdk::PooledMemory pool(17);
    {
      std::pmr::string buffer(&pool);
      for (int i = 0; i < push; ++i) {
        buffer.push_back('a');
      }
      ASSERT_GE(pool.GetInUse(), buffer.capacity());
      for (int i = 0; i < push; ++i) {
        ASSERT_EQ(buffer[i], 'a');
      }
      ASSERT_GT(pool.GetInUse(), 0);
      ASSERT_GT(pool.GetMallocs(), 0);
    }
    ASSERT_EQ(pool.GetInUse(), 0);
  }
}

TEST(MemoryPoolTest, StringAssign) {
    vmsdk::PooledMemory pool(17);
    {
      ASSERT_EQ(pool.GetInUse(), 0);
      std::pmr::string buffer(&pool);
      buffer = std::pmr::string("abczdefghijklasdfsadfasdfasdfasdf", &pool);
      ASSERT_GT(pool.GetInUse(), 0);
      ASSERT_GT(pool.GetMallocs(), 0);
    }
    ASSERT_EQ(pool.GetInUse(), 0);
}

TEST(MemoryPoolTest, HashSet) {
  for (auto push : {10, 20, 30}) {
    vmsdk::PooledMemory pool(17);
    {
      std::pmr::unordered_set<int> buffer(&pool);
      for (int i = 0; i < push; ++i) {
        buffer.insert(i);
      }
      ASSERT_GE(pool.GetInUse(), buffer.size());
      for (int i = 0; i < push; ++i) {
        ASSERT_TRUE(buffer.find(i) != buffer.end());
      }
      ASSERT_GT(pool.GetInUse(), 0);
      ASSERT_GT(pool.GetMallocs(), 0);
    }
    ASSERT_EQ(pool.GetInUse(), 0);
  }
}

}  // namespace vmsdk
