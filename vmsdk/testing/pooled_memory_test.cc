/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/pooled_memory.h"

#include <vector>

#include "gtest/gtest.h"

namespace valkey_search {

TEST(MemoryPoolTest, Basic) {
  for (auto push : {10, 20, 30}) {
    vmsdk::PooledMemory pool(17);
    {
      vmsdk::PooledVector<char> buffer(&pool);
      for (int i = 0; i < push; ++i) {
        buffer.push_back('a');
      }
      ASSERT_GE(pool.GetInUse(), buffer.capacity());
      for (int i = 0; i < push; ++i) {
        ASSERT_EQ(buffer[i], 'a');
      }
    }
    ASSERT_EQ(pool.GetInUse(), 0);
  }
}

}  // namespace valkey_search
