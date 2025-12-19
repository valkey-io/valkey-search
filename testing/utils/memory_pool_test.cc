/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/memory_pool.h"

#include <vector>

#include "gtest/gtest.h"

namespace valkey_search {

TEST(MemoryPoolTest, Basic) {
  for (auto push : {10, 20, 30}) {
    MemoryPool pool(17);
    {
      PooledVector<char> buffer(&pool);
      std::cerr << "Doing size " << push << std::endl;
      for (int i = 0; i < push; ++i) {
        if (i != 0)
          std::cerr << "Before push " << i << " Start now at "
                    << (void *)buffer.data() << std::endl;
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
