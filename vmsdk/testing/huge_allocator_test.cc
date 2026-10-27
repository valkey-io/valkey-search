/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/huge/allocator.h"

#include <cstdint>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
namespace {

using ::testing::_;

constexpr size_t kHugePage = huge_page_slab::kHugePageBytes;

class HugeAllocatorTest : public ValkeyTest {
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

  int64_t external_{0};
};

TEST_F(HugeAllocatorTest, SmallRequestsUseHeap) {
  HugeAllocator<uint64_t> alloc;
  constexpr size_t kN = kHugePage / sizeof(uint64_t) - 1;
  uint64_t *p = alloc.allocate(kN);
  EXPECT_EQ(external_, 0);
  p[kN - 1] = 1;
  alloc.deallocate(p, kN);
  EXPECT_EQ(external_, 0);
}

TEST_F(HugeAllocatorTest, LargeRequestsUseHugeRegions) {
  HugeAllocator<char> alloc;
  for (size_t bytes : {kHugePage, kHugePage + 1, 5 * kHugePage - 3}) {
    char *p = alloc.allocate(bytes);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(p) % kHugePage, 0);
    EXPECT_EQ(external_, huge_page_slab::RoundUpToHugePage(bytes));
    p[0] = 1;
    p[bytes - 1] = 1;
    alloc.deallocate(p, bytes);
    EXPECT_EQ(external_, 0);
  }
}

TEST_F(HugeAllocatorTest, VectorGrowsAcrossThreshold) {
  {
    std::vector<int, HugeAllocator<int>> v;
    for (int i = 0; i < 2 * int(kHugePage / sizeof(int)); ++i) {
      v.push_back(i);
    }
    EXPECT_GT(external_, 0);
    for (int i = 0; i < int(v.size()); ++i) {
      ASSERT_EQ(v[i], i);
    }
  }
  EXPECT_EQ(external_, 0);
}

TEST_F(HugeAllocatorTest, RebindAndEquality) {
  HugeAllocator<int> a;
  HugeAllocator<double> b(a);
  EXPECT_TRUE(a == b);
  EXPECT_FALSE(a != b);
  using Rebound = std::allocator_traits<HugeAllocator<int>>::rebind_alloc<char>;
  static_assert(std::is_same_v<Rebound, HugeAllocator<char>>);
}

TEST_F(HugeAllocatorTest, OversizedRequestThrows) {
  HugeAllocator<uint64_t> alloc;
  EXPECT_THROW(alloc.allocate(alloc.max_size() + 1), std::bad_array_new_length);
  // Passes max_size() but exceeds the address space.
  EXPECT_THROW(alloc.allocate(size_t{1} << 59), std::bad_alloc);
  EXPECT_EQ(external_, 0);
}

}  // namespace
}  // namespace vmsdk
