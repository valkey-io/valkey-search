/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#include "src/utils/vector_pool.h"

#include <cstdint>
#include <cstring>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/numa_slab.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search {
namespace {

using ::testing::_;

constexpr size_t kHugePage = vmsdk::numa_slab::kHugePageBytes;

class VectorPoolTest : public vmsdk::ValkeyTest {
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

TEST_F(VectorPoolTest, PoolsAreKeyedByTypeAndRoundedSize) {
  VectorPoolRegistry registry(4 * kHugePage, 2 * kHugePage);
  auto *a = registry.Get(1, 520);
  EXPECT_EQ(registry.Get(1, 520), a);
  EXPECT_EQ(registry.Get(1, 513), a);
  EXPECT_NE(registry.Get(1, 512), a);
  EXPECT_NE(registry.Get(2, 520), a);
  EXPECT_EQ(a->EntrySize(), 576);
  EXPECT_EQ(a->TypeTag(), 1);
}

// Data (after the 8-byte prefix) starts on a cache line, entries do not
// overlap or straddle pages, and a page holds (2 MiB - 56) / stride entries.
TEST_F(VectorPoolTest, DataIsCacheLineAligned) {
  VectorPoolRegistry registry(8 * kHugePage, 2 * kHugePage);
  // Data sizes congruent to 0, 8, 56 and 60 mod 64.
  for (size_t data_bytes : {3072u, 3080u, 3128u, 3132u, 256u}) {
    SCOPED_TRACE(data_bytes);
    auto *pool = registry.Get(1, VectorPool::kPrefixBytes + data_bytes);
    size_t stride = pool->EntrySize();
    EXPECT_EQ(stride % 64, 0u);
    EXPECT_GE(stride, VectorPool::kPrefixBytes + data_bytes);
    size_t per_page = (kHugePage - 56) / stride;
    std::vector<char *> ptrs;
    ptrs.reserve(per_page + 1);
    for (size_t i = 0; i <= per_page; ++i) {
      ptrs.push_back(pool->Allocate());
    }
    for (size_t i = 0; i < ptrs.size(); ++i) {
      char *data = ptrs[i] + VectorPool::kPrefixBytes;
      EXPECT_EQ(reinterpret_cast<uintptr_t>(data) % 64, 0u) << i;
      auto page = reinterpret_cast<uintptr_t>(ptrs[i]) / kHugePage;
      EXPECT_EQ(reinterpret_cast<uintptr_t>(data + data_bytes - 1) / kHugePage,
                page)
          << i;
      if (i > 0 && i < per_page) {
        EXPECT_EQ(ptrs[i] - ptrs[i - 1], stride);
      }
    }
    // Entry per_page did not fit and went to a second page.
    EXPECT_NE(reinterpret_cast<uintptr_t>(ptrs[per_page]) / kHugePage,
              reinterpret_cast<uintptr_t>(ptrs[0]) / kHugePage);
    for (auto *p : ptrs) {
      pool->Free(p);
    }
  }
}

TEST_F(VectorPoolTest, PoolOfIdentifiesOwner) {
  VectorPoolRegistry registry(4 * kHugePage, 2 * kHugePage);
  auto *f32 = registry.Get(1, 512);
  auto *f16 = registry.Get(2, 512);
  char *a = f32->Allocate();
  char *b = f16->Allocate();
  EXPECT_EQ(registry.PoolOf(a), f32);
  EXPECT_EQ(registry.PoolOf(b), f16);
  int on_stack;
  EXPECT_EQ(registry.PoolOf(&on_stack), nullptr);
  f32->Free(a);
  f16->Free(b);
}

TEST_F(VectorPoolTest, EntriesDoNotStraddlePagesAndAreReused) {
  VectorPoolRegistry registry(4 * kHugePage, 2 * kHugePage);
  constexpr size_t kEntry = 3080;  // 768 floats + 8-byte header.
  auto *pool = registry.Get(1, kEntry);
  const size_t kPerPage = (kHugePage - 56) / pool->EntrySize();
  std::vector<char *> ptrs;
  for (size_t i = 0; i < 2 * kPerPage; ++i) {
    char *p = pool->Allocate();
    ASSERT_NE(p, nullptr);
    auto offset = reinterpret_cast<uintptr_t>(p) % kHugePage;
    EXPECT_LE(offset + kEntry, kHugePage);
    std::memset(p, 0xab, kEntry);
    ptrs.push_back(p);
  }
  EXPECT_EQ(external_, 2 * kHugePage);
  EXPECT_EQ(absl::flat_hash_set<char *>(ptrs.begin(), ptrs.end()).size(),
            ptrs.size());
  char *freed = ptrs[5];
  pool->Free(freed);
  EXPECT_EQ(pool->Allocate(), freed);
  for (auto *p : ptrs) {
    pool->Free(p);
  }
  EXPECT_EQ(pool->ActiveAllocations(), 0);
}

TEST_F(VectorPoolTest, EmptyPageIsReleased) {
  VectorPoolRegistry registry(4 * kHugePage, 2 * kHugePage);
  auto *pool = registry.Get(1, kHugePage / 8);  // 7 entries per page.
  std::vector<char *> ptrs;
  for (int i = 0; i < 8; ++i) {  // One full page plus one entry on a second.
    ptrs.push_back(pool->Allocate());
  }
  EXPECT_EQ(external_, 2 * kHugePage);
  EXPECT_EQ(VectorPoolRegistry::GlobalMaterializedBytes(), 2 * kHugePage);
  pool->Free(ptrs[7]);
  EXPECT_EQ(external_, kHugePage);
  for (int i = 0; i < 7; ++i) {
    pool->Free(ptrs[i]);
  }
  EXPECT_EQ(external_, 0);
  EXPECT_EQ(VectorPoolRegistry::GlobalMaterializedBytes(), 0);
  EXPECT_EQ(VectorPoolRegistry::GlobalActiveAllocations(), 0);
  // The released page is materialized again on demand.
  pool->Free(pool->Allocate());
  EXPECT_EQ(external_, 0);
}

TEST_F(VectorPoolTest, ExhaustedRegionReturnsNull) {
  VectorPoolRegistry registry(2 * kHugePage, 2 * kHugePage);
  auto *pool = registry.Get(1, kHugePage / 2);  // One entry per page.
  char *a = pool->Allocate();
  char *b = pool->Allocate();
  ASSERT_NE(a, nullptr);
  ASSERT_NE(b, nullptr);
  EXPECT_EQ(pool->Allocate(), nullptr);
  // A second pool cannot claim a slab either.
  EXPECT_EQ(registry.Get(2, 64)->Allocate(), nullptr);
  pool->Free(a);
  pool->Free(b);
}

TEST_F(VectorPoolTest, DestructionReleasesMaterializedPages) {
  {
    VectorPoolRegistry registry(4 * kHugePage, 2 * kHugePage);
    registry.Get(1, 64)->Allocate();
    EXPECT_EQ(external_, kHugePage);
  }
  EXPECT_EQ(external_, 0);
}

}  // namespace
}  // namespace valkey_search
