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
  auto *a = registry.Get(1, 512);
  EXPECT_EQ(registry.Get(1, 512), a);
  EXPECT_EQ(registry.Get(1, 510), a);
  EXPECT_NE(registry.Get(2, 512), a);
  EXPECT_EQ(a->EntrySize(), 512);
  EXPECT_EQ(a->TypeTag(), 1);
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
  constexpr size_t kPerPage = kHugePage / kEntry;
  auto *pool = registry.Get(1, kEntry);
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
  constexpr size_t kEntry = kHugePage / 4;
  auto *pool = registry.Get(1, kEntry);
  std::vector<char *> ptrs;
  for (int i = 0; i < 5; ++i) {  // One full page plus one entry on a second.
    ptrs.push_back(pool->Allocate());
  }
  EXPECT_EQ(external_, 2 * kHugePage);
  EXPECT_EQ(VectorPoolRegistry::GlobalMaterializedBytes(), 2 * kHugePage);
  pool->Free(ptrs[4]);
  EXPECT_EQ(external_, kHugePage);
  for (int i = 0; i < 4; ++i) {
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
  auto *pool = registry.Get(1, kHugePage);
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
