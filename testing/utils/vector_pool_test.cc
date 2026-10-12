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

size_t RoundUp64(size_t n) { return (n + 63) & ~size_t{63}; }

constexpr size_t kPageHeaderBytes = 24;

TEST_F(VectorPoolTest, PoolsAreKeyedByTypeAndRoundedSize) {
  VectorPoolRegistry registry(2 * kHugePage);
  auto *a = registry.Get(1, 520);
  EXPECT_EQ(registry.Get(1, 520), a);
  EXPECT_EQ(registry.Get(1, 513), a);
  EXPECT_NE(registry.Get(1, 512), a);
  EXPECT_NE(registry.Get(2, 520), a);
  EXPECT_EQ(a->EntrySize(), 576);
  EXPECT_EQ(a->TypeTag(), 1);
}

// Entries are cache-line aligned with no prefix, as many fit as the page
// header and metadata table leave room for, and each entry's metadata is
// found from its address.
TEST_F(VectorPoolTest, LayoutAndMetadata) {
  VectorPoolRegistry registry(2 * kHugePage);
  // Data sizes congruent to 0, 8, 56 and 60 mod 64.
  for (size_t data_bytes : {3072u, 3080u, 3128u, 3132u, 256u}) {
    SCOPED_TRACE(data_bytes);
    auto *pool = registry.Get(1, data_bytes);
    size_t stride = pool->EntrySize();
    EXPECT_EQ(stride, RoundUp64(data_bytes));
    size_t n = pool->EntriesPerPage();
    auto first_entry = [](size_t entries) {
      return RoundUp64(kPageHeaderBytes +
                       entries * sizeof(VectorPool::EntryMeta));
    };
    EXPECT_LE(first_entry(n) + n * stride, kHugePage);
    EXPECT_GT(first_entry(n + 1) + (n + 1) * stride, kHugePage);

    std::vector<char *> ptrs;
    ptrs.reserve(n + 1);
    for (size_t i = 0; i <= n; ++i) {
      ptrs.push_back(pool->Allocate());
      ASSERT_NE(ptrs.back(), nullptr);
    }
    auto page_of = [](const void *p) {
      return reinterpret_cast<uintptr_t>(p) / kHugePage;
    };
    for (size_t i = 0; i < n; ++i) {
      char *data = ptrs[i];
      EXPECT_EQ(reinterpret_cast<uintptr_t>(data) % 64, 0u) << i;
      EXPECT_EQ(page_of(data + data_bytes - 1), page_of(ptrs[0])) << i;
      if (i > 0) {
        EXPECT_EQ(ptrs[i] - ptrs[i - 1], stride);
      }
      EXPECT_EQ(VectorPool::PoolOf(data), pool);
      auto &meta = VectorPool::MetaOf(data);
      char *page = reinterpret_cast<char *>(page_of(data) * kHugePage);
      EXPECT_EQ(reinterpret_cast<char *>(&meta),
                page + kPageHeaderBytes + i * sizeof(VectorPool::EntryMeta))
          << i;
    }
    // Entry n did not fit and went to a second page.
    EXPECT_NE(page_of(ptrs[n]), page_of(ptrs[0]));
    EXPECT_EQ(reinterpret_cast<char *>(&VectorPool::MetaOf(ptrs[n])),
              reinterpret_cast<char *>(page_of(ptrs[n]) * kHugePage) +
                  kPageHeaderBytes);
    for (auto *p : ptrs) {
      pool->Free(p);
    }
  }
}

TEST_F(VectorPoolTest, PoolOfIdentifiesOwner) {
  VectorPoolRegistry registry(2 * kHugePage);
  auto *f32 = registry.Get(1, 512);
  auto *f16 = registry.Get(2, 512);
  char *a = f32->Allocate();
  char *b = f16->Allocate();
  EXPECT_EQ(VectorPool::PoolOf(a), f32);
  EXPECT_EQ(VectorPool::PoolOf(b), f16);
  f32->Free(a);
  f16->Free(b);
}

TEST_F(VectorPoolTest, EntriesAreReused) {
  VectorPoolRegistry registry(2 * kHugePage);
  constexpr size_t kData = 3072;  // 768 floats.
  auto *pool = registry.Get(1, kData);
  const size_t kPerPage = pool->EntriesPerPage();
  std::vector<char *> ptrs;
  for (size_t i = 0; i < 2 * kPerPage; ++i) {
    char *p = pool->Allocate();
    ASSERT_NE(p, nullptr);
    std::memset(p, 0xab, kData);
    VectorPool::MetaOf(p).ref_count.store(1);
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
  VectorPoolRegistry registry(2 * kHugePage);
  auto *pool = registry.Get(1, kHugePage / 8);
  ASSERT_EQ(pool->EntriesPerPage(), 7u);
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

// Slabs are mapped as pages are needed and unmapped when all their pages are
// released.
TEST_F(VectorPoolTest, SlabsAreMappedOnDemandAndUnmappedWhenEmpty) {
  VectorPoolRegistry registry(2 * kHugePage);
  auto *pool = registry.Get(1, kHugePage / 2);  // One entry per page.
  ASSERT_EQ(pool->EntriesPerPage(), 1u);
  EXPECT_EQ(pool->SlabCount(), 0u);
  std::vector<char *> ptrs;
  for (int i = 0; i < 3; ++i) {
    ptrs.push_back(pool->Allocate());
    ASSERT_NE(ptrs.back(), nullptr);
  }
  EXPECT_EQ(pool->SlabCount(), 2u);
  pool->Free(ptrs[2]);  // Empties the second slab.
  EXPECT_EQ(pool->SlabCount(), 1u);
  pool->Free(ptrs[0]);
  EXPECT_EQ(pool->SlabCount(), 1u);
  pool->Free(ptrs[1]);
  EXPECT_EQ(pool->SlabCount(), 0u);
  EXPECT_EQ(external_, 0);
  // A new slab is mapped on demand.
  char *p = pool->Allocate();
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(pool->SlabCount(), 1u);
  pool->Free(p);
  EXPECT_EQ(pool->SlabCount(), 0u);
}

TEST_F(VectorPoolTest, DestructionReleasesMaterializedPages) {
  {
    VectorPoolRegistry registry(2 * kHugePage);
    registry.Get(1, 64)->Allocate();
    EXPECT_EQ(external_, kHugePage);
  }
  EXPECT_EQ(external_, 0);
}

}  // namespace
}  // namespace valkey_search
