/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <sys/mman.h>

#include <cstdint>
#include <cstring>
#include <new>

#include "absl/log/check.h"  // IWYU pragma: keep (needed by hnswlib)
#include "gtest/gtest.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/huge_page_allocation.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace {

using hnswlib::ChunkedArray;

size_t external_bytes = 0;
int external_calls = 0;
// When >= 0, the number of IncrExternalMemory calls that succeed before one
// fails.
int incr_successes_left = -1;

int FakeIncrExternalMemory(size_t bytes) {
  if (incr_successes_left == 0) {
    return VALKEYMODULE_ERR;
  }
  if (incr_successes_left > 0) {
    --incr_successes_left;
  }
  external_bytes += bytes;
  ++external_calls;
  return VALKEYMODULE_OK;
}

int FakeDecrExternalMemory(size_t bytes) {
  if (bytes > external_bytes) {
    return VALKEYMODULE_ERR;
  }
  external_bytes -= bytes;
  ++external_calls;
  return VALKEYMODULE_OK;
}

class ChunkedArrayTest : public ::testing::Test {
 protected:
  void SetUp() override {
    external_bytes = 0;
    external_calls = 0;
    incr_successes_left = -1;
  }
  void TearDown() override {
    ValkeyModule_IncrExternalMemory = nullptr;
    ValkeyModule_DecrExternalMemory = nullptr;
    vmsdk::SetHugePageAllocationEnabled(true);
  }
  static void InstallExternalMemoryApis() {
    ValkeyModule_IncrExternalMemory = &FakeIncrExternalMemory;
    ValkeyModule_DecrExternalMemory = &FakeDecrExternalMemory;
  }
};

constexpr size_t kElementSize = 152;
constexpr size_t kElementsPerChunk = 10 * 1024;

// Writes a distinct pattern to every element and reads it back.
void VerifyElements(const ChunkedArray &array, size_t count) {
  for (size_t i = 0; i < count; ++i) {
    memset(array[i], static_cast<int>(i % 251), kElementSize);
  }
  for (size_t i = 0; i < count; ++i) {
    const char *e = array[i];
    ASSERT_EQ(static_cast<unsigned char>(e[0]), i % 251) << i;
    ASSERT_EQ(static_cast<unsigned char>(e[kElementSize - 1]), i % 251) << i;
  }
}

TEST_F(ChunkedArrayTest, HugePagesWhenApisPresent) {
  InstallExternalMemoryApis();
  ASSERT_TRUE(vmsdk::UseHugePageAllocation());
  const size_t per_chunk = vmsdk::kHugePageSize / kElementSize;
  const uint64_t used_before = vmsdk::GetUsedMemoryCnt();
  {
    ChunkedArray array(kElementSize, kElementsPerChunk, 1);
    EXPECT_EQ(array.getSizePerChunk(), vmsdk::kHugePageSize);
    EXPECT_EQ(external_bytes, vmsdk::kHugePageSize);
    EXPECT_EQ(vmsdk::GetUsedMemoryCnt() - used_before, vmsdk::kHugePageSize);

    // Grow across several chunks; the element count is not chunk-aligned.
    const size_t count = 3 * per_chunk + 7;
    array.resize(count);
    EXPECT_EQ(external_bytes, 4 * vmsdk::kHugePageSize);
    EXPECT_EQ(vmsdk::GetUsedMemoryCnt() - used_before,
              4 * vmsdk::kHugePageSize);
    for (size_t chunk = 0; chunk < 4; ++chunk) {
      EXPECT_EQ(reinterpret_cast<uintptr_t>(array[chunk * per_chunk]) %
                    vmsdk::kHugePageSize,
                0u);
    }
    // Elements within a chunk are contiguous.
    EXPECT_EQ(array[per_chunk - 1] + kElementSize,
              array[0] + per_chunk * kElementSize);
    VerifyElements(array, count);
  }
  EXPECT_EQ(external_bytes, 0u);
  EXPECT_EQ(vmsdk::GetUsedMemoryCnt(), used_before);
}

TEST_F(ChunkedArrayTest, ElementLargerThanHugePage) {
  InstallExternalMemoryApis();
  const size_t big = vmsdk::kHugePageSize + 1;
  ChunkedArray array(big, kElementsPerChunk, 2);
  EXPECT_EQ(array.getSizePerChunk(), 2 * vmsdk::kHugePageSize);
  EXPECT_EQ(external_bytes, 4 * vmsdk::kHugePageSize);
  memset(array[1], 0x5a, big);
}

TEST_F(ChunkedArrayTest, FallbackWhenApisMissing) {
  ASSERT_FALSE(vmsdk::UseHugePageAllocation());
  ChunkedArray array(kElementSize, kElementsPerChunk, 2 * kElementsPerChunk);
  EXPECT_EQ(array.getSizePerChunk(), kElementsPerChunk * kElementSize);
  VerifyElements(array, 2 * kElementsPerChunk);
  EXPECT_EQ(external_calls, 0);
}

TEST_F(ChunkedArrayTest, FallbackWhenDisabled) {
  InstallExternalMemoryApis();
  vmsdk::SetHugePageAllocationEnabled(false);
  ASSERT_FALSE(vmsdk::UseHugePageAllocation());
  ChunkedArray array(kElementSize, kElementsPerChunk, 2 * kElementsPerChunk);
  EXPECT_EQ(array.getSizePerChunk(), kElementsPerChunk * kElementSize);
  VerifyElements(array, 2 * kElementsPerChunk);
  EXPECT_EQ(external_calls, 0);
}

// The allocation mode is fixed when the array is created, so toggling the
// config affects only arrays created afterwards.
TEST_F(ChunkedArrayTest, ModeFixedAtConstruction) {
  InstallExternalMemoryApis();
  {
    ChunkedArray array(kElementSize, kElementsPerChunk, 1);
    vmsdk::SetHugePageAllocationEnabled(false);
    array.resize(vmsdk::kHugePageSize / kElementSize + 1);
    EXPECT_EQ(external_bytes, 2 * vmsdk::kHugePageSize);
  }
  EXPECT_EQ(external_bytes, 0u);
}

// A failed growth leaves the array as it was: no chunks leak, the accounting
// is unchanged, and a later growth and the destructor still work.
TEST_F(ChunkedArrayTest, FailedGrowthRollsBack) {
  InstallExternalMemoryApis();
  const size_t per_chunk = vmsdk::kHugePageSize / kElementSize;
  const uint64_t used_before = vmsdk::GetUsedMemoryCnt();
  {
    ChunkedArray array(kElementSize, kElementsPerChunk, 1);
    incr_successes_left = 2;  // Two of the three new chunks succeed.
    EXPECT_THROW(array.resize(4 * per_chunk), std::bad_alloc);
    EXPECT_EQ(array.getCapacity(), 1u);
    EXPECT_EQ(external_bytes, vmsdk::kHugePageSize);
    EXPECT_EQ(vmsdk::GetUsedMemoryCnt() - used_before, vmsdk::kHugePageSize);

    incr_successes_left = -1;
    array.resize(4 * per_chunk);
    EXPECT_EQ(external_bytes, 4 * vmsdk::kHugePageSize);
    VerifyElements(array, 4 * per_chunk);
  }
  EXPECT_EQ(external_bytes, 0u);
  EXPECT_EQ(vmsdk::GetUsedMemoryCnt(), used_before);
}

// Consecutive mappings are placed next to each other so the kernel merges
// them, and freeing one from the middle still works.
TEST_F(ChunkedArrayTest, MappingsAreAdjacent) {
  InstallExternalMemoryApis();
  const size_t bytes = vmsdk::kHugePageSize;
  char *a = vmsdk::AllocateHugePages(bytes);
  char *b = vmsdk::AllocateHugePages(bytes);
  char *c = vmsdk::AllocateHugePages(bytes);
#ifdef MAP_FIXED_NOREPLACE
  EXPECT_EQ(b + bytes, a);
  EXPECT_EQ(c + bytes, b);
#endif
  for (char *p : {a, b, c}) {
    EXPECT_EQ(reinterpret_cast<uintptr_t>(p) % vmsdk::kHugePageSize, 0u);
    memset(p, 0x5a, bytes);
  }
  vmsdk::FreeHugePages(b, bytes);
  EXPECT_EQ(external_bytes, 2 * bytes);
  EXPECT_EQ(a[0], 0x5a);
  EXPECT_EQ(c[bytes - 1], 0x5a);
  vmsdk::FreeHugePages(a, bytes);
  vmsdk::FreeHugePages(c, bytes);
  EXPECT_EQ(external_bytes, 0u);
}

}  // namespace
