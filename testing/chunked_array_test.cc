/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <cstdint>
#include <cstring>

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

int FakeIncrExternalMemory(size_t bytes) {
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

}  // namespace
