/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/chunked_array.h"

#include <cstdint>
#include <cstring>
#include <stdexcept>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
namespace {

using ::testing::_;

constexpr size_t kHugePage = ChunkedArray::kHugePageBytes;

class ChunkedArrayTest : public ValkeyTest {
 protected:
  void SetUp() override {
    ValkeyTest::SetUp();
    default_slab_ = GetChunkedArraySlabSize().GetValue();
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

  void TearDown() override {
    VMSDK_EXPECT_OK(GetChunkedArraySlabSize().SetValue(default_slab_));
    ValkeyTest::TearDown();
  }

  void SetSlab(size_t bytes) {
    VMSDK_EXPECT_OK(GetChunkedArraySlabSize().SetValue(bytes));
  }

  long long default_slab_{0};
  int64_t external_{0};
};

TEST_F(ChunkedArrayTest, MaterializesInHugePagesAndReportsExternalMemory) {
  {
    ChunkedArray array(100);
    EXPECT_EQ(external_, 0);
    array.resize(1);
    EXPECT_EQ(external_, kHugePage);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(array[0]) % kHugePage, 0);
    array.resize(kHugePage / 100 + 1);
    EXPECT_EQ(external_, 2 * kHugePage);
    array.resize(1);
    EXPECT_EQ(external_, kHugePage);
  }
  EXPECT_EQ(external_, 0);
}

TEST_F(ChunkedArrayTest, DataSurvivesGrowthAcrossSegments) {
  SetSlab(kHugePage);
  constexpr size_t kElem = 1000;
  constexpr size_t kPerSegment = kHugePage / kElem;
  ChunkedArray array(kElem, 10);
  for (size_t i = 0; i < 10; ++i) {
    std::memset(array[i], static_cast<int>(i), kElem);
  }
  char *first = array[0];
  array.resize(3 * kPerSegment + 5);
  EXPECT_EQ(external_, 4 * kHugePage);
  EXPECT_EQ(array[0], first);
  for (size_t i = 0; i < 10; ++i) {
    EXPECT_EQ(array[i][kElem - 1], static_cast<char>(i));
  }
  // Every element is writable and lies wholly within its segment.
  for (size_t i = 0; i < array.getCapacity(); ++i) {
    auto offset = reinterpret_cast<uintptr_t>(array[i]) % kHugePage;
    EXPECT_LE(offset + kElem, kHugePage) << i;
    array[i][0] = 1;
    array[i][kElem - 1] = 1;
  }
  // The first element of each segment starts at the segment's aligned base.
  EXPECT_EQ(reinterpret_cast<uintptr_t>(array[kPerSegment]) % kHugePage, 0);
}

TEST_F(ChunkedArrayTest, ShrinkReleasesSegmentsAndCanRegrow) {
  SetSlab(2 * kHugePage);
  constexpr size_t kElem = 4096;
  constexpr size_t kPerSegment = 2 * kHugePage / kElem;
  ChunkedArray array(kElem, 3 * kPerSegment);
  EXPECT_EQ(external_, 6 * kHugePage);
  array.resize(kPerSegment / 2);
  EXPECT_EQ(external_, kHugePage);
  EXPECT_EQ(array.getCapacity(), kPerSegment / 2);
  array.clear();
  EXPECT_EQ(external_, 0);
  EXPECT_EQ(array.getCapacity(), 0);
  array.resize(2 * kPerSegment);
  EXPECT_EQ(external_, 4 * kHugePage);
  array[2 * kPerSegment - 1][kElem - 1] = 7;
  EXPECT_EQ(array[2 * kPerSegment - 1][kElem - 1], 7);
}

TEST_F(ChunkedArrayTest, ElementMustFitInSlab) {
  SetSlab(kHugePage);
  EXPECT_THROW(ChunkedArray(kHugePage + 1), std::length_error);
  EXPECT_THROW(ChunkedArray(0), std::length_error);
  ChunkedArray array(kHugePage, 3);
  EXPECT_EQ(external_, 3 * kHugePage);
}

TEST_F(ChunkedArrayTest, SlabSizeMustBeHugePageMultiple) {
  EXPECT_FALSE(GetChunkedArraySlabSize().SetValue(kHugePage + 4096).ok());
  EXPECT_FALSE(GetChunkedArraySlabSize().SetValue(0).ok());
  VMSDK_EXPECT_OK(GetChunkedArraySlabSize().SetValue(4 * kHugePage));
}

TEST_F(ChunkedArrayTest, NullExternalMemoryApiIsTolerated) {
  ValkeyModule_IncrExternalMemory = nullptr;
  ValkeyModule_DecrExternalMemory = nullptr;
  ChunkedArray array(64, 1000);
  array[999][0] = 1;
  array.clear();
}

}  // namespace
}  // namespace vmsdk
