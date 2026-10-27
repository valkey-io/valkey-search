/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/huge/huge_page_slab.h"

#include <cstdint>
#include <cstring>

#include "absl/status/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk::huge_page_slab {
namespace {

using ::testing::_;

constexpr size_t kHugePage = kHugePageBytes;

class HugePageSlabTest : public ValkeyTest {
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

TEST_F(HugePageSlabTest, RoundUpToHugePage) {
  EXPECT_EQ(RoundUpToHugePage(0), 0);
  EXPECT_EQ(RoundUpToHugePage(1), kHugePage);
  EXPECT_EQ(RoundUpToHugePage(kHugePage), kHugePage);
  EXPECT_EQ(RoundUpToHugePage(kHugePage + 1), 2 * kHugePage);
}

TEST_F(HugePageSlabTest, ReserveIsAlignedAndReportsNothing) {
  auto base = ReserveRegion(4 * kHugePage);
  ASSERT_TRUE(base.ok()) << base.status();
  EXPECT_EQ(reinterpret_cast<uintptr_t>(*base) % kHugePage, 0);
  EXPECT_EQ(external_, 0);
  UnreserveRegion(*base, 4 * kHugePage);
}

TEST_F(HugePageSlabTest, MaterializeAndReleaseReportExactly) {
  constexpr size_t kBytes = 3 * kHugePage;
  auto base = ReserveRegion(kBytes);
  ASSERT_TRUE(base.ok()) << base.status();
  VMSDK_EXPECT_OK(Materialize(*base, kBytes));
  EXPECT_EQ(external_, kBytes);
  std::memset(*base, 0x5a, kBytes);
  EXPECT_EQ((*base)[kBytes - 1], 0x5a);
  Release(*base, kBytes);
  EXPECT_EQ(external_, 0);
  UnreserveRegion(*base, kBytes);
}

TEST_F(HugePageSlabTest, ReleaseThenRematerialize) {
  constexpr size_t kBytes = 2 * kHugePage;
  auto base = ReserveRegion(kBytes);
  ASSERT_TRUE(base.ok()) << base.status();
  VMSDK_EXPECT_OK(Materialize(*base + kHugePage, kHugePage));
  (*base)[kHugePage] = 1;
  Release(*base + kHugePage, kHugePage);
  VMSDK_EXPECT_OK(Materialize(*base, kBytes));
  EXPECT_EQ(external_, kBytes);
  // Released pages come back zero-filled.
  EXPECT_EQ((*base)[kHugePage], 0);
  (*base)[kBytes - 1] = 7;
  Release(*base, kBytes);
  EXPECT_EQ(external_, 0);
  UnreserveRegion(*base, kBytes);
}

TEST_F(HugePageSlabTest, ImpossibleReserveReturnsError) {
  auto base = ReserveRegion(size_t{1} << 62);
  EXPECT_EQ(base.status().code(), absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(external_, 0);
}

TEST_F(HugePageSlabTest, NullExternalMemoryApiIsTolerated) {
  ValkeyModule_IncrExternalMemory = nullptr;
  ValkeyModule_DecrExternalMemory = nullptr;
  auto base = ReserveRegion(kHugePage);
  ASSERT_TRUE(base.ok()) << base.status();
  VMSDK_EXPECT_OK(Materialize(*base, kHugePage));
  (*base)[0] = 1;
  Release(*base, kHugePage);
  UnreserveRegion(*base, kHugePage);
}

}  // namespace
}  // namespace vmsdk::huge_page_slab
