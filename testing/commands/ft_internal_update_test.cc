/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include <gtest/gtest.h>

#include "absl/status/status.h"
#include "src/commands/commands.h"
#include "src/metrics.h"
#include "src/valkey_search_options.h"
#include "testing/common.h"
#include "vmsdk/src/module_config.h"

namespace valkey_search {

class FTInternalUpdateTest : public ValkeySearchTest {
 protected:
  void SetUp() override {
    ValkeySearchTest::SetUp();
    // Ensure each test starts with the skip flag disabled (the default).
    VMSDK_EXPECT_OK(const_cast<vmsdk::config::Boolean&>(
                        options::GetSkipCorruptedInternalUpdateEntries())
                        .SetValue(false));
  }

  void TearDown() override {
    VMSDK_EXPECT_OK(const_cast<vmsdk::config::Boolean&>(
                        options::GetSkipCorruptedInternalUpdateEntries())
                        .SetValue(false));
    ValkeySearchTest::TearDown();
  }
};

TEST_F(FTInternalUpdateTest, WrongArguments) {
  ValkeyModuleString* argv[2];
  argv[0] =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.INTERNAL_UPDATE");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_id");

  VMSDK_EXPECT_DEATH(
      [[maybe_unused]] auto res = FTInternalUpdateCmd(&fake_ctx_, argv, 2),
      "FT.INTERNAL_UPDATE called with wrong argument count: 2");

  TestValkeyModule_FreeString(&fake_ctx_, argv[0]);
  TestValkeyModule_FreeString(&fake_ctx_, argv[1]);
}

TEST_F(FTInternalUpdateTest, ParseErrorMetadata) {
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&fake_ctx_))
      .WillRepeatedly(testing::Return(0));

  ValkeyModuleString* argv[4];
  argv[0] =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.INTERNAL_UPDATE");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_id");
  argv[2] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "invalid");
  argv[3] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "invalid");

  auto status = FTInternalUpdateCmd(&fake_ctx_, argv, 4);
  EXPECT_FALSE(status.ok());
  EXPECT_THAT(status.message(),
              testing::HasSubstr("Failed to parse GlobalMetadataEntry"));

  for (int i = 0; i < 4; i++) {
    TestValkeyModule_FreeString(&fake_ctx_, argv[i]);
  }
}

// A corrupt entry during loading with the skip flag disabled (the default)
// must NOT abort the process. It returns a recoverable DataLossError that
// points at the skip flag.
TEST_F(FTInternalUpdateTest, ParseErrorWhileLoadingReturnsErrorNoCrash) {
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&fake_ctx_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_CTX_FLAGS_LOADING));

  ValkeyModuleString* argv[4];
  argv[0] =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.INTERNAL_UPDATE");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_id");
  argv[2] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "invalid");
  argv[3] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "invalid");

  auto status = FTInternalUpdateCmd(&fake_ctx_, argv, 4);
  EXPECT_EQ(status.code(), absl::StatusCode::kDataLoss);
  EXPECT_THAT(status.message(),
              testing::HasSubstr("skip-corrupted-internal-update-entries"));

  for (int i = 0; i < 4; i++) {
    TestValkeyModule_FreeString(&fake_ctx_, argv[i]);
  }
}

// With the skip flag enabled, a corrupt entry during loading is skipped: the
// command returns OK, increments the skipped metric, and does not crash.
TEST_F(FTInternalUpdateTest, ParseErrorWhileLoadingSkippedWhenConfigured) {
  VMSDK_EXPECT_OK(const_cast<vmsdk::config::Boolean&>(
                      options::GetSkipCorruptedInternalUpdateEntries())
                      .SetValue(true));
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&fake_ctx_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_CTX_FLAGS_LOADING));

  auto skipped_before =
      Metrics::GetStats().ft_internal_update_skipped_entries_cnt.load();

  ValkeyModuleString* argv[4];
  argv[0] =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.INTERNAL_UPDATE");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_id");
  argv[2] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "invalid");
  argv[3] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "invalid");

  auto status = FTInternalUpdateCmd(&fake_ctx_, argv, 4);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(Metrics::GetStats().ft_internal_update_skipped_entries_cnt.load(),
            skipped_before + 1);

  for (int i = 0; i < 4; i++) {
    TestValkeyModule_FreeString(&fake_ctx_, argv[i]);
  }
}

// A valid entry replayed while loading with no coordinator (MetadataManager
// uninitialized -- the standalone default) is a misconfiguration. It must not
// crash on the uninitialized singleton; it returns a recoverable error.
TEST_F(FTInternalUpdateTest, ValidEntryWhileLoadingWithoutCoordinatorNoCrash) {
  ASSERT_FALSE(coordinator::MetadataManager::IsInitialized());
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&fake_ctx_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_CTX_FLAGS_LOADING));

  // Empty strings parse as valid (empty) protobufs.
  ValkeyModuleString* argv[4];
  argv[0] =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.INTERNAL_UPDATE");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_id");
  argv[2] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "");
  argv[3] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "");

  auto status = FTInternalUpdateCmd(&fake_ctx_, argv, 4);
  EXPECT_FALSE(status.ok());

  for (int i = 0; i < 4; i++) {
    TestValkeyModule_FreeString(&fake_ctx_, argv[i]);
  }
}

// The same misconfiguration is skippable: with the skip flag set, a loading
// node without a coordinator returns OK instead of erroring.
TEST_F(FTInternalUpdateTest, WithoutCoordinatorWhileLoadingSkippable) {
  ASSERT_FALSE(coordinator::MetadataManager::IsInitialized());
  VMSDK_EXPECT_OK(const_cast<vmsdk::config::Boolean&>(
                      options::GetSkipCorruptedInternalUpdateEntries())
                      .SetValue(true));
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&fake_ctx_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_CTX_FLAGS_LOADING));

  ValkeyModuleString* argv[4];
  argv[0] =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.INTERNAL_UPDATE");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_id");
  argv[2] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "");
  argv[3] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "");

  auto status = FTInternalUpdateCmd(&fake_ctx_, argv, 4);
  EXPECT_TRUE(status.ok());

  for (int i = 0; i < 4; i++) {
    TestValkeyModule_FreeString(&fake_ctx_, argv[i]);
  }
}

TEST_F(FTInternalUpdateTest, TooManyArguments) {
  ValkeyModuleString* argv[5];
  argv[0] =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.INTERNAL_UPDATE");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_id");
  argv[2] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "data1");
  argv[3] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "data2");
  argv[4] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "extra");

  VMSDK_EXPECT_DEATH(
      [[maybe_unused]] auto res = FTInternalUpdateCmd(&fake_ctx_, argv, 5),
      "FT.INTERNAL_UPDATE called with wrong argument count: 5");

  for (int i = 0; i < 5; i++) {
    TestValkeyModule_FreeString(&fake_ctx_, argv[i]);
  }
}

}  // namespace valkey_search
