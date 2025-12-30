/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include <gtest/gtest.h>

#include "src/commands/commands.h"
#include "testing/common.h"

namespace valkey_search {

class FTInternalUpdateTest : public ValkeySearchTest {};

TEST_F(FTInternalUpdateTest, WrongArguments) {
  ValkeyModuleString* argv[2];
  argv[0] =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.INTERNAL_UPDATE");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_id");

  auto status = FTInternalUpdateCmd(&fake_ctx_, argv, 2);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.message(),
            "ERR wrong number of arguments for FT_INTERNAL_UPDATE");

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
              testing::HasSubstr("ERR failed to parse GlobalMetadataEntry"));

  for (int i = 0; i < 4; i++) {
    TestValkeyModule_FreeString(&fake_ctx_, argv[i]);
  }
}

TEST_F(FTInternalUpdateTest, ParseErrorWithLoadingFlagCrashes) {
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&fake_ctx_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_CTX_FLAGS_LOADING));

  ValkeyModuleString* argv[4];
  argv[0] =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.INTERNAL_UPDATE");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_id");
  argv[2] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "invalid");
  argv[3] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "invalid");

  // With LOADING flag but skip disabled by default, should crash
  EXPECT_DEATH(FTInternalUpdateCmd(&fake_ctx_, argv, 4),
               "Protobuf parse failure during AOF loading");

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

  auto status = FTInternalUpdateCmd(&fake_ctx_, argv, 5);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.message(),
            "ERR wrong number of arguments for FT_INTERNAL_UPDATE");

  for (int i = 0; i < 5; i++) {
    TestValkeyModule_FreeString(&fake_ctx_, argv[i]);
  }
}

}  // namespace valkey_search
