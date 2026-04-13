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

  EXPECT_DEATH(
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
  EXPECT_DEATH(
      [[maybe_unused]] auto res = FTInternalUpdateCmd(&fake_ctx_, argv, 4),
      "Internal update failure during AOF loading");

  for (int i = 0; i < 4; i++) {
    TestValkeyModule_FreeString(&fake_ctx_, argv[i]);
  }
}

// 4-arg call (no type name) is accepted; defaults to "vs_index_schema".
// Verify by checking we get a parse error (not a CHECK abort) with invalid
// metadata — proving the arg count check passed.
TEST_F(FTInternalUpdateTest, FourArgDefaultsToIndexSchema) {
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&fake_ctx_))
      .WillRepeatedly(testing::Return(0));

  ValkeyModuleString* argv[4];
  argv[0] =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.INTERNAL_UPDATE");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_id");
  argv[2] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "invalid");
  argv[3] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "invalid");

  // Should return a parse error, not abort — 4 args is valid.
  auto status = FTInternalUpdateCmd(&fake_ctx_, argv, 4);
  EXPECT_FALSE(status.ok());
  EXPECT_THAT(status.message(),
              testing::HasSubstr("Failed to parse GlobalMetadataEntry"));

  for (int i = 0; i < 4; i++) {
    TestValkeyModule_FreeString(&fake_ctx_, argv[i]);
  }
}

// 5-arg call with a trailing keyword but no value is accepted (the unpaired
// keyword is silently ignored). Verify by checking we get a parse error (not a
// CHECK abort) with invalid metadata — proving the arg count check passed.
TEST_F(FTInternalUpdateTest, FiveArgTrailingKeywordIgnored) {
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&fake_ctx_))
      .WillRepeatedly(testing::Return(0));

  ValkeyModuleString* argv[5];
  argv[0] =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.INTERNAL_UPDATE");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_id");
  argv[2] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "invalid");
  argv[3] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "invalid");
  argv[4] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "TYPE");

  // Should return a parse error, not abort — 5 args is valid (trailing
  // keyword without value is silently ignored).
  auto status = FTInternalUpdateCmd(&fake_ctx_, argv, 5);
  EXPECT_FALSE(status.ok());
  EXPECT_THAT(status.message(),
              testing::HasSubstr("Failed to parse GlobalMetadataEntry"));

  for (int i = 0; i < 5; i++) {
    TestValkeyModule_FreeString(&fake_ctx_, argv[i]);
  }
}

// 6-arg call with TYPE keyword sets the type name correctly.
// Verify by checking we get a parse error (not a CHECK abort) with invalid
// metadata — proving the arg count check and keyword parsing passed.
TEST_F(FTInternalUpdateTest, SixArgWithTypeKeyword) {
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&fake_ctx_))
      .WillRepeatedly(testing::Return(0));

  ValkeyModuleString* argv[6];
  argv[0] =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.INTERNAL_UPDATE");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_id");
  argv[2] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "invalid");
  argv[3] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "invalid");
  argv[4] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "TYPE");
  argv[5] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "vs_alias");

  // Should return a parse error, not abort — 6 args with TYPE keyword is valid.
  auto status = FTInternalUpdateCmd(&fake_ctx_, argv, 6);
  EXPECT_FALSE(status.ok());
  EXPECT_THAT(status.message(),
              testing::HasSubstr("Failed to parse GlobalMetadataEntry"));

  for (int i = 0; i < 6; i++) {
    TestValkeyModule_FreeString(&fake_ctx_, argv[i]);
  }
}

// Unknown keywords are silently ignored for forward compatibility.
TEST_F(FTInternalUpdateTest, UnknownKeywordSilentlyIgnored) {
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&fake_ctx_))
      .WillRepeatedly(testing::Return(0));

  ValkeyModuleString* argv[8];
  argv[0] =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.INTERNAL_UPDATE");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_id");
  argv[2] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "invalid");
  argv[3] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "invalid");
  argv[4] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FUTURE_FIELD");
  argv[5] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "some_value");
  argv[6] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "TYPE");
  argv[7] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "vs_alias");

  // Should return a parse error (invalid metadata), not abort — unknown
  // keywords are silently skipped.
  auto status = FTInternalUpdateCmd(&fake_ctx_, argv, 8);
  EXPECT_FALSE(status.ok());
  EXPECT_THAT(status.message(),
              testing::HasSubstr("Failed to parse GlobalMetadataEntry"));

  for (int i = 0; i < 8; i++) {
    TestValkeyModule_FreeString(&fake_ctx_, argv[i]);
  }
}

}  // namespace valkey_search
