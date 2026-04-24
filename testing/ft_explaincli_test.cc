/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "gtest/gtest.h"
#include "src/commands/commands.h"
#include "testing/common.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

class FTExplainCliTest : public ValkeySearchTest {};

TEST_F(FTExplainCliTest, WrongNumberOfArguments) {
  std::vector<ValkeyModuleString *> cmd_argv = {
      ValkeyModule_CreateString(&fake_ctx_, "FT.EXPLAINCLI", 13),
      ValkeyModule_CreateString(&fake_ctx_, "myindex", 7)};
  auto status = FTExplainCliCmd(&fake_ctx_, cmd_argv.data(), cmd_argv.size());
  // Should return error for too few arguments (argc < 3)
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(status));
}

TEST_F(FTExplainCliTest, NonExistentIndex) {
  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillRepeatedly(testing::Return(0));
  std::vector<ValkeyModuleString *> cmd_argv = {
      ValkeyModule_CreateString(&fake_ctx_, "FT.EXPLAINCLI", 13),
      ValkeyModule_CreateString(&fake_ctx_, "nonexistent", 11),
      ValkeyModule_CreateString(&fake_ctx_, "hello", 5)};
  auto status = FTExplainCliCmd(&fake_ctx_, cmd_argv.data(), cmd_argv.size());
  // Should return error for non-existent index
  EXPECT_FALSE(status.ok());
}

TEST_F(FTExplainCliTest, UnknownArgument) {
  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillRepeatedly(testing::Return(0));
  std::vector<ValkeyModuleString *> cmd_argv = {
      ValkeyModule_CreateString(&fake_ctx_, "FT.EXPLAINCLI", 13),
      ValkeyModule_CreateString(&fake_ctx_, "myindex", 7),
      ValkeyModule_CreateString(&fake_ctx_, "hello", 5),
      ValkeyModule_CreateString(&fake_ctx_, "BADARG", 6)};
  auto status = FTExplainCliCmd(&fake_ctx_, cmd_argv.data(), cmd_argv.size());
  // Should return error for unknown optional argument
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(status));
}

}  // namespace

}  // namespace valkey_search
