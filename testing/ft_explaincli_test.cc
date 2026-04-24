#include <gtest/gtest.h>

#include "src/commands/commands.h"
#include "vmsdk/src/testing_infra/module.h"

namespace valkey_search {

class FTExplainCliTest : public ::testing::Test {
 protected:
  void SetUp() override { fake_ctx_.reply_capture.Clear(); }

  vmsdk::testing_infra::FakeValkeyModuleCtx fake_ctx_;
};

TEST_F(FTExplainCliTest, WrongNumberOfArguments) {
  std::vector<ValkeyModuleString*> cmd_argv = {
      ValkeyModule_CreateString(&fake_ctx_, "FT.EXPLAINCLI", 13),
      ValkeyModule_CreateString(&fake_ctx_, "myindex", 7)};

  auto status = FTExplainCliCmd(&fake_ctx_, cmd_argv.data(), cmd_argv.size());

  // Should return error for too few arguments (argc < 3)
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(status));
}

TEST_F(FTExplainCliTest, NonExistentIndex) {
  std::vector<ValkeyModuleString*> cmd_argv = {
      ValkeyModule_CreateString(&fake_ctx_, "FT.EXPLAINCLI", 13),
      ValkeyModule_CreateString(&fake_ctx_, "nonexistent", 11),
      ValkeyModule_CreateString(&fake_ctx_, "hello", 5)};

  auto status = FTExplainCliCmd(&fake_ctx_, cmd_argv.data(), cmd_argv.size());
  EXPECT_TRUE(status.ok());

  // Should reply with error for non-existent index
  auto replies = fake_ctx_.reply_capture.GetReplies();
  EXPECT_EQ(replies.size(), 1);
  EXPECT_TRUE(replies[0].error_value.has_value());
}

}  // namespace valkey_search