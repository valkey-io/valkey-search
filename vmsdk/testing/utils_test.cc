/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/utils.h"

#include <string>

#include "absl/synchronization/blocking_counter.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

namespace {

class UtilsTest : public vmsdk::ValkeyTest {};

TEST_F(UtilsTest, RunByMain) {
  absl::BlockingCounter blocking_refcount(1);
  ThreadPool thread_pool("test-pool", 1);
  thread_pool.StartWorkers();
  ValkeyModuleEventLoopOneShotFunc captured_callback;
  void* captured_data;
  EXPECT_CALL(*kMockValkeyModule, EventLoopAddOneShot(testing::_, testing::_))
      .WillOnce([&](ValkeyModuleEventLoopOneShotFunc callback, void* data) {
        captured_callback = callback;
        captured_data = data;
        blocking_refcount.DecrementCount();
        return 0;
      });
  bool run = false;
  EXPECT_TRUE(thread_pool.Schedule(
      [&]() {
        RunByMain([&run] {
          EXPECT_TRUE(IsMainThread());
          run = true;
        });
      },
      ThreadPool::Priority::kLow));
  blocking_refcount.Wait();
  captured_callback(captured_data);
  EXPECT_TRUE(run);
  thread_pool.JoinWorkers();
}

TEST_F(UtilsTest, RunByMainWhileInMain) {
  absl::BlockingCounter blocking_refcount(1);
  EXPECT_CALL(*kMockValkeyModule, EventLoopAddOneShot(testing::_, testing::_))
      .Times(0);
  bool run = false;
  RunByMain([&blocking_refcount, &run] {
    EXPECT_TRUE(IsMainThread());
    blocking_refcount.DecrementCount();
    run = true;
  });
  blocking_refcount.Wait();
  EXPECT_TRUE(run);
}

TEST_F(UtilsTest, ParseTag) {
  struct {
    std::string str;
    std::optional<absl::string_view> expected;
  } test_cases[] = {
      {"", std::nullopt},   {"{", std::nullopt},    {"}", std::nullopt},
      {"{{", std::nullopt}, {"{a", std::nullopt},   {"{a}", "a"},
      {"a{b}", "b"},        {"}{", std::nullopt},   {"}{a}", "a"},
      {"{}", std::nullopt}, {"abc{cde}xyz", "cde"}, {"ab{c}{d}{e}", "c"},

  };
  for (auto& tc : test_cases) {
    auto actual = ParseHashTag(tc.str);
    EXPECT_EQ(actual, tc.expected);
  }
}

TEST_F(UtilsTest, MultiOrLua) {
  ValkeyModuleCtx fake_ctx;
  {
    EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&fake_ctx))
        .WillRepeatedly(testing::Return(0));
    EXPECT_FALSE(MultiOrLua(&fake_ctx));
  }
  {
    EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&fake_ctx))
        .WillRepeatedly(testing::Return(VALKEYMODULE_CTX_FLAGS_MULTI));
    EXPECT_TRUE(MultiOrLua(&fake_ctx));
  }
  {
    EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&fake_ctx))
        .WillRepeatedly(testing::Return(VALKEYMODULE_CTX_FLAGS_LUA));
    EXPECT_TRUE(MultiOrLua(&fake_ctx));
  }
}

TEST_F(UtilsTest, IsRealUserClient) {
  ValkeyModuleCtx fake_ctx;
  {
    EXPECT_CALL(*kMockValkeyModule, GetClientId(&fake_ctx))
        .WillRepeatedly(testing::Return(1));
    EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&fake_ctx))
        .WillRepeatedly(testing::Return(0));
    EXPECT_TRUE(IsRealUserClient(&fake_ctx));
  }
  {
    EXPECT_CALL(*kMockValkeyModule, GetClientId(&fake_ctx))
        .WillRepeatedly(testing::Return(0));
    EXPECT_FALSE(IsRealUserClient(&fake_ctx));
  }
  {
    EXPECT_CALL(*kMockValkeyModule, GetClientId(&fake_ctx))
        .WillRepeatedly(testing::Return(1));
    EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&fake_ctx))
        .WillRepeatedly(testing::Return(VALKEYMODULE_CTX_FLAGS_REPLICATED));
    EXPECT_FALSE(IsRealUserClient(&fake_ctx));
  }
}

TEST_F(UtilsTest, DisplayAsSIBytes) {
  std::vector<std::pair<size_t, std::string>> testcases{
    {0.0, "0"},
    {1023, "1023"},
    {1ull << 10, "1.00KiB"},
    {1ull << 20, "1.00MiB"},
    {1ull << 30, "1.00GiB"},
    {1ull << 40, "1.00TiB"},
    {1ull << 50, "1.00PiB"}
  };
  for (auto& [value, expected] : testcases) {
    char buffer[100];
    size_t bytes = DisplayAsSIBytes(value, buffer, sizeof(buffer));
    EXPECT_EQ(expected, std::string(buffer));
    std::memset(buffer, -1, sizeof(buffer));
    bytes = DisplayAsSIBytes(value, buffer, 1);
    EXPECT_EQ(buffer[0], 0);
    EXPECT_EQ(buffer[1], '\xff'); // untouched.
  }
}

TEST_F(UtilsTest, WrongArity) {
  // Test standard FT commands
  EXPECT_EQ(WrongArity("FT.INFO"), 
            "ERR wrong number of arguments for 'FT.INFO' command");
  EXPECT_EQ(WrongArity("FT.SEARCH"), 
            "ERR wrong number of arguments for 'FT.SEARCH' command");
  EXPECT_EQ(WrongArity("FT.DROPINDEX"), 
            "ERR wrong number of arguments for 'FT.DROPINDEX' command");
  EXPECT_EQ(WrongArity("FT._LIST"), 
            "ERR wrong number of arguments for 'FT._LIST' command");
  
  // Test edge cases
  EXPECT_EQ(WrongArity(""), 
            "ERR wrong number of arguments for '' command");
  EXPECT_EQ(WrongArity("SINGLE"), 
            "ERR wrong number of arguments for 'SINGLE' command");
  
  // Test with special characters
  EXPECT_EQ(WrongArity("CMD.WITH-DASH"), 
            "ERR wrong number of arguments for 'CMD.WITH-DASH' command");
  EXPECT_EQ(WrongArity("CMD_WITH_UNDERSCORE"), 
            "ERR wrong number of arguments for 'CMD_WITH_UNDERSCORE' command");
}

}  // namespace

}  // namespace vmsdk
