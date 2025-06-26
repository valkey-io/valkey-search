/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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

class UtilsTest : public vmsdk::RedisTest {};

TEST_F(UtilsTest, RunByMain) {
  absl::BlockingCounter blocking_refcount(1);
  ThreadPool thread_pool("test-pool", 1);
  thread_pool.StartWorkers();
  RedisModuleEventLoopOneShotFunc captured_callback;
  void* captured_data;
  EXPECT_CALL(*kMockRedisModule, EventLoopAddOneShot(testing::_, testing::_))
      .WillOnce([&](RedisModuleEventLoopOneShotFunc callback, void* data) {
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
  EXPECT_CALL(*kMockRedisModule, EventLoopAddOneShot(testing::_, testing::_))
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
  RedisModuleCtx fake_ctx;
  {
    EXPECT_CALL(*kMockRedisModule, GetContextFlags(&fake_ctx))
        .WillRepeatedly(testing::Return(0));
    EXPECT_FALSE(MultiOrLua(&fake_ctx));
  }
  {
    EXPECT_CALL(*kMockRedisModule, GetContextFlags(&fake_ctx))
        .WillRepeatedly(testing::Return(REDISMODULE_CTX_FLAGS_MULTI));
    EXPECT_TRUE(MultiOrLua(&fake_ctx));
  }
  {
    EXPECT_CALL(*kMockRedisModule, GetContextFlags(&fake_ctx))
        .WillRepeatedly(testing::Return(REDISMODULE_CTX_FLAGS_LUA));
    EXPECT_TRUE(MultiOrLua(&fake_ctx));
  }
}

TEST_F(UtilsTest, IsRealUserClient) {
  RedisModuleCtx fake_ctx;
  {
    EXPECT_CALL(*kMockRedisModule, GetClientId(&fake_ctx))
        .WillRepeatedly(testing::Return(1));
    EXPECT_CALL(*kMockRedisModule, GetContextFlags(&fake_ctx))
        .WillRepeatedly(testing::Return(0));
    EXPECT_TRUE(IsRealUserClient(&fake_ctx));
  }
  {
    EXPECT_CALL(*kMockRedisModule, GetClientId(&fake_ctx))
        .WillRepeatedly(testing::Return(0));
    EXPECT_FALSE(IsRealUserClient(&fake_ctx));
  }
  {
    EXPECT_CALL(*kMockRedisModule, GetClientId(&fake_ctx))
        .WillRepeatedly(testing::Return(1));
    EXPECT_CALL(*kMockRedisModule, GetContextFlags(&fake_ctx))
        .WillRepeatedly(testing::Return(REDISMODULE_CTX_FLAGS_REPLICATED));
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
    EXPECT_EQ(buffer[1], -1); // untouched.
  }
}

}  // namespace

}  // namespace vmsdk
