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

#include "vmsdk/src/log.h"

#include <atomic>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <optional>
#include <ostream>
#include <string>

#include "absl/log/log_entry.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

namespace {

class StreamMessageEvalCnt {
 public:
  StreamMessageEvalCnt(const std::string& str) : data_(str) {}

  friend std::ostream& operator<<(std::ostream& os,
                                  const StreamMessageEvalCnt& obj) {
    os << obj.data_;
    ++obj.cnt_;
    return os;
  }
  mutable int cnt_{0};
  std::string data_;
};

class LogTest : public vmsdk::RedisTest {
 protected:
  void SetUp() override {
    SetSinkFormatter(nullptr);
    RedisTest::SetUp();
  }
};

TEST_F(LogTest, WithInitValue) {
  RedisModuleCtx ctx;
  VMSDK_EXPECT_OK(InitLogging(&ctx, "VERBOSE"));
  StreamMessageEvalCnt stream_eval_cnt("hello");
  EXPECT_CALL(
      *kMockRedisModule,
      Log(&ctx, testing::StrEq(REDISMODULE_LOGLEVEL_WARNING), testing::_))
      .Times(6);
  VMSDK_LOG(NOTICE, &ctx) << "s1, expected" << stream_eval_cnt;
  for (int i = 0; i < 9; ++i) {
    VMSDK_LOG_EVERY_N(VERBOSE, &ctx, 5) << "s2, expected " << stream_eval_cnt;
  }
  for (int i = 0; i < 5; ++i) {
    VMSDK_LOG_EVERY_N_SEC(VERBOSE, &ctx, 0.1)
        << "s2, expected " << stream_eval_cnt;
    absl::SleepFor(absl::Milliseconds(50));
  }
  VMSDK_LOG(DEBUG, &ctx) << "s3, not expected! " << stream_eval_cnt;
  EXPECT_EQ(stream_eval_cnt.cnt_, 6);
}
std::atomic<int> custom_formatter_used;
std::string CustomSinkFormatter(const absl::LogEntry& entry) {
  ++custom_formatter_used;
  EXPECT_EQ(entry.verbosity(), static_cast<int>(LogLevel::kNotice));
  return std::string("CustomSinkFormatter");
}

TEST_F(LogTest, SinkOptions) {
  RedisModuleCtx ctx;
  VMSDK_EXPECT_OK(InitLogging(&ctx, "DEBUG"));
  SetSinkFormatter(CustomSinkFormatter);
  {
    ThreadPool thread_pool("test-pool-", 5);
    thread_pool.StartWorkers();
    RedisModuleCtx fake_ctxes[10];
    for (int i = 0; i < 10; ++i) {
      EXPECT_CALL(
          *kMockRedisModule,
          Log(&fake_ctxes[i], testing::StrEq(REDISMODULE_LOGLEVEL_WARNING),
              testing::_));
      thread_pool.Schedule(
          [&fake_ctxes, i]() mutable {
            VMSDK_LOG(NOTICE, &fake_ctxes[i]) << "s1, expected";
          },
          ThreadPool::Priority::kHigh);
    }
    thread_pool.JoinWorkers();
  }

  EXPECT_EQ(custom_formatter_used, 10);
}

TEST_F(LogTest, WithoutInitValue) {
  RedisModuleCtx ctx;
  EXPECT_CALL(*kMockRedisModule,
              Call(&ctx, testing::StrEq("CONFIG"), testing::StrEq("cc"),
                   testing::StrEq("GET"), testing::StrEq("loglevel")))
      .WillOnce([](RedisModuleCtx* ctx, const char* cmd, const char* fmt,
                   const char* arg1,
                   const char* arg2) -> RedisModuleCallReply* {
        auto reply = new RedisModuleCallReply;
        reply->msg = REDISMODULE_LOGLEVEL_NOTICE;
        EXPECT_CALL(*kMockRedisModule, FreeCallReply(reply))
            .WillOnce([](RedisModuleCallReply* reply) { delete reply; });
        EXPECT_CALL(*kMockRedisModule, CallReplyArrayElement(reply, 1))
            .WillOnce(testing::Return(reply));
        EXPECT_CALL(*kMockRedisModule, CallReplyType(reply))
            .WillOnce(testing::Return(REDISMODULE_REPLY_STRING));
        EXPECT_CALL(*kMockRedisModule, CallReplyStringPtr(reply, testing::_))
            .WillOnce([](RedisModuleCallReply* reply, size_t* len) {
              *len = reply->msg.size();
              return reply->msg.c_str();
            });
        return reply;
      });
  VMSDK_EXPECT_OK(InitLogging(&ctx, std::nullopt));
  StreamMessageEvalCnt stream_eval_cnt("hello");

  EXPECT_CALL(
      *kMockRedisModule,
      Log(&ctx, testing::StrEq(REDISMODULE_LOGLEVEL_NOTICE), testing::_));
  VMSDK_LOG(NOTICE, &ctx) << "s1, expected" << stream_eval_cnt;
  VMSDK_LOG(DEBUG, &ctx) << "s2, not expected! " << stream_eval_cnt;
  VMSDK_LOG(DEBUG, &ctx) << "s3, not expected! " << stream_eval_cnt;
  EXPECT_EQ(stream_eval_cnt.cnt_, 1);
}

TEST_F(LogTest, WithoutInitValueConfigGetError) {
  RedisModuleCtx ctx;
  EXPECT_CALL(*kMockRedisModule,
              Call(&ctx, testing::StrEq("CONFIG"), testing::StrEq("cc"),
                   testing::StrEq("GET"), testing::StrEq("loglevel")))
      .WillOnce(
          [](RedisModuleCtx* ctx, const char* cmd, const char* fmt,
             const char* arg1,
             const char* arg2) -> RedisModuleCallReply* { return nullptr; });
  VMSDK_EXPECT_OK(InitLogging(&ctx, std::nullopt));
  StreamMessageEvalCnt stream_eval_cnt("hello");

  EXPECT_CALL(
      *kMockRedisModule,
      Log(&ctx, testing::StrEq(REDISMODULE_LOGLEVEL_NOTICE), testing::_));
  VMSDK_LOG(NOTICE, &ctx) << "s1, expected" << stream_eval_cnt;
  VMSDK_LOG(DEBUG, &ctx) << "s2, not expected! " << stream_eval_cnt;
  VMSDK_LOG(DEBUG, &ctx) << "s3, not expected! " << stream_eval_cnt;
  EXPECT_EQ(stream_eval_cnt.cnt_, 1);
}

TEST_F(LogTest, IOWithInitValue) {
  RedisModuleCtx ctx;
  RedisModuleIO io;
  VMSDK_EXPECT_OK(InitLogging(&ctx, "VERBOSE"));
  StreamMessageEvalCnt stream_eval_cnt("hello");

  EXPECT_CALL(*kMockRedisModule, LogIOError(&io, testing::_, testing::_))
      .Times(6);
  VMSDK_IO_LOG(NOTICE, &io) << "s1, expected" << stream_eval_cnt;
  for (int i = 0; i < 9; ++i) {
    VMSDK_IO_LOG_EVERY_N(VERBOSE, &io, 5) << "s2, expected " << stream_eval_cnt;
  }
  for (int i = 0; i < 5; ++i) {
    VMSDK_IO_LOG_EVERY_N_SEC(VERBOSE, &io, 0.1)
        << "s2, expected " << stream_eval_cnt;
    absl::SleepFor(absl::Milliseconds(50));
  }
  VMSDK_IO_LOG(DEBUG, &io) << "s3, not expected! " << stream_eval_cnt;
  EXPECT_EQ(stream_eval_cnt.cnt_, 6);
}

}  // namespace

}  // namespace vmsdk
