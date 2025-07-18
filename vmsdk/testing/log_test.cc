/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
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

class LogTest : public vmsdk::ValkeyTest {
 protected:
  void SetUp() override {
    SetSinkFormatter(nullptr);
    ValkeyTest::SetUp();
  }
};

TEST_F(LogTest, WithInitValue) {
  ValkeyModuleCtx ctx;
  VMSDK_EXPECT_OK(InitLogging(&ctx, "VERBOSE"));
  StreamMessageEvalCnt stream_eval_cnt("hello");
  EXPECT_CALL(
      *kMockValkeyModule,
      Log(&ctx, testing::StrEq(VALKEYMODULE_LOGLEVEL_WARNING), testing::_))
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
#ifdef BROKEN_UNIT_TEST
TEST_F(LogTest, SinkOptions) {
  ValkeyModuleCtx ctx;
  VMSDK_EXPECT_OK(InitLogging(&ctx, "DEBUG"));
  SetSinkFormatter(CustomSinkFormatter);
  {
    ThreadPool thread_pool("test-pool-", 5);
    thread_pool.StartWorkers();
    ValkeyModuleCtx fake_ctxes[10];
    for (int i = 0; i < 10; ++i) {
      EXPECT_CALL(
          *kMockValkeyModule,
          Log(&fake_ctxes[i], testing::StrEq(VALKEYMODULE_LOGLEVEL_WARNING),
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
#endif
TEST_F(LogTest, WithoutInitValue) {
  ValkeyModuleCtx ctx;
  EXPECT_CALL(*kMockValkeyModule,
              Call(&ctx, testing::StrEq("CONFIG"), testing::StrEq("cc"),
                   testing::StrEq("GET"), testing::StrEq("loglevel")))
      .WillOnce([](ValkeyModuleCtx* ctx, const char* cmd, const char* fmt,
                   const char* arg1,
                   const char* arg2) -> ValkeyModuleCallReply* {
        auto reply = new ValkeyModuleCallReply;
        reply->msg = VALKEYMODULE_LOGLEVEL_NOTICE;
        EXPECT_CALL(*kMockValkeyModule, FreeCallReply(reply))
            .WillOnce([](ValkeyModuleCallReply* reply) { delete reply; });
        EXPECT_CALL(*kMockValkeyModule, CallReplyArrayElement(reply, 1))
            .WillOnce(testing::Return(reply));
        EXPECT_CALL(*kMockValkeyModule, CallReplyType(reply))
            .WillOnce(testing::Return(VALKEYMODULE_REPLY_STRING));
        EXPECT_CALL(*kMockValkeyModule, CallReplyStringPtr(reply, testing::_))
            .WillOnce([](ValkeyModuleCallReply* reply, size_t* len) {
              *len = reply->msg.size();
              return reply->msg.c_str();
            });
        return reply;
      });
  VMSDK_EXPECT_OK(InitLogging(&ctx, std::nullopt));
  StreamMessageEvalCnt stream_eval_cnt("hello");

  EXPECT_CALL(
      *kMockValkeyModule,
      Log(&ctx, testing::StrEq(VALKEYMODULE_LOGLEVEL_NOTICE), testing::_));
  VMSDK_LOG(NOTICE, &ctx) << "s1, expected" << stream_eval_cnt;
  VMSDK_LOG(DEBUG, &ctx) << "s2, not expected! " << stream_eval_cnt;
  VMSDK_LOG(DEBUG, &ctx) << "s3, not expected! " << stream_eval_cnt;
  EXPECT_EQ(stream_eval_cnt.cnt_, 1);
}

TEST_F(LogTest, WithoutInitValueConfigGetError) {
  ValkeyModuleCtx ctx;
  EXPECT_CALL(*kMockValkeyModule,
              Call(&ctx, testing::StrEq("CONFIG"), testing::StrEq("cc"),
                   testing::StrEq("GET"), testing::StrEq("loglevel")))
      .WillOnce(
          [](ValkeyModuleCtx* ctx, const char* cmd, const char* fmt,
             const char* arg1,
             const char* arg2) -> ValkeyModuleCallReply* { return nullptr; });
  VMSDK_EXPECT_OK(InitLogging(&ctx, std::nullopt));
  StreamMessageEvalCnt stream_eval_cnt("hello");

  EXPECT_CALL(
      *kMockValkeyModule,
      Log(&ctx, testing::StrEq(VALKEYMODULE_LOGLEVEL_NOTICE), testing::_));
  VMSDK_LOG(NOTICE, &ctx) << "s1, expected" << stream_eval_cnt;
  VMSDK_LOG(DEBUG, &ctx) << "s2, not expected! " << stream_eval_cnt;
  VMSDK_LOG(DEBUG, &ctx) << "s3, not expected! " << stream_eval_cnt;
  EXPECT_EQ(stream_eval_cnt.cnt_, 1);
}

TEST_F(LogTest, IOWithInitValue) {
  ValkeyModuleCtx ctx;
  ValkeyModuleIO io;
  VMSDK_EXPECT_OK(InitLogging(&ctx, "VERBOSE"));
  StreamMessageEvalCnt stream_eval_cnt("hello");

  EXPECT_CALL(*kMockValkeyModule, LogIOError(&io, testing::_, testing::_))
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
