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

#include "vmsdk/src/testing_infra/module.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

namespace {
#define INSTALLED_MODULES 3

class ModuleTest : public vmsdk::RedisTest {
 protected:
  RedisModuleCtx fake_ctx;
  RedisModuleCallReply json_key;
  RedisModuleCallReply json_value;
  RedisModuleCallReply some_key;
  RedisModuleCallReply reply_internal[INSTALLED_MODULES];
  void InstallCheckers();
};

void ModuleTest::InstallCheckers() {
  auto reply = new RedisModuleCallReply;
  EXPECT_CALL(*kMockRedisModule,
              Call(&fake_ctx, testing::StrEq("MODULE"), testing::StrEq("c"),
                   testing::StrEq("LIST")))
      .WillOnce(
          [reply](RedisModuleCtx* ctx, const char* cmd, const char* fmt,
                  const char* arg1) -> RedisModuleCallReply* { return reply; });
  EXPECT_CALL(*kMockRedisModule, FreeCallReply(reply))
      .WillOnce([](RedisModuleCallReply* reply) { delete reply; });

  EXPECT_CALL(*kMockRedisModule, CallReplyType(testing::_))
      .WillRepeatedly(
          [](RedisModuleCallReply* reply) { return REDISMODULE_REPLY_ARRAY; });
  EXPECT_CALL(*kMockRedisModule, CallReplyLength(testing::_))
      .WillRepeatedly([reply](RedisModuleCallReply* in_reply) -> size_t {
        if (reply == in_reply) {
          return INSTALLED_MODULES;
        }
        return 10;
      });

  EXPECT_CALL(*kMockRedisModule, CallReplyArrayElement(testing::_, testing::_))
      .WillRepeatedly([reply, this](RedisModuleCallReply* in_reply,
                                    size_t indx) -> RedisModuleCallReply* {
        if (reply == in_reply) {
          return &reply_internal[indx];
        }
        if (in_reply == &reply_internal[1]) {
          if (indx == 2) {
            return &json_key;
          } else if (indx == 3) {
            return &json_value;
          }
        }
        return &some_key;
      });

  EXPECT_CALL(*kMockRedisModule, CallReplyStringPtr(testing::_, testing::_))
      .WillRepeatedly(
          [&](RedisModuleCallReply* in_reply, size_t* len) -> const char* {
            if (in_reply == &json_key) {
              static const std::string str("name");
              *len = str.length();
              return str.c_str();
            }
            if (in_reply == &json_value) {
              static const std::string str("json");
              *len = str.length();
              return str.c_str();
            }
            static const std::string str("a_module_field");
            *len = str.length();
            return str.c_str();
          });
}

TEST_F(ModuleTest, ModuleLoaded) {
  InstallCheckers();
  EXPECT_TRUE(vmsdk::IsModuleLoaded(&fake_ctx, "json"));
  EXPECT_TRUE(vmsdk::IsModuleLoaded(&fake_ctx, "json"));
}

TEST_F(ModuleTest, ModuleNotLoaded) {
  InstallCheckers();
  EXPECT_FALSE(vmsdk::IsModuleLoaded(&fake_ctx, "json11"));
}

}  // namespace

}  // namespace vmsdk
