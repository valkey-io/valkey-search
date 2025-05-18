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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace vmsdk {

namespace {

using ::testing::_;
using ::testing::Eq;
using ::testing::StrEq;

class ConfigTest : public vmsdk::RedisTest {
 protected:
  RedisModuleCtx fake_ctx;
  void TearDown() override { vmsdk::RedisTest::TearDown(); }
};

TEST_F(ConfigTest, registration) {
  vmsdk::config::Number number("number", 42, 0, 1024);
  vmsdk::config::Boolean boolean("boolean", true);

  // 2 integer registration
  EXPECT_CALL(*kMockRedisModule,
              RegisterNumericConfig(&fake_ctx, StrEq("number"), Eq(42), _,
                                    Eq(0), Eq(1024), _, _, _, Eq(&number)))
      .Times(testing::AtLeast(1));

  EXPECT_CALL(*kMockRedisModule,
              RegisterBoolConfig(&fake_ctx, StrEq("boolean"), Eq(1), _, _, _, _,
                                 Eq(&boolean)))
      .Times(testing::AtLeast(1));
  vmsdk::config::ModuleConfigManager::Instance().Init(&fake_ctx).IgnoreError();
}

}  // namespace
}  // namespace vmsdk