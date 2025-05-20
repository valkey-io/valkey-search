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
  vmsdk::config::ModuleConfigManager::Instance()
      .RegisterAll(&fake_ctx)
      .IgnoreError();
}

TEST(Builder, WithModifyCallback) {
  size_t num_modify_calls = 0;
  auto num_modify_cb = [&num_modify_calls]([[maybe_unused]] int64_t new_value) {
    num_modify_calls++;
  };
  auto number_config =
      config::Builder<config::Number, long long>("number", 42, 0, 1024)
          .WithModifyCallback(num_modify_cb)
          .Build();

  EXPECT_EQ(42, number_config->GetValue());
  number_config->SetValue(41);
  EXPECT_EQ(41, number_config->GetValue());
  EXPECT_EQ(1, num_modify_calls);
}

TEST(Builder, WithModifyAndValidationCallbackAndFlags) {
  size_t num_modify_calls = 0;
  size_t num_valid_calls = 0;
  auto num_modify_cb =
      [&num_modify_calls]([[maybe_unused]] long long new_value) {
        num_modify_calls++;
      };
  auto validation_cb =
      [&num_valid_calls]([[maybe_unused]] long long new_value) -> bool {
    num_valid_calls++;
    return true;
  };
  auto number_config =
      config::Builder<config::Number, long long>("number", 42, 0, 1024)
          .WithModifyCallback(num_modify_cb)
          .WithValidationCallback(validation_cb)
          .WithFlags(config::Flags::kDefault)
          .Build();

  EXPECT_EQ(42, number_config->GetValue());
  number_config->SetValue(41);

  EXPECT_EQ(41, number_config->GetValue());

  // Make sure that both callbacks were called
  EXPECT_EQ(1, num_modify_calls);
  EXPECT_EQ(1, num_valid_calls);
}

TEST(Config, VetoChanges) {
  auto validation_cb = []([[maybe_unused]] long long new_value) -> bool {
    return false;
  };
  auto number_config =
      config::Builder<config::Number, long long>("number", 42, 0, 1024)
          .WithValidationCallback(validation_cb)
          .Build();

  EXPECT_EQ(42, number_config->GetValue());
  number_config->SetValue(41);
  // Change was vetoed, so it should still be 41
  EXPECT_EQ(42, number_config->GetValue());
}

}  // namespace
}  // namespace vmsdk
