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

#include <absl/strings/match.h>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace vmsdk {

namespace {

inline void FreeRedisArgs(std::vector<RedisModuleString *> &args) {
  for (const auto &arg : args) {
    TestRedisModule_FreeString(nullptr, arg);
  }
}

using ::testing::_;
using ::testing::Eq;
using ::testing::StrEq;
using vmsdk::config::ModuleConfigManager;

class ConfigTest : public vmsdk::RedisTest {
 protected:
  RedisModuleCtx fake_ctx;
  void SetUp() override { vmsdk::RedisTest::SetUp(); }
  void TearDown() override { vmsdk::RedisTest::TearDown(); }
};

TEST_F(ConfigTest, registration) {
  vmsdk::config::Number number("number", 42, 0, 1024);
  vmsdk::config::Boolean boolean("boolean", true);

  // Expect hide-user-data-from-log to be registered (it's auto-registered by
  // the system)
  EXPECT_CALL(*kMockValkeyModule,
              RegisterBoolConfig(&fake_ctx, StrEq("hide-user-data-from-log"),
                                 Eq(1), _, _, _, _, _))
      .Times(testing::AtLeast(1));

  // 2 integer registration
  EXPECT_CALL(*kMockRedisModule,
              RegisterNumericConfig(&fake_ctx, StrEq("number"), Eq(42), _,
                                    Eq(0), Eq(1024), _, _, _, Eq(&number)))
      .Times(testing::AtLeast(1));

  EXPECT_CALL(*kMockRedisModule,
              RegisterBoolConfig(&fake_ctx, StrEq("boolean"), Eq(1), _, _, _, _,
                                 Eq(&boolean)))
      .Times(testing::AtLeast(1));
  ModuleConfigManager::Instance().Init(&fake_ctx).IgnoreError();
}

TEST_F(ConfigTest, WithModifyCallback) {
  size_t num_modify_calls = 0;
  auto num_modify_cb = [&num_modify_calls]([[maybe_unused]] int64_t new_value) {
    num_modify_calls++;
  };

  auto number_config = config::Builder<long long>("number", 42, 0, 1024)
                           .WithModifyCallback(num_modify_cb)
                           .Build();

  EXPECT_EQ(42, number_config->GetValue());
  EXPECT_TRUE(number_config->SetValue(41).ok());
  EXPECT_EQ(41, number_config->GetValue());
  EXPECT_EQ(1, num_modify_calls);
}

TEST_F(ConfigTest, WithModifyAndValidationCallbackAndFlags) {
  size_t num_modify_calls = 0;
  size_t num_valid_calls = 0;
  auto num_modify_cb =
      [&num_modify_calls]([[maybe_unused]] long long new_value) {
        num_modify_calls++;
      };
  auto validation_cb =
      [&num_valid_calls]([[maybe_unused]] long long new_value) -> absl::Status {
    num_valid_calls++;
    return absl::OkStatus();
  };

  auto number_config = config::Builder<long long>("number", 42, 0, 1024)
                           .WithModifyCallback(num_modify_cb)
                           .WithValidationCallback(validation_cb)
                           .WithFlags(config::Flags::kDefault)
                           .Build();

  EXPECT_EQ(42, number_config->GetValue());
  EXPECT_TRUE(number_config->SetValue(41).ok());

  EXPECT_EQ(41, number_config->GetValue());

  // Make sure that both callbacks were called
  EXPECT_EQ(1, num_modify_calls);
  EXPECT_EQ(1, num_valid_calls);
}

TEST_F(ConfigTest, VetoChanges) {
  auto validation_cb =
      []([[maybe_unused]] long long new_value) -> absl::Status {
    return absl::InternalError("failed validation");
  };

  auto number_config = config::Builder<long long>("number", 42, 0, 1024)
                           .WithValidationCallback(validation_cb)
                           .Build();

  EXPECT_EQ(42, number_config->GetValue());
  EXPECT_FALSE(number_config->SetValue(41).ok());
  // Change was vetoed, so it should still be 41
  EXPECT_EQ(42, number_config->GetValue());
}

namespace {
std::vector<std::string_view> kEnumNames = {"2", "4", "8"};
std::vector<int> kEnumValues = {2, 4, 8};

absl::Status CheckEnumValues(int new_value) {
  auto where = std::find_if(kEnumValues.begin(), kEnumValues.end(),
                            [&new_value](int v) { return v == new_value; });
  if (where == kEnumValues.end()) {
    return absl::InvalidArgumentError("invalid value for enum");
  }
  return absl::OkStatus();
}

}  // namespace

TEST_F(ConfigTest, CheckEumerator) {
  auto enumerator = config::Builder<int>("my-enum", 2, kEnumNames, kEnumValues)
                        .WithValidationCallback(CheckEnumValues)
                        .Build();

  EXPECT_EQ(2, enumerator->GetValue());
  auto res = enumerator->SetValue(41);
  EXPECT_FALSE(res.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(res));

  EXPECT_TRUE(enumerator->SetValue(8).ok());
  EXPECT_TRUE(enumerator->SetValue(2).ok());
  EXPECT_TRUE(enumerator->SetValue(4).ok());
  EXPECT_EQ(4, enumerator->GetValue());
}

TEST_F(ConfigTest, CheckBoolean) {
  auto boolean = config::Builder<bool>("my-bool", true).Build();

  EXPECT_TRUE(boolean->GetValue());
  EXPECT_TRUE(boolean->SetValue(false).ok());
  EXPECT_FALSE(boolean->GetValue());
}

TEST_F(ConfigTest, parseArgsHappyPath) {
  // Define some configuration entries that will register themselves with the
  // configuration manager
  auto enumerator = config::Builder<int>("my-enum", 2, kEnumNames, kEnumValues)
                        .WithValidationCallback(CheckEnumValues)
                        .Build();
  auto number_config =
      config::Builder<long long>("my-number", 42, 0, 1024).Build();
  auto boolean = config::Builder<bool>("my-bool", true).Build();

  // Happy path
  auto args =
      vmsdk::ToRedisStringVector("--my-bool no --my-number 10 --my-enum 4");
  auto res = ModuleConfigManager::Instance().Init(&fake_ctx);
  EXPECT_TRUE(res.ok());
  res = ModuleConfigManager::Instance().ParseAndLoadArgv(&fake_ctx, args.data(),
                                                         args.size());
  EXPECT_TRUE(res.ok());
  // Check that command line arguments set new values
  EXPECT_FALSE(boolean->GetValue());
  EXPECT_EQ(enumerator->GetValue(), 4);
  EXPECT_EQ(number_config->GetValue(), 10);

  FreeRedisArgs(args);
}

TEST_F(ConfigTest, ParseArgsWithUnknownArgument) {
  auto args = vmsdk::ToRedisStringVector("--my-bool no");
  auto res = ModuleConfigManager::Instance().Init(&fake_ctx);
  EXPECT_TRUE(res.ok());

  res = ModuleConfigManager::Instance().ParseAndLoadArgv(&fake_ctx, args.data(),
                                                         args.size());
  EXPECT_FALSE(res.ok());
  EXPECT_TRUE(absl::IsUnknown(res));
  FreeRedisArgs(args);
}

// we made an exception for --use-coordinator, test it
TEST_F(ConfigTest, ParseArgsUseCoordinator) {
  auto use_coordinator =
      config::Builder<bool>("use-coordinator", false).Build();
  auto args = vmsdk::ToRedisStringVector("--use-coordinator");
  auto res = ModuleConfigManager::Instance().Init(&fake_ctx);
  EXPECT_TRUE(res.ok());
  res = ModuleConfigManager::Instance().ParseAndLoadArgv(&fake_ctx, args.data(),
                                                         args.size());
  EXPECT_TRUE(res.ok());
  EXPECT_TRUE(use_coordinator->GetValue());
  FreeRedisArgs(args);
}

TEST_F(ConfigTest, ParseArgsInvalidFormat) {
  auto boolean = config::Builder<bool>("enable-something", false).Build();
  auto args = vmsdk::ToRedisStringVector("enable-something yes");
  auto res = ModuleConfigManager::Instance().Init(&fake_ctx);
  EXPECT_TRUE(res.ok());
  res = ModuleConfigManager::Instance().ParseAndLoadArgv(&fake_ctx, args.data(),
                                                         args.size());
  // missing "--" prefix yields "InvalidArgument" error
  EXPECT_TRUE(absl::IsInvalidArgument(res));
  FreeRedisArgs(args);
}

TEST_F(ConfigTest, ParseArgsMissingValue) {
  auto number =
      config::Builder<long long>("possible-answers", 42, 0, 1024).Build();
  auto args = vmsdk::ToRedisStringVector("--possible-answers");
  auto res = ModuleConfigManager::Instance().Init(&fake_ctx);
  EXPECT_TRUE(res.ok());

  res = ModuleConfigManager::Instance().ParseAndLoadArgv(&fake_ctx, args.data(),
                                                         args.size());
  // Missing value yields "NotFound" error
  EXPECT_TRUE(absl::IsNotFound(res));
  FreeRedisArgs(args);
}

}  // namespace
}  // namespace vmsdk
