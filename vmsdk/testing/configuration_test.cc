/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <absl/strings/match.h>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace vmsdk {

namespace {

inline void FreeValkeyArgs(std::vector<ValkeyModuleString *> &args) {
  for (const auto &arg : args) {
    TestValkeyModule_FreeString(nullptr, arg);
  }
}

using ::testing::_;
using ::testing::Eq;
using ::testing::StrEq;
using vmsdk::config::ModuleConfigManager;

class ConfigTest : public vmsdk::ValkeyTest {
 protected:
  ValkeyModuleCtx fake_ctx;
  void SetUp() override { vmsdk::ValkeyTest::SetUp(); }
  void TearDown() override { vmsdk::ValkeyTest::TearDown(); }
};

TEST_F(ConfigTest, registration) {
  vmsdk::config::Number number("number", 42, 0, 1024);
  vmsdk::config::Boolean boolean("boolean", true);

  // 2 integer registration
  EXPECT_CALL(*kMockValkeyModule,
              RegisterNumericConfig(&fake_ctx, StrEq("number"), Eq(42), _,
                                    Eq(0), Eq(1024), _, _, _, Eq(&number)))
      .Times(testing::AtLeast(1));

  EXPECT_CALL(*kMockValkeyModule,
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

TEST_F(ConfigTest, CheckEnumerator) {
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
      vmsdk::ToValkeyStringVector("--my-bool no --my-number 10 --my-enum 4");
  auto res = ModuleConfigManager::Instance().Init(&fake_ctx);
  EXPECT_TRUE(res.ok());
  res = ModuleConfigManager::Instance().ParseAndLoadArgv(&fake_ctx, args.data(),
                                                         args.size());
  EXPECT_TRUE(res.ok());
  // Check that command line arguments set new values
  EXPECT_FALSE(boolean->GetValue());
  EXPECT_EQ(enumerator->GetValue(), 4);
  EXPECT_EQ(number_config->GetValue(), 10);

  FreeValkeyArgs(args);
}

TEST_F(ConfigTest, ParseArgsWithUnknownArgument) {
  auto args = vmsdk::ToValkeyStringVector("--my-bool no");
  auto res = ModuleConfigManager::Instance().Init(&fake_ctx);
  EXPECT_TRUE(res.ok());

  res = ModuleConfigManager::Instance().ParseAndLoadArgv(&fake_ctx, args.data(),
                                                         args.size());
  EXPECT_FALSE(res.ok());
  EXPECT_TRUE(absl::IsUnknown(res));
  FreeValkeyArgs(args);
}

// we made an exception for --use-coordinator, test it
TEST_F(ConfigTest, ParseArgsUseCoordinator) {
  auto use_coordinator =
      config::Builder<bool>("use-coordinator", false).Build();
  auto args = vmsdk::ToValkeyStringVector("--use-coordinator");
  auto res = ModuleConfigManager::Instance().Init(&fake_ctx);
  EXPECT_TRUE(res.ok());
  res = ModuleConfigManager::Instance().ParseAndLoadArgv(&fake_ctx, args.data(),
                                                         args.size());
  EXPECT_TRUE(res.ok());
  EXPECT_TRUE(use_coordinator->GetValue());
  FreeValkeyArgs(args);
}

TEST_F(ConfigTest, ParseArgsInvalidFormat) {
  auto boolean = config::Builder<bool>("enable-something", false).Build();
  auto args = vmsdk::ToValkeyStringVector("enable-something yes");
  auto res = ModuleConfigManager::Instance().Init(&fake_ctx);
  EXPECT_TRUE(res.ok());
  res = ModuleConfigManager::Instance().ParseAndLoadArgv(&fake_ctx, args.data(),
                                                         args.size());
  // missing "--" prefix yields "InvalidArgument" error
  EXPECT_TRUE(absl::IsInvalidArgument(res));
  FreeValkeyArgs(args);
}

TEST_F(ConfigTest, ParseArgsMissingValue) {
  auto number =
      config::Builder<long long>("possible-answers", 42, 0, 1024).Build();
  auto args = vmsdk::ToValkeyStringVector("--possible-answers");
  auto res = ModuleConfigManager::Instance().Init(&fake_ctx);
  EXPECT_TRUE(res.ok());

  res = ModuleConfigManager::Instance().ParseAndLoadArgv(&fake_ctx, args.data(),
                                                         args.size());
  // Missing value yields "NotFound" error
  EXPECT_TRUE(absl::IsNotFound(res));
  FreeValkeyArgs(args);
}

TEST_F(ConfigTest, CheckStringConfig) {
  auto str =
      config::StringBuilder("cpu-list", "1,2,3,4")
          .WithValidationCallback([](std::string new_val) -> absl::Status {
            if (new_val == "5,6") {
              return absl::OkStatus();
            } else {
              return absl::InvalidArgumentError("We only accept 5,6");
            }
          })
          .Build();
  auto args = vmsdk::ToValkeyStringVector("--cpu-list 5,6");
  auto res = ModuleConfigManager::Instance().Init(&fake_ctx);
  EXPECT_TRUE(res.ok());
  res = ModuleConfigManager::Instance().ParseAndLoadArgv(&fake_ctx, args.data(),
                                                         args.size());
  EXPECT_TRUE(res.ok());
  EXPECT_EQ(str->GetValue(), "5,6");

  res = str->SetValue("7,8");
  EXPECT_FALSE(res.ok());  // should fail validation
  EXPECT_EQ(
      res.code(),
      absl::StatusCode::kInvalidArgument);  // Failure reason: invalid argument
}

}  // namespace
}  // namespace vmsdk
