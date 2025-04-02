/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/configurable.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace vmsdk {

namespace {

using ::testing::_;
using ::testing::Eq;
using ::testing::StrEq;

class ConfigurableStringTest : public vmsdk::RedisTest {
 protected:
  RedisModuleCtx fake_ctx;
  void TearDown() override {
    vmsdk::config::ConfigurableBase::Reset();
    vmsdk::RedisTest::TearDown();
  }
};

TEST_F(ConfigurableStringTest, Normal) {
  vmsdk::config::String string_{"string", "default"};
  vmsdk::config::Boolean boolean_{"boolean", true};
  vmsdk::config::Number number_{"number", 1, -100, 100};
  vmsdk::config::Enum enum_{"enum", 1, {"a", "b", "c"}, {1, 2, 3}};
  vmsdk::config::Enum flags_{
      vmsdk::config::Flags::kBitFlags, "flags", 3, {"a", "b", "c"}, {1, 2, 4}};
  EXPECT_CALL(
      *kMockRedisModule,
      RegisterStringConfig(&fake_ctx, StrEq("string"), StrEq("default"),
                           Eq(static_cast<int>(config::Flags::kDefault)), _, _,
                           _, Eq(&string_)));
  EXPECT_CALL(*kMockRedisModule,
              RegisterBoolConfig(&fake_ctx, StrEq("boolean"), true,
                                 Eq(static_cast<int>(config::Flags::kDefault)),
                                 _, _, _, Eq(&boolean_)));
  EXPECT_CALL(
      *kMockRedisModule,
      RegisterNumericConfig(&fake_ctx, StrEq("number"), 1,
                            Eq(static_cast<int>(config::Flags::kDefault)), -100,
                            100, _, _, _, Eq(&number_)));
  EXPECT_CALL(*kMockRedisModule,
              RegisterEnumConfig(&fake_ctx, StrEq("enum"), 1,
                                 Eq(static_cast<int>(config::Flags::kDefault)),
                                 _, _, 3, _, _, _, Eq(&enum_)))
      .With(::testing::AllOf(
          ::testing::Args<4, 6>(
              ElementsAre(StrEq("a"), StrEq("b"), StrEq("c"))),
          ::testing::Args<5, 6>(::testing::ElementsAre(Eq(1), Eq(2), Eq(3)))));
  EXPECT_CALL(*kMockRedisModule,
              RegisterEnumConfig(&fake_ctx, StrEq("flags"), 3,
                                 Eq(static_cast<int>(config::Flags::kBitFlags)),
                                 _, _, 3, _, _, _, Eq(&flags_)))
      .With(::testing::AllOf(
          ::testing::Args<4, 6>(
              ElementsAre(StrEq("a"), StrEq("b"), StrEq("c"))),
          ::testing::Args<5, 6>(::testing::ElementsAre(Eq(1), Eq(2), Eq(4)))));
  EXPECT_EQ(config::ConfigurableBase::OnStartup(&fake_ctx), absl::OkStatus());
  auto values = config::ConfigurableBase::GetAllAsMap();
  EXPECT_EQ(string_.Get(), "default");
  EXPECT_EQ(boolean_.Get(), true);
  EXPECT_EQ(number_.Get(), 1);
  EXPECT_EQ(enum_.Get(), 1);
  EXPECT_EQ(flags_.Get(), 3);

  EXPECT_EQ(values.at("string").first, "default");
  EXPECT_EQ(values.at("string").second, "");
  EXPECT_EQ(values.at("boolean").first, "On");
  EXPECT_EQ(values.at("boolean").second, "");
  EXPECT_EQ(values.at("number").first, "1");
  EXPECT_EQ(values.at("number").second, "");
  EXPECT_EQ(values.at("enum").first, "a");
  EXPECT_EQ(values.at("enum").second, "");
  EXPECT_EQ(values.at("flags").first, "{a+b}");
  EXPECT_EQ(values.at("flags").second, "[BitFlags]");
}

TEST_F(ConfigurableStringTest, Redacted) {
  vmsdk::config::String string_{config::Flags::kSensitive, "string", "default"};
  vmsdk::config::Boolean boolean_{config::Flags::kSensitive, "boolean", true};
  vmsdk::config::Number number_{config::Flags::kSensitive, "number", 1, -100,
                                100};
  vmsdk::config::Enum enum_{
      config::Flags::kSensitive, "enum", 1, {"a", "b", "c"}, {1, 2, 3}};
  vmsdk::config::Enum flags_{
      config::Flags::kSensitive | vmsdk::config::Flags::kBitFlags,
      "flags",
      3,
      {"a", "b", "c"},
      {1, 2, 4}};
  EXPECT_CALL(
      *kMockRedisModule,
      RegisterStringConfig(&fake_ctx, StrEq("string"), StrEq("default"),
                           Eq(static_cast<int>(config::Flags::kSensitive)), _,
                           _, _, Eq(&string_)));
  EXPECT_CALL(
      *kMockRedisModule,
      RegisterBoolConfig(&fake_ctx, StrEq("boolean"), true,
                         Eq(static_cast<int>(config::Flags::kSensitive)), _, _,
                         _, Eq(&boolean_)));
  EXPECT_CALL(
      *kMockRedisModule,
      RegisterNumericConfig(&fake_ctx, StrEq("number"), 1,
                            Eq(static_cast<int>(config::Flags::kSensitive)),
                            -100, 100, _, _, _, Eq(&number_)));
  EXPECT_CALL(
      *kMockRedisModule,
      RegisterEnumConfig(&fake_ctx, StrEq("enum"), 1,
                         Eq(static_cast<int>(config::Flags::kSensitive)), _, _,
                         3, _, _, _, Eq(&enum_)))
      .With(::testing::AllOf(
          ::testing::Args<4, 6>(
              ElementsAre(StrEq("a"), StrEq("b"), StrEq("c"))),
          ::testing::Args<5, 6>(::testing::ElementsAre(Eq(1), Eq(2), Eq(3)))));
  EXPECT_CALL(
      *kMockRedisModule,
      RegisterEnumConfig(&fake_ctx, StrEq("flags"), 3,
                         Eq(static_cast<int>(config::Flags::kBitFlags |
                                             config::Flags::kSensitive)),
                         _, _, 3, _, _, _, Eq(&flags_)))
      .With(::testing::AllOf(
          ::testing::Args<4, 6>(
              ElementsAre(StrEq("a"), StrEq("b"), StrEq("c"))),
          ::testing::Args<5, 6>(::testing::ElementsAre(Eq(1), Eq(2), Eq(4)))));
  EXPECT_EQ(config::ConfigurableBase::OnStartup(&fake_ctx), absl::OkStatus());
  auto values = config::ConfigurableBase::GetAllAsMap();
  EXPECT_EQ(values.at("string").first, "**__redacted__**");
  EXPECT_EQ(values.at("string").second, "[Sensitive]");
  EXPECT_EQ(values.at("boolean").first, "**__redacted__**");
  EXPECT_EQ(values.at("boolean").second, "[Sensitive]");
  EXPECT_EQ(values.at("number").first, "**__redacted__**");
  EXPECT_EQ(values.at("number").second, "[Sensitive]");
  EXPECT_EQ(values.at("enum").first, "**__redacted__**");
  EXPECT_EQ(values.at("enum").second, "[Sensitive]");
  EXPECT_EQ(values.at("flags").first, "**__redacted__**");
  EXPECT_EQ(values.at("flags").second, "[Sensitive,BitFlags]");
}

TEST_F(ConfigurableStringTest, CommandLine) {
  vmsdk::config::String string_{"string", "default"};
  vmsdk::config::Boolean boolean_{"boolean", true};
  vmsdk::config::Number number_{"number", 1, -100, 100};
  vmsdk::config::Enum enum_{"enum", 1, {"a", "b", "c"}, {1, 2, 3}};
  vmsdk::config::Enum flags_{
      vmsdk::config::Flags::kBitFlags, "flags", 3, {"a", "b", "c"}, {1, 2, 4}};
  EXPECT_CALL(
      *kMockRedisModule,
      RegisterStringConfig(&fake_ctx, StrEq("string"), StrEq("default"),
                           Eq(static_cast<int>(config::Flags::kDefault)), _, _,
                           _, Eq(&string_)));
  EXPECT_CALL(*kMockRedisModule,
              RegisterBoolConfig(&fake_ctx, StrEq("boolean"), true,
                                 Eq(static_cast<int>(config::Flags::kDefault)),
                                 _, _, _, Eq(&boolean_)));
  EXPECT_CALL(
      *kMockRedisModule,
      RegisterNumericConfig(&fake_ctx, StrEq("number"), 1,
                            Eq(static_cast<int>(config::Flags::kDefault)), -100,
                            100, _, _, _, Eq(&number_)));
  EXPECT_CALL(*kMockRedisModule,
              RegisterEnumConfig(&fake_ctx, StrEq("enum"), 1,
                                 Eq(static_cast<int>(config::Flags::kDefault)),
                                 _, _, 3, _, _, _, Eq(&enum_)))
      .With(::testing::AllOf(
          ::testing::Args<4, 6>(
              ElementsAre(StrEq("a"), StrEq("b"), StrEq("c"))),
          ::testing::Args<5, 6>(::testing::ElementsAre(Eq(1), Eq(2), Eq(3)))));
  EXPECT_CALL(*kMockRedisModule,
              RegisterEnumConfig(&fake_ctx, StrEq("flags"), 3,
                                 Eq(static_cast<int>(config::Flags::kBitFlags)),
                                 _, _, 3, _, _, _, Eq(&flags_)))
      .With(::testing::AllOf(
          ::testing::Args<4, 6>(
              ElementsAre(StrEq("a"), StrEq("b"), StrEq("c"))),
          ::testing::Args<5, 6>(::testing::ElementsAre(Eq(1), Eq(2), Eq(4)))));
  EXPECT_EQ(config::ConfigurableBase::OnStartup(&fake_ctx), absl::OkStatus());

#define DO_BAD(str)                                                           \
  {                                                                           \
    auto argv = vmsdk::ToRedisStringVector(str);                              \
    EXPECT_NE(                                                                \
        config::ConfigurableBase::ParseCommandLine(argv.data(), argv.size()), \
        absl::OkStatus());                                                    \
  }
  DO_BAD("X");
  DO_BAD("--X");
  DO_BAD("--number -1000");
  DO_BAD("--number 10000");
  DO_BAD("--number 10 z");
  DO_BAD("--number 10 --number");
  DO_BAD("--flags x")

#define DO_GOOD(str)                                                          \
  {                                                                           \
    auto argv = vmsdk::ToRedisStringVector(str);                              \
    EXPECT_EQ(                                                                \
        config::ConfigurableBase::ParseCommandLine(argv.data(), argv.size()), \
        absl::OkStatus());                                                    \
  }
  DO_GOOD("");
  DO_GOOD("--number 10");
  EXPECT_EQ(number_.Get(), 10);
  DO_GOOD("--Number 11");
  EXPECT_EQ(number_.Get(), 11);
  DO_GOOD("--boolean yes");
  // EXPECT_EQ(boolean_.Get(), true);
  DO_GOOD("--Boolean no");
  EXPECT_EQ(boolean_.Get(), false);
  DO_GOOD("--string test");
  EXPECT_EQ(string_.Get(), "test");
  DO_GOOD("--enum a");
  EXPECT_EQ(enum_.Get(), 1);
}
}  // namespace
}  // namespace vmsdk