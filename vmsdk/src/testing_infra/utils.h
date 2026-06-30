/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_TESTING_INFRA_UTILS
#define VMSDK_SRC_TESTING_INFRA_UTILS

#include <sys/resource.h>

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

using ::testing::TestWithParam;

struct RLimitGuard {
  RLimitGuard(size_t type, size_t new_size) : type_(type) {
    EXPECT_EQ(getrlimit(type_, &saved_size_), 0);
    struct rlimit new_sizer = saved_size_;
    new_sizer.rlim_cur = new_size;
    EXPECT_EQ(setrlimit(type_, &new_sizer), 0);
  }
  ~RLimitGuard() { EXPECT_EQ(setrlimit(type_, &saved_size_), 0); }
  size_t type_;
  struct rlimit saved_size_;
};

#define VMSDK_EXPECT_DEATH(statement, matcher) \
  {                                            \
    RLimitGuard core(RLIMIT_CORE, 0);          \
    EXPECT_DEATH(statement, matcher);          \
  }

#define VMSDK_ASSERT_DEATH(statement, matcher) \
  {                                            \
    RLimitGuard core(RLIMIT_CORE, 0);          \
    ASSERT_DEATH(statement, matcher);          \
  }

#define VMSDK_EXPECT_OK(status) EXPECT_TRUE((status).ok())

#define VMSDK_EXPECT_OK_STATUS(expr)                                         \
  {                                                                          \
    auto __status = expr;                                                    \
    EXPECT_TRUE(__status.ok()) << " Failure Message:" << __status.message(); \
  }

#define VMSDK_EXPECT_OK_STATUSOR(expr)                         \
  {                                                            \
    auto& __status = expr;                                     \
    EXPECT_TRUE(__status.ok())                                 \
        << " Failure Message:" << __status.status().message(); \
  }

namespace vmsdk {

class ValkeyTest : public testing::Test {
 protected:
  void SetUp() override { TestValkeyModule_Init(); }

  void TearDown() override { TestValkeyModule_Teardown(); }
};

template <typename T>
class ValkeyTestWithParam : public TestWithParam<T> {
 protected:
  void SetUp() override { TestValkeyModule_Init(); }

  void TearDown() override { TestValkeyModule_Teardown(); }
};

std::vector<ValkeyModuleString*> ToValkeyStringVector(
    absl::string_view params_str, absl::string_view exclude = "");

MATCHER_P(ValkeyModuleStringEq, value, "") {
  return *((std::string*)arg) == *((std::string*)value);
}

MATCHER_P(ValkeyModuleStringValueEq, value, "") {
  *result_listener << "where the string is " << *((std::string*)arg);
  return *((std::string*)arg) == value;
}

MATCHER_P(ValkeyModuleKeyIsForString, value, "") {
  *result_listener << "where the key is " << ((ValkeyModuleKey*)arg)->key;
  return ((ValkeyModuleKey*)arg)->key == value;
}

MATCHER_P(IsValkeyModuleEvent, expected, "") {
  return arg.id == expected.id && arg.dataver == expected.dataver;
}

}  // namespace vmsdk

#endif  // VMSDK_SRC_TESTING_INFRA_UTILS
