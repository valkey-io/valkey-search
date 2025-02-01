/*
 * Copyright (c) 2025, ValkeySearch contributors
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

#ifndef VMSDK_SRC_TESTING_INFRA_UTILS
#define VMSDK_SRC_TESTING_INFRA_UTILS

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

using ::testing::TestWithParam;

#define VMSDK_EXPECT_OK(status) EXPECT_TRUE((status).ok())

#define VMSDK_EXPECT_OK_STATUS(expr)                                         \
  {                                                                          \
    auto __status = expr;                                                    \
    EXPECT_TRUE(__status.ok()) << " Failure Message:" << __status.message(); \
  }

#define VMSDK_EXPECT_OK_STATUSOR(expr)                         \
  {                                                            \
    auto __status = expr;                                      \
    EXPECT_TRUE(__status.ok())                                 \
        << " Failure Message:" << __status.status().message(); \
  }

namespace vmsdk {

class RedisTest : public testing::Test {
 protected:
  void SetUp() override { TestRedisModule_Init(); }

  void TearDown() override { TestRedisModule_Teardown(); }
};

template <typename T>
class RedisTestWithParam : public TestWithParam<T> {
 protected:
  void SetUp() override { TestRedisModule_Init(); }

  void TearDown() override { TestRedisModule_Teardown(); }
};

std::vector<RedisModuleString *> ToRedisStringVector(
    absl::string_view params_str, absl::string_view exclude = "");

MATCHER_P(RedisModuleStringEq, value, "") {
  return *((std::string *)arg) == *((std::string *)value);
}

MATCHER_P(RedisModuleStringValueEq, value, "") {
  *result_listener << "where the string is " << *((std::string *)arg);
  return *((std::string *)arg) == value;
}

MATCHER_P(RedisModuleKeyIsForString, value, "") {
  *result_listener << "where the key is " << ((RedisModuleKey *)arg)->key;
  return ((RedisModuleKey *)arg)->key == value;
}

MATCHER_P(IsRedisModuleEvent, expected, "") {
  return arg.id == expected.id && arg.dataver == expected.dataver;
}

}  // namespace vmsdk

#endif  // VMSDK_SRC_TESTING_INFRA_UTILS
