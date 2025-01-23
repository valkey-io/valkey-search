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

#include "vmsdk/src/module_type.h"

#include <cstddef>
#include <string>

#include "absl/log/log.h"
#include "absl/status/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

namespace {

// Test fixture for module_type tests
class ModuleTypeTest : public vmsdk::RedisTest {
 protected:
  // Helper function to create RedisModuleString
  RedisModuleString* CreateRedisModuleString(const std::string& str) {
    return RedisModule_CreateString(NULL, str.c_str(), str.size());
  }
};

TEST_F(ModuleTypeTest, FailOpenRegisterKey) {
  EXPECT_CALL(*kMockRedisModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(
          testing::Return(reinterpret_cast<RedisModuleKey*>(REDISMODULE_OK)));

  std::string key = "test_key";
  RedisModuleType* module_type = reinterpret_cast<RedisModuleType*>(2);
  void* ptr = reinterpret_cast<void*>(3);

  // Call the Register function
  absl::Status result = ModuleType::Register(NULL, key, ptr, module_type);

  EXPECT_EQ(result.message(), "failed to open Redis module key: test_key");
  EXPECT_TRUE(result.code() != absl::StatusCode::kOk);
}

TEST_F(ModuleTypeTest, SuccessfullyRegistersKey) {
  auto fake_key = reinterpret_cast<RedisModuleKey*>(1);
  EXPECT_CALL(*kMockRedisModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(fake_key));
  EXPECT_CALL(*kMockRedisModule, CloseKey(fake_key))
      .WillOnce(testing::Return());
  EXPECT_CALL(*kMockRedisModule, KeyType(fake_key))
      .WillOnce(testing::Return(REDISMODULE_KEYTYPE_EMPTY));

  std::string key = "test_key";
  RedisModuleType* module_type = reinterpret_cast<RedisModuleType*>(2);
  void* ptr = reinterpret_cast<void*>(3);
  EXPECT_CALL(*kMockRedisModule, ModuleTypeSetValue(fake_key, module_type, ptr))
      .WillOnce(testing::Return(REDISMODULE_OK));

  // Call the Register function
  absl::Status result = ModuleType::Register(NULL, key, ptr, module_type);

  LOG(INFO) << "result: " << result.code();
  EXPECT_TRUE(result.code() == absl::StatusCode::kOk);
}

TEST_F(ModuleTypeTest, RegisterKeyAlreadyExistsError) {
  auto fake_key = reinterpret_cast<RedisModuleKey*>(1);
  EXPECT_CALL(*kMockRedisModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(fake_key));
  EXPECT_CALL(*kMockRedisModule, CloseKey(fake_key))
      .WillOnce(testing::Return());
  EXPECT_CALL(*kMockRedisModule, KeyType(testing::_))
      .WillOnce(testing::Return(REDISMODULE_KEYTYPE_STRING));

  std::string key = "test_key";
  RedisModuleType* module_type = reinterpret_cast<RedisModuleType*>(2);
  void* ptr = reinterpret_cast<void*>(3);

  // Call the Register function
  absl::Status result = ModuleType::Register(NULL, key, ptr, module_type);

  LOG(INFO) << "result: " << result.code();
  EXPECT_TRUE(result.code() == absl::StatusCode::kAlreadyExists);
}

TEST_F(ModuleTypeTest, RegisterInternalError) {
  auto fake_key = reinterpret_cast<RedisModuleKey*>(1);
  EXPECT_CALL(*kMockRedisModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(fake_key));
  EXPECT_CALL(*kMockRedisModule, CloseKey(fake_key))
      .WillOnce(testing::Return());
  EXPECT_CALL(*kMockRedisModule, KeyType(fake_key))
      .WillOnce(testing::Return(REDISMODULE_KEYTYPE_EMPTY));
  EXPECT_CALL(*kMockRedisModule, DeleteKey(fake_key))
      .WillOnce(testing::Return(REDISMODULE_OK));

  EXPECT_CALL(*kMockRedisModule,
              ModuleTypeSetValue(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(1));

  std::string key = "test_key";
  RedisModuleType* module_type = reinterpret_cast<RedisModuleType*>(2);
  void* ptr = reinterpret_cast<void*>(3);

  // Call the Register function
  absl::Status result = ModuleType::Register(NULL, key, ptr, module_type);

  EXPECT_TRUE(result.code() == absl::StatusCode::kInternal);
}

TEST_F(ModuleTypeTest, DeregisterSuccessfully) {
  auto fake_key = reinterpret_cast<RedisModuleKey*>(1);
  EXPECT_CALL(*kMockRedisModule, KeyExists(testing::_, testing::_))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(*kMockRedisModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(fake_key));
  EXPECT_CALL(*kMockRedisModule, CloseKey(fake_key))
      .WillOnce(testing::Return());
  EXPECT_CALL(*kMockRedisModule, DeleteKey(fake_key))
      .WillOnce(testing::Return(REDISMODULE_OK));

  // Call the Deregister function
  absl::Status result = ModuleType::Deregister(NULL, "test_key");

  LOG(INFO) << "result: " << result.code();
  EXPECT_TRUE(result.code() == absl::StatusCode::kOk);
}

TEST_F(ModuleTypeTest, DeregisterUnregisteredKey) {
  EXPECT_CALL(*kMockRedisModule, KeyExists(testing::_, testing::_))
      .WillOnce(testing::Return(0));

  // Call the Deregister function
  absl::Status result = ModuleType::Deregister(NULL, "test_key");

  LOG(INFO) << "result: " << result.code();
  VMSDK_EXPECT_OK(result);
}

#ifdef NDEBUG
TEST_F(ModuleTypeTest, DeregisterFailOpenRedisKey) {
  EXPECT_CALL(*kMockRedisModule, KeyExists(testing::_, testing::_))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(*kMockRedisModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(nullptr));
  // Call the Deregister function
  absl::Status result = ModuleType::Deregister(NULL, "test_key");

  LOG(INFO) << "result: " << result.code();
  EXPECT_TRUE(result.code() == absl::StatusCode::kInternal);
}
#endif

}  // namespace

}  // namespace vmsdk
