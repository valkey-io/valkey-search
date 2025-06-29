/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/module_type.h"

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
class ModuleTypeTest : public vmsdk::ValkeyTest {
 protected:
  // Helper function to create ValkeyModuleString
  ValkeyModuleString* CreateValkeyModuleString(const std::string& str) {
    return ValkeyModule_CreateString(nullptr, str.c_str(), str.size());
  }
};

TEST_F(ModuleTypeTest, FailOpenRegisterKey) {
  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(
          testing::Return(reinterpret_cast<ValkeyModuleKey*>(VALKEYMODULE_OK)));

  std::string key = "test_key";
  ValkeyModuleType* module_type = reinterpret_cast<ValkeyModuleType*>(2);
  void* ptr = reinterpret_cast<void*>(3);

  // Call the Register function
  absl::Status result = ModuleType::Register(nullptr, key, ptr, module_type);

  EXPECT_EQ(result.message(), "failed to open Valkey module key: test_key");
  EXPECT_TRUE(result.code() != absl::StatusCode::kOk);
}

TEST_F(ModuleTypeTest, SuccessfullyRegistersKey) {
  auto fake_key = reinterpret_cast<ValkeyModuleKey*>(1);
  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(fake_key));
  EXPECT_CALL(*kMockValkeyModule, CloseKey(fake_key))
      .WillOnce(testing::Return());
  EXPECT_CALL(*kMockValkeyModule, KeyType(fake_key))
      .WillOnce(testing::Return(VALKEYMODULE_KEYTYPE_EMPTY));

  std::string key = "test_key";
  ValkeyModuleType* module_type = reinterpret_cast<ValkeyModuleType*>(2);
  void* ptr = reinterpret_cast<void*>(3);
  EXPECT_CALL(*kMockValkeyModule,
              ModuleTypeSetValue(fake_key, module_type, ptr))
      .WillOnce(testing::Return(VALKEYMODULE_OK));

  // Call the Register function
  absl::Status result = ModuleType::Register(nullptr, key, ptr, module_type);

  LOG(INFO) << "result: " << result.code();
  EXPECT_TRUE(result.code() == absl::StatusCode::kOk);
}

TEST_F(ModuleTypeTest, RegisterKeyAlreadyExistsError) {
  auto fake_key = reinterpret_cast<ValkeyModuleKey*>(1);
  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(fake_key));
  EXPECT_CALL(*kMockValkeyModule, CloseKey(fake_key))
      .WillOnce(testing::Return());
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillOnce(testing::Return(VALKEYMODULE_KEYTYPE_STRING));

  std::string key = "test_key";
  ValkeyModuleType* module_type = reinterpret_cast<ValkeyModuleType*>(2);
  void* ptr = reinterpret_cast<void*>(3);

  // Call the Register function
  absl::Status result = ModuleType::Register(nullptr, key, ptr, module_type);

  LOG(INFO) << "result: " << result.code();
  EXPECT_TRUE(result.code() == absl::StatusCode::kAlreadyExists);
}

TEST_F(ModuleTypeTest, RegisterInternalError) {
  auto fake_key = reinterpret_cast<ValkeyModuleKey*>(1);
  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(fake_key));
  EXPECT_CALL(*kMockValkeyModule, CloseKey(fake_key))
      .WillOnce(testing::Return());
  EXPECT_CALL(*kMockValkeyModule, KeyType(fake_key))
      .WillOnce(testing::Return(VALKEYMODULE_KEYTYPE_EMPTY));
  EXPECT_CALL(*kMockValkeyModule, DeleteKey(fake_key))
      .WillOnce(testing::Return(VALKEYMODULE_OK));

  EXPECT_CALL(*kMockValkeyModule,
              ModuleTypeSetValue(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(1));

  std::string key = "test_key";
  ValkeyModuleType* module_type = reinterpret_cast<ValkeyModuleType*>(2);
  void* ptr = reinterpret_cast<void*>(3);

  // Call the Register function
  absl::Status result = ModuleType::Register(nullptr, key, ptr, module_type);

  EXPECT_TRUE(result.code() == absl::StatusCode::kInternal);
}

TEST_F(ModuleTypeTest, DeregisterSuccessfully) {
  auto fake_key = reinterpret_cast<ValkeyModuleKey*>(1);
  EXPECT_CALL(*kMockValkeyModule, KeyExists(testing::_, testing::_))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(fake_key));
  EXPECT_CALL(*kMockValkeyModule, CloseKey(fake_key))
      .WillOnce(testing::Return());
  EXPECT_CALL(*kMockValkeyModule, DeleteKey(fake_key))
      .WillOnce(testing::Return(VALKEYMODULE_OK));

  // Call the Deregister function
  absl::Status result = ModuleType::Deregister(nullptr, "test_key");

  LOG(INFO) << "result: " << result.code();
  EXPECT_TRUE(result.code() == absl::StatusCode::kOk);
}

TEST_F(ModuleTypeTest, DeregisterUnregisteredKey) {
  EXPECT_CALL(*kMockValkeyModule, KeyExists(testing::_, testing::_))
      .WillOnce(testing::Return(0));

  // Call the Deregister function
  absl::Status result = ModuleType::Deregister(nullptr, "test_key");

  LOG(INFO) << "result: " << result.code();
  VMSDK_EXPECT_OK(result);
}

#ifdef NDEBUG
TEST_F(ModuleTypeTest, DeregisterFailOpenValkeyKey) {
  EXPECT_CALL(*kMockValkeyModule, KeyExists(testing::_, testing::_))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(nullptr));
  // Call the Deregister function
  absl::Status result = ModuleType::Deregister(nullptr, "test_key");

  LOG(INFO) << "result: " << result.code();
  EXPECT_TRUE(result.code() == absl::StatusCode::kInternal);
}
#endif

}  // namespace

}  // namespace vmsdk
