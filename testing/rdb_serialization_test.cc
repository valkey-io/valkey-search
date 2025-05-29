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

#include "src/rdb_serialization.h"

#include <cstddef>
#include <cstdint>

#include "absl/status/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "testing/common.h"
#include "third_party/hnswlib/iostream.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

class SafeRDBTest : public ValkeySearchTest {
 protected:
  void SetUp() override { TestRedisModule_Init(); }
  RedisModuleIO* fake_redis_module_io_ = (RedisModuleIO*)0xBADF00D1;
};

TEST_F(SafeRDBTest, LoadSizeTSuccess) {
  size_t expected_value = 34;
  EXPECT_CALL(*kMockRedisModule, LoadUnsigned(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_redis_module_io_);
  auto res = rdb_stream.LoadSizeT();
  VMSDK_EXPECT_OK(res);
  EXPECT_EQ(res.value(), expected_value);
}

TEST_F(SafeRDBTest, LoadSizeTFailure) {
  size_t expected_value = 34;
  EXPECT_CALL(*kMockRedisModule, LoadUnsigned(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.LoadSizeT().status().code(),
            absl::StatusCode::kInternal);
}

TEST_F(SafeRDBTest, LoadUnsignedSuccess) {
  unsigned int expected_value = 34;
  EXPECT_CALL(*kMockRedisModule, LoadUnsigned(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_redis_module_io_);
  auto res = rdb_stream.LoadUnsigned();
  VMSDK_EXPECT_OK(res);
  EXPECT_EQ(res.value(), expected_value);
}

TEST_F(SafeRDBTest, LoadUnsignedFailure) {
  unsigned int actual_value;
  unsigned int expected_value = 34;
  EXPECT_CALL(*kMockRedisModule, LoadUnsigned(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.LoadUnsigned().status().code(),
            absl::StatusCode::kInternal);
}

TEST_F(SafeRDBTest, LoadSignedSuccess) {
  int expected_value = 34;
  EXPECT_CALL(*kMockRedisModule, LoadSigned(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_redis_module_io_);
  auto res = rdb_stream.LoadSigned();
  VMSDK_EXPECT_OK(res);
  EXPECT_EQ(res.value(), expected_value);
}

TEST_F(SafeRDBTest, LoadSignedFailure) {
  int expected_value = 34;
  EXPECT_CALL(*kMockRedisModule, LoadSigned(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.LoadSigned().status().code(),
            absl::StatusCode::kInternal);
}

TEST_F(SafeRDBTest, LoadDoubleSuccess) {
  double actual_value;
  double expected_value = 34.5;
  EXPECT_CALL(*kMockRedisModule, LoadDouble(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_redis_module_io_);
  auto res = rdb_stream.LoadDouble();
  VMSDK_EXPECT_OK(res);
  EXPECT_EQ(res.value(), expected_value);
}

TEST_F(SafeRDBTest, LoadDoubleFailure) {
  double expected_value = 34.5;
  EXPECT_CALL(*kMockRedisModule, LoadDouble(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.LoadDouble().status().code(),
            absl::StatusCode::kInternal);
}

TEST_F(SafeRDBTest, LoadStringSuccess) {
  RedisModuleString* expected_value =
      TestRedisModule_CreateStringPrintf(nullptr, "test");
  EXPECT_CALL(*kMockRedisModule, LoadString(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_redis_module_io_);
  auto actual_value_or = rdb_stream.LoadString();
  VMSDK_EXPECT_OK(actual_value_or.status());
  EXPECT_EQ(actual_value_or.value().get(), expected_value);
}

TEST_F(SafeRDBTest, LoadStringFailure) {
  RedisModuleString* expected_value =
      TestRedisModule_CreateStringPrintf(nullptr, "test");
  EXPECT_CALL(*kMockRedisModule, LoadString(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.LoadString().status().code(),
            absl::StatusCode::kInternal);
}

TEST_F(SafeRDBTest, SaveSizeTSuccess) {
  size_t value = 34;
  EXPECT_CALL(*kMockRedisModule, SaveUnsigned(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.SaveSizeT(value));
}

TEST_F(SafeRDBTest, SaveSizeTFailure) {
  size_t value = 34;
  EXPECT_CALL(*kMockRedisModule, SaveUnsigned(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.SaveSizeT(value).code(), absl::StatusCode::kInternal);
}

TEST_F(SafeRDBTest, SaveUnsignedSuccess) {
  unsigned int value = 34;
  EXPECT_CALL(*kMockRedisModule, SaveUnsigned(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.SaveUnsigned(value));
}

TEST_F(SafeRDBTest, SaveUnsignedFailure) {
  unsigned int value = 34;
  EXPECT_CALL(*kMockRedisModule, SaveUnsigned(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.SaveUnsigned(value).code(), absl::StatusCode::kInternal);
}

TEST_F(SafeRDBTest, SaveSignedSuccess) {
  int64_t value = 34;
  EXPECT_CALL(*kMockRedisModule, SaveSigned(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.SaveSigned(value));
}

TEST_F(SafeRDBTest, SaveSignedFailure) {
  int64_t value = 34;
  EXPECT_CALL(*kMockRedisModule, SaveSigned(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.SaveSigned(value).code(), absl::StatusCode::kInternal);
}

TEST_F(SafeRDBTest, SaveDoubleSuccess) {
  double value = 34.5;
  EXPECT_CALL(*kMockRedisModule, SaveDouble(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.SaveDouble(value));
}

TEST_F(SafeRDBTest, SaveDoubleFailure) {
  double value = 34.5;
  EXPECT_CALL(*kMockRedisModule, SaveDouble(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.SaveDouble(value).code(), absl::StatusCode::kInternal);
}

TEST_F(SafeRDBTest, SaveStringBufferSuccess) {
  absl::string_view value = "test";
  EXPECT_CALL(*kMockRedisModule, SaveStringBuffer(fake_redis_module_io_,
                                                  value.data(), value.size()));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.SaveStringBuffer(value));
}

TEST_F(SafeRDBTest, SaveStringBufferFailureIOError) {
  absl::string_view value = "test";
  EXPECT_CALL(*kMockRedisModule, SaveStringBuffer(fake_redis_module_io_,
                                                  value.data(), value.size()));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.SaveStringBuffer(value).code(),
            absl::StatusCode::kInternal);
}

class MockRDBSectionCallback {
 public:
  MOCK_METHOD(absl::Status, load,
              (RedisModuleCtx * ctx,
               std::unique_ptr<data_model::RDBSection> section,
               SupplementalContentIter&& iter));
  MOCK_METHOD(absl::Status, save,
              (RedisModuleCtx * ctx, SafeRDB* rdb, int when));
  MOCK_METHOD(int, section_count, (RedisModuleCtx * ctx, int when));
  MOCK_METHOD(int, minimum_semantic_version, (RedisModuleCtx * ctx, int when));
};

class RDBSerializationTest : public ValkeySearchTest {
 protected:
  struct TestRDBSectionCallbacks {
    std::shared_ptr<MockRDBSectionCallback> mock_callbacks;
    RDBSectionCallbacks callbacks_struct;
  };
  void SetUp() override { TestRedisModule_Init(); }
  void TearDown() override {
    TestRedisModule_Teardown();
    ClearRDBCallbacks();
  }
  TestRDBSectionCallbacks GenerateRDBSectionCallbacks() {
    auto mock_callbacks =
        std::make_shared<testing::NiceMock<MockRDBSectionCallback>>();
    RDBSectionCallbacks callbacks_struct{
        .load =
            [mock_callbacks](RedisModuleCtx* ctx,
                             std::unique_ptr<data_model::RDBSection> section,
                             SupplementalContentIter&& iter) {
              return mock_callbacks->load(ctx, std::move(section),
                                          std::move(iter));
            },
        .save = [mock_callbacks](
                    RedisModuleCtx* ctx, SafeRDB* rdb,
                    int when) { return mock_callbacks->save(ctx, rdb, when); },
        .section_count =
            [mock_callbacks](RedisModuleCtx* ctx, int when) {
              return mock_callbacks->section_count(ctx, when);
            },
        .minimum_semantic_version =
            [mock_callbacks](RedisModuleCtx* ctx, int when) {
              return mock_callbacks->minimum_semantic_version(ctx, when);
            },
    };
    return {
        .mock_callbacks = std::move(mock_callbacks),
        .callbacks_struct = std::move(callbacks_struct),
    };
  }
};

TEST_F(RDBSerializationTest, RegisterModuleTypeHappyPath) {
  EXPECT_CALL(
      *kMockRedisModule,
      CreateDataType(&fake_ctx_, testing::StrEq(kValkeySearchModuleTypeName),
                     kCurrentEncVer, testing::_))
      .WillOnce(testing::Invoke(
          [](RedisModuleCtx* ctx, const char* name, int encver,
             RedisModuleTypeMethods* type_methods) -> RedisModuleType* {
            EXPECT_EQ(type_methods->aux_load, AuxLoadCallback);
            EXPECT_EQ(type_methods->aux_save2, AuxSaveCallback);
            EXPECT_EQ(type_methods->aux_save, nullptr);
            EXPECT_EQ(type_methods->aux_save_triggers,
                      REDISMODULE_AUX_AFTER_RDB);
            return (RedisModuleType*)0xBAADF00D;
          }));
  VMSDK_EXPECT_OK(RegisterModuleType(&fake_ctx_));
}

TEST_F(RDBSerializationTest, RegisterModuleTypeReturnNullptr) {
  EXPECT_CALL(
      *kMockRedisModule,
      CreateDataType(&fake_ctx_, testing::StrEq(kValkeySearchModuleTypeName),
                     kCurrentEncVer, testing::_))
      .WillOnce(testing::Invoke(
          [](RedisModuleCtx* ctx, const char* name, int encver,
             RedisModuleTypeMethods* type_methods) -> RedisModuleType* {
            return nullptr;
          }));
  EXPECT_EQ(RegisterModuleType(&fake_ctx_).code(), absl::StatusCode::kInternal);
}

TEST_F(RDBSerializationTest, PerformRDBSaveNoRegisteredTypes) {
  FakeSafeRDB fake_rdb;
  VMSDK_EXPECT_OK(
      PerformRDBSave(&fake_ctx_, &fake_rdb, REDISMODULE_AUX_BEFORE_RDB));
  EXPECT_EQ(fake_rdb.buffer_.rdbuf()->in_avail(), 0);
}

TEST_F(RDBSerializationTest, PerformRDBSaveNoRDBSectionDoesNothing) {
  FakeSafeRDB fake_rdb;
  auto test_cb = GenerateRDBSectionCallbacks();
  EXPECT_CALL(*test_cb.mock_callbacks, load(testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_CALL(*test_cb.mock_callbacks, save(testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_CALL(*test_cb.mock_callbacks,
              section_count(&fake_ctx_, REDISMODULE_AUX_BEFORE_RDB))
      .WillOnce(testing::Return(0));
  EXPECT_CALL(*test_cb.mock_callbacks,
              minimum_semantic_version(testing::_, testing::_))
      .Times(0);
  RegisterRDBCallback(data_model::RDB_SECTION_INDEX_SCHEMA,
                      std::move(test_cb.callbacks_struct));
  VMSDK_EXPECT_OK(
      PerformRDBSave(&fake_ctx_, &fake_rdb, REDISMODULE_AUX_BEFORE_RDB));
  EXPECT_EQ(fake_rdb.buffer_.rdbuf()->in_avail(), 0);
}

TEST_F(RDBSerializationTest, PerformRDBSaveOneRDBSection) {
  FakeSafeRDB fake_rdb;
  auto test_cb = GenerateRDBSectionCallbacks();
  EXPECT_CALL(*test_cb.mock_callbacks, load(testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_CALL(*test_cb.mock_callbacks, save(testing::_, testing::_, testing::_))
      .WillOnce(testing::Invoke(
          [](RedisModuleCtx* ctx, SafeRDB* rdb, int when) -> absl::Status {
            EXPECT_EQ(when, REDISMODULE_AUX_BEFORE_RDB);
            VMSDK_EXPECT_OK(rdb->SaveStringBuffer("test-string"));
            return absl::OkStatus();
          }));
  EXPECT_CALL(*test_cb.mock_callbacks,
              section_count(&fake_ctx_, REDISMODULE_AUX_BEFORE_RDB))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(*test_cb.mock_callbacks,
              minimum_semantic_version(testing::_, testing::_))
      .WillOnce(testing::Return(0x0100ff));  // 1.0.255
  RegisterRDBCallback(data_model::RDB_SECTION_INDEX_SCHEMA,
                      std::move(test_cb.callbacks_struct));
  VMSDK_EXPECT_OK(
      PerformRDBSave(&fake_ctx_, &fake_rdb, REDISMODULE_AUX_BEFORE_RDB));
  auto sem_ver = fake_rdb.LoadUnsigned();
  VMSDK_EXPECT_OK_STATUSOR(sem_ver);
  EXPECT_EQ(sem_ver.value(), 0x0100ff);
  auto section_count = fake_rdb.LoadUnsigned();
  VMSDK_EXPECT_OK_STATUSOR(section_count);
  EXPECT_EQ(section_count.value(), 1);
  auto section_data = fake_rdb.LoadString();
  VMSDK_EXPECT_OK_STATUSOR(section_data);
  EXPECT_EQ(vmsdk::ToStringView(section_data.value().get()), "test-string");
}

TEST_F(RDBSerializationTest, PerformRDBSaveTwoRDBSection) {
  FakeSafeRDB fake_rdb;

  for (int i = 0; i < 2; i++) {
    auto test_cb = GenerateRDBSectionCallbacks();
    EXPECT_CALL(*test_cb.mock_callbacks,
                load(testing::_, testing::_, testing::_))
        .Times(0);
    EXPECT_CALL(*test_cb.mock_callbacks,
                save(testing::_, testing::_, testing::_))
        .WillOnce(testing::Invoke(
            [i](RedisModuleCtx* ctx, SafeRDB* rdb, int when) -> absl::Status {
              EXPECT_EQ(when, REDISMODULE_AUX_BEFORE_RDB);
              std::string save_value = absl::StrCat("test-string-", i);
              VMSDK_EXPECT_OK(rdb->SaveStringBuffer(save_value));
              return absl::OkStatus();
            }));
    EXPECT_CALL(*test_cb.mock_callbacks,
                section_count(&fake_ctx_, REDISMODULE_AUX_BEFORE_RDB))
        .WillOnce(testing::Return(1));
    EXPECT_CALL(*test_cb.mock_callbacks,
                minimum_semantic_version(testing::_, testing::_))
        .WillOnce(testing::Return(i == 0 ? 0x0100ff : 0x0200ff));  // vi.00.255
    RegisterRDBCallback(i == 0 ? data_model::RDB_SECTION_INDEX_SCHEMA
                               : data_model::RDB_SECTION_GLOBAL_METADATA,
                        std::move(test_cb.callbacks_struct));
  }

  VMSDK_EXPECT_OK(
      PerformRDBSave(&fake_ctx_, &fake_rdb, REDISMODULE_AUX_BEFORE_RDB));
  auto sem_ver = fake_rdb.LoadUnsigned();
  VMSDK_EXPECT_OK(sem_ver);
  EXPECT_EQ(sem_ver.value(), 0x0200ff);  // Larger of the two
  auto section_count = fake_rdb.LoadUnsigned();
  VMSDK_EXPECT_OK(section_count);
  EXPECT_EQ(section_count.value(), 2);  // Sum of the counts

  // Could be saved in either order - doesn't matter.
  auto section_data = fake_rdb.LoadString();
  VMSDK_EXPECT_OK_STATUSOR(section_data);
  EXPECT_TRUE(
      vmsdk::ToStringView(section_data.value().get()) == "test-string-0" ||
      vmsdk::ToStringView(section_data.value().get()) == "test-string-1");
  auto section_data_2 = fake_rdb.LoadString();
  VMSDK_EXPECT_OK_STATUSOR(section_data_2);
  EXPECT_TRUE(
      vmsdk::ToStringView(section_data_2.value().get()) == "test-string-0" ||
      vmsdk::ToStringView(section_data_2.value().get()) == "test-string-1");
  EXPECT_NE(vmsdk::ToStringView(section_data.value().get()),
            vmsdk::ToStringView(section_data_2.value().get()));
}

TEST_F(RDBSerializationTest, PerformRDBSaveSectionSaveFail) {
  FakeSafeRDB fake_rdb;
  auto test_cb = GenerateRDBSectionCallbacks();
  EXPECT_CALL(*test_cb.mock_callbacks, load(testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_CALL(*test_cb.mock_callbacks, save(testing::_, testing::_, testing::_))
      .WillOnce(testing::Invoke(
          [](RedisModuleCtx* ctx, SafeRDB* rdb, int when) -> absl::Status {
            return absl::InternalError("test error");
          }));
  EXPECT_CALL(*test_cb.mock_callbacks,
              section_count(&fake_ctx_, REDISMODULE_AUX_BEFORE_RDB))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(*test_cb.mock_callbacks,
              minimum_semantic_version(testing::_, testing::_))
      .WillOnce(testing::Return(0x0100ff));  // 1.0.255
  RegisterRDBCallback(data_model::RDB_SECTION_INDEX_SCHEMA,
                      std::move(test_cb.callbacks_struct));

  EXPECT_EQ(
      PerformRDBSave(&fake_ctx_, &fake_rdb, REDISMODULE_AUX_BEFORE_RDB).code(),
      absl::StatusCode::kInternal);
}

TEST_F(RDBSerializationTest, PerformRDBSaveTwoRDBSectionOneEmpty) {
  FakeSafeRDB fake_rdb;

  for (int i = 0; i < 2; i++) {
    auto test_cb = GenerateRDBSectionCallbacks();
    EXPECT_CALL(*test_cb.mock_callbacks,
                load(testing::_, testing::_, testing::_))
        .Times(0);
    if (i == 0) {
      EXPECT_CALL(*test_cb.mock_callbacks,
                  save(testing::_, testing::_, testing::_))
          .WillOnce(testing::Invoke(
              [i](RedisModuleCtx* ctx, SafeRDB* rdb, int when) -> absl::Status {
                EXPECT_EQ(when, REDISMODULE_AUX_BEFORE_RDB);
                std::string save_value = absl::StrCat("test-string-", i);
                VMSDK_EXPECT_OK(rdb->SaveStringBuffer(save_value));
                return absl::OkStatus();
              }));
      EXPECT_CALL(*test_cb.mock_callbacks,
                  minimum_semantic_version(testing::_, testing::_))
          .WillOnce(testing::Return(0x0100ff));
    } else {
      EXPECT_CALL(*test_cb.mock_callbacks,
                  save(testing::_, testing::_, testing::_))
          .Times(0);
      EXPECT_CALL(*test_cb.mock_callbacks,
                  minimum_semantic_version(testing::_, testing::_))
          .Times(0);
    }
    EXPECT_CALL(*test_cb.mock_callbacks,
                section_count(&fake_ctx_, REDISMODULE_AUX_BEFORE_RDB))
        .WillOnce(testing::Return(i == 0 ? 1 : 0));
    RegisterRDBCallback(i == 0 ? data_model::RDB_SECTION_INDEX_SCHEMA
                               : data_model::RDB_SECTION_GLOBAL_METADATA,
                        std::move(test_cb.callbacks_struct));
  }

  VMSDK_EXPECT_OK(
      PerformRDBSave(&fake_ctx_, &fake_rdb, REDISMODULE_AUX_BEFORE_RDB));
  auto sem_ver = fake_rdb.LoadUnsigned();
  VMSDK_EXPECT_OK(sem_ver);
  EXPECT_EQ(sem_ver.value(), 0x0100ff);
  auto section_count = fake_rdb.LoadUnsigned();
  VMSDK_EXPECT_OK(section_count);
  EXPECT_EQ(section_count.value(), 1);

  auto section_data = fake_rdb.LoadString();
  VMSDK_EXPECT_OK_STATUSOR(section_data);
  EXPECT_EQ(vmsdk::ToStringView(section_data.value().get()), "test-string-0");
}

TEST_F(RDBSerializationTest, PerformRDBLoadInvalidEncVer) {
  FakeSafeRDB fake_rdb;
  EXPECT_EQ(PerformRDBLoad(&fake_ctx_, &fake_rdb, kCurrentEncVer + 1).code(),
            absl::StatusCode::kInternal);
}

TEST_F(RDBSerializationTest, PerformRDBLoadNewerSemVer) {
  FakeSafeRDB fake_rdb;
  VMSDK_EXPECT_OK(fake_rdb.SaveUnsigned(kCurrentSemanticVersion + 0x010000));
  EXPECT_EQ(PerformRDBLoad(&fake_ctx_, &fake_rdb, kCurrentEncVer).code(),
            absl::StatusCode::kInternal);
}

TEST_F(RDBSerializationTest, PerformRDBLoadNoRDBSections) {
  FakeSafeRDB fake_rdb;
  VMSDK_EXPECT_OK(fake_rdb.SaveUnsigned(kCurrentSemanticVersion));
  VMSDK_EXPECT_OK(fake_rdb.SaveUnsigned(0));
  VMSDK_EXPECT_OK(PerformRDBLoad(&fake_ctx_, &fake_rdb, kCurrentEncVer));
}

TEST_F(RDBSerializationTest, PerformRDBLoadRDBSectionNotRegistered) {
  FakeSafeRDB fake_rdb;
  VMSDK_EXPECT_OK(fake_rdb.SaveUnsigned(kCurrentSemanticVersion));
  VMSDK_EXPECT_OK(fake_rdb.SaveUnsigned(1));
  data_model::RDBSection section;
  section.set_type(data_model::RDB_SECTION_INDEX_SCHEMA);
  section.set_supplemental_count(1);
  std::string serialized_sec = section.SerializeAsString();
  VMSDK_EXPECT_OK(fake_rdb.SaveStringBuffer(serialized_sec));

  data_model::SupplementalContentHeader supp;
  std::string serialized_supp = supp.SerializeAsString();
  VMSDK_EXPECT_OK(fake_rdb.SaveStringBuffer(serialized_supp));

  data_model::SupplementalContentChunk chunk_1;
  chunk_1.set_binary_content("test-string");
  std::string serialized_chunk_1 = chunk_1.SerializeAsString();
  VMSDK_EXPECT_OK(fake_rdb.SaveStringBuffer(serialized_chunk_1));

  data_model::SupplementalContentChunk chunk_2;
  std::string serialized_chunk_2 = chunk_2.SerializeAsString();
  VMSDK_EXPECT_OK(fake_rdb.SaveStringBuffer(serialized_chunk_2));

  VMSDK_EXPECT_OK(PerformRDBLoad(&fake_ctx_, &fake_rdb, kCurrentEncVer));

  // Full buffer should be consumed.
  EXPECT_EQ(fake_rdb.buffer_.rdbuf()->in_avail(), 0);
}

TEST_F(RDBSerializationTest, PerformRDBLoadRDBSectionRegistered) {
  FakeSafeRDB fake_rdb;
  VMSDK_EXPECT_OK(fake_rdb.SaveUnsigned(kCurrentSemanticVersion));
  VMSDK_EXPECT_OK(fake_rdb.SaveUnsigned(1));
  data_model::RDBSection section;
  section.set_type(data_model::RDB_SECTION_INDEX_SCHEMA);
  section.mutable_index_schema_contents()->add_subscribed_key_prefixes("test");
  std::string serialized = section.SerializeAsString();

  auto test_cb = GenerateRDBSectionCallbacks();
  EXPECT_CALL(*test_cb.mock_callbacks, load(testing::_, testing::_, testing::_))
      .WillOnce(
          testing::Invoke([](RedisModuleCtx* ctx,
                             std::unique_ptr<data_model::RDBSection> section,
                             SupplementalContentIter&& iter) {
            EXPECT_EQ(section->type(), data_model::RDB_SECTION_INDEX_SCHEMA);
            EXPECT_EQ(
                section->index_schema_contents().subscribed_key_prefixes_size(),
                1);
            EXPECT_FALSE(iter.HasNext());
            return absl::OkStatus();
          }));
  EXPECT_CALL(*test_cb.mock_callbacks, save(testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_CALL(*test_cb.mock_callbacks,
              section_count(&fake_ctx_, REDISMODULE_AUX_BEFORE_RDB))
      .Times(0);
  EXPECT_CALL(*test_cb.mock_callbacks,
              minimum_semantic_version(testing::_, testing::_))
      .Times(0);
  RegisterRDBCallback(data_model::RDB_SECTION_INDEX_SCHEMA,
                      std::move(test_cb.callbacks_struct));

  VMSDK_EXPECT_OK(fake_rdb.SaveStringBuffer(serialized));
  VMSDK_EXPECT_OK(PerformRDBLoad(&fake_ctx_, &fake_rdb, kCurrentEncVer));
}

TEST_F(RDBSerializationTest, PerformRDBLoadRDBSectionCallbackFailure) {
  FakeSafeRDB fake_rdb;
  VMSDK_EXPECT_OK(fake_rdb.SaveUnsigned(kCurrentSemanticVersion));
  VMSDK_EXPECT_OK(fake_rdb.SaveUnsigned(1));
  data_model::RDBSection section;
  section.set_type(data_model::RDB_SECTION_INDEX_SCHEMA);
  section.mutable_index_schema_contents()->add_subscribed_key_prefixes("test");
  std::string serialized = section.SerializeAsString();

  auto test_cb = GenerateRDBSectionCallbacks();
  EXPECT_CALL(*test_cb.mock_callbacks, load(testing::_, testing::_, testing::_))
      .WillOnce(
          testing::Invoke([](RedisModuleCtx* ctx,
                             std::unique_ptr<data_model::RDBSection> section,
                             SupplementalContentIter&& iter) {
            return absl::InternalError("test");
          }));
  EXPECT_CALL(*test_cb.mock_callbacks, save(testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_CALL(*test_cb.mock_callbacks,
              section_count(&fake_ctx_, REDISMODULE_AUX_BEFORE_RDB))
      .Times(0);
  EXPECT_CALL(*test_cb.mock_callbacks,
              minimum_semantic_version(testing::_, testing::_))
      .Times(0);
  RegisterRDBCallback(data_model::RDB_SECTION_INDEX_SCHEMA,
                      std::move(test_cb.callbacks_struct));

  VMSDK_EXPECT_OK(fake_rdb.SaveStringBuffer(serialized));
  EXPECT_EQ(PerformRDBLoad(&fake_ctx_, &fake_rdb, kCurrentEncVer).code(),
            absl::StatusCode::kInternal);
}

}  // namespace

}  // namespace valkey_search
