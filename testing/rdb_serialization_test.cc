/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
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
  void SetUp() override { TestValkeyModule_Init(); }
  ValkeyModuleIO* fake_valkey_module_io_ = (ValkeyModuleIO*)0xBADF00D1;
};

TEST_F(SafeRDBTest, LoadSizeTSuccess) {
  size_t expected_value = 34;
  EXPECT_CALL(*kMockValkeyModule, LoadUnsigned(fake_valkey_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  auto res = rdb_stream.LoadSizeT();
  VMSDK_EXPECT_OK(res);
  EXPECT_EQ(res.value(), expected_value);
}

TEST_F(SafeRDBTest, LoadSizeTFailure) {
  size_t expected_value = 34;
  EXPECT_CALL(*kMockValkeyModule, LoadUnsigned(fake_valkey_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  EXPECT_EQ(rdb_stream.LoadSizeT().status().code(),
            absl::StatusCode::kInternal);
}

TEST_F(SafeRDBTest, LoadUnsignedSuccess) {
  unsigned int expected_value = 34;
  EXPECT_CALL(*kMockValkeyModule, LoadUnsigned(fake_valkey_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  auto res = rdb_stream.LoadUnsigned();
  VMSDK_EXPECT_OK(res);
  EXPECT_EQ(res.value(), expected_value);
}

TEST_F(SafeRDBTest, LoadUnsignedFailure) {
  unsigned int actual_value;
  unsigned int expected_value = 34;
  EXPECT_CALL(*kMockValkeyModule, LoadUnsigned(fake_valkey_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  EXPECT_EQ(rdb_stream.LoadUnsigned().status().code(),
            absl::StatusCode::kInternal);
}

TEST_F(SafeRDBTest, LoadSignedSuccess) {
  int expected_value = 34;
  EXPECT_CALL(*kMockValkeyModule, LoadSigned(fake_valkey_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  auto res = rdb_stream.LoadSigned();
  VMSDK_EXPECT_OK(res);
  EXPECT_EQ(res.value(), expected_value);
}

TEST_F(SafeRDBTest, LoadSignedFailure) {
  int expected_value = 34;
  EXPECT_CALL(*kMockValkeyModule, LoadSigned(fake_valkey_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  EXPECT_EQ(rdb_stream.LoadSigned().status().code(),
            absl::StatusCode::kInternal);
}

TEST_F(SafeRDBTest, LoadDoubleSuccess) {
  double actual_value;
  double expected_value = 34.5;
  EXPECT_CALL(*kMockValkeyModule, LoadDouble(fake_valkey_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  auto res = rdb_stream.LoadDouble();
  VMSDK_EXPECT_OK(res);
  EXPECT_EQ(res.value(), expected_value);
}

TEST_F(SafeRDBTest, LoadDoubleFailure) {
  double expected_value = 34.5;
  EXPECT_CALL(*kMockValkeyModule, LoadDouble(fake_valkey_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  EXPECT_EQ(rdb_stream.LoadDouble().status().code(),
            absl::StatusCode::kInternal);
}

TEST_F(SafeRDBTest, LoadStringSuccess) {
  ValkeyModuleString* expected_value =
      TestValkeyModule_CreateStringPrintf(nullptr, "test");
  EXPECT_CALL(*kMockValkeyModule, LoadString(fake_valkey_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  auto actual_value_or = rdb_stream.LoadString();
  VMSDK_EXPECT_OK(actual_value_or.status());
  EXPECT_EQ(actual_value_or.value().get(), expected_value);
}

TEST_F(SafeRDBTest, LoadStringFailure) {
  ValkeyModuleString* expected_value =
      TestValkeyModule_CreateStringPrintf(nullptr, "test");
  EXPECT_CALL(*kMockValkeyModule, LoadString(fake_valkey_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  EXPECT_EQ(rdb_stream.LoadString().status().code(),
            absl::StatusCode::kInternal);
}

TEST_F(SafeRDBTest, SaveSizeTSuccess) {
  size_t value = 34;
  EXPECT_CALL(*kMockValkeyModule, SaveUnsigned(fake_valkey_module_io_, value));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.SaveSizeT(value));
}

TEST_F(SafeRDBTest, SaveSizeTFailure) {
  size_t value = 34;
  EXPECT_CALL(*kMockValkeyModule, SaveUnsigned(fake_valkey_module_io_, value));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  EXPECT_EQ(rdb_stream.SaveSizeT(value).code(), absl::StatusCode::kInternal);
}

TEST_F(SafeRDBTest, SaveUnsignedSuccess) {
  unsigned int value = 34;
  EXPECT_CALL(*kMockValkeyModule, SaveUnsigned(fake_valkey_module_io_, value));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.SaveUnsigned(value));
}

TEST_F(SafeRDBTest, SaveUnsignedFailure) {
  unsigned int value = 34;
  EXPECT_CALL(*kMockValkeyModule, SaveUnsigned(fake_valkey_module_io_, value));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  EXPECT_EQ(rdb_stream.SaveUnsigned(value).code(), absl::StatusCode::kInternal);
}

TEST_F(SafeRDBTest, SaveSignedSuccess) {
  int64_t value = 34;
  EXPECT_CALL(*kMockValkeyModule, SaveSigned(fake_valkey_module_io_, value));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.SaveSigned(value));
}

TEST_F(SafeRDBTest, SaveSignedFailure) {
  int64_t value = 34;
  EXPECT_CALL(*kMockValkeyModule, SaveSigned(fake_valkey_module_io_, value));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  EXPECT_EQ(rdb_stream.SaveSigned(value).code(), absl::StatusCode::kInternal);
}

TEST_F(SafeRDBTest, SaveDoubleSuccess) {
  double value = 34.5;
  EXPECT_CALL(*kMockValkeyModule, SaveDouble(fake_valkey_module_io_, value));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.SaveDouble(value));
}

TEST_F(SafeRDBTest, SaveDoubleFailure) {
  double value = 34.5;
  EXPECT_CALL(*kMockValkeyModule, SaveDouble(fake_valkey_module_io_, value));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  EXPECT_EQ(rdb_stream.SaveDouble(value).code(), absl::StatusCode::kInternal);
}

TEST_F(SafeRDBTest, SaveStringBufferSuccess) {
  absl::string_view value = "test";
  EXPECT_CALL(*kMockValkeyModule, SaveStringBuffer(fake_valkey_module_io_,
                                                   value.data(), value.size()));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(0));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.SaveStringBuffer(value));
}

TEST_F(SafeRDBTest, SaveStringBufferFailureIOError) {
  absl::string_view value = "test";
  EXPECT_CALL(*kMockValkeyModule, SaveStringBuffer(fake_valkey_module_io_,
                                                   value.data(), value.size()));
  EXPECT_CALL(*kMockValkeyModule, IsIOError(fake_valkey_module_io_))
      .WillOnce(testing::Return(1));
  SafeRDB rdb_stream(fake_valkey_module_io_);
  EXPECT_EQ(rdb_stream.SaveStringBuffer(value).code(),
            absl::StatusCode::kInternal);
}

class MockRDBSectionCallback {
 public:
  MOCK_METHOD(absl::Status, load,
              (ValkeyModuleCtx * ctx,
               std::unique_ptr<data_model::RDBSection> section,
               SupplementalContentIter&& iter));
  MOCK_METHOD(absl::Status, save,
              (ValkeyModuleCtx * ctx, SafeRDB* rdb, int when));
  MOCK_METHOD(int, section_count, (ValkeyModuleCtx * ctx, int when));
  MOCK_METHOD(int, minimum_semantic_version, (ValkeyModuleCtx * ctx, int when));
};

class RDBSerializationTest : public ValkeySearchTest {
 protected:
  struct TestRDBSectionCallbacks {
    std::shared_ptr<MockRDBSectionCallback> mock_callbacks;
    RDBSectionCallbacks callbacks_struct;
  };
  void SetUp() override { TestValkeyModule_Init(); }
  void TearDown() override {
    TestValkeyModule_Teardown();
    ClearRDBCallbacks();
  }
  TestRDBSectionCallbacks GenerateRDBSectionCallbacks() {
    auto mock_callbacks =
        std::make_shared<testing::NiceMock<MockRDBSectionCallback>>();
    RDBSectionCallbacks callbacks_struct{
        .load =
            [mock_callbacks](ValkeyModuleCtx* ctx,
                             std::unique_ptr<data_model::RDBSection> section,
                             SupplementalContentIter&& iter) {
              return mock_callbacks->load(ctx, std::move(section),
                                          std::move(iter));
            },
        .save = [mock_callbacks](
                    ValkeyModuleCtx* ctx, SafeRDB* rdb,
                    int when) { return mock_callbacks->save(ctx, rdb, when); },
        .section_count =
            [mock_callbacks](ValkeyModuleCtx* ctx, int when) {
              return mock_callbacks->section_count(ctx, when);
            },
        .minimum_semantic_version =
            [mock_callbacks](ValkeyModuleCtx* ctx, int when) {
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
      *kMockValkeyModule,
      CreateDataType(&fake_ctx_, testing::StrEq(kValkeySearchModuleTypeName),
                     kCurrentEncVer, testing::_))
      .WillOnce([](ValkeyModuleCtx* ctx, const char* name, int encver,
                   ValkeyModuleTypeMethods* type_methods) -> ValkeyModuleType* {
        EXPECT_EQ(type_methods->aux_load, AuxLoadCallback);
        EXPECT_EQ(type_methods->aux_save2, AuxSaveCallback);
        EXPECT_EQ(type_methods->aux_save, nullptr);
        EXPECT_EQ(type_methods->aux_save_triggers, VALKEYMODULE_AUX_AFTER_RDB);
        return (ValkeyModuleType*)0xBAADF00D;
      });
  VMSDK_EXPECT_OK(RegisterModuleType(&fake_ctx_));
}

TEST_F(RDBSerializationTest, RegisterModuleTypeReturnNullptr) {
  EXPECT_CALL(
      *kMockValkeyModule,
      CreateDataType(&fake_ctx_, testing::StrEq(kValkeySearchModuleTypeName),
                     kCurrentEncVer, testing::_))
      .WillOnce([](ValkeyModuleCtx* ctx, const char* name, int encver,
                   ValkeyModuleTypeMethods* type_methods) -> ValkeyModuleType* {
        return nullptr;
      });
  EXPECT_EQ(RegisterModuleType(&fake_ctx_).code(), absl::StatusCode::kInternal);
}

TEST_F(RDBSerializationTest, PerformRDBSaveNoRegisteredTypes) {
  FakeSafeRDB fake_rdb;
  VMSDK_EXPECT_OK(
      PerformRDBSave(&fake_ctx_, &fake_rdb, VALKEYMODULE_AUX_BEFORE_RDB));
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
              section_count(&fake_ctx_, VALKEYMODULE_AUX_BEFORE_RDB))
      .WillOnce(testing::Return(0));
  EXPECT_CALL(*test_cb.mock_callbacks,
              minimum_semantic_version(testing::_, testing::_))
      .Times(0);
  RegisterRDBCallback(data_model::RDB_SECTION_INDEX_SCHEMA,
                      std::move(test_cb.callbacks_struct));
  VMSDK_EXPECT_OK(
      PerformRDBSave(&fake_ctx_, &fake_rdb, VALKEYMODULE_AUX_BEFORE_RDB));
  EXPECT_EQ(fake_rdb.buffer_.rdbuf()->in_avail(), 0);
}

TEST_F(RDBSerializationTest, PerformRDBSaveOneRDBSection) {
  FakeSafeRDB fake_rdb;
  auto test_cb = GenerateRDBSectionCallbacks();
  EXPECT_CALL(*test_cb.mock_callbacks, load(testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_CALL(*test_cb.mock_callbacks, save(testing::_, testing::_, testing::_))
      .WillOnce(
          [](ValkeyModuleCtx* ctx, SafeRDB* rdb, int when) -> absl::Status {
            EXPECT_EQ(when, VALKEYMODULE_AUX_BEFORE_RDB);
            VMSDK_EXPECT_OK(rdb->SaveStringBuffer("test-string"));
            return absl::OkStatus();
          });
  EXPECT_CALL(*test_cb.mock_callbacks,
              section_count(&fake_ctx_, VALKEYMODULE_AUX_BEFORE_RDB))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(*test_cb.mock_callbacks,
              minimum_semantic_version(testing::_, testing::_))
      .WillOnce(testing::Return(0x0100ff));  // 1.0.255
  RegisterRDBCallback(data_model::RDB_SECTION_INDEX_SCHEMA,
                      std::move(test_cb.callbacks_struct));
  VMSDK_EXPECT_OK(
      PerformRDBSave(&fake_ctx_, &fake_rdb, VALKEYMODULE_AUX_BEFORE_RDB));
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
        .WillOnce(
            [i](ValkeyModuleCtx* ctx, SafeRDB* rdb, int when) -> absl::Status {
              EXPECT_EQ(when, VALKEYMODULE_AUX_BEFORE_RDB);
              std::string save_value = absl::StrCat("test-string-", i);
              VMSDK_EXPECT_OK(rdb->SaveStringBuffer(save_value));
              return absl::OkStatus();
            });
    EXPECT_CALL(*test_cb.mock_callbacks,
                section_count(&fake_ctx_, VALKEYMODULE_AUX_BEFORE_RDB))
        .WillOnce(testing::Return(1));
    EXPECT_CALL(*test_cb.mock_callbacks,
                minimum_semantic_version(testing::_, testing::_))
        .WillOnce(testing::Return(i == 0 ? 0x0100ff : 0x0200ff));  // vi.00.255
    RegisterRDBCallback(i == 0 ? data_model::RDB_SECTION_INDEX_SCHEMA
                               : data_model::RDB_SECTION_GLOBAL_METADATA,
                        std::move(test_cb.callbacks_struct));
  }

  VMSDK_EXPECT_OK(
      PerformRDBSave(&fake_ctx_, &fake_rdb, VALKEYMODULE_AUX_BEFORE_RDB));
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
      .WillOnce(
          [](ValkeyModuleCtx* ctx, SafeRDB* rdb, int when) -> absl::Status {
            return absl::InternalError("test error");
          });
  EXPECT_CALL(*test_cb.mock_callbacks,
              section_count(&fake_ctx_, VALKEYMODULE_AUX_BEFORE_RDB))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(*test_cb.mock_callbacks,
              minimum_semantic_version(testing::_, testing::_))
      .WillOnce(testing::Return(0x0100ff));  // 1.0.255
  RegisterRDBCallback(data_model::RDB_SECTION_INDEX_SCHEMA,
                      std::move(test_cb.callbacks_struct));

  EXPECT_EQ(
      PerformRDBSave(&fake_ctx_, &fake_rdb, VALKEYMODULE_AUX_BEFORE_RDB).code(),
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
          .WillOnce([i](ValkeyModuleCtx* ctx, SafeRDB* rdb,
                        int when) -> absl::Status {
            EXPECT_EQ(when, VALKEYMODULE_AUX_BEFORE_RDB);
            std::string save_value = absl::StrCat("test-string-", i);
            VMSDK_EXPECT_OK(rdb->SaveStringBuffer(save_value));
            return absl::OkStatus();
          });
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
                section_count(&fake_ctx_, VALKEYMODULE_AUX_BEFORE_RDB))
        .WillOnce(testing::Return(i == 0 ? 1 : 0));
    RegisterRDBCallback(i == 0 ? data_model::RDB_SECTION_INDEX_SCHEMA
                               : data_model::RDB_SECTION_GLOBAL_METADATA,
                        std::move(test_cb.callbacks_struct));
  }

  VMSDK_EXPECT_OK(
      PerformRDBSave(&fake_ctx_, &fake_rdb, VALKEYMODULE_AUX_BEFORE_RDB));
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
      .WillOnce([](ValkeyModuleCtx* ctx,
                   std::unique_ptr<data_model::RDBSection> section,
                   SupplementalContentIter&& iter) {
        EXPECT_EQ(section->type(), data_model::RDB_SECTION_INDEX_SCHEMA);
        EXPECT_EQ(
            section->index_schema_contents().subscribed_key_prefixes_size(), 1);
        EXPECT_FALSE(iter.HasNext());
        return absl::OkStatus();
      });
  EXPECT_CALL(*test_cb.mock_callbacks, save(testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_CALL(*test_cb.mock_callbacks,
              section_count(&fake_ctx_, VALKEYMODULE_AUX_BEFORE_RDB))
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
      .WillOnce([](ValkeyModuleCtx* ctx,
                   std::unique_ptr<data_model::RDBSection> section,
                   SupplementalContentIter&& iter) {
        return absl::InternalError("test");
      });
  EXPECT_CALL(*test_cb.mock_callbacks, save(testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_CALL(*test_cb.mock_callbacks,
              section_count(&fake_ctx_, VALKEYMODULE_AUX_BEFORE_RDB))
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
