/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/schema_manager.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "src/coordinator/metadata_manager.h"
#include "testing/common.h"
#include "testing/coordinator/common.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

class SchemaManagerTest : public ValkeySearchTest {
 public:
  void SetUp() override {
    ValkeySearchTest::SetUp();
    std::string test_index_schema_proto_str = R"(
        name: "test_key"
        db_num: 0
        subscribed_key_prefixes: "prefix_1"
        attribute_data_type: ATTRIBUTE_DATA_TYPE_HASH
        attributes: {
          alias: "test_attribute_1"
          identifier: "test_identifier_1"
          index: {
            vector_index: {
              dimension_count: 10
              normalize: true
              distance_metric: DISTANCE_METRIC_COSINE
              vector_data_type: VECTOR_DATA_TYPE_FLOAT32
              initial_cap: 100
              hnsw_algorithm {
                m: 240
                ef_construction: 400
                ef_runtime: 30
              }
            }
          }
        }
      )";
    ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
        test_index_schema_proto_str, &test_index_schema_proto_));
    mock_client_pool_ = std::make_unique<coordinator::MockClientPool>();
    ON_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
        .WillByDefault(testing::Return(db_num_));
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault(testing::Return(&fake_ctx_));
    ON_CALL(*kMockValkeyModule, FreeThreadSafeContext(testing::_))
        .WillByDefault(testing::Return());
    ON_CALL(*kMockValkeyModule, SelectDb(testing::_, db_num_))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));
    test_metadata_manager_ = std::make_unique<coordinator::MetadataManager>(
        &fake_ctx_, *mock_client_pool_);
  }
  void TearDown() override {
    test_metadata_manager_.reset(nullptr);
    ValkeySearchTest::TearDown();
  }
  std::unique_ptr<coordinator::MockClientPool> mock_client_pool_;
  std::unique_ptr<coordinator::MetadataManager> test_metadata_manager_;
  data_model::IndexSchema test_index_schema_proto_;
  int db_num_ = 0;
  std::string index_name_ = "test_key";
};

TEST_F(SchemaManagerTest, TestCreateIndexSchema) {
  for (bool coordinator_enabled : {true, false}) {
    bool callback_triggered = false;
    if (coordinator_enabled) {
      coordinator::MetadataManager::InitInstance(
          std::move(test_metadata_manager_));
    }
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, [&callback_triggered]() { callback_triggered = true; },
        nullptr, coordinator_enabled));
    VMSDK_EXPECT_OK(SchemaManager::Instance()
                        .CreateIndexSchema(&fake_ctx_, test_index_schema_proto_)
                        .status());
    auto index_schema =
        SchemaManager::Instance().GetIndexSchema(db_num_, index_name_);
    VMSDK_EXPECT_OK(index_schema);
    EXPECT_THAT(
        SchemaManager::Instance().GetIndexSchema(db_num_, index_name_).value(),
        testing::NotNull());
    EXPECT_TRUE(callback_triggered);
  }
}

TEST_F(SchemaManagerTest, TestCreateIndexSchemaCallbackOnlyTriggeredOnce) {
  for (bool coordinator_enabled : {true, false}) {
    int callback_triggered = 0;
    if (coordinator_enabled) {
      coordinator::MetadataManager::InitInstance(
          std::move(test_metadata_manager_));
    }
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, [&callback_triggered]() { callback_triggered++; }, nullptr,
        coordinator_enabled));
    VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
        &fake_ctx_, test_index_schema_proto_));
    data_model::IndexSchema test_index_schema_proto_2 =
        test_index_schema_proto_;
    test_index_schema_proto_2.set_name("test_key_2");
    VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
        &fake_ctx_, test_index_schema_proto_2));
    EXPECT_EQ(callback_triggered, 1);
  }
}

TEST_F(SchemaManagerTest, TestCreateIndexSchemaAlreadyExists) {
  for (bool coordinator_enabled : {true, false}) {
    int callback_triggered = 0;
    if (coordinator_enabled) {
      coordinator::MetadataManager::InitInstance(
          std::move(test_metadata_manager_));
    }
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, [&callback_triggered]() { callback_triggered++; }, nullptr,
        coordinator_enabled));
    VMSDK_EXPECT_OK(SchemaManager::Instance()
                        .CreateIndexSchema(&fake_ctx_, test_index_schema_proto_)
                        .status());
    auto status = SchemaManager::Instance()
                      .CreateIndexSchema(&fake_ctx_, test_index_schema_proto_)
                      .status();
    EXPECT_EQ(status.code(), absl::StatusCode::kAlreadyExists);
    EXPECT_EQ(status.message(),
              absl::StrFormat("Index %s already exists.", index_name_));
    EXPECT_EQ(callback_triggered, 1);
  }
}

TEST_F(SchemaManagerTest, TestCreateIndexSchemaInvalid) {
  for (bool coordinator_enabled : {true, false}) {
    if (coordinator_enabled) {
      coordinator::MetadataManager::InitInstance(
          std::move(test_metadata_manager_));
    }
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, coordinator_enabled));
    EXPECT_EQ(SchemaManager::Instance()
                  .CreateIndexSchema(&fake_ctx_, data_model::IndexSchema())
                  .status()
                  .code(),
              absl::StatusCode::kInvalidArgument);
  }
}

TEST_F(SchemaManagerTest, TestRemoveIndexSchema) {
  for (bool coordinator_enabled : {true, false}) {
    if (coordinator_enabled) {
      coordinator::MetadataManager::InitInstance(
          std::move(test_metadata_manager_));
    }
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, coordinator_enabled));
    VMSDK_EXPECT_OK(SchemaManager::Instance()
                        .CreateIndexSchema(&fake_ctx_, test_index_schema_proto_)
                        .status());
    VMSDK_EXPECT_OK(
        SchemaManager::Instance().RemoveIndexSchema(db_num_, index_name_));
    EXPECT_EQ(SchemaManager::Instance()
                  .GetIndexSchema(db_num_, index_name_)
                  .status()
                  .code(),
              absl::StatusCode::kNotFound);
  }
}

TEST_F(SchemaManagerTest, TestRemoveIndexSchemaNotFound) {
  for (bool coordinator_enabled : {true, false}) {
    if (coordinator_enabled) {
      coordinator::MetadataManager::InitInstance(
          std::move(test_metadata_manager_));
    }
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, coordinator_enabled));
    EXPECT_EQ(SchemaManager::Instance()
                  .RemoveIndexSchema(&fake_ctx_, db_num_, index_name_)
                  .code(),
              absl::StatusCode::kNotFound);
  }
}

TEST_F(SchemaManagerTest, TestOnFlushDB) {
  for (bool coordinator_enabled : {true, false}) {
    if (coordinator_enabled) {
      coordinator::MetadataManager::InitInstance(
          std::move(test_metadata_manager_));
    }
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, coordinator_enabled));
    VMSDK_EXPECT_OK(SchemaManager::Instance()
                        .CreateIndexSchema(&fake_ctx_, test_index_schema_proto_)
                        .status());
    auto previous_schema_or =
        SchemaManager::Instance().GetIndexSchema(db_num_, index_name_);
    VMSDK_EXPECT_OK(previous_schema_or);
    auto previous_schema = previous_schema_or.value();
    SchemaManager::Instance().OnFlushDBEnded(&fake_ctx_);
    if (!coordinator_enabled) {
      // Expect it to be flushed
      EXPECT_EQ(SchemaManager::Instance().GetNumberOfIndexSchemas(), 0);
    } else {
      // Should be kept, but recreated
      EXPECT_EQ(SchemaManager::Instance().GetNumberOfIndexSchemas(), 1);
      auto new_schema_or =
          SchemaManager::Instance().GetIndexSchema(db_num_, index_name_);
      VMSDK_EXPECT_OK(new_schema_or);
      auto new_schema = new_schema_or.value();
      EXPECT_NE(new_schema, previous_schema);
    }
  }
}

TEST_F(SchemaManagerTest, TestSaveIndexesBeforeRDB) {
  ON_CALL(*kMockValkeyModule, GetContextFromIO(testing::_))
      .WillByDefault(testing::Return(&fake_ctx_));
  auto schema =
      CreateIndexSchema(index_name_, &fake_ctx_, nullptr, {}, db_num_).value();
  ValkeyModuleIO *fake_rdb = reinterpret_cast<ValkeyModuleIO *>(0xDEADBEEF);
  EXPECT_CALL(*kMockValkeyModule, SaveUnsigned(fake_rdb, testing::_)).Times(0);
  EXPECT_CALL(*schema, RDBSave(testing::_)).Times(0);
  SafeRDB fake_safe_rdb(fake_rdb);
  VMSDK_EXPECT_OK(SchemaManager::Instance().SaveIndexes(
      &fake_ctx_, &fake_safe_rdb, VALKEYMODULE_AUX_BEFORE_RDB));
}

TEST_F(SchemaManagerTest, TestSaveIndexesAfterRDB) {
  ON_CALL(*kMockValkeyModule, GetContextFromIO(testing::_))
      .WillByDefault(testing::Return(&fake_ctx_));
  auto schema =
      CreateIndexSchema(index_name_, &fake_ctx_, nullptr, {}, db_num_).value();
  ValkeyModuleIO *fake_rdb = reinterpret_cast<ValkeyModuleIO *>(0xDEADBEEF);
  EXPECT_CALL(*schema, RDBSave(testing::_))
      .WillOnce(testing::Return(absl::OkStatus()));
  SafeRDB fake_safe_rdb(fake_rdb);
  VMSDK_EXPECT_OK(SchemaManager::Instance().SaveIndexes(
      &fake_ctx_, &fake_safe_rdb, VALKEYMODULE_AUX_AFTER_RDB));
}

TEST_F(SchemaManagerTest, TestLoadIndexDuringReplication) {
  ValkeyModuleEvent eid;
  std::string existing_index_name = "test_key_2";
  auto test_index_schema_or = CreateVectorHNSWSchema(
      existing_index_name, &fake_ctx_, nullptr, {}, db_num_);
  ON_CALL(*kMockValkeyModule, GetContextFromIO(testing::_))
      .WillByDefault(testing::Return(&fake_ctx_));
  SchemaManager::Instance().OnLoadingCallback(
      &fake_ctx_, eid, VALKEYMODULE_SUBEVENT_LOADING_REPL_START, nullptr);

  FakeSafeRDB fake_rdb;
  auto section = std::make_unique<data_model::RDBSection>();
  section->set_type(data_model::RDB_SECTION_INDEX_SCHEMA);
  section->mutable_index_schema_contents()->CopyFrom(test_index_schema_proto_);
  section->set_supplemental_count(0);

  VMSDK_EXPECT_OK(SchemaManager::Instance().LoadIndex(
      &fake_ctx_, std::move(section), SupplementalContentIter(&fake_rdb, 0)));

  // Should be staged, but not applied.
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().GetIndexSchema(db_num_, existing_index_name));
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(db_num_, index_name_)
                .status()
                .code(),
            absl::StatusCode::kNotFound);

  // Loading callback should apply the new schemas.
  SchemaManager::Instance().OnLoadingCallback(
      &fake_ctx_, eid, VALKEYMODULE_SUBEVENT_LOADING_ENDED, nullptr);
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(db_num_, existing_index_name)
                .status()
                .code(),
            absl::StatusCode::kNotFound);
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().GetIndexSchema(db_num_, index_name_));
}

TEST_F(SchemaManagerTest, TestLoadIndexNoReplication) {
  ValkeyModuleEvent eid;
  ON_CALL(*kMockValkeyModule, GetContextFromIO(testing::_))
      .WillByDefault(testing::Return(&fake_ctx_));

  FakeSafeRDB fake_rdb;
  auto section = std::make_unique<data_model::RDBSection>();
  section->set_type(data_model::RDB_SECTION_INDEX_SCHEMA);
  section->mutable_index_schema_contents()->CopyFrom(test_index_schema_proto_);
  section->set_supplemental_count(0);

  VMSDK_EXPECT_OK(SchemaManager::Instance().LoadIndex(
      &fake_ctx_, std::move(section), SupplementalContentIter(&fake_rdb, 0)));

  // Should be loaded already, no callback needed.
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().GetIndexSchema(db_num_, index_name_));

  // Loading callback should not remove the new schemas.
  SchemaManager::Instance().OnLoadingCallback(
      &fake_ctx_, eid, VALKEYMODULE_SUBEVENT_LOADING_ENDED, nullptr);
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().GetIndexSchema(db_num_, index_name_));
}

TEST_F(SchemaManagerTest, TestLoadIndexExistingData) {
  ValkeyModuleEvent eid;
  ON_CALL(*kMockValkeyModule, GetContextFromIO(testing::_))
      .WillByDefault(testing::Return(&fake_ctx_));

  // Load two indices as existing
  FakeSafeRDB fake_rdb;
  for (int i = 0; i < 2; i++) {
    auto section = std::make_unique<data_model::RDBSection>();
    section->set_type(data_model::RDB_SECTION_INDEX_SCHEMA);
    auto existing = test_index_schema_proto_;
    existing.set_name(absl::StrFormat("existing_%d", i));
    section->mutable_index_schema_contents()->CopyFrom(existing);
    section->set_supplemental_count(0);

    VMSDK_EXPECT_OK(SchemaManager::Instance().LoadIndex(
        &fake_ctx_, std::move(section), SupplementalContentIter(&fake_rdb, 0)));
  }
  SchemaManager::Instance().OnLoadingCallback(
      &fake_ctx_, eid, VALKEYMODULE_SUBEVENT_LOADING_ENDED, nullptr);
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().GetIndexSchema(db_num_, "existing_0"));
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().GetIndexSchema(db_num_, "existing_1"));

  // Replace one index and add a new one.
  for (int i = 1; i < 3; i++) {
    auto section = std::make_unique<data_model::RDBSection>();
    section->set_type(data_model::RDB_SECTION_INDEX_SCHEMA);
    auto existing = test_index_schema_proto_;
    existing.set_name(absl::StrFormat("existing_%d", i));
    existing.mutable_subscribed_key_prefixes()->Add("new_prefix");
    section->mutable_index_schema_contents()->CopyFrom(existing);
    section->set_supplemental_count(0);

    VMSDK_EXPECT_OK(SchemaManager::Instance().LoadIndex(
        &fake_ctx_, std::move(section), SupplementalContentIter(&fake_rdb, 0)));
  }

  SchemaManager::Instance().OnLoadingCallback(
      &fake_ctx_, eid, VALKEYMODULE_SUBEVENT_LOADING_ENDED, nullptr);
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().GetIndexSchema(db_num_, "existing_0"));
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().GetIndexSchema(db_num_, "existing_1"));
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().GetIndexSchema(db_num_, "existing_2"));
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(db_num_, "existing_1")
                .value()
                ->GetKeyPrefixes()
                .size(),
            2);
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(db_num_, "existing_2")
                .value()
                ->GetKeyPrefixes()
                .size(),
            2);
}

TEST_F(SchemaManagerTest, OnServerCronCallback) {
  InitThreadPools(10, 5);
  auto test_index_schema_or = CreateVectorHNSWSchema(
      "index_schema_key", &fake_ctx_, nullptr, {}, db_num_);
  ValkeyModuleEvent eid;
  EXPECT_TRUE(SchemaManager::Instance().IsIndexingInProgress());
  SchemaManager::Instance().OnServerCronCallback(&fake_ctx_, eid, 0, nullptr);
  EXPECT_FALSE(SchemaManager::Instance().IsIndexingInProgress());
}

struct OnSwapDBCallbackTestCase {
  std::string test_name;
  int32_t index_schema_db_num;
  int32_t swap_dbnum_first;
  int32_t swap_dbnum_second;
  bool is_backfill_in_progress{false};
};

class OnSwapDBCallbackTest
    : public ValkeySearchTestWithParam<OnSwapDBCallbackTestCase> {};

INSTANTIATE_TEST_SUITE_P(
    OnSwapDBCallbackTests, OnSwapDBCallbackTest,
    testing::ValuesIn<OnSwapDBCallbackTestCase>({
        {
            .test_name = "swap_first",
            .index_schema_db_num = 0,
            .swap_dbnum_first = 1,
            .swap_dbnum_second = 0,
        },
        {
            .test_name = "swap_second",
            .index_schema_db_num = 0,
            .swap_dbnum_first = 0,
            .swap_dbnum_second = 2,
        },
        {
            .test_name = "no_swap",
            .index_schema_db_num = 0,
            .swap_dbnum_first = 0,
            .swap_dbnum_second = 0,
        },
        {
            .test_name = "invalid_swap",
            .index_schema_db_num = 0,
            .swap_dbnum_first = 1,
            .swap_dbnum_second = 2,
        },
        {
            .test_name = "swap_first_backfill",
            .index_schema_db_num = 0,
            .swap_dbnum_first = 1,
            .swap_dbnum_second = 0,
            .is_backfill_in_progress = true,
        },
        {
            .test_name = "swap_second_backfill",
            .index_schema_db_num = 0,
            .swap_dbnum_first = 0,
            .swap_dbnum_second = 2,
            .is_backfill_in_progress = true,
        },
        {
            .test_name = "no_swap_backfill",
            .index_schema_db_num = 0,
            .swap_dbnum_first = 0,
            .swap_dbnum_second = 0,
            .is_backfill_in_progress = true,
        },
        {
            .test_name = "invalid_swap_backfill",
            .index_schema_db_num = 0,
            .swap_dbnum_first = 1,
            .swap_dbnum_second = 2,
            .is_backfill_in_progress = true,
        },
    }),
    [](const testing::TestParamInfo<OnSwapDBCallbackTestCase> &info) {
      return info.param.test_name;
    });

TEST_P(OnSwapDBCallbackTest, OnSwapDBCallback) {
  const OnSwapDBCallbackTestCase &test_case = GetParam();
  auto test_index_schema_or =
      CreateVectorHNSWSchema("index_schema_key", &fake_ctx_, nullptr, {},
                             test_case.index_schema_db_num);
  VMSDK_EXPECT_OK(test_index_schema_or);
  auto test_index_schema = test_index_schema_or.value();
  EXPECT_TRUE(SchemaManager::Instance().IsIndexingInProgress());
  ValkeyModuleSwapDbInfo swap_db_info;
  swap_db_info.dbnum_first = test_case.swap_dbnum_first;
  swap_db_info.dbnum_second = test_case.swap_dbnum_second;
  ValkeyModuleEvent eid;
  int32_t expected_dbnum = -1;
  if (test_case.index_schema_db_num == test_case.swap_dbnum_first) {
    expected_dbnum = test_case.swap_dbnum_second;
  } else if (test_case.index_schema_db_num == test_case.swap_dbnum_second) {
    expected_dbnum = test_case.swap_dbnum_first;
  }
  if (test_case.is_backfill_in_progress) {
    if (expected_dbnum == -1) {
      EXPECT_CALL(
          *kMockValkeyModule,
          SelectDb(test_index_schema->backfill_job_.Get()->scan_ctx.get(),
                   test_case.index_schema_db_num))
          .Times(0);
    } else {
      EXPECT_CALL(
          *kMockValkeyModule,
          SelectDb(test_index_schema->backfill_job_.Get()->scan_ctx.get(),
                   expected_dbnum))
          .WillOnce(testing::Return(1));
    }
  } else {
    SchemaManager::Instance().OnServerCronCallback(nullptr, eid, 0, nullptr);
    EXPECT_FALSE(SchemaManager::Instance().IsIndexingInProgress());
  }
  if (test_case.index_schema_db_num == test_case.swap_dbnum_first ||
      test_case.index_schema_db_num == test_case.swap_dbnum_second) {
    EXPECT_CALL(*test_index_schema, OnSwapDB(&swap_db_info)).Times(1);
  } else {
    EXPECT_CALL(*test_index_schema, OnSwapDB(&swap_db_info)).Times(0);
  }
  SchemaManager::Instance().OnSwapDB(&swap_db_info);

  EXPECT_EQ(test_index_schema->db_num_, expected_dbnum != -1
                                            ? expected_dbnum
                                            : test_case.index_schema_db_num);
}

}  // namespace valkey_search
