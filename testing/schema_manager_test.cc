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
#include "google/protobuf/any.pb.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "src/coordinator/metadata_manager.h"
#include "src/rdb_section.pb.h"
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

    // Mock cluster operations to prevent abort in MetadataManager
    ON_CALL(*kMockValkeyModule,
            Call(testing::_, testing::StrEq("CLUSTER"), testing::StrEq("c"),
                 testing::StrEq("SLOTS")))
        .WillByDefault(testing::Return(
            reinterpret_cast<ValkeyModuleCallReply *>(0xDEADBEEF)));
    ON_CALL(
        *kMockValkeyModule,
        CallReplyType(reinterpret_cast<ValkeyModuleCallReply *>(0xDEADBEEF)))
        .WillByDefault(testing::Return(VALKEYMODULE_REPLY_ARRAY));
    ON_CALL(
        *kMockValkeyModule,
        CallReplyLength(reinterpret_cast<ValkeyModuleCallReply *>(0xDEADBEEF)))
        .WillByDefault(testing::Return(0));
    ON_CALL(*kMockValkeyModule, GetMyClusterID())
        .WillByDefault(testing::Return("fake_node_id"));

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
    EXPECT_EQ(
        status.message(),
        absl::StrFormat("Index %s in database 0 already exists.", index_name_));
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
                  .RemoveIndexSchema(db_num_, index_name_)
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

TEST_F(SchemaManagerTest, TestOnShutdownCallback) {
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
    EXPECT_EQ(SchemaManager::Instance().GetNumberOfIndexSchemas(), 1);
    ValkeyModuleEvent eid;
    SchemaManager::Instance().OnShutdownCallback(&fake_ctx_, eid, 0, nullptr);
    EXPECT_EQ(SchemaManager::Instance().GetNumberOfIndexSchemas(), 0);
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
  InitThreadPools(10, 5, 1);
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

class SchemaManagerAliasTest : public ValkeySearchTest {
 public:
  void SetUp() override {
    ValkeySearchTest::SetUp();
    std::string proto_str = R"(
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
        proto_str, &index_schema_proto_));

    std::string proto_str_db1 = R"(
        name: "test_key"
        db_num: 1
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
        proto_str_db1, &index_schema_proto_db1_));

    ON_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
        .WillByDefault(testing::Return(0));
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault(testing::Return(&fake_ctx_));
    ON_CALL(*kMockValkeyModule, FreeThreadSafeContext(testing::_))
        .WillByDefault(testing::Return());
    ON_CALL(*kMockValkeyModule, SelectDb(testing::_, testing::_))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));

    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, false));
    ASSERT_TRUE(SchemaManager::Instance()
                    .CreateIndexSchema(&fake_ctx_, index_schema_proto_)
                    .ok());
    ASSERT_TRUE(SchemaManager::Instance()
                    .CreateIndexSchema(&fake_ctx_, index_schema_proto_db1_)
                    .ok());
  }

 protected:
  data_model::IndexSchema index_schema_proto_;
  data_model::IndexSchema index_schema_proto_db1_;
  const uint32_t kDb0 = 0;
  const uint32_t kDb1 = 1;
  const std::string kIndexName = "test_key";
};

// After AddAlias succeeds, GetIndexSchema(db, alias) returns same object as
// GetIndexSchema(db, index).
TEST_F(SchemaManagerAliasTest, AliasAddRoundTrip) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "my_alias", kIndexName));

  auto by_alias = SchemaManager::Instance().GetIndexSchema(kDb0, "my_alias");
  auto by_name = SchemaManager::Instance().GetIndexSchema(kDb0, kIndexName);
  VMSDK_EXPECT_OK(by_alias);
  VMSDK_EXPECT_OK(by_name);
  EXPECT_EQ(by_alias.value().get(), by_name.value().get());
}

// Second AddAlias with same alias returns kAlreadyExists, existing mapping
// unchanged.
TEST_F(SchemaManagerAliasTest, DuplicateAliasRejected) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "dup_alias", kIndexName));

  auto status =
      SchemaManager::Instance().AddAlias(kDb0, "dup_alias", kIndexName);
  EXPECT_EQ(status.code(), absl::StatusCode::kAlreadyExists);
  EXPECT_EQ(status.message(), "Alias already exists");

  // Existing mapping still resolves correctly.
  VMSDK_EXPECT_OK(SchemaManager::Instance().GetIndexSchema(kDb0, "dup_alias"));
}

// AddAlias with non-existent index returns kNotFound.
TEST_F(SchemaManagerAliasTest, MissingIndexRejected) {
  auto status = SchemaManager::Instance().AddAlias(kDb0, "some_alias",
                                                   "nonexistent_index");
  EXPECT_EQ(status.code(), absl::StatusCode::kNotFound);
}

// Using an alias name as the index target returns kInvalidArgument.
TEST_F(SchemaManagerAliasTest, AliasToAliasRejected) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "first_alias", kIndexName));

  auto status =
      SchemaManager::Instance().AddAlias(kDb0, "second_alias", "first_alias");
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(status.message(), "Unknown index name or name is an alias");
}

// N distinct aliases all pointing to same index each resolve to the same
// IndexSchema pointer.
TEST_F(SchemaManagerAliasTest, MultipleAliasesToSameIndex) {
  const std::vector<std::string> aliases = {"alias_a", "alias_b", "alias_c",
                                            "alias_d", "alias_e"};
  for (const auto &alias : aliases) {
    VMSDK_EXPECT_OK(
        SchemaManager::Instance().AddAlias(kDb0, alias, kIndexName));
  }

  auto by_name = SchemaManager::Instance().GetIndexSchema(kDb0, kIndexName);
  VMSDK_EXPECT_OK(by_name);

  for (const auto &alias : aliases) {
    auto by_alias = SchemaManager::Instance().GetIndexSchema(kDb0, alias);
    VMSDK_EXPECT_OK(by_alias);
    EXPECT_EQ(by_alias.value().get(), by_name.value().get())
        << "Alias '" << alias << "' did not resolve to the same IndexSchema";
  }
}

// Alias in db N is not visible in db M (using db 0 and db 1).
TEST_F(SchemaManagerAliasTest, AliasIsolationAcrossDatabases) {
  // Add alias in db 0 only.
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "cross_db_alias", kIndexName));

  // Alias should resolve in db 0.
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().GetIndexSchema(kDb0, "cross_db_alias"));

  // Alias must NOT be visible in db 1.
  auto status =
      SchemaManager::Instance().GetIndexSchema(kDb1, "cross_db_alias");
  EXPECT_EQ(status.status().code(), absl::StatusCode::kNotFound);
}

// A name that is neither an index nor an alias returns kNotFound.
TEST_F(SchemaManagerAliasTest, UnknownNameReturnsNotFound) {
  auto status =
      SchemaManager::Instance().GetIndexSchema(kDb0, "totally_unknown_name");
  EXPECT_EQ(status.status().code(), absl::StatusCode::kNotFound);
}

// After RemoveAlias succeeds, GetIndexSchema(db, alias) returns kNotFound;
// underlying index still accessible by its original name.
TEST_F(SchemaManagerAliasTest, AliasDeleteRoundTrip) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "del_alias", kIndexName));
  VMSDK_EXPECT_OK(SchemaManager::Instance().RemoveAlias(kDb0, "del_alias"));

  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(kDb0, "del_alias")
                .status()
                .code(),
            absl::StatusCode::kNotFound);
  // Underlying index still accessible.
  VMSDK_EXPECT_OK(SchemaManager::Instance().GetIndexSchema(kDb0, kIndexName));
}

// RemoveAlias on unknown alias returns kNotFound.
TEST_F(SchemaManagerAliasTest, DeleteNonExistentAliasRejected) {
  auto status =
      SchemaManager::Instance().RemoveAlias(kDb0, "nonexistent_alias");
  EXPECT_EQ(status.code(), absl::StatusCode::kNotFound);
  EXPECT_EQ(status.message(), "Alias does not exist");
}

// UpdateAlias on an existing alias succeeds
TEST_F(SchemaManagerAliasTest, UpdateAliasUpsertSemantics) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "upsert_alias", kIndexName));
  // Calling UpdateAlias on an already-existing alias must succeed, not error.
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().UpdateAlias(kDb0, "upsert_alias", kIndexName));
  // Alias still resolves correctly.
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().GetIndexSchema(kDb0, "upsert_alias"));
}

// UpdateAlias with alias-to-alias target returns kInvalidArgument
TEST_F(SchemaManagerAliasTest, UpdateAliasToAliasRejected) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "first_alias", kIndexName));
  auto status = SchemaManager::Instance().UpdateAlias(kDb0, "second_alias",
                                                      "first_alias");
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(status.message(), "Unknown index name or name is an alias");
}

// FT.DROPINDEX via alias: after dropping, both the alias and the index are
// gone. The alias cleanup in RemoveIndexSchemaInternal must fire on the real
// index name, not the alias name passed to FT.DROPINDEX.
TEST_F(SchemaManagerAliasTest, DropIndexViaAliasRemovesAlias) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "drop_alias", kIndexName));

  // Resolve alias to real name (as ft_dropindex.cc does) then drop.
  auto schema = SchemaManager::Instance().GetIndexSchema(kDb0, "drop_alias");
  VMSDK_EXPECT_OK(schema);
  const std::string real_name = schema.value()->GetName();
  VMSDK_EXPECT_OK(SchemaManager::Instance().RemoveIndexSchema(kDb0, real_name));

  // Both the index and the alias must be gone.
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(kDb0, kIndexName)
                .status()
                .code(),
            absl::StatusCode::kNotFound);
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(kDb0, "drop_alias")
                .status()
                .code(),
            absl::StatusCode::kNotFound);
}

// After UpdateAlias succeeds, GetIndexSchema(db, alias) returns IndexSchema
// for the new index.
TEST_F(SchemaManagerAliasTest, AliasUpdateSetsNewTarget) {
  // Create a second index in db 0.
  std::string proto_str2 = R"(
      name: "test_key2"
      db_num: 0
      subscribed_key_prefixes: "prefix_2"
      attribute_data_type: ATTRIBUTE_DATA_TYPE_HASH
      attributes: {
        alias: "test_attribute_2"
        identifier: "test_identifier_2"
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
  data_model::IndexSchema proto2;
  ASSERT_TRUE(
      google::protobuf::TextFormat::ParseFromString(proto_str2, &proto2));
  ASSERT_TRUE(
      SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, proto2).ok());

  // Point alias at first index, then update to second.
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "upd_alias", kIndexName));
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().UpdateAlias(kDb0, "upd_alias", "test_key2"));

  auto by_alias = SchemaManager::Instance().GetIndexSchema(kDb0, "upd_alias");
  auto by_new_name =
      SchemaManager::Instance().GetIndexSchema(kDb0, "test_key2");
  VMSDK_EXPECT_OK(by_alias);
  VMSDK_EXPECT_OK(by_new_name);
  EXPECT_EQ(by_alias.value().get(), by_new_name.value().get());
}

// UpdateAlias with non-existent index returns kNotFound; old mapping preserved.
TEST_F(SchemaManagerAliasTest, MissingIndexRejectedOnUpdate) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "upd_alias2", kIndexName));

  auto status = SchemaManager::Instance().UpdateAlias(kDb0, "upd_alias2",
                                                      "nonexistent_index");
  EXPECT_EQ(status.code(), absl::StatusCode::kNotFound);

  // Old mapping must still resolve correctly.
  auto by_alias = SchemaManager::Instance().GetIndexSchema(kDb0, "upd_alias2");
  auto by_name = SchemaManager::Instance().GetIndexSchema(kDb0, kIndexName);
  VMSDK_EXPECT_OK(by_alias);
  VMSDK_EXPECT_OK(by_name);
  EXPECT_EQ(by_alias.value().get(), by_name.value().get());
}

// RDB round-trip: SaveAliases -> LoadAliasMap restores aliases correctly.
// We construct the AliasMap proto directly (bypassing the RDB byte-stream
// layer) to keep the test focused on the alias save/load logic.
TEST_F(SchemaManagerAliasTest, AliasSaveLoadRoundTrip) {
  ON_CALL(*kMockValkeyModule, GetContextFromIO(testing::_))
      .WillByDefault(testing::Return(&fake_ctx_));

  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "rdb_alias", kIndexName));

  // Build the RDB section proto that SaveAliases would produce.
  data_model::RDBSection section_proto;
  section_proto.set_type(data_model::RDB_SECTION_ALIAS_MAP);
  section_proto.set_supplemental_count(0);
  auto *entry = section_proto.mutable_alias_map_contents()->add_entries();
  entry->set_db_num(kDb0);
  entry->set_alias("rdb_alias");
  entry->set_index_name(kIndexName);

  // Remove the alias so we can verify it is restored by LoadAliasMap.
  VMSDK_EXPECT_OK(SchemaManager::Instance().RemoveAlias(kDb0, "rdb_alias"));
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(kDb0, "rdb_alias")
                .status()
                .code(),
            absl::StatusCode::kNotFound);

  // Load the section back.
  FakeSafeRDB fake_rdb;
  auto section = std::make_unique<data_model::RDBSection>(section_proto);
  VMSDK_EXPECT_OK(SchemaManager::Instance().LoadAliasMap(
      &fake_ctx_, std::move(section), SupplementalContentIter(&fake_rdb, 0)));

  // Alias must resolve again after load.
  auto by_alias = SchemaManager::Instance().GetIndexSchema(kDb0, "rdb_alias");
  auto by_name = SchemaManager::Instance().GetIndexSchema(kDb0, kIndexName);
  VMSDK_EXPECT_OK(by_alias);
  VMSDK_EXPECT_OK(by_name);
  EXPECT_EQ(by_alias.value().get(), by_name.value().get());
}

// LoadAliasMap with an orphaned alias (index missing) logs a warning and
// skips the entry rather than failing the entire load.
TEST_F(SchemaManagerAliasTest, LoadAliasMapSkipsOrphanedAlias) {
  ON_CALL(*kMockValkeyModule, GetContextFromIO(testing::_))
      .WillByDefault(testing::Return(&fake_ctx_));

  // Build a section that references a non-existent index.
  data_model::RDBSection section_proto;
  section_proto.set_type(data_model::RDB_SECTION_ALIAS_MAP);
  section_proto.set_supplemental_count(0);
  auto *entry = section_proto.mutable_alias_map_contents()->add_entries();
  entry->set_db_num(kDb0);
  entry->set_alias("orphan_alias");
  entry->set_index_name("nonexistent_index");

  auto section = std::make_unique<data_model::RDBSection>(section_proto);
  FakeSafeRDB fake_rdb;
  // Must succeed (skip, not fail).
  VMSDK_EXPECT_OK(SchemaManager::Instance().LoadAliasMap(
      &fake_ctx_, std::move(section), SupplementalContentIter(&fake_rdb, 0)));

  // Orphaned alias must not have been inserted.
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(kDb0, "orphan_alias")
                .status()
                .code(),
            absl::StatusCode::kNotFound);
}

// OnSwapDB swaps alias maps in lockstep with index schemas.
// Uses CreateVectorHNSWSchema so the stored IndexSchema is a MockIndexSchema
// with OnSwapDB properly set up, matching the pattern in OnSwapDBCallbackTest.
TEST_F(SchemaManagerTest, OnSwapDBAliasesSwapped) {
  const int32_t kDb0 = 0;
  const int32_t kDb1 = 1;

  // Create a mock index in db 0 via the helper (registers with SchemaManager).
  auto schema_or =
      CreateVectorHNSWSchema("swap_idx", &fake_ctx_, nullptr, nullptr, kDb0);
  VMSDK_EXPECT_OK(schema_or);
  auto schema = schema_or.value();

  // Complete backfill so OnSwapDB doesn't need to update the scan context db.
  ValkeyModuleEvent eid;
  SchemaManager::Instance().OnServerCronCallback(nullptr, eid, 0, nullptr);

  // Add alias in db 0 pointing to the index.
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "swap_alias", "swap_idx"));

  // Alias resolves in db 0 before swap.
  VMSDK_EXPECT_OK(SchemaManager::Instance().GetIndexSchema(kDb0, "swap_alias"));

  ValkeyModuleSwapDbInfo swap_db_info;
  swap_db_info.dbnum_first = kDb0;
  swap_db_info.dbnum_second = kDb1;

  // OnSwapDB calls schema->OnSwapDB; the mock default delegates to the real
  // implementation which is safe once backfill is complete.
  EXPECT_CALL(*schema, OnSwapDB(&swap_db_info)).Times(1);
  SchemaManager::Instance().OnSwapDB(&swap_db_info);

  // After swap: alias must be visible in db 1, not db 0.
  VMSDK_EXPECT_OK(SchemaManager::Instance().GetIndexSchema(kDb1, "swap_alias"));
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(kDb0, "swap_alias")
                .status()
                .code(),
            absl::StatusCode::kNotFound);
}

// Helper: build a coordinator-enabled SchemaManager backed by a real
// MetadataManager.  Returns the index schema proto used for setup.
class SchemaManagerCoordinatorAliasTest : public ValkeySearchTest {
 public:
  void SetUp() override {
    ValkeySearchTest::SetUp();

    ON_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
        .WillByDefault(testing::Return(0));
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault(testing::Return(&fake_ctx_));
    ON_CALL(*kMockValkeyModule, FreeThreadSafeContext(testing::_))
        .WillByDefault(testing::Return());
    ON_CALL(*kMockValkeyModule, SelectDb(testing::_, testing::_))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));
    ON_CALL(*kMockValkeyModule,
            Call(testing::_, testing::StrEq("CLUSTER"), testing::StrEq("c"),
                 testing::StrEq("SLOTS")))
        .WillByDefault(testing::Return(
            reinterpret_cast<ValkeyModuleCallReply *>(0xDEADBEEF)));
    ON_CALL(
        *kMockValkeyModule,
        CallReplyType(reinterpret_cast<ValkeyModuleCallReply *>(0xDEADBEEF)))
        .WillByDefault(testing::Return(VALKEYMODULE_REPLY_ARRAY));
    ON_CALL(
        *kMockValkeyModule,
        CallReplyLength(reinterpret_cast<ValkeyModuleCallReply *>(0xDEADBEEF)))
        .WillByDefault(testing::Return(0));
    ON_CALL(*kMockValkeyModule, GetMyClusterID())
        .WillByDefault(testing::Return("fake_node_id"));

    mock_client_pool_ = std::make_unique<coordinator::MockClientPool>();
    coordinator::MetadataManager::InitInstance(
        std::make_unique<coordinator::MetadataManager>(&fake_ctx_,
                                                       *mock_client_pool_));

    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, /*coordinator_enabled=*/true));

    // Create the test index.
    std::string proto_str = R"(
        name: "test_idx"
        db_num: 0
        subscribed_key_prefixes: "prefix_1"
        attribute_data_type: ATTRIBUTE_DATA_TYPE_HASH
        attributes: {
          alias: "attr1"
          identifier: "id1"
          index: {
            vector_index: {
              dimension_count: 10
              normalize: true
              distance_metric: DISTANCE_METRIC_COSINE
              vector_data_type: VECTOR_DATA_TYPE_FLOAT32
              initial_cap: 100
              hnsw_algorithm { m: 16 ef_construction: 200 ef_runtime: 10 }
            }
          }
        }
      )";
    ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(proto_str,
                                                              &index_proto_));
    ASSERT_TRUE(SchemaManager::Instance()
                    .CreateIndexSchema(&fake_ctx_, index_proto_)
                    .ok());
  }

  void TearDown() override {
    coordinator::MetadataManager::InitInstance(nullptr);
    ValkeySearchTest::TearDown();
  }

 protected:
  std::unique_ptr<coordinator::MockClientPool> mock_client_pool_;
  data_model::IndexSchema index_proto_;
  const uint32_t kDb0 = 0;
  const std::string kIndexName = "test_idx";
};

TEST_F(SchemaManagerCoordinatorAliasTest,
       ComputeAliasFingerprintIsDeterministic) {
  // ComputeAliasFingerprint is private; test determinism indirectly via
  // AddAlias + UpdateAlias with the same payload. MetadataManager accepts the
  // reapply without error only if the fingerprint is stable.
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "fp_alias", kIndexName));
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().UpdateAlias(kDb0, "fp_alias", kIndexName));
  auto by_alias = SchemaManager::Instance().GetIndexSchema(kDb0, "fp_alias");
  auto by_name = SchemaManager::Instance().GetIndexSchema(kDb0, kIndexName);
  VMSDK_EXPECT_OK(by_alias);
  VMSDK_EXPECT_OK(by_name);
  EXPECT_EQ(by_alias.value().get(), by_name.value().get());
}

TEST_F(SchemaManagerCoordinatorAliasTest,
       ComputeAliasFingerprintUnpackFailure) {
  // An Any with a mismatched type_url fails to unpack; ComputeFingerprint
  // must return an error rather than silently producing a garbage hash.
  google::protobuf::Any bad_any;
  bad_any.set_type_url(
      "type.googleapis.com/valkey_search.data_model.IndexSchema");
  bad_any.set_value("not-valid-proto-bytes");
  // ComputeFingerprint unpacks as IndexSchema; that will fail on bad bytes.
  auto status = SchemaManager::ComputeFingerprint(bad_any);
  EXPECT_FALSE(status.ok());
}

TEST_F(SchemaManagerCoordinatorAliasTest, OnAliasMetadataCallbackCreate) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "cb_alias", kIndexName));

  auto by_alias = SchemaManager::Instance().GetIndexSchema(kDb0, "cb_alias");
  auto by_name = SchemaManager::Instance().GetIndexSchema(kDb0, kIndexName);
  VMSDK_EXPECT_OK(by_alias);
  VMSDK_EXPECT_OK(by_name);
  EXPECT_EQ(by_alias.value().get(), by_name.value().get());
}

TEST_F(SchemaManagerCoordinatorAliasTest, OnAliasMetadataCallbackDelete) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "del_alias", kIndexName));
  VMSDK_EXPECT_OK(SchemaManager::Instance().RemoveAlias(kDb0, "del_alias"));

  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(kDb0, "del_alias")
                .status()
                .code(),
            absl::StatusCode::kNotFound);
  // Underlying index still accessible.
  VMSDK_EXPECT_OK(SchemaManager::Instance().GetIndexSchema(kDb0, kIndexName));
}

TEST_F(SchemaManagerCoordinatorAliasTest, OnAliasMetadataCallbackIdempotent) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "idem_alias", kIndexName));
  // UpdateAlias with same target is an upsert; triggers callback again.
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().UpdateAlias(kDb0, "idem_alias", kIndexName));

  auto by_alias = SchemaManager::Instance().GetIndexSchema(kDb0, "idem_alias");
  auto by_name = SchemaManager::Instance().GetIndexSchema(kDb0, kIndexName);
  VMSDK_EXPECT_OK(by_alias);
  VMSDK_EXPECT_OK(by_name);
  EXPECT_EQ(by_alias.value().get(), by_name.value().get());
}

TEST_F(SchemaManagerCoordinatorAliasTest, CoordAddAliasDuplicate) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "dup_alias", kIndexName));
  auto status =
      SchemaManager::Instance().AddAlias(kDb0, "dup_alias", kIndexName);
  EXPECT_EQ(status.code(), absl::StatusCode::kAlreadyExists);
}

TEST_F(SchemaManagerCoordinatorAliasTest, CoordAddAliasToAlias) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "first_alias", kIndexName));
  auto status =
      SchemaManager::Instance().AddAlias(kDb0, "second_alias", "first_alias");
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
}

TEST_F(SchemaManagerCoordinatorAliasTest, CoordAddAliasMissingIndex) {
  auto status =
      SchemaManager::Instance().AddAlias(kDb0, "x_alias", "no_such_index");
  EXPECT_EQ(status.code(), absl::StatusCode::kNotFound);
}

TEST_F(SchemaManagerCoordinatorAliasTest, CoordRemoveAliasMissing) {
  auto status =
      SchemaManager::Instance().RemoveAlias(kDb0, "nonexistent_alias");
  EXPECT_EQ(status.code(), absl::StatusCode::kNotFound);
  EXPECT_EQ(status.message(), "Alias does not exist");
}

TEST_F(SchemaManagerCoordinatorAliasTest, CoordUpdateAliasToAlias) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "first_alias", kIndexName));
  auto status = SchemaManager::Instance().UpdateAlias(kDb0, "second_alias",
                                                      "first_alias");
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
}

TEST_F(SchemaManagerCoordinatorAliasTest, CoordUpdateAliasUpsert) {
  // Create when absent.
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().UpdateAlias(kDb0, "upsert_alias", kIndexName));
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().GetIndexSchema(kDb0, "upsert_alias"));

  // Update when present (same target; still succeeds).
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().UpdateAlias(kDb0, "upsert_alias", kIndexName));
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().GetIndexSchema(kDb0, "upsert_alias"));
}

TEST_F(SchemaManagerCoordinatorAliasTest, OnFlushDBEndedAliasesPurged) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "flush_alias", kIndexName));

  // Alias resolves before flush.
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().GetIndexSchema(kDb0, "flush_alias"));

  SchemaManager::Instance().OnFlushDBEnded(&fake_ctx_);

  // After flush: alias is gone from db_to_aliases_ (RemoveIndexSchemaInternal
  // purges it), but MetadataManager still holds the entry.
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(kDb0, "flush_alias")
                .status()
                .code(),
            absl::StatusCode::kNotFound);
  // MetadataManager entry survives the flush.
  VMSDK_EXPECT_OK(coordinator::MetadataManager::Instance().GetEntryContent(
      kAliasMetadataTypeName, coordinator::ObjName(kDb0, "flush_alias")));
}

TEST_F(SchemaManagerCoordinatorAliasTest, RemoveIndexSchemaTombstonesAliases) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "alias_a", kIndexName));
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "alias_b", kIndexName));

  VMSDK_EXPECT_OK(
      SchemaManager::Instance().RemoveIndexSchema(kDb0, kIndexName));

  // Both aliases must be gone from in-memory state.
  EXPECT_EQ(
      SchemaManager::Instance().GetIndexSchema(kDb0, "alias_a").status().code(),
      absl::StatusCode::kNotFound);
  EXPECT_EQ(
      SchemaManager::Instance().GetIndexSchema(kDb0, "alias_b").status().code(),
      absl::StatusCode::kNotFound);

  // Both alias MetadataManager entries must be tombstoned (NotFound).
  EXPECT_EQ(coordinator::MetadataManager::Instance()
                .GetEntryContent(kAliasMetadataTypeName,
                                 coordinator::ObjName(kDb0, "alias_a"))
                .status()
                .code(),
            absl::StatusCode::kNotFound);
  EXPECT_EQ(coordinator::MetadataManager::Instance()
                .GetEntryContent(kAliasMetadataTypeName,
                                 coordinator::ObjName(kDb0, "alias_b"))
                .status()
                .code(),
            absl::StatusCode::kNotFound);
}

// SaveAliases serializes aliases correctly across multiple DBs.
// Calls SaveAliases with a FakeSafeRDB, deserializes the captured bytes back
// into an AliasMap proto, and verifies the entries match.
TEST_F(SchemaManagerAliasTest, SaveAliasesAfterRDB) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "save_alias_a", kIndexName));
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "save_alias_b", kIndexName));
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb1, "save_alias_c", kIndexName));

  FakeSafeRDB fake_rdb;
  VMSDK_EXPECT_OK(SchemaManager::Instance().SaveAliases(
      &fake_ctx_, &fake_rdb, VALKEYMODULE_AUX_AFTER_RDB));

  // Deserialize: SaveStringBuffer writes size then data.
  auto saved_str = fake_rdb.LoadString();
  VMSDK_EXPECT_OK(saved_str);
  size_t len;
  const char *buf = ValkeyModule_StringPtrLen(saved_str.value().get(), &len);
  data_model::RDBSection section;
  ASSERT_TRUE(section.ParseFromArray(buf, len));
  EXPECT_EQ(section.type(), data_model::RDB_SECTION_ALIAS_MAP);
  EXPECT_EQ(section.supplemental_count(), 0);

  const auto &entries = section.alias_map_contents().entries();
  EXPECT_EQ(entries.size(), 3);

  // Collect into a set for order-independent comparison (flat_hash_map
  // iteration order is unspecified).
  absl::flat_hash_set<std::string> found;
  for (const auto &e : entries) {
    found.insert(
        absl::StrFormat("%d/%s->%s", e.db_num(), e.alias(), e.index_name()));
  }
  EXPECT_TRUE(found.contains("0/save_alias_a->test_key"));
  EXPECT_TRUE(found.contains("0/save_alias_b->test_key"));
  EXPECT_TRUE(found.contains("1/save_alias_c->test_key"));
}

// SaveAliases BEFORE_RDB is a no-op.
TEST_F(SchemaManagerAliasTest, SaveAliasesBeforeRDBIsNoop) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "noop_alias", kIndexName));

  FakeSafeRDB fake_rdb;
  VMSDK_EXPECT_OK(SchemaManager::Instance().SaveAliases(
      &fake_ctx_, &fake_rdb, VALKEYMODULE_AUX_BEFORE_RDB));

  // Nothing should have been written to the RDB buffer.
  EXPECT_EQ(fake_rdb.buffer_.str().size(), 0u);
}

// OnAliasMetadataCallback with a missing target index must return OkStatus
// without inserting the alias (transient during reconciliation; will retry).
TEST_F(SchemaManagerCoordinatorAliasTest,
       OnAliasMetadataCallbackMissingTargetIndex) {
  data_model::AliasEntry entry;
  entry.set_index_name("nonexistent_index");
  auto any_proto = std::make_unique<google::protobuf::Any>();
  any_proto->PackFrom(entry);

  // OnAliasMetadataCallback is private; exercise it via MetadataManager.
  auto status = coordinator::MetadataManager::Instance().CreateEntry(
      kAliasMetadataTypeName, coordinator::ObjName(kDb0, "orphan_alias"),
      std::move(any_proto));
  // CreateEntry itself succeeds (the entry is stored in MetadataManager).
  VMSDK_EXPECT_OK(status);

  // But the alias must NOT resolve because the target index is missing.
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(kDb0, "orphan_alias")
                .status()
                .code(),
            absl::StatusCode::kNotFound);
}

// ComputeAliasFingerprint must not crash on a malformed Any payload.
TEST_F(SchemaManagerCoordinatorAliasTest,
       CreateEntryWithMalformedAnyHandledGracefully) {
  auto bad_any = std::make_unique<google::protobuf::Any>();
  bad_any->set_type_url(
      "type.googleapis.com/valkey_search.data_model.AliasEntry");
  bad_any->set_value("not-valid-proto-bytes");
  // Protobuf is lenient with unknown bytes so UnpackTo may succeed on garbage;
  // either outcome is acceptable as long as the alias does not resolve.
  auto status = coordinator::MetadataManager::Instance().CreateEntry(
      kAliasMetadataTypeName, coordinator::ObjName(kDb0, "bad_alias"),
      std::move(bad_any));
  // Either succeeds (protobuf parsed garbage) or returns an error — both OK.
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(kDb0, "bad_alias")
                .status()
                .code(),
            absl::StatusCode::kNotFound);
}

// RemoveAll (via OnShutdownCallback) clears aliases.
TEST_F(SchemaManagerAliasTest, ShutdownClearsAliases) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "shutdown_alias", kIndexName));

  ValkeyModuleEvent eid;
  SchemaManager::Instance().OnShutdownCallback(&fake_ctx_, eid, 0, nullptr);
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(kDb0, kIndexName)
                .status()
                .code(),
            absl::StatusCode::kNotFound);
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(kDb0, "shutdown_alias")
                .status()
                .code(),
            absl::StatusCode::kNotFound);
}

// OnLoadingEnded during replication clears stale aliases so LoadAliasMap
// starts from a clean slate.
TEST_F(SchemaManagerAliasTest, ReplicationLoadClearsStaleAliases) {
  ON_CALL(*kMockValkeyModule, GetContextFromIO(testing::_))
      .WillByDefault(testing::Return(&fake_ctx_));

  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "stale_alias", kIndexName));

  ValkeyModuleEvent eid;
  SchemaManager::Instance().OnLoadingCallback(
      &fake_ctx_, eid, VALKEYMODULE_SUBEVENT_LOADING_REPL_START, nullptr);

  FakeSafeRDB fake_rdb;
  auto section = std::make_unique<data_model::RDBSection>();
  section->set_type(data_model::RDB_SECTION_INDEX_SCHEMA);
  section->mutable_index_schema_contents()->CopyFrom(index_schema_proto_);
  section->set_supplemental_count(0);
  VMSDK_EXPECT_OK(SchemaManager::Instance().LoadIndex(
      &fake_ctx_, std::move(section), SupplementalContentIter(&fake_rdb, 0)));

  SchemaManager::Instance().OnLoadingCallback(
      &fake_ctx_, eid, VALKEYMODULE_SUBEVENT_LOADING_ENDED, nullptr);

  VMSDK_EXPECT_OK(SchemaManager::Instance().GetIndexSchema(kDb0, kIndexName));

  // db_to_aliases_ is cleared by OnLoadingEnded; the alias is gone.
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(kDb0, "stale_alias")
                .status()
                .code(),
            absl::StatusCode::kNotFound);
}

// OnSwapDB — both DBs have aliases (has_a && has_b branch of swap_aliases).
TEST_F(SchemaManagerAliasTest, OnSwapDBAliasesBothDBsHaveAliases) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb0, "alias_in_0", kIndexName));
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb1, "alias_in_1", kIndexName));

  VMSDK_EXPECT_OK(SchemaManager::Instance().GetIndexSchema(kDb0, "alias_in_0"));
  VMSDK_EXPECT_OK(SchemaManager::Instance().GetIndexSchema(kDb1, "alias_in_1"));
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(kDb0, "alias_in_1")
                .status()
                .code(),
            absl::StatusCode::kNotFound);
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(kDb1, "alias_in_0")
                .status()
                .code(),
            absl::StatusCode::kNotFound);

  ValkeyModuleSwapDbInfo swap_db_info;
  swap_db_info.dbnum_first = kDb0;
  swap_db_info.dbnum_second = kDb1;
  SchemaManager::Instance().OnSwapDB(&swap_db_info);

  VMSDK_EXPECT_OK(SchemaManager::Instance().GetIndexSchema(kDb1, "alias_in_0"));
  VMSDK_EXPECT_OK(SchemaManager::Instance().GetIndexSchema(kDb0, "alias_in_1"));
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(kDb0, "alias_in_0")
                .status()
                .code(),
            absl::StatusCode::kNotFound);
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(kDb1, "alias_in_1")
                .status()
                .code(),
            absl::StatusCode::kNotFound);
}

// OnSwapDB — only DB 1 has aliases (!has_a && has_b branch of swap_aliases).
TEST_F(SchemaManagerAliasTest, OnSwapDBAliasesOnlySecondDBHasAlias) {
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(kDb1, "alias_in_1", kIndexName));

  ValkeyModuleSwapDbInfo swap_db_info;
  swap_db_info.dbnum_first = kDb0;
  swap_db_info.dbnum_second = kDb1;
  SchemaManager::Instance().OnSwapDB(&swap_db_info);

  VMSDK_EXPECT_OK(SchemaManager::Instance().GetIndexSchema(kDb0, "alias_in_1"));
  EXPECT_EQ(SchemaManager::Instance()
                .GetIndexSchema(kDb1, "alias_in_1")
                .status()
                .code(),
            absl::StatusCode::kNotFound);
}

}  // namespace valkey_search
