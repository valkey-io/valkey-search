/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <algorithm>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "src/commands/commands.h"
#include "src/schema_manager.h"
#include "src/valkey_search_options.h"
#include "testing/common.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

using ::testing::TestParamInfo;
using ::testing::ValuesIn;

// Index schema proto text used across all test cases.
constexpr absl::string_view kTestIndexSchemaPbtxt = R"(
  name: "test_idx"
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

struct FTAliasAddTestCase {
  std::string test_name;
  std::vector<std::string> argv;
  // If set, create this index before running the command.
  std::optional<std::string> index_schema_pbtxt;
  // If set, add this alias before running the command (to test duplicate).
  std::optional<std::string> pre_existing_alias;
  absl::StatusCode return_code;
};

class FTAliasAddTest : public ValkeySearchTestWithParam<FTAliasAddTestCase> {};

TEST_P(FTAliasAddTest, FTAliasAddTests) {
  const FTAliasAddTestCase& test_case = GetParam();

  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 5);
  SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
      &fake_ctx_, []() {}, &mutations_thread_pool, false));

  if (test_case.index_schema_pbtxt.has_value()) {
    data_model::IndexSchema index_schema_proto;
    ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
        test_case.index_schema_pbtxt.value(), &index_schema_proto));
    VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
        &fake_ctx_, index_schema_proto));
  }

  // Pre-add an alias if needed (e.g. to test duplicate rejection).
  if (test_case.pre_existing_alias.has_value()) {
    VMSDK_EXPECT_OK(SchemaManager::Instance().AddAlias(
        0, test_case.pre_existing_alias.value(), "test_idx"));
  }

  std::vector<ValkeyModuleString*> cmd_argv;
  std::transform(test_case.argv.begin(), test_case.argv.end(),
                 std::back_inserter(cmd_argv), [this](const std::string& val) {
                   return TestValkeyModule_CreateStringPrintf(

                       &fake_ctx_, "%s", val.data());
                 });

  EXPECT_EQ(FTAliasAddCmd(&fake_ctx_, cmd_argv.data(), cmd_argv.size()).code(),
            test_case.return_code);

  for (auto* arg : cmd_argv) {
    TestValkeyModule_FreeString(&fake_ctx_, arg);
  }
}

INSTANTIATE_TEST_SUITE_P(
    FTAliasAddTests, FTAliasAddTest,
    ValuesIn<FTAliasAddTestCase>({
        {
            .test_name = "happy_path",
            .argv = {"FT.ALIASADD", "my_alias", "test_idx"},
            .index_schema_pbtxt = std::string(kTestIndexSchemaPbtxt),
            .pre_existing_alias = std::nullopt,
            .return_code = absl::StatusCode::kOk,
        },
        {
            .test_name = "arity_error_no_args",
            .argv = {"FT.ALIASADD"},
            .index_schema_pbtxt = std::nullopt,
            .pre_existing_alias = std::nullopt,
            .return_code = absl::StatusCode::kInvalidArgument,
        },
        {
            .test_name = "arity_error_one_arg",
            .argv = {"FT.ALIASADD", "my_alias"},
            .index_schema_pbtxt = std::nullopt,
            .pre_existing_alias = std::nullopt,
            .return_code = absl::StatusCode::kInvalidArgument,
        },
        {
            .test_name = "duplicate_alias",
            .argv = {"FT.ALIASADD", "my_alias", "test_idx"},
            .index_schema_pbtxt = std::string(kTestIndexSchemaPbtxt),
            .pre_existing_alias = "my_alias",
            .return_code = absl::StatusCode::kAlreadyExists,
        },
        {
            .test_name = "non_existent_index",
            .argv = {"FT.ALIASADD", "my_alias", "no_such_index"},
            .index_schema_pbtxt = std::nullopt,
            .pre_existing_alias = std::nullopt,
            .return_code = absl::StatusCode::kNotFound,
        },
        {
            .test_name = "alias_to_alias",
            .argv = {"FT.ALIASADD", "alias2", "my_alias"},
            .index_schema_pbtxt = std::string(kTestIndexSchemaPbtxt),
            .pre_existing_alias = "my_alias",
            .return_code = absl::StatusCode::kInvalidArgument,
        },
        {
            // alias == index_name is allowed when neither is already an alias
            .test_name = "self_referential_alias",
            .argv = {"FT.ALIASADD", "test_idx", "test_idx"},
            .index_schema_pbtxt = std::string(kTestIndexSchemaPbtxt),
            .pre_existing_alias = std::nullopt,
            .return_code = absl::StatusCode::kOk,
        },
        {
            .test_name = "arity_error_too_many_args",
            .argv = {"FT.ALIASADD", "my_alias", "test_idx", "extra"},
            .index_schema_pbtxt = std::nullopt,
            .pre_existing_alias = std::nullopt,
            .return_code = absl::StatusCode::kInvalidArgument,
        },
    }),
    [](const TestParamInfo<FTAliasAddTestCase>& info) {
      return info.param.test_name;
    });

TEST_F(FTAliasAddTest, ReplicationBehaviorCoordinatorEnabled) {
  VMSDK_EXPECT_OK(
      const_cast<vmsdk::config::Boolean&>(options::GetUseCoordinator())
          .SetValue(true));

  data_model::IndexSchema index_schema_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kTestIndexSchemaPbtxt), &index_schema_proto));
  VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
      &fake_ctx_, index_schema_proto));

  EXPECT_CALL(*kMockValkeyModule, ReplicateVerbatim(&fake_ctx_))
      .Times(1);  // Always replicates: aliases have no MetadataManager path

  ValkeyModuleString* argv[3];
  argv[0] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.ALIASADD");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "my_alias");
  argv[2] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_idx");

  VMSDK_EXPECT_OK(FTAliasAddCmd(&fake_ctx_, argv, 3));

  for (auto* arg : argv) {
    TestValkeyModule_FreeString(&fake_ctx_, arg);
  }
}

TEST_F(FTAliasAddTest, ReplicationBehaviorCoordinatorDisabled) {
  VMSDK_EXPECT_OK(
      const_cast<vmsdk::config::Boolean&>(options::GetUseCoordinator())
          .SetValue(false));

  data_model::IndexSchema index_schema_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kTestIndexSchemaPbtxt), &index_schema_proto));
  VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
      &fake_ctx_, index_schema_proto));

  EXPECT_CALL(*kMockValkeyModule, ReplicateVerbatim(&fake_ctx_))
      .Times(1);  // Should replicate when coordinator disabled

  ValkeyModuleString* argv[3];
  argv[0] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.ALIASADD");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "my_alias");
  argv[2] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_idx");

  VMSDK_EXPECT_OK(FTAliasAddCmd(&fake_ctx_, argv, 3));

  for (auto* arg : argv) {
    TestValkeyModule_FreeString(&fake_ctx_, arg);
  }
}

struct FTAliasDelTestCase {
  std::string test_name;
  std::vector<std::string> argv;
  std::optional<std::string> index_schema_pbtxt;
  std::optional<std::string> pre_existing_alias;
  absl::StatusCode return_code;
};

class FTAliasDelTest : public ValkeySearchTestWithParam<FTAliasDelTestCase> {};

TEST_P(FTAliasDelTest, FTAliasDelTests) {
  const FTAliasDelTestCase& test_case = GetParam();

  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 5);
  SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
      &fake_ctx_, []() {}, &mutations_thread_pool, false));

  if (test_case.index_schema_pbtxt.has_value()) {
    data_model::IndexSchema index_schema_proto;
    ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
        test_case.index_schema_pbtxt.value(), &index_schema_proto));
    VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
        &fake_ctx_, index_schema_proto));
  }

  if (test_case.pre_existing_alias.has_value()) {
    VMSDK_EXPECT_OK(SchemaManager::Instance().AddAlias(
        0, test_case.pre_existing_alias.value(), "test_idx"));
  }

  std::vector<ValkeyModuleString*> cmd_argv;
  std::transform(test_case.argv.begin(), test_case.argv.end(),
                 std::back_inserter(cmd_argv), [this](const std::string& val) {
                   return TestValkeyModule_CreateStringPrintf(&fake_ctx_, "%s",
                                                              val.data());
                 });

  EXPECT_EQ(FTAliasDelCmd(&fake_ctx_, cmd_argv.data(), cmd_argv.size()).code(),
            test_case.return_code);

  // After successful delete: alias must be gone, underlying index must remain.
  if (test_case.return_code == absl::StatusCode::kOk &&
      test_case.index_schema_pbtxt.has_value()) {
    VMSDK_EXPECT_OK(SchemaManager::Instance().GetIndexSchema(0, "test_idx"));
    // The alias itself must no longer resolve.
    EXPECT_EQ(
        SchemaManager::Instance().GetIndexSchema(0, "my_alias").status().code(),
        absl::StatusCode::kNotFound);
  }

  for (auto* arg : cmd_argv) {
    TestValkeyModule_FreeString(&fake_ctx_, arg);
  }
}

INSTANTIATE_TEST_SUITE_P(
    FTAliasDelTests, FTAliasDelTest,
    ValuesIn<FTAliasDelTestCase>({
        {
            .test_name = "happy_path",
            .argv = {"FT.ALIASDEL", "my_alias"},
            .index_schema_pbtxt = std::string(kTestIndexSchemaPbtxt),
            .pre_existing_alias = "my_alias",
            .return_code = absl::StatusCode::kOk,
        },
        {
            .test_name = "arity_error_no_args",
            .argv = {"FT.ALIASDEL"},
            .index_schema_pbtxt = std::nullopt,
            .pre_existing_alias = std::nullopt,
            .return_code = absl::StatusCode::kInvalidArgument,
        },
        {
            .test_name = "arity_error_extra_args",
            .argv = {"FT.ALIASDEL", "alias1", "extra"},
            .index_schema_pbtxt = std::nullopt,
            .pre_existing_alias = std::nullopt,
            .return_code = absl::StatusCode::kInvalidArgument,
        },
        {
            .test_name = "non_existent_alias",
            .argv = {"FT.ALIASDEL", "no_such_alias"},
            .index_schema_pbtxt = std::nullopt,
            .pre_existing_alias = std::nullopt,
            .return_code = absl::StatusCode::kNotFound,
        },
    }),
    [](const TestParamInfo<FTAliasDelTestCase>& info) {
      return info.param.test_name;
    });

TEST_F(FTAliasDelTest, ReplicationBehaviorCoordinatorEnabled) {
  VMSDK_EXPECT_OK(
      const_cast<vmsdk::config::Boolean&>(options::GetUseCoordinator())
          .SetValue(true));

  data_model::IndexSchema index_schema_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kTestIndexSchemaPbtxt), &index_schema_proto));
  VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
      &fake_ctx_, index_schema_proto));
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(0, "my_alias", "test_idx"));

  EXPECT_CALL(*kMockValkeyModule, ReplicateVerbatim(&fake_ctx_)).Times(1);

  ValkeyModuleString* argv[2];
  argv[0] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.ALIASDEL");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "my_alias");
  VMSDK_EXPECT_OK(FTAliasDelCmd(&fake_ctx_, argv, 2));
  for (auto* arg : argv) TestValkeyModule_FreeString(&fake_ctx_, arg);
}

TEST_F(FTAliasDelTest, ReplicationBehaviorCoordinatorDisabled) {
  VMSDK_EXPECT_OK(
      const_cast<vmsdk::config::Boolean&>(options::GetUseCoordinator())
          .SetValue(false));

  data_model::IndexSchema index_schema_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kTestIndexSchemaPbtxt), &index_schema_proto));
  VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
      &fake_ctx_, index_schema_proto));
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(0, "my_alias", "test_idx"));

  EXPECT_CALL(*kMockValkeyModule, ReplicateVerbatim(&fake_ctx_)).Times(1);

  ValkeyModuleString* argv[2];
  argv[0] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.ALIASDEL");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "my_alias");

  VMSDK_EXPECT_OK(FTAliasDelCmd(&fake_ctx_, argv, 2));
  for (auto* arg : argv) TestValkeyModule_FreeString(&fake_ctx_, arg);
}

struct FTAliasUpdateTestCase {
  std::string test_name;
  std::vector<std::string> argv;
  std::optional<std::string> index_schema_pbtxt;
  // Alias to pre-add before running the command.
  std::optional<std::string> pre_existing_alias;
  // Second index proto to create (for update-to-new-index tests).
  std::optional<std::string> second_index_pbtxt;
  absl::StatusCode return_code;
};

// Second index proto used in update tests.
constexpr absl::string_view kSecondIndexSchemaPbtxt = R"(
  name: "test_idx2"
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

class FTAliasUpdateTest
    : public ValkeySearchTestWithParam<FTAliasUpdateTestCase> {};

TEST_P(FTAliasUpdateTest, FTAliasUpdateTests) {
  const FTAliasUpdateTestCase& test_case = GetParam();

  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 5);
  SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
      &fake_ctx_, []() {}, &mutations_thread_pool, false));

  if (test_case.index_schema_pbtxt.has_value()) {
    data_model::IndexSchema index_schema_proto;
    ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
        test_case.index_schema_pbtxt.value(), &index_schema_proto));
    VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
        &fake_ctx_, index_schema_proto));
  }

  if (test_case.second_index_pbtxt.has_value()) {
    data_model::IndexSchema second_proto;
    ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
        test_case.second_index_pbtxt.value(), &second_proto));
    VMSDK_EXPECT_OK(
        SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, second_proto));
  }

  if (test_case.pre_existing_alias.has_value()) {
    VMSDK_EXPECT_OK(SchemaManager::Instance().AddAlias(
        0, test_case.pre_existing_alias.value(), "test_idx"));
  }

  std::vector<ValkeyModuleString*> cmd_argv;
  std::transform(test_case.argv.begin(), test_case.argv.end(),
                 std::back_inserter(cmd_argv), [this](const std::string& val) {
                   return TestValkeyModule_CreateStringPrintf(&fake_ctx_, "%s",
                                                              val.data());
                 });

  EXPECT_EQ(
      FTAliasUpdateCmd(&fake_ctx_, cmd_argv.data(), cmd_argv.size()).code(),
      test_case.return_code);

  // Atomicity check: when an existing alias is reassigned to a new index,
  // the alias must resolve to the new index and the old index must still exist
  // but no longer be reachable via the alias.
  if (test_case.return_code == absl::StatusCode::kOk &&
      test_case.pre_existing_alias.has_value() &&
      test_case.second_index_pbtxt.has_value()) {
    // Alias now points to test_idx2.
    auto result = SchemaManager::Instance().GetIndexSchema(
        0, test_case.pre_existing_alias.value());
    VMSDK_EXPECT_OK(result);
    EXPECT_EQ(result->get()->GetName(), "test_idx2");
    // Original index is still accessible by its real name.
    VMSDK_EXPECT_OK(SchemaManager::Instance().GetIndexSchema(0, "test_idx"));
  }

  for (auto* arg : cmd_argv) {
    TestValkeyModule_FreeString(&fake_ctx_, arg);
  }
}

INSTANTIATE_TEST_SUITE_P(
    FTAliasUpdateTests, FTAliasUpdateTest,
    ValuesIn<FTAliasUpdateTestCase>({
        {
            .test_name = "happy_path_update_existing",
            .argv = {"FT.ALIASUPDATE", "my_alias", "test_idx2"},
            .index_schema_pbtxt = std::string(kTestIndexSchemaPbtxt),
            .pre_existing_alias = "my_alias",
            .second_index_pbtxt = std::string(kSecondIndexSchemaPbtxt),
            .return_code = absl::StatusCode::kOk,
        },
        {
            .test_name = "happy_path_create_new",
            .argv = {"FT.ALIASUPDATE", "new_alias", "test_idx"},
            .index_schema_pbtxt = std::string(kTestIndexSchemaPbtxt),
            .pre_existing_alias = std::nullopt,
            .second_index_pbtxt = std::nullopt,
            .return_code = absl::StatusCode::kOk,
        },
        {
            .test_name = "arity_error_no_args",
            .argv = {"FT.ALIASUPDATE"},
            .index_schema_pbtxt = std::nullopt,
            .pre_existing_alias = std::nullopt,
            .second_index_pbtxt = std::nullopt,
            .return_code = absl::StatusCode::kInvalidArgument,
        },
        {
            .test_name = "arity_error_one_arg",
            .argv = {"FT.ALIASUPDATE", "my_alias"},
            .index_schema_pbtxt = std::nullopt,
            .pre_existing_alias = std::nullopt,
            .second_index_pbtxt = std::nullopt,
            .return_code = absl::StatusCode::kInvalidArgument,
        },
        {
            .test_name = "non_existent_index",
            .argv = {"FT.ALIASUPDATE", "my_alias", "no_such_index"},
            .index_schema_pbtxt = std::nullopt,
            .pre_existing_alias = std::nullopt,
            .second_index_pbtxt = std::nullopt,
            .return_code = absl::StatusCode::kNotFound,
        },
        {
            .test_name = "alias_to_alias",
            .argv = {"FT.ALIASUPDATE", "alias2", "my_alias"},
            .index_schema_pbtxt = std::string(kTestIndexSchemaPbtxt),
            .pre_existing_alias = "my_alias",
            .second_index_pbtxt = std::nullopt,
            .return_code = absl::StatusCode::kInvalidArgument,
        },
        {
            .test_name = "arity_error_too_many_args",
            .argv = {"FT.ALIASUPDATE", "my_alias", "test_idx", "extra"},
            .index_schema_pbtxt = std::nullopt,
            .pre_existing_alias = std::nullopt,
            .second_index_pbtxt = std::nullopt,
            .return_code = absl::StatusCode::kInvalidArgument,
        },
        {
            // ALIASUPDATE on an already-existing alias must succeed (upsert).
            // Matches RediSearch: del old + add new, no "already exists" error.
            .test_name = "upsert_existing_alias",
            .argv = {"FT.ALIASUPDATE", "my_alias", "test_idx2"},
            .index_schema_pbtxt = std::string(kTestIndexSchemaPbtxt),
            .pre_existing_alias = "my_alias",
            .second_index_pbtxt = std::string(kSecondIndexSchemaPbtxt),
            .return_code = absl::StatusCode::kOk,
        },
    }),
    [](const TestParamInfo<FTAliasUpdateTestCase>& info) {
      return info.param.test_name;
    });

TEST_F(FTAliasUpdateTest, ReplicationBehaviorCoordinatorEnabled) {
  VMSDK_EXPECT_OK(
      const_cast<vmsdk::config::Boolean&>(options::GetUseCoordinator())
          .SetValue(true));

  data_model::IndexSchema index_schema_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kTestIndexSchemaPbtxt), &index_schema_proto));
  VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
      &fake_ctx_, index_schema_proto));

  EXPECT_CALL(*kMockValkeyModule, ReplicateVerbatim(&fake_ctx_)).Times(1);

  ValkeyModuleString* argv[3];
  argv[0] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.ALIASUPDATE");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "my_alias");
  argv[2] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_idx");
  VMSDK_EXPECT_OK(FTAliasUpdateCmd(&fake_ctx_, argv, 3));
  for (auto* arg : argv) TestValkeyModule_FreeString(&fake_ctx_, arg);
}

TEST_F(FTAliasUpdateTest, ReplicationBehaviorCoordinatorDisabled) {
  VMSDK_EXPECT_OK(
      const_cast<vmsdk::config::Boolean&>(options::GetUseCoordinator())
          .SetValue(false));

  data_model::IndexSchema index_schema_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kTestIndexSchemaPbtxt), &index_schema_proto));
  VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
      &fake_ctx_, index_schema_proto));

  EXPECT_CALL(*kMockValkeyModule, ReplicateVerbatim(&fake_ctx_)).Times(1);

  ValkeyModuleString* argv[3];
  argv[0] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.ALIASUPDATE");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "my_alias");
  argv[2] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_idx");
  VMSDK_EXPECT_OK(FTAliasUpdateCmd(&fake_ctx_, argv, 3));
  for (auto* arg : argv) TestValkeyModule_FreeString(&fake_ctx_, arg);
}

}  // namespace

}  // namespace valkey_search
