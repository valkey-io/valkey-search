/*
 * Copyright (c) 2026, valkey-search contributors
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
#include "absl/strings/str_cat.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "src/commands/commands.h"
#include "src/coordinator/metadata_manager.h"
#include "src/schema_manager.h"
#include "src/valkey_search_options.h"
#include "testing/common.h"
#include "testing/coordinator/common.h"
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

// Second index proto used in collision tests.
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
            // alias == index_name is allowed. Real index lookup takes
            // precedence over alias resolution, so the alias is inert
            // but still recorded in the alias map.
            .test_name = "alias_same_as_index_name",
            .argv = {"FT.ALIASADD", "test_idx", "test_idx"},
            .index_schema_pbtxt = std::string(kTestIndexSchemaPbtxt),
            .pre_existing_alias = std::nullopt,
            .return_code = absl::StatusCode::kOk,
        },
        {
            .test_name = "empty_alias_name",
            .argv = {"FT.ALIASADD", "", "test_idx"},
            .index_schema_pbtxt = std::string(kTestIndexSchemaPbtxt),
            .pre_existing_alias = std::nullopt,
            .return_code = absl::StatusCode::kInvalidArgument,
        },

    }),
    [](const TestParamInfo<FTAliasAddTestCase>& info) {
      return info.param.test_name;
    });

TEST_F(FTAliasAddTest, AliasMatchingExistingIndexNameSucceeds) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 5);
  SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
      &fake_ctx_, []() {}, &mutations_thread_pool, false));

  data_model::IndexSchema index_schema_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kTestIndexSchemaPbtxt), &index_schema_proto));
  VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
      &fake_ctx_, index_schema_proto));

  data_model::IndexSchema second_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kSecondIndexSchemaPbtxt), &second_proto));
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, second_proto));

  // FT.ALIASADD allows an alias whose name matches a different existing index.
  // Real index lookup takes precedence: resolving "test_idx2" must return the
  // real "test_idx2" index, not follow the alias to "test_idx".
  ValkeyModuleString* argv[3];
  argv[0] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.ALIASADD");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_idx2");
  argv[2] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_idx");

  VMSDK_EXPECT_OK(FTAliasAddCmd(&fake_ctx_, argv, 3));

  // Verify real-index-precedence: "test_idx2" resolves to the real index, not
  // following the alias chain to "test_idx".
  auto resolved = SchemaManager::Instance().GetIndexSchema(0, "test_idx2");
  VMSDK_EXPECT_OK(resolved);
  EXPECT_EQ(resolved.value()->GetName(), "test_idx2");

  // Verify the alias mapping is still recorded.
  auto aliases = SchemaManager::Instance().GetAllAliases(0);
  ASSERT_EQ(aliases.size(), 1);
  EXPECT_EQ(aliases[0].first, "test_idx2");
  EXPECT_EQ(aliases[0].second, "test_idx");

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
            .test_name = "non_existent_alias",
            .argv = {"FT.ALIASDEL", "no_such_alias"},
            .index_schema_pbtxt = std::nullopt,
            .pre_existing_alias = std::nullopt,
            .return_code = absl::StatusCode::kNotFound,
        },
        {
            .test_name = "empty_alias_name",
            .argv = {"FT.ALIASDEL", ""},
            .index_schema_pbtxt = std::nullopt,
            .pre_existing_alias = std::nullopt,
            .return_code = absl::StatusCode::kInvalidArgument,
        },
    }),
    [](const TestParamInfo<FTAliasDelTestCase>& info) {
      return info.param.test_name;
    });

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
            .test_name = "empty_alias_name",
            .argv = {"FT.ALIASUPDATE", "", "test_idx"},
            .index_schema_pbtxt = std::string(kTestIndexSchemaPbtxt),
            .pre_existing_alias = std::nullopt,
            .second_index_pbtxt = std::nullopt,
            .return_code = absl::StatusCode::kInvalidArgument,
        },
    }),
    [](const TestParamInfo<FTAliasUpdateTestCase>& info) {
      return info.param.test_name;
    });

TEST_F(FTAliasUpdateTest, AliasMatchingExistingIndexNameSucceeds) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 5);
  SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
      &fake_ctx_, []() {}, &mutations_thread_pool, false));

  data_model::IndexSchema index_schema_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kTestIndexSchemaPbtxt), &index_schema_proto));
  VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
      &fake_ctx_, index_schema_proto));

  data_model::IndexSchema second_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kSecondIndexSchemaPbtxt), &second_proto));
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, second_proto));

  // ALIASUPDATE with alias name matching an existing index is allowed.
  // Real index lookup takes precedence over alias resolution.
  ValkeyModuleString* argv[3];
  argv[0] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.ALIASUPDATE");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_idx2");
  argv[2] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_idx");

  VMSDK_EXPECT_OK(FTAliasUpdateCmd(&fake_ctx_, argv, 3));

  // Verify real-index-precedence: "test_idx2" resolves to the real index, not
  // following the alias chain to "test_idx".
  auto resolved = SchemaManager::Instance().GetIndexSchema(0, "test_idx2");
  VMSDK_EXPECT_OK(resolved);
  EXPECT_EQ(resolved.value()->GetName(), "test_idx2");

  // Verify the alias mapping is still recorded.
  auto aliases = SchemaManager::Instance().GetAllAliases(0);
  ASSERT_EQ(aliases.size(), 1);
  EXPECT_EQ(aliases[0].first, "test_idx2");
  EXPECT_EQ(aliases[0].second, "test_idx");

  for (auto* arg : argv) {
    TestValkeyModule_FreeString(&fake_ctx_, arg);
  }
}

TEST_F(FTAliasUpdateTest, SelfReferentialAliasSucceeds) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 5);
  SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
      &fake_ctx_, []() {}, &mutations_thread_pool, false));

  data_model::IndexSchema index_schema_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kTestIndexSchemaPbtxt), &index_schema_proto));
  VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
      &fake_ctx_, index_schema_proto));

  ValkeyModuleString* argv[3];
  argv[0] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.ALIASUPDATE");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_idx");
  argv[2] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_idx");

  VMSDK_EXPECT_OK(FTAliasUpdateCmd(&fake_ctx_, argv, 3));

  // Real index lookup takes precedence: "test_idx" resolves to the index
  // directly, the self-referential alias is inert but recorded.
  auto resolved = SchemaManager::Instance().GetIndexSchema(0, "test_idx");
  VMSDK_EXPECT_OK(resolved);
  EXPECT_EQ(resolved.value()->GetName(), "test_idx");

  auto aliases = SchemaManager::Instance().GetAllAliases(0);
  ASSERT_EQ(aliases.size(), 1);
  EXPECT_EQ(aliases[0].first, "test_idx");
  EXPECT_EQ(aliases[0].second, "test_idx");

  for (auto* arg : argv) {
    TestValkeyModule_FreeString(&fake_ctx_, arg);
  }
}

struct FTAliasReplicationTestCase {
  std::string test_name;
  absl::Status (*cmd)(ValkeyModuleCtx*, ValkeyModuleString**, int);
  std::vector<std::string> argv;
  bool pre_existing_alias;
  bool use_coordinator;
  bool alias_resolves_after;
};

// Coordinator mode propagates through MetadataManager and must not replicate
// verbatim; standalone mode must. The coordinator cases drive a
// coordinator-enabled SchemaManager, so the metadata write path runs too.
class FTAliasReplicationTest
    : public ValkeySearchTestWithParam<FTAliasReplicationTestCase> {};

TEST_P(FTAliasReplicationTest, ReplicatesOnlyWithoutCoordinator) {
  const auto& test_case = GetParam();
  auto& coordinator_flag =
      const_cast<vmsdk::config::Boolean&>(options::GetUseCoordinator());
  const bool original_coordinator_value = coordinator_flag.GetValue();
  VMSDK_EXPECT_OK(coordinator_flag.SetValue(test_case.use_coordinator));

  coordinator::MockClientPool client_pool;
  if (test_case.use_coordinator) {
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault(testing::Return(&fake_ctx_));
    ON_CALL(*kMockValkeyModule, FreeThreadSafeContext(testing::_))
        .WillByDefault(testing::Return());
    ON_CALL(*kMockValkeyModule, GetMyClusterID())
        .WillByDefault(testing::Return("fake_node_id"));
    coordinator::MetadataManager::InitInstance(
        std::make_unique<coordinator::MetadataManager>(&fake_ctx_,
                                                       client_pool));
  }
  SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
      &fake_ctx_, []() {}, nullptr, test_case.use_coordinator));

  data_model::IndexSchema index_schema_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kTestIndexSchemaPbtxt), &index_schema_proto));
  VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
      &fake_ctx_, index_schema_proto));
  if (test_case.pre_existing_alias) {
    VMSDK_EXPECT_OK(
        SchemaManager::Instance().AddAlias(0, "my_alias", "test_idx"));
  }

  EXPECT_CALL(*kMockValkeyModule, ReplicateVerbatim(&fake_ctx_))
      .Times(test_case.use_coordinator ? 0 : 1);

  std::vector<ValkeyModuleString*> cmd_argv;
  for (const auto& arg : test_case.argv) {
    cmd_argv.push_back(
        TestValkeyModule_CreateStringPrintf(&fake_ctx_, "%s", arg.data()));
  }
  VMSDK_EXPECT_OK(test_case.cmd(&fake_ctx_, cmd_argv.data(), cmd_argv.size()));

  auto resolved = SchemaManager::Instance().GetIndexSchema(0, "my_alias");
  if (test_case.alias_resolves_after) {
    VMSDK_EXPECT_OK(resolved);
    EXPECT_EQ(resolved.value()->GetName(), "test_idx");
  } else {
    EXPECT_EQ(resolved.status().code(), absl::StatusCode::kNotFound);
  }

  for (auto* arg : cmd_argv) {
    TestValkeyModule_FreeString(&fake_ctx_, arg);
  }
  SchemaManager::InitInstance(nullptr);
  coordinator::MetadataManager::InitInstance(nullptr);
  VMSDK_EXPECT_OK(coordinator_flag.SetValue(original_coordinator_value));
}

INSTANTIATE_TEST_SUITE_P(
    FTAliasReplicationTests, FTAliasReplicationTest,
    ValuesIn<FTAliasReplicationTestCase>({
        {
            .test_name = "add_coordinator",
            .cmd = &FTAliasAddCmd,
            .argv = {"FT.ALIASADD", "my_alias", "test_idx"},
            .pre_existing_alias = false,
            .use_coordinator = true,
            .alias_resolves_after = true,
        },
        {
            .test_name = "add_standalone",
            .cmd = &FTAliasAddCmd,
            .argv = {"FT.ALIASADD", "my_alias", "test_idx"},
            .pre_existing_alias = false,
            .use_coordinator = false,
            .alias_resolves_after = true,
        },
        {
            .test_name = "del_coordinator",
            .cmd = &FTAliasDelCmd,
            .argv = {"FT.ALIASDEL", "my_alias"},
            .pre_existing_alias = true,
            .use_coordinator = true,
            .alias_resolves_after = false,
        },
        {
            .test_name = "del_standalone",
            .cmd = &FTAliasDelCmd,
            .argv = {"FT.ALIASDEL", "my_alias"},
            .pre_existing_alias = true,
            .use_coordinator = false,
            .alias_resolves_after = false,
        },
        {
            .test_name = "update_coordinator",
            .cmd = &FTAliasUpdateCmd,
            .argv = {"FT.ALIASUPDATE", "my_alias", "test_idx"},
            .pre_existing_alias = false,
            .use_coordinator = true,
            .alias_resolves_after = true,
        },
        {
            .test_name = "update_standalone",
            .cmd = &FTAliasUpdateCmd,
            .argv = {"FT.ALIASUPDATE", "my_alias", "test_idx"},
            .pre_existing_alias = false,
            .use_coordinator = false,
            .alias_resolves_after = true,
        },
    }),
    [](const TestParamInfo<FTAliasReplicationTestCase>& info) {
      return info.param.test_name;
    });

// FT.INFO must reply with aliases as bulk strings: a simple string stops at a
// null byte and cannot carry CR/LF.
class FTInfoAliasesTest : public ValkeySearchTest {};

TEST_F(FTInfoAliasesTest, RepliesAliasesAsBulkStrings) {
  SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
      &fake_ctx_, []() {}, nullptr, false));
  data_model::IndexSchema index_schema_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kTestIndexSchemaPbtxt), &index_schema_proto));
  VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
      &fake_ctx_, index_schema_proto));
  const std::string alias("a\0b\r\n", 5);
  VMSDK_EXPECT_OK(SchemaManager::Instance().AddAlias(0, alias, "test_idx"));

  ValkeyModuleString* argv[2];
  argv[0] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.INFO");
  argv[1] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "test_idx");
  VMSDK_EXPECT_OK(FTInfoCmd(&fake_ctx_, argv, 2));
  EXPECT_THAT(fake_ctx_.reply_capture.GetReply(),
              testing::HasSubstr(
                  absl::StrCat("+aliases\r\n*1\r\n$5\r\n", alias, "\r\n")));
  for (auto* arg : argv) {
    TestValkeyModule_FreeString(&fake_ctx_, arg);
  }
}

class FTAliasListTest : public ValkeySearchTest {};

TEST_F(FTAliasListTest, EmptyWhenNoAliases) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 5);
  SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
      &fake_ctx_, []() {}, &mutations_thread_pool, false));

  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillRepeatedly(testing::Return(0));
  EXPECT_CALL(*kMockValkeyModule, ReplyWithArray(&fake_ctx_, 0));

  ValkeyModuleString* argv[1];
  argv[0] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.ALIASLIST");

  VMSDK_EXPECT_OK(FTAliasListCmd(&fake_ctx_, argv, 1));

  TestValkeyModule_FreeString(&fake_ctx_, argv[0]);
}

TEST_F(FTAliasListTest, SingleAlias) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 5);
  SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
      &fake_ctx_, []() {}, &mutations_thread_pool, false));

  data_model::IndexSchema index_schema_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kTestIndexSchemaPbtxt), &index_schema_proto));
  VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
      &fake_ctx_, index_schema_proto));
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(0, "my_alias", "test_idx"));

  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillRepeatedly(testing::Return(0));

  ValkeyModuleString* argv[1];
  argv[0] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.ALIASLIST");

  VMSDK_EXPECT_OK(FTAliasListCmd(&fake_ctx_, argv, 1));

  // Expected reply: array of 2 elements (alias, index_name).
  EXPECT_EQ(fake_ctx_.reply_capture.GetReply(),
            "*2\r\n$8\r\nmy_alias\r\n$8\r\ntest_idx\r\n");

  TestValkeyModule_FreeString(&fake_ctx_, argv[0]);
}

TEST_F(FTAliasListTest, MultipleAliasesSortedByName) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 5);
  SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
      &fake_ctx_, []() {}, &mutations_thread_pool, false));

  data_model::IndexSchema index_schema_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kTestIndexSchemaPbtxt), &index_schema_proto));
  VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
      &fake_ctx_, index_schema_proto));
  VMSDK_EXPECT_OK(SchemaManager::Instance().AddAlias(0, "z_alias", "test_idx"));
  VMSDK_EXPECT_OK(SchemaManager::Instance().AddAlias(0, "a_alias", "test_idx"));
  VMSDK_EXPECT_OK(SchemaManager::Instance().AddAlias(0, "m_alias", "test_idx"));

  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillRepeatedly(testing::Return(0));

  ValkeyModuleString* argv[1];
  argv[0] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.ALIASLIST");

  VMSDK_EXPECT_OK(FTAliasListCmd(&fake_ctx_, argv, 1));

  // Sorted: a_alias, m_alias, z_alias — each with its index_name.
  EXPECT_EQ(fake_ctx_.reply_capture.GetReply(),
            "*6\r\n$7\r\na_alias\r\n$8\r\ntest_idx\r\n"
            "$7\r\nm_alias\r\n$8\r\ntest_idx\r\n"
            "$7\r\nz_alias\r\n$8\r\ntest_idx\r\n");

  TestValkeyModule_FreeString(&fake_ctx_, argv[0]);
}

TEST_F(FTAliasListTest, OnlyShowsCurrentDbAliases) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 5);
  SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
      &fake_ctx_, []() {}, &mutations_thread_pool, false));

  data_model::IndexSchema index_schema_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kTestIndexSchemaPbtxt), &index_schema_proto));
  VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
      &fake_ctx_, index_schema_proto));

  // Add aliases in db 0.
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(0, "db0_alias", "test_idx"));

  // Create an index in db 1 and add alias there.
  data_model::IndexSchema db1_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kSecondIndexSchemaPbtxt), &db1_proto));
  db1_proto.set_db_num(1);
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, db1_proto));
  VMSDK_EXPECT_OK(
      SchemaManager::Instance().AddAlias(1, "db1_alias", "test_idx2"));

  // Query from db 0: should only see db0_alias.
  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillRepeatedly(testing::Return(0));

  ValkeyModuleString* argv[1];
  argv[0] = TestValkeyModule_CreateStringPrintf(&fake_ctx_, "FT.ALIASLIST");

  VMSDK_EXPECT_OK(FTAliasListCmd(&fake_ctx_, argv, 1));

  EXPECT_EQ(fake_ctx_.reply_capture.GetReply(),
            "*2\r\n$9\r\ndb0_alias\r\n$8\r\ntest_idx\r\n");

  TestValkeyModule_FreeString(&fake_ctx_, argv[0]);
}

}  // namespace

}  // namespace valkey_search
