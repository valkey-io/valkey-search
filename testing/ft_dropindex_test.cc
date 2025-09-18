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
#include "src/index_schema.h"
#include "src/keyspace_event_manager.h"
#include "src/schema_manager.h"
#include "testing/common.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

using ::testing::TestParamInfo;
using ::testing::ValuesIn;

struct SingleFtDropIndexTestCase {
  std::vector<std::string> argv;
  std::optional<std::string> index_schema_pbtxt;
  bool expect_index_schema_exists_after_test;
  bool expect_subscription_exists_after_test;
  absl::StatusCode return_code;
};

struct MultiFtDropIndexTestCase {
  std::string test_name;
  std::vector<SingleFtDropIndexTestCase> test_cases;
};

class FTDropIndexTest
    : public ValkeySearchTestWithParam<MultiFtDropIndexTestCase> {};

TEST_P(FTDropIndexTest, FTDropIndexTests) {
  const MultiFtDropIndexTestCase& test_cases = GetParam();
  for (bool use_thread_pool : {true, false}) {
    for (const auto& test_case : test_cases.test_cases) {
      // Set up the data structures for the test case.
      ValkeyModuleCtx fake_ctx;
      vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 5);
      SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
          &fake_ctx_, []() {},
          use_thread_pool ? &mutations_thread_pool : nullptr, false));

      data_model::IndexSchema index_schema_proto;
      IndexSchema* test_index_schema_raw = nullptr;
      if (test_case.index_schema_pbtxt.has_value()) {
        ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
            test_case.index_schema_pbtxt.value(), &index_schema_proto));
        VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
            &fake_ctx, index_schema_proto));
        EXPECT_EQ(SchemaManager::Instance().GetNumberOfIndexSchemas(), 1);
        test_index_schema_raw = SchemaManager::Instance()
                                    .GetIndexSchema(index_schema_proto.db_num(),
                                                    index_schema_proto.name())
                                    ->get();
      }

      // Run the command.
      std::vector<ValkeyModuleString*> cmd_argv;
      std::transform(test_case.argv.begin(), test_case.argv.end(),
                     std::back_inserter(cmd_argv),
                     [&fake_ctx](std::string val) {
                       return TestValkeyModule_CreateStringPrintf(
                           &fake_ctx, "%s", val.data());
                     });

      EXPECT_EQ(
          FTDropIndexCmd(&fake_ctx, cmd_argv.data(), cmd_argv.size()).code(),
          test_case.return_code);

      if (test_case.index_schema_pbtxt.has_value()) {
        // Verify that we have the expected state of the data structures.
        if (test_case.expect_subscription_exists_after_test) {
          EXPECT_TRUE(KeyspaceEventManager::Instance().HasSubscription(
              test_index_schema_raw));
        } else {
          EXPECT_FALSE(KeyspaceEventManager::Instance().HasSubscription(
              test_index_schema_raw));
        }
        if (test_case.expect_index_schema_exists_after_test) {
          VMSDK_EXPECT_OK(SchemaManager::Instance().RemoveIndexSchema(
              &fake_ctx_, index_schema_proto.db_num(),
              index_schema_proto.name()));
          EXPECT_EQ(SchemaManager::Instance().GetNumberOfIndexSchemas(), 0);
        } else {
          EXPECT_EQ(
              SchemaManager::Instance()
                  .RemoveIndexSchema(&fake_ctx_, index_schema_proto.db_num(),
                                     index_schema_proto.name())
                  .code(),
              absl::StatusCode::kNotFound);
        }
      }

      // Clean up
      for (auto cmd_arg : cmd_argv) {
        TestValkeyModule_FreeString(&fake_ctx, cmd_arg);
      }
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
    FTDropIndexTests, FTDropIndexTest,
    ValuesIn<MultiFtDropIndexTestCase>({
        {
            .test_name = "happy_path",
            .test_cases =
                {
                    {
                        .argv = {"FT.DROPINDEX", "test_key"},
                        .index_schema_pbtxt = R"(
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
                        )",
                        .expect_index_schema_exists_after_test = false,
                        .expect_subscription_exists_after_test = false,
                        .return_code = absl::StatusCode::kOk,
                    },
                },
        },
        {
            .test_name = "incorrect_param_count",
            .test_cases =
                {
                    {
                        .argv = {"FT.DROPINDEX"},
                        .return_code = absl::StatusCode::kInvalidArgument,
                    },
                },
        },
        {
            .test_name = "index_schema_does_not_exist",
            .test_cases =
                {
                    {
                        .argv = {"FT.DROPINDEX", "test_key"},
                        .index_schema_pbtxt = std::nullopt,
                        .return_code = absl::StatusCode::kNotFound,
                    },
                },
        },
        {
            .test_name = "drop_then_readd_same_name",
            .test_cases =
                {
                    {
                        .argv = {"FT.DROPINDEX", "test_key"},
                        .index_schema_pbtxt = R"(
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
                        )",
                        .expect_index_schema_exists_after_test = false,
                        .expect_subscription_exists_after_test = false,
                        .return_code = absl::StatusCode::kOk,
                    },
                    {
                        .argv = {"FT.DROPINDEX", "test_key"},
                        .index_schema_pbtxt = R"(
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
                        )",
                        .expect_index_schema_exists_after_test = false,
                        .expect_subscription_exists_after_test = false,
                        .return_code = absl::StatusCode::kOk,
                    },
                },
        },
    }),
    [](const TestParamInfo<MultiFtDropIndexTestCase>& info) {
      return info.param.test_name;
    });

}  // namespace

}  // namespace valkey_search
