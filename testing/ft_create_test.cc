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

#include <algorithm>
#include <iterator>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/commands/commands.h"
#include "src/indexes/index_base.h"
#include "src/schema_manager.h"
#include "testing/common.h"
#include "vmsdk/src/module.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

using ::testing::TestParamInfo;
using ::testing::ValuesIn;

struct ExpectedIndex {
  std::string attribute_alias;
  indexes::IndexerType indexer_type;
};

struct FTCreateTestCase {
  std::string test_name;
  std::vector<std::string> argv;
  std::string index_schema_name;
  int expected_run_return;
  std::string expected_reply_message;
  std::vector<ExpectedIndex> expected_indexes;
};

class FTCreateTest : public ValkeySearchTestWithParam<FTCreateTestCase> {};

TEST_P(FTCreateTest, FTCreateTests) {
  const FTCreateTestCase& test_case = GetParam();
  int db_num = 1;
  ON_CALL(*kMockRedisModule, GetSelectedDb(&fake_ctx_))
      .WillByDefault(testing::Return(db_num));

  std::vector<RedisModuleString*> cmd_argv;
  std::transform(test_case.argv.begin(), test_case.argv.end(),
                 std::back_inserter(cmd_argv), [&](std::string val) {
                   return TestRedisModule_CreateStringPrintf(&fake_ctx_, "%s",
                                                             val.data());
                 });
  EXPECT_EQ(vmsdk::CreateCommand<FTCreateCmd>(&fake_ctx_, cmd_argv.data(),
                                              cmd_argv.size()),
            test_case.expected_run_return);
  EXPECT_EQ(fake_ctx_.reply_capture.GetReply(),
            test_case.expected_reply_message);
  auto index_schema = SchemaManager::Instance().GetIndexSchema(
      db_num, test_case.index_schema_name);
  VMSDK_EXPECT_OK(index_schema);
  for (const auto& expected_index : test_case.expected_indexes) {
    auto index = index_schema.value()->GetIndex(expected_index.attribute_alias);
    VMSDK_EXPECT_OK(index);
    EXPECT_EQ(index.value()->GetIndexerType(), expected_index.indexer_type);
  }
  VMSDK_EXPECT_OK(SchemaManager::Instance().RemoveIndexSchema(
      db_num, test_case.index_schema_name));
  for (auto cmd_arg : cmd_argv) {
    TestRedisModule_FreeString(&fake_ctx_, cmd_arg);
  }
}

INSTANTIATE_TEST_SUITE_P(
    FTCreateTests, FTCreateTest,
    ValuesIn<FTCreateTestCase>({
        {
            .test_name = "happy_path_hnsw",
            .argv = {"FT.CREATE", "test_index_schema", "schema", "vector",
                     "vector", "HNSW", "12", "m", "100", "TYPE", "FLOAT32",
                     "DIM", "100", "DISTANCE_METRIC", "IP", "EF_CONSTRUCTION",
                     "40", "INITIAL_CAP", "15000"},
            .index_schema_name = "test_index_schema",
            .expected_run_return = REDISMODULE_OK,
            .expected_reply_message = "+OK\r\n",
            .expected_indexes =
                {
                    {
                        .attribute_alias = "vector",
                        .indexer_type = indexes::IndexerType::kHNSW,
                    },
                },
        },
        {
            .test_name = "happy_path_hnsw_with_numeric",
            .argv = {"FT.CREATE", "test_index_schema",
                     "schema",    "field1",
                     "numeric",   "vector",
                     "vector",    "HNSW",
                     "12",        "m",
                     "100",       "TYPE",
                     "FLOAT32",   "DIM",
                     "100",       "DISTANCE_METRIC",
                     "IP",        "EF_CONSTRUCTION",
                     "40",        "INITIAL_CAP",
                     "15000"},
            .index_schema_name = "test_index_schema",
            .expected_run_return = REDISMODULE_OK,
            .expected_reply_message = "+OK\r\n",
            .expected_indexes =
                {
                    {
                        .attribute_alias = "field1",
                        .indexer_type = indexes::IndexerType::kNumeric,
                    },
                    {
                        .attribute_alias = "vector",
                        .indexer_type = indexes::IndexerType::kHNSW,
                    },
                },
        },
        {
            .test_name = "happy_path_flat",
            .argv = {"FT.CREATE", "test_index_schema", "schema", "vector",
                     "vector", "Flat", "8", "TYPE", "FLOAT32", "DIM", "100",
                     "DISTANCE_METRIC", "IP", "INITIAL_CAP", "15000"},
            .index_schema_name = "test_index_schema",
            .expected_run_return = REDISMODULE_OK,
            .expected_reply_message = "+OK\r\n",
            .expected_indexes =
                {
                    {
                        .attribute_alias = "vector",
                        .indexer_type = indexes::IndexerType::kFlat,
                    },
                },
        },
        {
            .test_name = "happy_path_flat_with_tag",
            .argv = {"FT.CREATE", "test_index_schema", "schema", "vector",
                     "vector", "Flat", "8", "TYPE", "FLOAT32", "DIM", "100",
                     "DISTANCE_METRIC", "IP", "INITIAL_CAP", "15000", "field1",
                     "tag", "separator", "|"},
            .index_schema_name = "test_index_schema",
            .expected_run_return = REDISMODULE_OK,
            .expected_reply_message = "+OK\r\n",
            .expected_indexes =
                {
                    {
                        .attribute_alias = "field1",
                        .indexer_type = indexes::IndexerType::kTag,
                    },
                    {
                        .attribute_alias = "vector",
                        .indexer_type = indexes::IndexerType::kFlat,
                    },
                },
        },
    }),
    [](const TestParamInfo<FTCreateTestCase>& info) {
      return info.param.test_name;
    });

}  // namespace

}  // namespace valkey_search
