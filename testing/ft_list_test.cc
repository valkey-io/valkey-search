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

#include <memory>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "src/commands/commands.h"
#include "src/index_schema.pb.h"
#include "src/schema_manager.h"
#include "testing/common.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

class FTListTest : public ValkeySearchTest {};

TEST_F(FTListTest, basic) {
  // Setup the data structures for the test case.
  for (bool use_thread_pool : {true, false}) {
    RedisModuleCtx fake_ctx;
    vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 5);
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, use_thread_pool ? &mutations_thread_pool : nullptr,
        false));

    auto fake_prefixes = std::vector<absl::string_view>{"prefix_1"};
    std::string index_schema_name_1_str = "index_schema_name_1";
    std::string index_schema_name_2_str = "index_schema_name_2";
    std::string different_db_index_schema_name_str = "index_schema_name_3";
    data_model::IndexSchema base_index_schema;
    ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
        R"(
          subscribed_key_prefixes: "prefix_1"
          attribute_data_type: ATTRIBUTE_DATA_TYPE_HASH
          attributes: {
            alias: "attribute_1"
            identifier: "attribute_1"
            index: {
              vector_index: {
                dimension_count: 756
                normalize: true
                distance_metric: DISTANCE_METRIC_COSINE
                vector_data_type: VECTOR_DATA_TYPE_FLOAT32
                initial_cap: 10240
                hnsw_algorithm {
                  m: 128
                  ef_construction: 128
                  ef_runtime: 128
                }
              }
            }
          }
        )",
        &base_index_schema));
    data_model::IndexSchema index_schema_1(base_index_schema);
    index_schema_1.set_name(index_schema_name_1_str);
    index_schema_1.set_db_num(0);
    data_model::IndexSchema index_schema_2(base_index_schema);
    index_schema_2.set_name(index_schema_name_2_str);
    index_schema_2.set_db_num(0);
    data_model::IndexSchema different_db_index_schema(base_index_schema);
    different_db_index_schema.set_name(different_db_index_schema_name_str);
    different_db_index_schema.set_db_num(1);
    VMSDK_EXPECT_OK(
        SchemaManager::Instance().CreateIndexSchema(&fake_ctx, index_schema_1));
    VMSDK_EXPECT_OK(
        SchemaManager::Instance().CreateIndexSchema(&fake_ctx, index_schema_2));
    VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
        &fake_ctx, different_db_index_schema));

    EXPECT_CALL(*kMockRedisModule, GetSelectedDb(&fake_ctx))
        .WillRepeatedly(testing::Return(0));
    VMSDK_EXPECT_OK(FTListCmd(&fake_ctx, nullptr, 0));
    EXPECT_THAT(fake_ctx.reply_capture.GetReply(),
                testing::AnyOf(
                    "*2\r\n+index_schema_name_1\r\n+index_schema_name_2\r\n",
                    "*2\r\n+index_schema_name_2\r\n+index_schema_name_1\r\n"));
    VMSDK_EXPECT_OK(SchemaManager::Instance().RemoveIndexSchema(
        0, index_schema_name_1_str));
    VMSDK_EXPECT_OK(SchemaManager::Instance().RemoveIndexSchema(
        0, index_schema_name_2_str));
    VMSDK_EXPECT_OK(SchemaManager::Instance().RemoveIndexSchema(
        1, different_db_index_schema_name_str));
  }
}
TEST_F(FTListTest, no_indexes) {
  EXPECT_CALL(*kMockRedisModule, ReplyWithArray(&fake_ctx_, 0));
  VMSDK_EXPECT_OK(FTListCmd(&fake_ctx_, nullptr, 0));
}

}  // namespace

}  // namespace valkey_search
