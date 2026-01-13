/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/ft_search.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/notification.h"
#include "gmock/gmock.h"
#include "grpcpp/support/status.h"
#include "gtest/gtest.h"
#include "re2/re2.h"
#include "src/commands/commands.h"
#include "src/commands/ft_create_parser.h"
#include "src/commands/ft_search_parser.h"
#include "src/coordinator/client.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/coordinator/util.h"
#include "src/indexes/vector_base.h"
#include "src/metrics.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search.h"
#include "src/vector_externalizer.h"
#include "testing/common.h"
#include "testing/coordinator/common.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/module.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/time_sliced_mrmw_mutex.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

using testing::An;
using testing::TestParamInfo;
using testing::ValuesIn;

struct SendReplyTestInput {
  std::deque<NeighborTest> neighbors;
  std::string attribute_alias;
  std::string score_as;
  query::LimitParameter limit;
  std::vector<TestReturnAttribute> return_attributes;
};

struct SendReplyTestCase {
  std::string test_name;
  SendReplyTestInput input;
  absl::string_view expected_output;
  absl::string_view expected_output_no_content;
  std::set<std::string> hash_get_exclude_ids;
  std::set<std::string> open_key_exclude_ids;
};

class SendReplyTest : public ValkeySearchTestWithParam<SendReplyTestCase> {
 public:
  void DoSendReplyTest(const SendReplyTestInput &input, bool no_content,
                       const RespReply &expected_output,
                       const std::set<std::string> &hash_get_exclude_ids,
                       const std::set<std::string> &open_key_exclude_ids,
                       vmsdk::ThreadPool *mutations_thread_pool);
};

void SendReplyTest::DoSendReplyTest(
    const SendReplyTestInput &input, bool no_content,
    const RespReply &expected_output,
    const std::set<std::string> &hash_get_exclude_ids,
    const std::set<std::string> &open_key_exclude_ids,
    vmsdk::ThreadPool *mutations_thread_pool) {
  ValkeyModuleCtx fake_ctx;
  SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
      &fake_ctx, []() {}, mutations_thread_pool, false));

  const std::string attribute_id = "attribute_id";
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(An<ValkeyModuleKey *>(),
                      VALKEYMODULE_HASH_CFIELDS | VALKEYMODULE_HASH_EXISTS,
                      An<const char *>(), An<int *>(), An<void *>()))
      .WillRepeatedly([&](ValkeyModuleKey *module_key, int flags,
                          const char *field, int *exists,
                          void *terminating_null) {
        *exists = 1;
        EXPECT_EQ(attribute_id, field);
        std::string key_str{module_key->key};
        if (hash_get_exclude_ids.find(key_str) != hash_get_exclude_ids.end()) {
          *exists = 0;
        }
        return VALKEYMODULE_OK;
      });
  EXPECT_CALL(*kMockValkeyModule,
              ScanKey(An<ValkeyModuleKey *>(), An<ValkeyModuleScanCursor *>(),
                      An<ValkeyModuleScanKeyCB>(), An<void *>()))
      .WillRepeatedly([&](ValkeyModuleKey *key,
                          ValkeyModuleScanCursor *scan_cursor,
                          ValkeyModuleScanKeyCB fn, void *privdata) {
        ++scan_cursor->cursor;
        if ((scan_cursor->cursor % 5) == 0) {
          return 0;
        }
        if ((scan_cursor->cursor % 5) == 1) {
          static const absl::string_view field_str = "field1";
          static const absl::string_view value_str = "value1";
          auto field = vmsdk::MakeUniqueValkeyString(field_str);
          auto value = vmsdk::MakeUniqueValkeyString(value_str);
          fn(key, field.get(), value.get(), privdata);
          return 1;
        }
        if ((scan_cursor->cursor % 5) == 2) {
          std::string value2_str = input.attribute_alias + "_hash_value";
          auto field = vmsdk::MakeUniqueValkeyString(input.attribute_alias);
          auto value = vmsdk::MakeUniqueValkeyString(value2_str);
          fn(key, field.get(), value.get(), privdata);
          return 1;
        }
        if ((scan_cursor->cursor % 5) == 3) {
          static const absl::string_view field_str = "";
          static const absl::string_view value_str = "value1";
          auto field = vmsdk::MakeUniqueValkeyString(field_str);
          auto value = vmsdk::MakeUniqueValkeyString(value_str);
          fn(key, field.get(), value.get(), privdata);
          return 1;
        }
        fn(key, nullptr, nullptr, privdata);
        return 1;
      });
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(&fake_ctx, An<ValkeyModuleString *>(), testing::_))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  for (const auto &key : open_key_exclude_ids) {
    EXPECT_CALL(
        *kMockValkeyModule,
        OpenKey(&fake_ctx, vmsdk::ValkeyModuleStringValueEq(key), testing::_))
        .WillRepeatedly(testing::Return(nullptr));
  }

  // using non-null terminated strings for attribute_alias and score_as
  std::string attribute_alias_with_extra_data{input.attribute_alias +
                                              "_hash_value"};
  absl::string_view attribute_alias{attribute_alias_with_extra_data.c_str(),
                                    input.attribute_alias.size()};

  std::string score_as_with_extra_data{input.score_as + "_hash_value"};
  absl::string_view score_as{score_as_with_extra_data.c_str(),
                             input.score_as.size()};

  auto test_index_schema = CreateVectorHNSWSchema("index_schema_key", &fake_ctx,
                                                  mutations_thread_pool)
                               .value();
  EXPECT_CALL(*test_index_schema, GetIdentifier(input.attribute_alias))
      .WillRepeatedly(testing::Return(attribute_id));
  std::vector<indexes::Neighbor> neighbors;
  for (const auto &neighbor : input.neighbors) {
    neighbors.push_back(ToIndexesNeighbor(neighbor));
  }
  auto parameters = std::make_unique<SearchCommand>(0);
  parameters->timeout_ms = 10000;
  parameters->index_schema = test_index_schema;
  parameters->attribute_alias = attribute_alias;
  parameters->score_as = vmsdk::MakeUniqueValkeyString(score_as);
  parameters->k = 20;
  parameters->limit = input.limit;
  parameters->no_content = no_content;
  for (const auto &return_attribute : input.return_attributes) {
    parameters->return_attributes.push_back(
        ToReturnAttribute(return_attribute));
  }
  auto neighbor_count = neighbors.size();
  query::SearchResult wrapper(neighbor_count, std::move(neighbors),
                              *parameters);
  parameters->SendReply(&fake_ctx, wrapper);
  EXPECT_EQ(ParseRespReply(fake_ctx.reply_capture.GetReply()), expected_output);
}

TEST_P(SendReplyTest, SendReply) {
  const SendReplyTestCase &test_case = GetParam();
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 5);
  for (bool use_thread_pool : {true, false}) {
    DoSendReplyTest(
        test_case.input, false, ParseRespReply(test_case.expected_output),
        test_case.hash_get_exclude_ids, test_case.open_key_exclude_ids,
        use_thread_pool ? &mutations_thread_pool : nullptr);
    std::cout << "starting no content test \n";
    DoSendReplyTest(test_case.input, true,
                    ParseRespReply(test_case.expected_output_no_content),
                    test_case.hash_get_exclude_ids,
                    test_case.open_key_exclude_ids,
                    use_thread_pool ? &mutations_thread_pool : nullptr);
  }
}

INSTANTIATE_TEST_SUITE_P(
    SendReplyTests, SendReplyTest,
    ValuesIn<SendReplyTestCase>({
        {
            .test_name = "basic",
            .input =
                {
                    .neighbors =
                        {{.external_id = "abc", .distance = 0.00999999977648},
                         {.external_id = "def", .distance = 0.019999999553}},
                    .attribute_alias = "attribute_alias_1",
                    .score_as = "score_as_1",
                    .limit = {.first_index = 0, .number = 10},
                },
            .expected_output =
                "*5\r\n:2\r\n$3\r\nabc\r\n*6\r\n$10\r\nscore_as_1\r\n$16\r\n0."
                "00999999977648\r\n$17\r\nattribute_alias_1\r\n$"
                "28\r\nattribute_alias_1_hash_value\r\n$6\r\nfield1\r\n$"
                "6\r\nvalue1\r\n$3\r\ndef\r\n*6\r\n$10\r\nscore_as_1\r\n$"
                "14\r\n0.019999999553\r\n$17\r\nattribute_alias_1\r\n$"
                "28\r\nattribute_alias_1_hash_value\r\n$6\r\nfield1\r\n$"
                "6\r\nvalue1\r\n",
            .expected_output_no_content =
                "*3\r\n:2\r\n$3\r\nabc\r\n$3\r\ndef\r\n",
        },
        {
            .test_name = "external_id_not_found",
            .input =
                {
                    .neighbors =
                        {{.external_id = "abc", .distance = 0.00999999977648},
                         {.external_id = "def", .distance = 0.019999999553},
                         {.external_id = "ghi", .distance = 0.03}},
                    .attribute_alias = "attribute_alias_1",
                    .score_as = "score_as_1",
                    .limit = {.first_index = 0, .number = 10},
                },
            .expected_output =
                "*3\r\n:1\r\n$3\r\ndef\r\n*6\r\n$10\r\nscore_as_1\r\n$14\r\n0."
                "019999999553\r\n$17\r\nattribute_alias_1\r\n$28\r\nattribute_"
                "alias_1_hash_value\r\n$6\r\nfield1\r\n$6\r\nvalue1\r\n",
            .expected_output_no_content =
                "*4\r\n:3\r\n$3\r\nabc\r\n$3\r\ndef\r\n$3\r\nghi\r\n",
            .hash_get_exclude_ids = {"abc"},
            .open_key_exclude_ids = {"ghi"},
        },
        {
            .test_name = "limit_out_of_range",
            .input =
                {
                    .neighbors =
                        {{.external_id = "abc", .distance = 0.00999999977648},
                         {.external_id = "def", .distance = 0.019999999553}},
                    .attribute_alias = "attribute_alias_1",
                    .score_as = "score_as_1",
                    .limit = {.first_index = 100, .number = 105},
                },
            .expected_output = "*1\r\n:2\r\n",
            .expected_output_no_content = "*1\r\n:2\r\n",
        },
        {
            .test_name = "just_result_count",
            .input =
                {
                    .neighbors =
                        {{.external_id = "abc", .distance = 0.00999999977648},
                         {.external_id = "def", .distance = 0.019999999553}},
                    .attribute_alias = "attribute_alias_1",
                    .score_as = "score_as_1",
                    .limit = {.first_index = 0, .number = 0},
                },
            .expected_output = "*1\r\n:2\r\n",
            .expected_output_no_content = "*1\r\n:2\r\n",
        },
        {
            .test_name = "only_first",
            .input =
                {
                    .neighbors =
                        {{.external_id = "ext_1", .distance = 0.00999999977648},
                         {.external_id = "ext_2", .distance = 0.019999999553}},
                    .attribute_alias = "attribute_alias_2",
                    .score_as = "score_as_2",
                    .limit = {.first_index = 0, .number = 1},
                },
            .expected_output =
                "*3\r\n:2\r\n$5\r\next_1\r\n*6\r\n$10\r\nscore_as_2\r\n$"
                "16\r\n0."
                "00999999977648\r\n$17\r\nattribute_alias_2\r\n$"
                "28\r\nattribute_alias_2_hash_value\r\n$6\r\nfield1\r\n$"
                "6\r\nvalue1\r\n",
            .expected_output_no_content = "*2\r\n:2\r\n$5\r\next_1\r\n",
        },
        {
            .test_name = "only_second",
            .input =
                {
                    .neighbors =
                        {{.external_id = "ext_1", .distance = 0.00999999977648},
                         {.external_id = "ext_2", .distance = 0.019999999553}},
                    .attribute_alias = "attribute_alias_2",
                    .score_as = "__vector_score",
                    .limit = {.first_index = 1, .number = 1},
                },
            .expected_output =
                "*3\r\n:2\r\n$5\r\next_2\r\n*6\r\n$14\r\n__vector_score\r\n$"
                "14\r\n0.019999999553\r\n$17\r\nattribute_alias_2\r\n$"
                "28\r\nattribute_alias_2_hash_value\r\n$6\r\nfield1\r\n$"
                "6\r\nvalue1\r\n",
            .expected_output_no_content = "*2\r\n:2\r\n$5\r\next_2\r\n",
        },
        {
            .test_name = "return_1",
            .input =
                {
                    .neighbors =
                        {{.external_id = "abc", .distance = 0.00999999977648},
                         {.external_id = "def", .distance = 0.019999999553}},
                    .attribute_alias = "attribute_alias_1",
                    .score_as = "score_as_1",
                    .limit = {.first_index = 0, .number = 10},
                    .return_attributes = {{.identifier = "attribute_alias_1",
                                           .alias = "attribute_alias_11"}},
                },
            .expected_output =
                "*5\r\n:2\r\n$3\r\nabc\r\n*2\r\n$18\r\nattribute_alias_11\r\n$"
                "28\r\nattribute_alias_1_hash_value\r\n$3\r\ndef\r\n*2\r\n$"
                "18\r\nattribute_alias_11\r\n$28\r\nattribute_alias_1_hash_"
                "value\r\n",
            .expected_output_no_content =
                "*3\r\n:2\r\n$3\r\nabc\r\n$3\r\ndef\r\n",
        },
        {
            .test_name = "return_2",
            .input =
                {
                    .neighbors =
                        {{.external_id = "abc", .distance = 0.00999999977648},
                         {.external_id = "def", .distance = 0.019999999553}},
                    .attribute_alias = "attribute_alias_1",
                    .score_as = "score_as_1",
                    .limit = {.first_index = 0, .number = 10},
                    .return_attributes = {{.identifier = "attribute_alias_1",
                                           .alias = "attribute_alias_11"},
                                          {.identifier = "attribute_alias_1",
                                           .alias = "attribute_alias_1"}},
                },
            .expected_output =
                "*5\r\n:2\r\n$3\r\nabc\r\n*4\r\n$18\r\nattribute_alias_11\r\n$"
                "28\r\nattribute_alias_1_hash_value\r\n$17\r\nattribute_alias_"
                "1\r\n$28\r\nattribute_alias_1_hash_value\r\n$3\r\ndef\r\n*"
                "4\r\n$18\r\nattribute_alias_11\r\n$28\r\nattribute_alias_1_"
                "hash_value\r\n$17\r\nattribute_alias_1\r\n$28\r\nattribute_"
                "alias_1_hash_value\r\n",
            .expected_output_no_content =
                "*3\r\n:2\r\n$3\r\nabc\r\n$3\r\ndef\r\n",
        },
        {
            .test_name = "return_3",
            .input =
                {
                    .neighbors =
                        {{.external_id = "abc", .distance = 0.00999999977648},
                         {.external_id = "def", .distance = 0.019999999553}},
                    .attribute_alias = "attribute_alias_1",
                    .score_as = "score_as_1",
                    .limit = {.first_index = 0, .number = 10},
                    .return_attributes = {{.identifier = "attribute_alias_1",
                                           .alias = "attribute_alias_11"},
                                          {.identifier = "attribute_alias_10",
                                           .alias = "attribute_alias_10"},
                                          {.identifier = "attribute_alias_1",
                                           .alias = "attribute_alias_1"}},
                },
            .expected_output =
                "*5\r\n:2\r\n$3\r\nabc\r\n*4\r\n$18\r\nattribute_alias_11\r\n$"
                "28\r\nattribute_alias_1_hash_value\r\n$17\r\nattribute_alias_"
                "1\r\n$28\r\nattribute_alias_1_hash_value\r\n$3\r\ndef\r\n*"
                "4\r\n$18\r\nattribute_alias_11\r\n$28\r\nattribute_alias_1_"
                "hash_value\r\n$17\r\nattribute_alias_1\r\n$28\r\nattribute_"
                "alias_1_hash_value\r\n",
            .expected_output_no_content =
                "*3\r\n:2\r\n$3\r\nabc\r\n$3\r\ndef\r\n",
        },
        {
            .test_name = "return_4",
            .input =
                {
                    .neighbors =
                        {{.external_id = "abc", .distance = 0.00999999977648},
                         {.external_id = "def", .distance = 0.019999999553}},
                    .attribute_alias = "attribute_alias_1",
                    .score_as = "score_as_1",
                    .limit = {.first_index = 0, .number = 10},
                    .return_attributes = {{.identifier = "attribute_alias_1",
                                           .alias = "attribute_alias_11"},
                                          {.identifier = "attribute_alias_10",
                                           .alias = "attribute_alias_10"},
                                          {.identifier = "attribute_alias_1",
                                           .alias = "attribute_alias_1"},
                                          {.identifier = "score_as_1",
                                           .alias = "score_as_1"}},
                },
            .expected_output =
                "*5\r\n:2\r\n$3\r\nabc\r\n*6\r\n$18\r\nattribute_alias_11\r\n$"
                "28\r\nattribute_alias_1_hash_value\r\n$17\r\nattribute_alias_"
                "1\r\n$28\r\nattribute_alias_1_hash_value\r\n$10\r\nscore_as_"
                "1\r\n$16\r\n0.00999999977648\r\n$3\r\ndef\r\n*6\r\n$"
                "18\r\nattribute_alias_11\r\n$28\r\nattribute_alias_1_hash_"
                "value\r\n$17\r\nattribute_alias_1\r\n$28\r\nattribute_alias_1_"
                "hash_value\r\n$10\r\nscore_as_1\r\n$14\r\n0.019999999553\r\n",
            .expected_output_no_content =
                "*3\r\n:2\r\n$3\r\nabc\r\n$3\r\ndef\r\n",
        },
    }),
    [](const TestParamInfo<SendReplyTestCase> &info) {
      return info.param.test_name;
    });

using ::testing::TestParamInfo;
using ::testing::ValuesIn;

struct FTSearchTestCase {
  std::string test_name;
  std::vector<std::string> argv;
  int expected_run_return;
};

class FTSearchTest : public ValkeySearchTestWithParam<
                         ::testing::tuple<bool, bool, FTSearchTestCase>> {
 public:
  void AddVectors(const std::vector<std::vector<float>> &vectors) {
    auto index_schema =
        SchemaManager::Instance().GetIndexSchema(db_num, index_name);
    VMSDK_EXPECT_OK(index_schema);
    auto index = index_schema.value()->GetIndex("vector");
    VMSDK_EXPECT_OK(index);
    for (size_t i = 0; i < vectors.size(); ++i) {
      auto key = std::to_string(i);
      std::string vector = std::string((char *)vectors[i].data(),
                                       vectors[i].size() * sizeof(float));
      auto interned_key = StringInternStore::Intern(key);

      VMSDK_EXPECT_OK(index.value()->AddRecord(interned_key, vector));
    }
  }
  const std::string index_name = "my_index";
  int dimensions = 100;
  int db_num = 0;
};

std::string GetNodeId(int i) {
  return {VALKEYMODULE_NODE_ID_LEN, static_cast<char>('a' + i)};
}

TEST_P(FTSearchTest, FTSearchTests) {
  auto &params = GetParam();
  bool use_thread_pool = std::get<1>(params);
  if (use_thread_pool) {
    InitThreadPools(5, std::nullopt, 1);
  }
  bool use_fanout = std::get<0>(params);
  if (use_fanout) {
    if (!use_thread_pool) {
      // Only test fanout with thread pool.
      return;
    }
    auto mock_client_pool = std::make_unique<coordinator::MockClientPool>();
    auto mock_client_pool_raw = mock_client_pool.get();
    ValkeySearch::Instance().SetCoordinatorClientPool(
        std::move(mock_client_pool));
    auto mock_server = std::make_unique<coordinator::MockServer>();
    ValkeySearch::Instance().SetCoordinatorServer(std::move(mock_server));
    std::vector<std::string> node_ids = {GetNodeId(0), GetNodeId(1),
                                         GetNodeId(2)};
    EXPECT_CALL(*kMockValkeyModule,
                GetClusterNodesList(testing::_, testing::An<size_t *>()))
        .WillRepeatedly([node_ids](ValkeyModuleCtx *ctx, size_t *numnodes) {
          *numnodes = node_ids.size();
          char **res = new char *[3];
          res[0] = new char[VALKEYMODULE_NODE_ID_LEN];
          res[1] = new char[VALKEYMODULE_NODE_ID_LEN];
          res[2] = new char[VALKEYMODULE_NODE_ID_LEN];
          memcpy(res[0], node_ids[0].c_str(), VALKEYMODULE_NODE_ID_LEN);
          memcpy(res[1], node_ids[1].c_str(), VALKEYMODULE_NODE_ID_LEN);
          memcpy(res[2], node_ids[2].c_str(), VALKEYMODULE_NODE_ID_LEN);
          return res;
        });
    EXPECT_CALL(*kMockValkeyModule, FreeClusterNodesList(testing::_))
        .WillRepeatedly([](char **ids) {
          delete[] ids[0];
          delete[] ids[1];
          delete[] ids[2];
          delete[] ids;
        });
    for (size_t i = 0; i < node_ids.size(); ++i) {
      const auto &node_id = node_ids[i];
      EXPECT_CALL(
          *kMockValkeyModule,
          GetClusterNodeInfo(testing::_, testing::StrEq(node_id), testing::_,
                             testing::_, testing::_, testing::_))
          .WillRepeatedly([i](ValkeyModuleCtx *ctx, const char *node_id,
                              char *ip, char *master_id, int *port,
                              int *flags) {
            memcpy(ip, "127.0.0.1", 9);
            *port = i;
            if (i == 0) {
              *flags = VALKEYMODULE_NODE_MYSELF;
            } else {
              *flags = VALKEYMODULE_NODE_MASTER;
            }
            return VALKEYMODULE_OK;
          });
      if (i != 0) {
        auto mock_client = std::make_shared<coordinator::MockClient>();
        auto coord_port = coordinator::GetCoordinatorPort(i);
        EXPECT_CALL(
            *mock_client_pool_raw,
            GetClient(testing::StrEq(absl::StrCat("127.0.0.1:", coord_port))))
            .WillRepeatedly(testing::Return(mock_client));
        EXPECT_CALL(*mock_client, SearchIndexPartition(testing::_, testing::_))
            .WillRepeatedly(
                [&](std::unique_ptr<coordinator::SearchIndexPartitionRequest>
                        request,
                    coordinator::SearchIndexPartitionCallback done) {
                  // For this test, we just have the remote nodes return
                  // nothing.
                  coordinator::SearchIndexPartitionResponse response;
                  done(grpc::Status::OK, response);
                });
      }
    }
  }
  const FTSearchTestCase &test_case = std::get<2>(params);
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(An<ValkeyModuleKey *>(),
                      VALKEYMODULE_HASH_CFIELDS | VALKEYMODULE_HASH_EXISTS,
                      An<const char *>(), An<int *>(), An<void *>()))
      .WillRepeatedly([&](ValkeyModuleKey *module_key, int flags,
                          const char *field, int *exists,
                          void *terminating_null) {
        *exists = 1;
        return VALKEYMODULE_OK;
      });
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(VectorExternalizer::Instance().GetCtx(),
                      An<ValkeyModuleString *>(), testing::_))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(&fake_ctx_, An<ValkeyModuleString *>(), testing::_))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  auto index_schema = CreateVectorHNSWSchema(index_name, &fake_ctx_).value();
  EXPECT_CALL(*index_schema, GetIdentifier(::testing::_))
      .Times(::testing::AnyNumber());
  auto vectors = DeterministicallyGenerateVectors(100, dimensions, 10.0);
  AddVectors(vectors);

  // Capture initial metrics before starting searches
  auto &stats = Metrics::GetStats();
  auto &mrmw_stats = vmsdk::GetGlobalTimeSlicedMRMWStats();
  uint64_t initial_queries = stats.time_slice_queries;
  uint64_t initial_read_periods = mrmw_stats.read_periods;
  uint64_t initial_read_time = mrmw_stats.read_time_microseconds;

  RE2 reply_regex(R"(\*3\r\n:1\r\n\+\d+\r\n\*2\r\n\+score\r\n\+.*\r\n)");
  uint64_t i = 0;
  for (auto &vector : vectors) {
    ++i;
    std::vector<ValkeyModuleString *> cmd_argv;
    std::transform(
        test_case.argv.begin(), test_case.argv.end(),
        std::back_inserter(cmd_argv), [&](std::string val) {
          if (val == "$index_name") {
            return ValkeyModule_CreateString(&fake_ctx_, index_name.data(),
                                             index_name.size());
          }
          if (val == "$embedding") {
            return ValkeyModule_CreateString(&fake_ctx_, (char *)vector.data(),
                                             vector.size() * sizeof(float));
          }
          return ValkeyModule_CreateString(&fake_ctx_, val.data(), val.size());
        });
    absl::Notification search_done;
    void *private_data_external = nullptr;
    if (use_thread_pool) {
      EXPECT_CALL(*kMockValkeyModule,
                  BlockClient(testing::_, testing::_, testing::_, testing::_,
                              testing::_))
          .WillOnce(testing::Return((ValkeyModuleBlockedClient *)i));
      EXPECT_CALL(*kMockValkeyModule,
                  UnblockClient((ValkeyModuleBlockedClient *)i, testing::_))
          .WillOnce([&](ValkeyModuleBlockedClient *client, void *private_data) {
            private_data_external = private_data;
            search_done.Notify();
            return VALKEYMODULE_OK;
          });
    }
    EXPECT_EQ(vmsdk::CreateCommand<FTSearchCmd>(&fake_ctx_, cmd_argv.data(),
                                                cmd_argv.size()),
              test_case.expected_run_return);
    if (use_thread_pool) {
      fake_ctx_.reply_capture.ClearReply();
      search_done.WaitForNotification();
      EXPECT_CALL(*kMockValkeyModule, GetBlockedClientPrivateData(&fake_ctx_))
          .WillRepeatedly(testing::InvokeWithoutArgs(
              [&] { return private_data_external; }));
      async::Reply(&fake_ctx_, nullptr, 0);
      async::Free(&fake_ctx_, private_data_external);
    }
    // std::cout << "reply: " << fake_ctx_.reply_capture.GetReply() << "\n";
    //  EXPECT_TRUE(
    //      RE2::FullMatch(fake_ctx_.reply_capture.GetReply(), reply_regex));
    fake_ctx_.reply_capture.ClearReply();
    for (auto cmd_arg : cmd_argv) {
      TestValkeyModule_FreeString(&fake_ctx_, cmd_arg);
    }
  }

  // Verify that metrics were incremented correctly
  EXPECT_EQ(stats.time_slice_queries, initial_queries + vectors.size());
  EXPECT_GT(mrmw_stats.read_periods, initial_read_periods);
  EXPECT_GT(mrmw_stats.read_time_microseconds, initial_read_time);
}

INSTANTIATE_TEST_SUITE_P(
    FTSearchTests, FTSearchTest,
    testing::Combine(testing::Bool(), testing::Bool(),
                     ValuesIn<FTSearchTestCase>({
                         {
                             .test_name = "happy_path",
                             .argv =
                                 {
                                     "FT.SEARCH",
                                     "my_index",
                                     "*=>[KNN 1 @vector $query_vector "
                                     "EF_RUNTIME 100 AS score]",
                                     "params",
                                     "2",
                                     "query_vector",
                                     "$embedding",
                                     "DIALECT",
                                     "2",
                                 },
                             .expected_run_return = VALKEYMODULE_OK,
                         },
                     })),
    [](const TestParamInfo<::testing::tuple<bool, bool, FTSearchTestCase>>
           &info) {
      return std::get<2>(info.param).test_name + "_" +
             (std::get<1>(info.param) ? "WithThreadPool"
                                      : "WithoutThreadPool") +
             "_" + (std::get<0>(info.param) ? "WithFanout" : "WithoutFanout");
    });

// Struct to hold parameters for max limit tests
struct MaxLimitTestCase {
  std::string test_name;
  std::function<absl::Status()> set_limit_func;
  std::function<absl::Status()> reset_limit_func;
  std::vector<std::string> valid_argv;
  std::vector<std::string> exceed_argv;
  std::string expected_error_message;
};

class FTSearchMaxLimitTest
    : public ValkeySearchTestWithParam<MaxLimitTestCase> {
 public:
  void AddVectors(const std::vector<std::vector<float>> &vectors) {
    auto index_schema =
        SchemaManager::Instance().GetIndexSchema(db_num, index_name);
    VMSDK_EXPECT_OK(index_schema);
    auto index = index_schema.value()->GetIndex("vector");
    VMSDK_EXPECT_OK(index);
    for (size_t i = 0; i < vectors.size(); ++i) {
      auto key = std::to_string(i);
      std::string vector = std::string((char *)vectors[i].data(),
                                       vectors[i].size() * sizeof(float));
      auto interned_key = StringInternStore::Intern(key);

      VMSDK_EXPECT_OK(index.value()->AddRecord(interned_key, vector));
    }
  }

 protected:
  std::shared_ptr<IndexSchema> index_schema_;
  std::vector<std::vector<float>> vectors_;
  const std::string index_name = "my_index";
  int dimensions = 100;
  int db_num = 0;
};

TEST_P(FTSearchMaxLimitTest, FTSearchMaxLimitTest) {
  const MaxLimitTestCase &test_case = GetParam();

  // Set the limit to the specified value for this test
  VMSDK_EXPECT_OK(test_case.set_limit_func());

  auto index_schema = CreateVectorHNSWSchema(index_name, &fake_ctx_).value();
  auto vectors = DeterministicallyGenerateVectors(1, dimensions, 10.0);
  AddVectors(vectors);
  uint64_t i = 0;
  for (auto &vector : vectors) {
    ++i;
    std::vector<ValkeyModuleString *> cmd_argv;
    std::transform(
        test_case.exceed_argv.begin(), test_case.exceed_argv.end(),
        std::back_inserter(cmd_argv), [&](std::string val) {
          if (val == "$index_name") {
            return ValkeyModule_CreateString(&fake_ctx_, index_name.data(),
                                             index_name.size());
          }
          if (val == "$embedding") {
            return ValkeyModule_CreateString(&fake_ctx_, (char *)vector.data(),
                                             vector.size() * sizeof(float));
          }
          return ValkeyModule_CreateString(&fake_ctx_, val.data(), val.size());
        });
    EXPECT_EQ(vmsdk::CreateCommand<FTSearchCmd>(&fake_ctx_, cmd_argv.data(),
                                                cmd_argv.size()),
              0);

    EXPECT_EQ(fake_ctx_.reply_capture.GetReply(),
              test_case.expected_error_message);

    VMSDK_EXPECT_OK(test_case.reset_limit_func());

    for (auto cmd_arg : cmd_argv) {
      TestValkeyModule_FreeString(&fake_ctx_, cmd_arg);
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
    FTSearchMaxLimitTests, FTSearchMaxLimitTest,
    ValuesIn<MaxLimitTestCase>({
        {
            .test_name = "MaxKnnLimit",
            .set_limit_func = []() { return options::GetMaxKnn().SetValue(5); },
            .reset_limit_func =
                []() { return options::GetMaxKnn().SetValue(1000); },
            .valid_argv = {"FT.SEARCH", "my_index",
                           "*=>[KNN 5 @vector $query_vector AS score]",
                           "params", "2", "query_vector", "$embedding",
                           "DIALECT", "2"},
            .exceed_argv = {"FT.SEARCH", "my_index",
                            "*=>[KNN 6 @vector $query_vector AS score]",
                            "params", "2", "query_vector", "$embedding",
                            "DIALECT", "2"},
            .expected_error_message =
                "-Invalid range: Value above maximum; KNN parameter "
                "must be a positive integer greater than 0 and cannot exceed "
                "5.\r\n",
        },
        {
            .test_name = "MaxEfRuntimeLimit",
            .set_limit_func =
                []() { return options::GetMaxEfRuntime().SetValue(5); },
            .reset_limit_func =
                []() { return options::GetMaxEfRuntime().SetValue(100); },
            .valid_argv =
                {"FT.SEARCH", "my_index",
                 "*=>[KNN 3 @vector $query_vector EF_RUNTIME 5 AS score]",
                 "params", "2", "query_vector", "$embedding", "DIALECT", "2"},
            .exceed_argv =
                {"FT.SEARCH", "my_index",
                 "*=>[KNN 3 @vector $query_vector EF_RUNTIME 6 AS score]",
                 "params", "2", "query_vector", "$embedding", "DIALECT", "2"},
            .expected_error_message =
                "-Invalid range: Value above maximum; `EF_RUNTIME` must "
                "be a positive integer greater than 0 and cannot exceed 5.\r\n",
        },
    }),
    [](const TestParamInfo<MaxLimitTestCase> &info) {
      return info.param.test_name;
    });

}  // namespace

}  // namespace valkey_search
