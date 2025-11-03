/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/fanout.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "google/protobuf/util/message_differencer.h"
#include "grpcpp/support/status.h"
#include "gtest/gtest.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/coordinator/search_converter.h"
#include "src/coordinator/util.h"
#include "src/index_schema.pb.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/valkey_search.h"
#include "testing/common.h"
#include "testing/coordinator/common.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace query {

using ::testing::ElementsAreArray;
using ::testing::Invoke;
using ::testing::Return;

struct FanoutTestTarget {
  fanout::FanoutSearchTarget target;
  absl::StatusOr<std::vector<NeighborTest>> neighbors;
};

struct FanoutTestParams {
  std::string test_name;
  std::vector<FanoutTestTarget> targets;
  std::string parameters_pbtxt;
  absl::StatusOr<std::vector<NeighborTest>> expected_neighbors;
};

class FanoutTest : public ValkeySearchTestWithParam<FanoutTestParams> {};

FanoutTestParams GenerateMultiTargetTestParams(std::string test_name,
                                               int target_count) {
  FanoutTestParams result;
  for (int i = 1; i <= target_count; ++i) {
    result.targets.push_back(FanoutTestTarget{
        .target =
            {
                .type = fanout::FanoutSearchTarget::Type::kRemote,
                .address = absl::StrCat("127.0.0.1:", i + 1234),
            },
        .neighbors = {{
            {
                .external_id = std::to_string(i),
                .distance = i / 10.0f,
                .attribute_contents = {{{absl::StrCat("attr", i),
                                         absl::StrCat("content", i)}}},
            },
        }},
    });
  }
  result.test_name = test_name;
  result.parameters_pbtxt = R"(
                index_schema_name: "test_index"
                attribute_alias: "attribute"
                score_as: "__vector_score"
                k: 3
                limit: {
                  first_index: 0
                  number: 3
                }
                no_content: false
            )";
  result.expected_neighbors = {{
      {
          .external_id = "3",
          .distance = 0.3f,
          .attribute_contents = {{{"attr3", "content3"}}},
      },
      {
          .external_id = "2",
          .distance = 0.2f,
          .attribute_contents = {{{"attr2", "content2"}}},
      },
      {
          .external_id = "1",
          .distance = 0.1f,
          .attribute_contents = {{{"attr1", "content1"}}},
      },
  }};
  return result;
}

FanoutTestParams GenerateMultiTargetTestParamsWithLimit(
    std::string test_name, int target_count, int neighbors_per_target, int k,
    int limit_first, int limit_number) {
  FanoutTestParams result;
  for (int i = 1; i <= target_count; ++i) {
    std::vector<NeighborTest> neighbors;

    // Note that this is the worst case for LIMIT, i.e. the smallest distance
    // entries are spread evenly across the targets.
    for (int j = 0; j < neighbors_per_target; ++j) {
      int id = i + j * target_count;
      neighbors.push_back({
          .external_id = std::to_string(id),
          .distance = id / 10.0f,
          .attribute_contents = {{{absl::StrCat("attr", id),
                                   absl::StrCat("content", id)}}},
      });
    }
    result.targets.push_back(FanoutTestTarget{
        .target =
            {
                .type = fanout::FanoutSearchTarget::Type::kRemote,
                .address = absl::StrCat("127.0.0.1:", i + 1234),
            },
        .neighbors = std::move(neighbors),
    });
  }
  result.test_name = test_name;
  result.parameters_pbtxt = absl::Substitute(R"(
                index_schema_name: "test_index"
                attribute_alias: "attribute"
                score_as: "__vector_score"
                k: $0
                limit: {
                  first_index: $1
                  number: $2
                }
                no_content: false
            )",
                                             k, limit_first, limit_number);

  int end_number = std::min({neighbors_per_target, k});
  result.expected_neighbors = std::vector<NeighborTest>();
  for (int i = 1; i <= target_count; ++i) {
    for (int j = 0; j < end_number; ++j) {
      int id = i + j * target_count;
      result.expected_neighbors.value().push_back({
          .external_id = std::to_string(id),
          .distance = id / 10.0f,
          .attribute_contents = {{{absl::StrCat("attr", id),
                                   absl::StrCat("content", id)}}},
      });
    }
  }

  // The upper bound of the final fanout result is k (while this limit k also
  // applied to each local query).
  std::sort(
      result.expected_neighbors.value().begin(),
      result.expected_neighbors.value().end(),
      [](const auto &a, const auto &b) { return a.distance < b.distance; });
  if (k < result.expected_neighbors.value().size()) {
    result.expected_neighbors.value().resize(k);
  }

  // The fanout results are sorted by distance descending, so we need to sort
  // the expected results to match.
  std::sort(
      result.expected_neighbors.value().begin(),
      result.expected_neighbors.value().end(),
      [](const auto &a, const auto &b) { return a.distance > b.distance; });

  return result;
}

INSTANTIATE_TEST_SUITE_P(
    FanoutTests, FanoutTest,
    testing::ValuesIn<FanoutTestParams>({
        {
            .test_name = "SingleFanoutTarget",
            .targets =
                {
                    {
                        .target =
                            {
                                .type =
                                    fanout::FanoutSearchTarget::Type::kRemote,
                                .address = "127.0.0.1:1234",
                            },
                        .neighbors = {{
                            {
                                .external_id = "1",
                                .distance = 0.1,
                                .attribute_contents = {{{"attr1", "content1"}}},
                            },
                        }},
                    },
                },
            .parameters_pbtxt = R"(
                index_schema_name: "test_index"
                attribute_alias: "attribute"
                score_as: "__vector_score"
                k: 1
                limit: {
                  first_index: 0
                  number: 1
                }
                no_content: false
            )",
            .expected_neighbors =
                {
                    {
                        {
                            .external_id = "1",
                            .distance = 0.1,
                            .attribute_contents = {{{"attr1", "content1"}}},
                        },
                    },
                },
        },
        {
            .test_name = "WithLocalFanoutTarget",
            .targets =
                {
                    {
                        .target =
                            {
                                .type =
                                    fanout::FanoutSearchTarget::Type::kRemote,
                                .address = "127.0.0.1:1234",
                            },
                        .neighbors = {{{
                            .external_id = "1",
                            .distance = 0.1,
                            .attribute_contents = {{{"attr1", "content1"}}},
                        }}},
                    },
                    {
                        .target =
                            {
                                .type =
                                    fanout::FanoutSearchTarget::Type::kLocal,
                            },
                        .neighbors = {{
                            {
                                .external_id = "1",
                                .distance = 0.1,
                                .attribute_contents = {{{"attr1", "content1"}}},
                            },
                        }},
                    },
                },
            .parameters_pbtxt = R"(
                index_schema_name: "test_index"
                attribute_alias: "attribute"
                score_as: "__vector_score"
                k: 1
                limit: {
                  first_index: 0
                  number: 1
                }
                no_content: false
            )",
            .expected_neighbors = {{{
                .external_id = "1",
                .distance = 0.1,
                .attribute_contents = {{{"attr1", "content1"}}},
            }}},
        },
        {
            .test_name = "WithPredicate",
            .targets =
                {
                    {
                        .target =
                            {
                                .type =
                                    fanout::FanoutSearchTarget::Type::kRemote,
                                .address = "127.0.0.1:1234",
                            },
                        .neighbors = {{{
                            .external_id = "1",
                            .distance = 0.1,
                            .attribute_contents = {{{"attr1", "content1"}}},
                        }}},
                    },
                },
            .parameters_pbtxt = R"(
                index_schema_name: "test_index"
                attribute_alias: "attribute"
                score_as: "__vector_score"
                k: 1
                limit: {
                  first_index: 0
                  number: 1
                }
                no_content: false
                root_filter_predicate: {
                  and: {
                    children: [
                      {
                        negate: {
                          predicate: {
                            tag: {
                              attribute_alias: "tag_alias"
                              raw_tag_string: "content1"
                            }
                          }
                        }
                      },
                      {
                        or: {
                          children: [
                            {
                              tag: {
                                attribute_alias: "tag_alias"
                                raw_tag_string: "content2,content3"
                              }
                            },
                            {
                              numeric: {
                                attribute_alias: "numeric_alias"
                                start: 0.1
                                is_inclusive_start: true
                                end: 0.2
                                is_inclusive_end: true
                              }
                            }
                          ]
                        }
                      }
                    ]
                  }
                }
            )",
            .expected_neighbors = {{{
                .external_id = "1",
                .distance = 0.1,
                .attribute_contents = {{{"attr1", "content1"}}},
            }}},
        },
        GenerateMultiTargetTestParams("10TargetFanout", 10),
        GenerateMultiTargetTestParams("100TargetFanout", 100),
        GenerateMultiTargetTestParamsWithLimit("10TargetFanoutWithLimit_0_10",
                                               10, 100, 100, 0, 10),
        GenerateMultiTargetTestParamsWithLimit("10TargetFanoutWithLimit_10_10",
                                               10, 100, 100, 10, 10),
        GenerateMultiTargetTestParamsWithLimit("10TargetFanoutWithLimit_90_10",
                                               10, 100, 100, 90, 10),
        GenerateMultiTargetTestParamsWithLimit(
            "10TargetFanoutWithLimit_0_10_small_k", 10, 10, 3, 0, 10),
        GenerateMultiTargetTestParamsWithLimit(
            "10TargetFanoutWithLimit_0_10_small_entries", 10, 1, 3, 0, 10),
    }),
    [](const testing::TestParamInfo<FanoutTestParams> &info) {
      return info.param.test_name;
    });

TEST_P(FanoutTest, TestFanout) {
  auto &params = GetParam();

  coordinator::SearchIndexPartitionRequest search_parameters;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      params.parameters_pbtxt, &search_parameters));
  std::vector<fanout::FanoutSearchTarget> targets;
  targets.reserve(params.targets.size());
  for (const auto &target : params.targets) {
    targets.push_back(target.target);
  }

  auto schema = CreateVectorHNSWSchema("test_index", &fake_ctx_);
  VMSDK_EXPECT_OK(schema);
  EXPECT_CALL(**schema, GetIdentifier(::testing::_))
      .Times(::testing::AnyNumber());

  data_model::TagIndex tag_index;
  tag_index.set_separator(",");
  tag_index.set_case_sensitive(false);
  VMSDK_EXPECT_OK(schema.value()->AddIndex(
      "tag_alias", "tag_id", std::make_shared<indexes::Tag>(tag_index)));

  data_model::NumericIndex numeric_index;
  VMSDK_EXPECT_OK(schema.value()->AddIndex(
      "numeric_alias", "numeric_id",
      std::make_shared<indexes::Numeric>(numeric_index)));

  InitThreadPools(5, 0);
  auto mock_coordinator_client_pool =
      std::make_unique<coordinator::MockClientPool>();
  absl::flat_hash_map<std::string, std::shared_ptr<coordinator::MockClient>>
      mock_coordinator_clients;

  for (const auto &target : params.targets) {
    auto mock_client = std::make_shared<coordinator::MockClient>();
    mock_coordinator_clients[target.target.address] = mock_client;
    if (target.target.type == fanout::FanoutSearchTarget::Type::kRemote) {
      EXPECT_CALL(*mock_coordinator_client_pool,
                  GetClient(target.target.address))
          .WillOnce(Return(mock_client));
      EXPECT_CALL(*mock_client, SearchIndexPartition(testing::_, testing::_))
          .WillOnce(Invoke(
              [target, search_parameters](auto request, auto callback) mutable {
                search_parameters.mutable_limit()->set_first_index(
                    request->limit().first_index());
                search_parameters.mutable_limit()->set_number(
                    request->limit().number());
                EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
                    *request, search_parameters));
                EXPECT_EQ(request->limit().first_index(), 0);
                EXPECT_EQ(request->limit().number(), search_parameters.k());
                coordinator::SearchIndexPartitionResponse response;
                if (!target.neighbors.ok()) {
                  callback(ToGrpcStatus(target.neighbors.status()), response);
                  return;
                }
                for (const auto &neighbor : *target.neighbors) {
                  auto *response_neighbor = response.add_neighbors();
                  response_neighbor->set_key(neighbor.external_id);
                  response_neighbor->set_score(neighbor.distance);
                  if (!neighbor.attribute_contents.has_value()) {
                    continue;
                  }
                  for (const auto &[identifier, record] :
                       neighbor.attribute_contents.value()) {
                    auto *attribute_content =
                        response_neighbor->add_attribute_contents();
                    attribute_content->set_identifier(identifier);
                    attribute_content->set_content(record);
                  }
                }
                callback(grpc::Status::OK, response);
              }));
    }
  }

  auto callback = [params, search_parameters](auto &neighbors,
                                              auto parameters) {
    EXPECT_EQ(neighbors.ok(), params.expected_neighbors.ok());
    if (neighbors.ok()) {
      std::vector<NeighborTest> neighbors_test;
      for (const auto &neighbor : *neighbors) {
        neighbors_test.push_back(ToNeighborTest(neighbor));
      }
      EXPECT_THAT(neighbors_test,
                  ElementsAreArray(params.expected_neighbors.value()));
    }
    auto grpc_request = coordinator::ParametersToGRPCSearchRequest(*parameters);
    EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
        *grpc_request, search_parameters));
  };
  auto parameters = GRPCSearchRequestToParameters(search_parameters, nullptr);
  VMSDK_EXPECT_OK(parameters);
  VMSDK_EXPECT_OK(fanout::PerformSearchFanoutAsync(
      &fake_ctx_, targets, mock_coordinator_client_pool.get(),
      std::move(*parameters), ValkeySearch::Instance().GetReaderThreadPool(),
      std::move(callback)));
  ValkeySearch::Instance().GetReaderThreadPool()->JoinWorkers();
}
struct GetTargetsTestNode {
  std::string node_id;
  std::string ip;
  int port;
  bool myself;
  bool pfail;
  std::optional<std::string> master_id;
  bool fail_to_get_node_info;
};

struct GetTargetsTestParam {
  std::string test_name;
  std::vector<GetTargetsTestNode> nodes;
  std::vector<std::vector<fanout::FanoutSearchTarget>>
      possible_expected_targets;
};

class GetTargetsTest : public ValkeySearchTestWithParam<GetTargetsTestParam> {};

std::string GetNodeId(int i) {
  return std::string(VALKEYMODULE_NODE_ID_LEN, 'a' + i);
}

INSTANTIATE_TEST_SUITE_P(
    GetTargetsTests, GetTargetsTest,
    testing::ValuesIn<GetTargetsTestParam>({
        {
            .test_name = "JustMyself",
            .nodes =
                {
                    {
                        .node_id = GetNodeId(0),
                        .ip = "127.0.0.1",
                        .port = 1234,
                        .myself = true,
                        .pfail = false,
                        .master_id = std::nullopt,
                    },
                },
            .possible_expected_targets = {{
                {
                    .type = fanout::FanoutSearchTarget::Type::kLocal,
                },
            }},
        },
        {
            .test_name = "ShardWithMyselfAndAnother",
            .nodes =
                {
                    {
                        .node_id = GetNodeId(0),
                        .ip = "127.0.0.1",
                        .port = 1234,
                        .myself = true,
                        .pfail = false,
                        .master_id = std::nullopt,
                    },
                    {
                        .node_id = GetNodeId(1),
                        .ip = "127.0.0.1",
                        .port = 1235,
                        .myself = false,
                        .pfail = false,
                        .master_id = GetNodeId(0),
                    },
                },
            .possible_expected_targets =
                {
                    {
                        {
                            .type = fanout::FanoutSearchTarget::Type::kLocal,
                        },
                        {
                            .type = fanout::FanoutSearchTarget::Type::kRemote,
                            .address = absl::StrCat(
                                "127.0.0.1:",
                                coordinator::GetCoordinatorPort(1235)),
                        },
                    },
                },
        },
        {
            .test_name = "ManyShards",
            .nodes =
                {
                    {
                        .node_id = GetNodeId(0),
                        .ip = "127.0.0.1",
                        .port = 1234,
                        .myself = true,
                        .pfail = false,
                        .master_id = std::nullopt,
                    },
                    {
                        .node_id = GetNodeId(1),
                        .ip = "127.0.0.1",
                        .port = 1235,
                        .myself = false,
                        .pfail = false,
                        .master_id = GetNodeId(0),
                    },
                    {
                        .node_id = GetNodeId(2),
                        .ip = "127.0.0.1",
                        .port = 1236,
                        .myself = false,
                        .pfail = false,
                        .master_id = std::nullopt,
                    },
                    {
                        .node_id = GetNodeId(3),
                        .ip = "127.0.0.1",
                        .port = 1237,
                        .myself = false,
                        .pfail = false,
                        .master_id = GetNodeId(2),
                    },
                    {
                        .node_id = GetNodeId(4),
                        .ip = "127.0.0.1",
                        .port = 1238,
                        .myself = false,
                        .pfail = false,
                        .master_id = std::nullopt,
                    },
                    {
                        .node_id = GetNodeId(5),
                        .ip = "127.0.0.1",
                        .port = 1239,
                        .myself = false,
                        .pfail = false,
                        .master_id = GetNodeId(4),
                    },
                },
            .possible_expected_targets =
                {
                    {
                        {
                            .type = fanout::FanoutSearchTarget::Type::kLocal,
                        },
                        {
                            .type = fanout::FanoutSearchTarget::Type::kRemote,
                            .address = absl::StrCat(
                                "127.0.0.1:",
                                coordinator::GetCoordinatorPort(1235)),
                        },
                    },
                    {
                        {
                            .type = fanout::FanoutSearchTarget::Type::kRemote,
                            .address = absl::StrCat(
                                "127.0.0.1:",
                                coordinator::GetCoordinatorPort(1236)),
                        },
                        {
                            .type = fanout::FanoutSearchTarget::Type::kRemote,
                            .address = absl::StrCat(
                                "127.0.0.1:",
                                coordinator::GetCoordinatorPort(1237)),
                        },
                    },
                    {
                        {
                            .type = fanout::FanoutSearchTarget::Type::kRemote,
                            .address = absl::StrCat(
                                "127.0.0.1:",
                                coordinator::GetCoordinatorPort(1238)),
                        },
                        {
                            .type = fanout::FanoutSearchTarget::Type::kRemote,
                            .address = absl::StrCat(
                                "127.0.0.1:",
                                coordinator::GetCoordinatorPort(1239)),
                        },
                    },
                },
        },
        {
            .test_name = "ShardWithFailingNode",
            .nodes =
                {
                    {
                        .node_id = GetNodeId(0),
                        .ip = "127.0.0.1",
                        .port = 1234,
                        .myself = false,
                        .pfail = true,
                        .master_id = std::nullopt,
                    },
                    {
                        .node_id = GetNodeId(1),
                        .ip = "127.0.0.1",
                        .port = 1235,
                        .myself = false,
                        .pfail = false,
                        .master_id = GetNodeId(0),
                    },
                },
            .possible_expected_targets =
                {
                    {
                        {
                            .type = fanout::FanoutSearchTarget::Type::kRemote,
                            .address = absl::StrCat(
                                "127.0.0.1:",
                                coordinator::GetCoordinatorPort(1235)),
                        },
                    },
                },
        },
        {
            .test_name = "EntireShardFailing",
            .nodes =
                {
                    {
                        .node_id = GetNodeId(0),
                        .ip = "127.0.0.1",
                        .port = 1234,
                        .myself = true,
                        .pfail = false,
                        .master_id = std::nullopt,
                    },
                    {
                        .node_id = GetNodeId(1),
                        .ip = "127.0.0.1",
                        .port = 1235,
                        .myself = false,
                        .pfail = false,
                        .master_id = GetNodeId(0),
                    },
                    {
                        .node_id = GetNodeId(2),
                        .ip = "127.0.0.1",
                        .port = 1236,
                        .myself = false,
                        .pfail = true,
                        .master_id = std::nullopt,
                    },
                    {
                        .node_id = GetNodeId(3),
                        .ip = "127.0.0.1",
                        .port = 1237,
                        .myself = false,
                        .pfail = true,
                        .master_id = GetNodeId(2),
                    },
                },
            .possible_expected_targets =
                {
                    {
                        {
                            .type = fanout::FanoutSearchTarget::Type::kLocal,
                        },
                        {
                            .type = fanout::FanoutSearchTarget::Type::kRemote,
                            .address = absl::StrCat(
                                "127.0.0.1:",
                                coordinator::GetCoordinatorPort(1235)),
                        },
                    },
                },
        },
        {
            .test_name = "EntireClusterFailing",
            .nodes =
                {
                    {
                        .node_id = GetNodeId(0),
                        .ip = "127.0.0.1",
                        .port = 1234,
                        .myself = false,
                        .pfail = true,
                        .master_id = std::nullopt,
                    },
                    {
                        .node_id = GetNodeId(1),
                        .ip = "127.0.0.1",
                        .port = 1235,
                        .myself = false,
                        .pfail = true,
                        .master_id = GetNodeId(0),
                    },
                    {
                        .node_id = GetNodeId(2),
                        .ip = "127.0.0.1",
                        .port = 1236,
                        .myself = false,
                        .pfail = true,
                        .master_id = std::nullopt,
                    },
                    {
                        .node_id = GetNodeId(3),
                        .ip = "127.0.0.1",
                        .port = 1237,
                        .myself = false,
                        .pfail = true,
                        .master_id = GetNodeId(2),
                    },
                },
            .possible_expected_targets = {},
        },
        {
            .test_name = "FailToGetNodeInfo",
            .nodes =
                {
                    {
                        .node_id = GetNodeId(0),
                        .ip = "127.0.0.1",
                        .port = 1234,
                        .myself = false,
                        .pfail = false,
                        .master_id = std::nullopt,
                        .fail_to_get_node_info = true,
                    },
                    {
                        .node_id = GetNodeId(1),
                        .ip = "127.0.0.1",
                        .port = 1235,
                        .myself = false,
                        .pfail = false,
                        .master_id = GetNodeId(0),
                    },
                },
            .possible_expected_targets =
                {
                    {
                        {
                            .type = fanout::FanoutSearchTarget::Type::kRemote,
                            .address = absl::StrCat(
                                "127.0.0.1:",
                                coordinator::GetCoordinatorPort(1235)),
                        },
                    },
                },
        },
    }),
    [](const testing::TestParamInfo<GetTargetsTestParam> &info) {
      return info.param.test_name;
    });

char **GenerateClusterNodesList(std::vector<GetTargetsTestNode> node_ids) {
  char **res = new char *[node_ids.size()];
  for (size_t i = 0; i < node_ids.size(); ++i) {
    res[i] = new char[VALKEYMODULE_NODE_ID_LEN];
    memcpy(res[i], node_ids[i].node_id.c_str(), VALKEYMODULE_NODE_ID_LEN);
  }
  return res;
}

void FreeClusterNodesList(char **ids, size_t num_nodes) {
  for (size_t i = 0; i < num_nodes; ++i) {
    delete[] ids[i];
  }
  delete[] ids;
}

TEST_P(GetTargetsTest, TestGetTargets) {
  auto params = GetParam();
  EXPECT_CALL(*kMockValkeyModule,
              GetClusterNodesList(testing::_, testing::An<size_t *>()))
      .WillRepeatedly([params](ValkeyModuleCtx *ctx, size_t *numnodes) {
        *numnodes = params.nodes.size();
        return GenerateClusterNodesList(params.nodes);
      });
  EXPECT_CALL(*kMockValkeyModule, FreeClusterNodesList(testing::_))
      .WillRepeatedly([params](char **ids) {
        FreeClusterNodesList(ids, params.nodes.size());
      });
  for (auto &node : params.nodes) {
    EXPECT_CALL(
        *kMockValkeyModule,
        GetClusterNodeInfo(testing::_, testing::StrEq(node.node_id), testing::_,
                           testing::_, testing::_, testing::_))
        .WillRepeatedly([node](ValkeyModuleCtx *ctx, const char *node_id,
                               char *ip, char *master_id, int *port,
                               int *flags) {
          if (node.fail_to_get_node_info) {
            return VALKEYMODULE_ERR;
          }
          if (ip != nullptr) {
            // Note this is intentionally not null terminated.
            memcpy(ip, node.ip.c_str(), node.ip.size());
          }
          if (port != nullptr) {
            *port = node.port;
          }
          if (flags != nullptr) {
            *flags = 0;
            if (node.myself) {
              *flags |= VALKEYMODULE_NODE_MYSELF;
            }
            if (node.pfail) {
              *flags |= VALKEYMODULE_NODE_PFAIL;
            }
            if (node.master_id.has_value()) {
              *flags |= VALKEYMODULE_NODE_SLAVE;
              memcpy(master_id, node.master_id->c_str(),
                     node.master_id->size());
            } else {
              *flags |= VALKEYMODULE_NODE_MASTER;
            }
          }
          return VALKEYMODULE_OK;
        });
  }

  // Each element must match at least one of the possible expected targets
  // lists.
  std::vector<testing::Matcher<const fanout::FanoutSearchTarget &>>
      target_matchers;
  target_matchers.reserve(params.possible_expected_targets.size());
  for (const auto &possible_target_list : params.possible_expected_targets) {
    target_matchers.push_back(testing::AnyOfArray(possible_target_list));
  }
  EXPECT_THAT(fanout::GetSearchTargetsForFanout(
                  &fake_ctx_, fanout::FanoutTargetMode::kRandom),
              UnorderedElementsAreArray(target_matchers));
}
}  // namespace query
}  // namespace valkey_search
