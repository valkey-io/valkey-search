/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/cluster_map.h"

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
namespace cluster_map {

namespace {

// Helper structure to define a node (primary or replica)
struct NodeConfig {
  std::string primary_endpoint;
  int port;
  std::string node_id;  // 40-character hex string
  std::vector<std::string> additional_network_metadata;
};

struct NodeConfigWithMap {
  std::string primary_endpoint;
  int port;
  std::string node_id;  // 40-character hex string
  std::unordered_map<std::string, std::string> additional_network_metadata;
};

// Helper structure to define a slot range with its nodes
struct SlotRangeConfig {
  int start_slot;
  int end_slot;
  std::optional<NodeConfig> primary;
  std::vector<NodeConfig> replicas;
};

struct SlotRangeConfigWithMap {
  int start_slot;
  int end_slot;
  std::optional<NodeConfigWithMap> primary;
  std::vector<NodeConfigWithMap> replicas;
};

class ClusterMapTest : public vmsdk::ValkeyTest {
 protected:
  ValkeyModuleCtx fake_ctx;

  // Pre-generated 40-character node IDs for testing
  const std::vector<std::string> primary_ids = {
      "c9d93d9f2c0c524ff34cc11838c2003d8c29e013",
      "d4e5f6789012345678901234567890abcda1b2c3",
      "f6789012345678901234567890abcda1b2c3d4e5",
      "a1b2c3d4e5f67890123456789abcdef012345678",
      "b2c3d4e5f67890123456789abcdef0123456789a",
      "c3d4e5f67890123456789abcdef0123456789ab1",
      "d4e5f67890123456789abcdef0123456789ab1c2",
      "e5f67890123456789abcdef0123456789ab1c2d3",
      "f67890123456789abcdef0123456789ab1c2d3e4",
      "67890123456789abcdef0123456789ab1c2d3e4f5"};

  const std::vector<std::string> replica_ids = {
      "a1b2c3d4e5f6789012345678901234567890abcd",
      "e5f6789012345678901234567890abcda1b2c3d4",
      "1234567890abcdef1234567890abcdef12345678",
      "234567890abcdef1234567890abcdef123456789a",
      "34567890abcdef1234567890abcdef123456789ab",
      "4567890abcdef1234567890abcdef123456789abc",
      "567890abcdef1234567890abcdef123456789abcd",
      "67890abcdef1234567890abcdef123456789abcde",
      "7890abcdef1234567890abcdef123456789abcdef",
      "890abcdef1234567890abcdef123456789abcdef1"};

  // Helper: Create a node array [primary_endpoint, port, node_id]
  CallReplyArray CreateNodeArray(const NodeConfig& node) {
    CallReplyArray node_array;
    node_array.push_back(
        CreateValkeyModuleCallReply(CallReplyString(node.primary_endpoint)));
    node_array.push_back(
        CreateValkeyModuleCallReply(CallReplyInteger(node.port)));
    node_array.push_back(
        CreateValkeyModuleCallReply(CallReplyString(node.node_id)));
    // Add the 4th element: additional_network_metadata as an array
    CallReplyArray metadata_array;
    for (const auto& metadata : node.additional_network_metadata) {
      metadata_array.push_back(
          CreateValkeyModuleCallReply(CallReplyString(metadata)));
    }
    node_array.push_back(
        CreateValkeyModuleCallReply(std::move(metadata_array)));
    return node_array;
  }

  // Helper: Create a slot range array [start, end, primary, replica1, replica2,
  // ...]
  CallReplyArray CreateSlotRangeArray(const SlotRangeConfig& config) {
    CallReplyArray range;
    range.push_back(
        CreateValkeyModuleCallReply(CallReplyInteger(config.start_slot)));
    range.push_back(
        CreateValkeyModuleCallReply(CallReplyInteger(config.end_slot)));

    if (config.primary.has_value()) {
      range.push_back(
          CreateValkeyModuleCallReply(CreateNodeArray(config.primary.value())));
    } else {
      range.push_back(CreateValkeyModuleCallReply(
          CreateNodeArray(CreateInvalidNodeConfig())));
    }

    for (const auto& replica : config.replicas) {
      range.push_back(CreateValkeyModuleCallReply(CreateNodeArray(replica)));
    }

    return range;
  }

  // Helper: Create complete CLUSTER SLOTS reply
  ValkeyModuleCallReply* CreateClusterSlotsReply(
      const std::vector<SlotRangeConfig>& slot_ranges) {
    CallReplyArray slots_array;
    for (const auto& range_config : slot_ranges) {
      slots_array.push_back(
          CreateValkeyModuleCallReply(CreateSlotRangeArray(range_config)));
    }
    auto reply = new ValkeyModuleCallReply();
    reply->type = VALKEYMODULE_REPLY_ARRAY;
    reply->val = std::move(slots_array);
    return reply;
  }

  // Helper: Setup all mock expectations for CLUSTER SLOTS call
  void SetupCallReplyMocks(ValkeyModuleCallReply* reply, size_t num_ranges) {
    EXPECT_CALL(*kMockValkeyModule, CallReplyType(reply))
        .WillRepeatedly(testing::Return(VALKEYMODULE_REPLY_ARRAY));

    EXPECT_CALL(*kMockValkeyModule, CallReplyLength(reply))
        .WillRepeatedly(testing::Return(num_ranges));

    EXPECT_CALL(*kMockValkeyModule,
                CallReplyArrayElement(testing::_, testing::_))
        .WillRepeatedly(&TestValkeyModule_CallReplyArrayElementImpl);

    EXPECT_CALL(*kMockValkeyModule, CallReplyType(testing::Ne(reply)))
        .WillRepeatedly(&TestValkeyModule_CallReplyTypeImpl);

    EXPECT_CALL(*kMockValkeyModule, CallReplyLength(testing::Ne(reply)))
        .WillRepeatedly([](ValkeyModuleCallReply* r) -> size_t {
          if (r && r->type == VALKEYMODULE_REPLY_ARRAY) {
            return std::get<CallReplyArray>(r->val).size();
          }
          return 0;
        });

    EXPECT_CALL(*kMockValkeyModule, CallReplyInteger(testing::_))
        .WillRepeatedly(&TestValkeyModule_CallReplyIntegerImpl);

    EXPECT_CALL(*kMockValkeyModule, CallReplyStringPtr(testing::_, testing::_))
        .WillRepeatedly(&TestValkeyModule_CallReplyStringPtrImpl);

    EXPECT_CALL(*kMockValkeyModule, CallReplyMapElement(testing::_, testing::_,
                                                        testing::_, testing::_))
        .WillRepeatedly(&TestValkeyModule_CallReplyMapElementImpl);
  }

  // Helper: Mock CLUSTER SLOTS command
  void MockClusterSlotsCall(const std::vector<SlotRangeConfig>& slot_ranges) {
    auto reply = CreateClusterSlotsReply(slot_ranges);

    EXPECT_CALL(*kMockValkeyModule,
                Call(&fake_ctx, testing::StrEq("CLUSTER"), testing::StrEq("c"),
                     testing::StrEq("SLOTS")))
        .WillOnce(testing::Return(reply));

    EXPECT_CALL(*kMockValkeyModule, FreeCallReply(reply))
        .WillOnce([](ValkeyModuleCallReply* r) { delete r; });

    SetupCallReplyMocks(reply, slot_ranges.size());
  }

  // Helper: Mock GetMyClusterID
  void MockGetMyClusterID(const std::string& my_node_id) {
    std::string padded_id = my_node_id;
    if (padded_id.size() < VALKEYMODULE_NODE_ID_LEN) {
      padded_id.resize(VALKEYMODULE_NODE_ID_LEN, '0');
    }
    static std::string stored_id;
    stored_id = padded_id;

    EXPECT_CALL(*kMockValkeyModule, GetMyClusterID())
        .WillRepeatedly(testing::Return(stored_id.c_str()));
  }

  // Helper: Setup cluster and create map
  std::shared_ptr<ClusterMap> CreateClusterMapWithConfig(
      const std::vector<SlotRangeConfig>& ranges,
      const std::string& local_node_id) {
    MockGetMyClusterID(local_node_id);
    MockClusterSlotsCall(ranges);
    return ClusterMap::CreateNewClusterMap(&fake_ctx);
  }

  // Helper: Create a node array with map metadata [primary_endpoint, port,
  // node_id, metadata_map]
  CallReplyArray CreateNodeArrayWithMap(const NodeConfigWithMap& node) {
    CallReplyArray node_array;
    node_array.push_back(
        CreateValkeyModuleCallReply(CallReplyString(node.primary_endpoint)));
    node_array.push_back(
        CreateValkeyModuleCallReply(CallReplyInteger(node.port)));
    node_array.push_back(
        CreateValkeyModuleCallReply(CallReplyString(node.node_id)));

    // Add the 4th element: additional_network_metadata as a map (RESP3)
    CallReplyMap metadata_map;
    for (const auto& [key, value] : node.additional_network_metadata) {
      metadata_map.push_back(
          std::make_pair(CreateValkeyModuleCallReply(CallReplyString(key)),
                         CreateValkeyModuleCallReply(CallReplyString(value))));
    }
    node_array.push_back(CreateValkeyModuleCallReply(std::move(metadata_map)));

    return node_array;
  }

  // Helper: Create a slot range array with map metadata
  CallReplyArray CreateSlotRangeArrayWithMap(
      const SlotRangeConfigWithMap& config) {
    CallReplyArray range;
    range.push_back(
        CreateValkeyModuleCallReply(CallReplyInteger(config.start_slot)));
    range.push_back(
        CreateValkeyModuleCallReply(CallReplyInteger(config.end_slot)));

    if (config.primary.has_value()) {
      range.push_back(CreateValkeyModuleCallReply(
          CreateNodeArrayWithMap(config.primary.value())));
    } else {
      range.push_back(CreateValkeyModuleCallReply(
          CreateNodeArrayWithMap(CreateInvalidNodeConfigWithMap())));
    }

    for (const auto& replica : config.replicas) {
      range.push_back(
          CreateValkeyModuleCallReply(CreateNodeArrayWithMap(replica)));
    }

    return range;
  }

  // Helper: Create complete CLUSTER SLOTS reply with map metadata
  ValkeyModuleCallReply* CreateClusterSlotsReplyWithMap(
      const std::vector<SlotRangeConfigWithMap>& slot_ranges) {
    CallReplyArray slots_array;
    for (const auto& range_config : slot_ranges) {
      slots_array.push_back(CreateValkeyModuleCallReply(
          CreateSlotRangeArrayWithMap(range_config)));
    }
    auto reply = new ValkeyModuleCallReply();
    reply->type = VALKEYMODULE_REPLY_ARRAY;
    reply->val = std::move(slots_array);
    return reply;
  }

  // Helper: Mock CLUSTER SLOTS command with map metadata
  void MockClusterSlotsCallWithMap(
      const std::vector<SlotRangeConfigWithMap>& slot_ranges) {
    auto reply = CreateClusterSlotsReplyWithMap(slot_ranges);

    EXPECT_CALL(*kMockValkeyModule,
                Call(&fake_ctx, testing::StrEq("CLUSTER"), testing::StrEq("c"),
                     testing::StrEq("SLOTS")))
        .WillOnce(testing::Return(reply));

    EXPECT_CALL(*kMockValkeyModule, FreeCallReply(reply))
        .WillOnce([](ValkeyModuleCallReply* r) { delete r; });

    SetupCallReplyMocks(reply, slot_ranges.size());
  }

  // Helper: Setup cluster with map metadata and create map
  std::shared_ptr<ClusterMap> CreateClusterMapWithMapConfig(
      const std::vector<SlotRangeConfigWithMap>& ranges,
      const std::string& local_node_id) {
    MockGetMyClusterID(local_node_id);
    MockClusterSlotsCallWithMap(ranges);
    return ClusterMap::CreateNewClusterMap(&fake_ctx);
  }

  // Helper: Create an invalid node config
  NodeConfig CreateInvalidNodeConfig() {
    return NodeConfig{"?", 30001, primary_ids.at(0), {}};
  }

  NodeConfigWithMap CreateInvalidNodeConfigWithMap() {
    return NodeConfigWithMap{"?", 30001, primary_ids.at(0), {}};
  }

  // Helper: Create standard 3-shard configuration
  std::vector<SlotRangeConfig> CreateStandard3ShardConfig() {
    return {
        {.start_slot = 0,
         .end_slot = 5460,
         .primary = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0), {}},
         .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0), {}}}},
        {.start_slot = 5461,
         .end_slot = 10922,
         .primary = NodeConfig{"127.0.0.1", 30002, primary_ids.at(1), {}},
         .replicas = {NodeConfig{"127.0.0.1", 30005, replica_ids.at(1), {}}}},
        {.start_slot = 10923,
         .end_slot = 16383,
         .primary = NodeConfig{"127.0.0.1", 30003, primary_ids.at(2), {}},
         .replicas = {NodeConfig{"127.0.0.1", 30006, replica_ids.at(2), {}}}}};
  }

  // Helper: Verify target list consistency
  void VerifyTargetListConsistency(const ClusterMap* cluster_map,
                                   size_t expected_primaries,
                                   size_t expected_replicas) {
    const auto& primary_targets =
        cluster_map->GetTargets(FanoutTargetMode::kPrimary);
    const auto& replica_targets =
        cluster_map->GetTargets(FanoutTargetMode::kReplicas);
    const auto& all_targets = cluster_map->GetTargets(FanoutTargetMode::kAll);

    EXPECT_EQ(primary_targets.size(), expected_primaries);
    EXPECT_EQ(replica_targets.size(), expected_replicas);
    EXPECT_EQ(all_targets.size(), expected_primaries + expected_replicas);

    for (const auto& target : primary_targets) {
      EXPECT_TRUE(target.is_primary);
      EXPECT_NE(target.shard, nullptr);
    }

    for (const auto& target : replica_targets) {
      EXPECT_FALSE(target.is_primary);
      EXPECT_NE(target.shard, nullptr);
    }
  }
};

// ============================================================================
// Basic Cluster Configuration Tests
// ============================================================================

TEST_F(ClusterMapTest, SingleShardFullCoverage) {
  SlotRangeConfig full_range{
      .start_slot = 0,
      .end_slot = 16383,
      .primary = NodeConfig{"127.0.0.1", 6379, primary_ids.at(0), {}},
      .replicas = {}};

  auto cluster_map =
      CreateClusterMapWithConfig({full_range}, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  EXPECT_TRUE(cluster_map->IsConsistent());
  EXPECT_TRUE(cluster_map->IOwnSlot(0));
  EXPECT_TRUE(cluster_map->IOwnSlot(16383));

  VerifyTargetListConsistency(cluster_map.get(), 1, 0);
}

TEST_F(ClusterMapTest, MultipleShards) {
  std::vector<SlotRangeConfig> ranges = {
      {.start_slot = 0,
       .end_slot = 5460,
       .primary = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0), {}}}},
      {.start_slot = 5461,
       .end_slot = 10922,
       .primary = NodeConfig{"127.0.0.1", 30002, primary_ids.at(1), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30005, replica_ids.at(1), {}}}},
      {.start_slot = 10923,
       .end_slot = 16383,
       .primary = NodeConfig{"127.0.0.1", 30003, primary_ids.at(2), {}},
       .replicas = {}}};

  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  EXPECT_TRUE(cluster_map->IsConsistent());
  EXPECT_TRUE(cluster_map->IOwnSlot(100));
  EXPECT_FALSE(cluster_map->IOwnSlot(10000));

  VerifyTargetListConsistency(cluster_map.get(), 3, 2);
}

TEST_F(ClusterMapTest, PartialCoverage) {
  std::vector<SlotRangeConfig> ranges = {
      {.start_slot = 0,
       .end_slot = 5000,
       .primary = NodeConfig{"127.0.0.1", 6379, primary_ids.at(0), {}},
       .replicas = {}},
      {.start_slot = 10000,  // Gap from 5001-9999
       .end_slot = 16383,
       .primary = NodeConfig{"127.0.0.1", 6380, primary_ids.at(1), {}},
       .replicas = {}}};

  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  EXPECT_FALSE(cluster_map->IsConsistent());

  VerifyTargetListConsistency(cluster_map.get(), 2, 0);
}

TEST_F(ClusterMapTest, EmptyClusterSlot) {
  auto cluster_map = CreateClusterMapWithConfig({}, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  EXPECT_FALSE(cluster_map->IsConsistent());
  EXPECT_EQ(cluster_map->GetShardBySlot(0), nullptr);
  EXPECT_FALSE(cluster_map->IOwnSlot(5000));

  VerifyTargetListConsistency(cluster_map.get(), 0, 0);
}

TEST_F(ClusterMapTest, AdditionalNetworkMetadata) {
  SlotRangeConfig full_range{
      .start_slot = 0,
      .end_slot = 16383,
      .primary = NodeConfig{"127.0.0.1",
                            6379,
                            primary_ids.at(0),
                            {"hostname", "test.valkey.io"}},
      .replicas = {}};

  auto cluster_map =
      CreateClusterMapWithConfig({full_range}, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  EXPECT_TRUE(cluster_map->IsConsistent());
  auto additional_network_metadata =
      cluster_map->GetTargets(FanoutTargetMode::kPrimary)
          .at(0)
          .additional_network_metadata;
  auto it = additional_network_metadata.find("hostname");
  EXPECT_TRUE(it != additional_network_metadata.end());
  EXPECT_EQ(it->second, "test.valkey.io");
}

TEST_F(ClusterMapTest, AdditionalNetworkMetadataWithMap) {
  std::unordered_map<std::string, std::string> metadata1 = {
      {"hostname", "test.valkey.io"}};
  SlotRangeConfigWithMap full_range{
      .start_slot = 0,
      .end_slot = 16383,
      .primary =
          NodeConfigWithMap{"127.0.0.1", 6379, primary_ids.at(0), metadata1},
      .replicas = {}};

  auto cluster_map =
      CreateClusterMapWithMapConfig({full_range}, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  EXPECT_TRUE(cluster_map->IsConsistent());
  auto additional_network_metadata =
      cluster_map->GetTargets(FanoutTargetMode::kPrimary)
          .at(0)
          .additional_network_metadata;
  auto it = additional_network_metadata.find("hostname");
  EXPECT_TRUE(it != additional_network_metadata.end());
  EXPECT_EQ(it->second, "test.valkey.io");
}

// ============================================================================
// Shard Lookup Tests
// ============================================================================

TEST_F(ClusterMapTest, GetShardBySlotTest) {
  auto ranges = CreateStandard3ShardConfig();
  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);

  EXPECT_EQ(cluster_map->GetShardBySlot(0)->shard_id, primary_ids.at(0));
  EXPECT_EQ(cluster_map->GetShardBySlot(5461)->shard_id, primary_ids.at(1));
  EXPECT_EQ(cluster_map->GetShardBySlot(10923)->shard_id, primary_ids.at(2));
  EXPECT_EQ(cluster_map->GetShardBySlot(16384), nullptr);  // Invalid slot
}

TEST_F(ClusterMapTest, GetShardByIdTest) {
  std::vector<SlotRangeConfig> ranges = {
      {.start_slot = 0,
       .end_slot = 5460,
       .primary = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0), {}}}},
      {.start_slot = 5461,
       .end_slot = 10922,
       .primary = NodeConfig{"127.0.0.1", 30002, primary_ids.at(1), {}},
       .replicas = {}}};

  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);

  const ShardInfo* shard = cluster_map->GetShardById(primary_ids.at(0));
  ASSERT_NE(shard, nullptr);
  EXPECT_EQ(shard->shard_id, primary_ids.at(0));
  EXPECT_EQ(shard->replicas.size(), 1);

  shard = cluster_map->GetShardById(primary_ids.at(1));
  ASSERT_NE(shard, nullptr);
  EXPECT_EQ(shard->shard_id, primary_ids.at(1));
  EXPECT_EQ(shard->replicas.size(), 0);

  EXPECT_EQ(cluster_map->GetShardById("nonexistent_id"), nullptr);
  EXPECT_EQ(cluster_map->GetShardById(""), nullptr);
}

TEST_F(ClusterMapTest, SlotInGapTest) {
  std::vector<SlotRangeConfig> ranges = {
      {.start_slot = 0,
       .end_slot = 5000,
       .primary = NodeConfig{"127.0.0.1", 6379, primary_ids.at(0), {}},
       .replicas = {}},
      {.start_slot = 10000,  // Gap from 5001-9999
       .end_slot = 16383,
       .primary = NodeConfig{"127.0.0.1", 6380, primary_ids.at(1), {}},
       .replicas = {}}};

  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  EXPECT_FALSE(cluster_map->IsConsistent());

  // Slots in ranges should work
  EXPECT_NE(cluster_map->GetShardBySlot(0), nullptr);
  EXPECT_NE(cluster_map->GetShardBySlot(5000), nullptr);
  EXPECT_NE(cluster_map->GetShardBySlot(10000), nullptr);
  EXPECT_NE(cluster_map->GetShardBySlot(16383), nullptr);

  // Slots in gap should return nullptr
  EXPECT_EQ(cluster_map->GetShardBySlot(5001), nullptr);
  EXPECT_EQ(cluster_map->GetShardBySlot(7500), nullptr);
  EXPECT_EQ(cluster_map->GetShardBySlot(9999), nullptr);
}

// ============================================================================
// Boundary and Edge Case Tests
// ============================================================================

TEST_F(ClusterMapTest, SlotBoundaryTest) {
  std::vector<SlotRangeConfig> ranges = {
      {.start_slot = 0,
       .end_slot = 8191,
       .primary = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0), {}},
       .replicas = {}},
      {.start_slot = 8192,
       .end_slot = 16383,
       .primary = NodeConfig{"127.0.0.1", 30002, primary_ids.at(1), {}},
       .replicas = {}}};

  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);

  EXPECT_TRUE(cluster_map->IOwnSlot(0));       // First slot
  EXPECT_TRUE(cluster_map->IOwnSlot(8191));    // Last slot of first range
  EXPECT_FALSE(cluster_map->IOwnSlot(8192));   // First slot of second range
  EXPECT_FALSE(cluster_map->IOwnSlot(16383));  // Last slot

  const ShardInfo* shard1 = cluster_map->GetShardBySlot(8191);
  const ShardInfo* shard2 = cluster_map->GetShardBySlot(8192);
  ASSERT_NE(shard1, nullptr);
  ASSERT_NE(shard2, nullptr);
  EXPECT_NE(shard1->shard_id, shard2->shard_id);
  EXPECT_EQ(shard1->shard_id, primary_ids.at(0));
  EXPECT_EQ(shard2->shard_id, primary_ids.at(1));
}

TEST_F(ClusterMapTest, SingleSlotRangeTest) {
  SlotRangeConfig single_slot{
      .start_slot = 100,
      .end_slot = 100,  // Single slot
      .primary = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0), {}},
      .replicas = {}};

  auto cluster_map =
      CreateClusterMapWithConfig({single_slot}, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  EXPECT_FALSE(cluster_map->IsConsistent());
  EXPECT_TRUE(cluster_map->IOwnSlot(100));
  EXPECT_FALSE(cluster_map->IOwnSlot(99));
  EXPECT_FALSE(cluster_map->IOwnSlot(101));

  const ShardInfo* shard = cluster_map->GetShardBySlot(100);
  ASSERT_NE(shard, nullptr);
  EXPECT_EQ(shard->owned_slots.size(), 1);
  EXPECT_EQ(shard->shard_id, primary_ids.at(0));
}

TEST_F(ClusterMapTest, DiscreteSlotRangeTest) {
  std::vector<SlotRangeConfig> ranges = {
      {.start_slot = 0,
       .end_slot = 5460,
       .primary = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0), {}}}},
      {.start_slot = 5461,
       .end_slot = 10922,
       .primary = NodeConfig{"127.0.0.1", 30002, primary_ids.at(1), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30005, replica_ids.at(1), {}}}},
      {.start_slot = 10923,
       .end_slot = 16383,
       .primary = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0), {}}}}};

  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  EXPECT_TRUE(cluster_map->IsConsistent());
  EXPECT_TRUE(cluster_map->IOwnSlot(0));
  EXPECT_TRUE(cluster_map->IOwnSlot(5460));
  EXPECT_FALSE(cluster_map->IOwnSlot(5461));
  EXPECT_FALSE(cluster_map->IOwnSlot(10922));
  EXPECT_TRUE(cluster_map->IOwnSlot(10923));
  EXPECT_TRUE(cluster_map->IOwnSlot(16383));

  EXPECT_EQ(cluster_map->GetTargets(FanoutTargetMode::kPrimary).size(), 2);
  EXPECT_EQ(cluster_map->GetTargets(FanoutTargetMode::kReplicas).size(), 2);
  EXPECT_EQ(cluster_map->GetTargets(FanoutTargetMode::kAll).size(), 4);
}

TEST_F(ClusterMapTest, InvalidPrimaryEndpoint) {
  std::vector<SlotRangeConfig> ranges = {
      {.start_slot = 0,
       .end_slot = 5460,
       .primary = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0), {}}}},
      {.start_slot = 5461,
       .end_slot = 10922,
       .primary = NodeConfig{"", 30002, primary_ids.at(1), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30005, replica_ids.at(1), {}}}},
      {.start_slot = 10923,
       .end_slot = 16383,
       .primary = NodeConfig{"?", 30003, primary_ids.at(2), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30006, replica_ids.at(2), {}}}}};

  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  ASSERT_FALSE(cluster_map->IsConsistent());
  ASSERT_TRUE(cluster_map->IOwnSlot(0));
  ASSERT_FALSE(cluster_map->IOwnSlot(10000));
  EXPECT_EQ(cluster_map->GetTargets(FanoutTargetMode::kPrimary).size(), 1);
  EXPECT_EQ(cluster_map->GetTargets(FanoutTargetMode::kReplicas).size(), 1);
  EXPECT_EQ(cluster_map->GetTargets(FanoutTargetMode::kAll).size(), 2);
}

// ============================================================================
// Replica and Node Location Tests
// ============================================================================

TEST_F(ClusterMapTest, LocalNodeIsReplicaTest) {
  std::vector<SlotRangeConfig> ranges = {
      {.start_slot = 0,
       .end_slot = 8191,
       .primary = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0), {}}}},
      {.start_slot = 8192,
       .end_slot = 16383,
       .primary = NodeConfig{"127.0.0.1", 30002, primary_ids.at(1), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30005, replica_ids.at(1), {}}}}};

  auto cluster_map = CreateClusterMapWithConfig(ranges, replica_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  EXPECT_TRUE(cluster_map->IsConsistent());

  // Should own slots from the first shard since we're part of it
  EXPECT_TRUE(cluster_map->IOwnSlot(0));
  EXPECT_TRUE(cluster_map->IOwnSlot(4000));
  EXPECT_TRUE(cluster_map->IOwnSlot(8191));
  EXPECT_FALSE(cluster_map->IOwnSlot(8192));
  EXPECT_FALSE(cluster_map->IOwnSlot(16383));

  // Verify only the first replica is local
  const ShardInfo* shard = cluster_map->GetShardById(primary_ids.at(0));
  ASSERT_NE(shard, nullptr);
  EXPECT_FALSE(shard->primary->is_local);
  EXPECT_TRUE(shard->replicas[0].is_local);

  // Second shard should be remote
  const ShardInfo* shard2 = cluster_map->GetShardById(primary_ids.at(1));
  ASSERT_NE(shard2, nullptr);
  EXPECT_FALSE(shard2->primary->is_local);
  EXPECT_FALSE(shard2->replicas[0].is_local);
}

TEST_F(ClusterMapTest, MultipleReplicasPerShardTest) {
  SlotRangeConfig full_range{
      .start_slot = 0,
      .end_slot = 16383,
      .primary = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0), {}},
      .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0), {}},
                   NodeConfig{"127.0.0.1", 30005, replica_ids.at(1), {}},
                   NodeConfig{"127.0.0.1", 30006, replica_ids.at(2), {}}}};

  auto cluster_map =
      CreateClusterMapWithConfig({full_range}, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);

  const ShardInfo* shard = cluster_map->GetShardById(primary_ids.at(0));
  ASSERT_NE(shard, nullptr);
  EXPECT_EQ(shard->replicas.size(), 3);

  for (const auto& replica : shard->replicas) {
    EXPECT_EQ(replica.shard, shard);
    EXPECT_FALSE(replica.is_primary);
  }

  VerifyTargetListConsistency(cluster_map.get(), 1, 3);
}

// ============================================================================
// Target Selection Tests
// ============================================================================

TEST_F(ClusterMapTest, GetRandomTargetsTest) {
  auto ranges = CreateStandard3ShardConfig();
  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);

  auto random_targets = cluster_map->GetTargets(FanoutTargetMode::kRandom);
  EXPECT_EQ(random_targets.size(), 3);  // One per shard

  // Verify each target belongs to a different shard
  std::set<std::string> shard_ids;
  for (const auto& target : random_targets) {
    ASSERT_NE(target.shard, nullptr);
    shard_ids.insert(target.shard->shard_id);
  }
  EXPECT_EQ(shard_ids.size(), 3);
}

TEST_F(ClusterMapTest, TargetListConsistencyTest) {
  std::vector<SlotRangeConfig> ranges = {
      {.start_slot = 0,
       .end_slot = 8191,
       .primary = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0), {}}}},
      {.start_slot = 8192,
       .end_slot = 16383,
       .primary = NodeConfig{"127.0.0.1", 30002, primary_ids.at(1), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30005, replica_ids.at(1), {}}}}};

  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  VerifyTargetListConsistency(cluster_map.get(), 2, 2);
}

TEST_F(ClusterMapTest, GetRandomReplicaPerShardTest) {
  auto ranges = CreateStandard3ShardConfig();
  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);

  auto random_targets =
      cluster_map->GetTargets(FanoutTargetMode::kOneReplicaPerShard);
  EXPECT_EQ(random_targets.size(), 3);  // One per shard

  // Verify each target belongs to a different shard
  std::set<std::string> shard_ids;
  for (const auto& target : random_targets) {
    ASSERT_NE(target.shard, nullptr);
    ASSERT_FALSE(target.is_primary);
    shard_ids.insert(target.shard->shard_id);
  }
  EXPECT_EQ(shard_ids.size(), 3);
}

// ============================================================================
// Fingerprint and Metadata Tests
// ============================================================================

TEST_F(ClusterMapTest, FingerprintConsistencyTest) {
  SlotRangeConfig range{
      .start_slot = 0,
      .end_slot = 5460,
      .primary = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0), {}},
      .replicas = {}};

  auto cluster_map1 = CreateClusterMapWithConfig({range}, primary_ids.at(0));
  auto cluster_map2 = CreateClusterMapWithConfig({range}, primary_ids.at(0));

  // Fingerprints should be identical for same configuration
  EXPECT_EQ(cluster_map1->GetClusterSlotsFingerprint(),
            cluster_map2->GetClusterSlotsFingerprint());

  // Create map with different slots
  SlotRangeConfig different_range{
      .start_slot = 0,
      .end_slot = 8000,  // Different end slot
      .primary = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0), {}},
      .replicas = {}};

  auto cluster_map3 =
      CreateClusterMapWithConfig({different_range}, primary_ids.at(0));

  // Fingerprint should be different
  EXPECT_NE(cluster_map1->GetClusterSlotsFingerprint(),
            cluster_map3->GetClusterSlotsFingerprint());
}

TEST_F(ClusterMapTest, ExpirationTimeTest) {
  SlotRangeConfig full_range{
      .start_slot = 0,
      .end_slot = 16383,
      .primary = NodeConfig{"127.0.0.1", 6379, primary_ids.at(0), {}},
      .replicas = {}};

  auto before = std::chrono::steady_clock::now();
  auto cluster_map =
      CreateClusterMapWithConfig({full_range}, primary_ids.at(0));
  auto after = std::chrono::steady_clock::now();

  ASSERT_NE(cluster_map, nullptr);

  auto expiration = cluster_map->GetExpirationTime();

  // Expiration should be in the future
  EXPECT_GT(expiration, after);

  // Expiration should be reasonable (within expected range)
  // Default is 250ms
  auto min_expiration = before + std::chrono::milliseconds(100);
  auto max_expiration = after + std::chrono::milliseconds(300);
  EXPECT_GE(expiration, min_expiration);
  EXPECT_LE(expiration, max_expiration);
}

// ============================================================================
// GetRandomNodeFromShard and GetLocalNodeFromShard Tests
// ============================================================================

TEST_F(ClusterMapTest, GetLocalNodeFromShardTest) {
  // Create a shard with both local and remote nodes
  std::vector<SlotRangeConfig> ranges = {
      {.start_slot = 0,
       .end_slot = 5460,
       .primary = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0), {}},
                    NodeConfig{"127.0.0.1", 30005, replica_ids.at(1), {}}}}};

  // Test when local node is primary
  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));
  ASSERT_NE(cluster_map, nullptr);

  const ShardInfo* shard = cluster_map->GetShardById(primary_ids.at(0));
  ASSERT_NE(shard, nullptr);

  // Test GetLocalNodeFromShard without replica_only (should return primary)
  auto targets_with_local =
      cluster_map->GetTargets(FanoutTargetMode::kRandom, true);
  EXPECT_EQ(targets_with_local.size(), 1);
  EXPECT_TRUE(targets_with_local[0].is_local);
  EXPECT_TRUE(targets_with_local[0].is_primary);

  // Test when local node is replica
  auto cluster_map2 = CreateClusterMapWithConfig(ranges, replica_ids.at(0));
  ASSERT_NE(cluster_map2, nullptr);

  const ShardInfo* shard2 = cluster_map2->GetShardById(primary_ids.at(0));
  ASSERT_NE(shard2, nullptr);

  auto targets_with_local2 =
      cluster_map2->GetTargets(FanoutTargetMode::kRandom, true);
  EXPECT_EQ(targets_with_local2.size(), 1);
  EXPECT_TRUE(targets_with_local2[0].is_local);
  EXPECT_FALSE(targets_with_local2[0].is_primary);

  // Test when no local nodes exist
  auto cluster_map3 = CreateClusterMapWithConfig(ranges, "nonexistent_node_id");
  ASSERT_NE(cluster_map3, nullptr);

  auto targets_with_local3 =
      cluster_map3->GetTargets(FanoutTargetMode::kRandom, true);
  EXPECT_EQ(targets_with_local3.size(), 1);
  EXPECT_FALSE(targets_with_local3[0].is_local);
}

TEST_F(ClusterMapTest, GetRandomNodeFromShardTest) {
  // Create a shard with multiple nodes
  std::vector<SlotRangeConfig> ranges = {
      {.start_slot = 0,
       .end_slot = 5460,
       .primary = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0), {}},
                    NodeConfig{"127.0.0.1", 30005, replica_ids.at(1), {}},
                    NodeConfig{"127.0.0.1", 30006, replica_ids.at(2), {}}}}};

  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));
  ASSERT_NE(cluster_map, nullptr);

  // Test random selection without preference
  std::set<std::string> selected_nodes;
  for (int i = 0; i < 20; ++i) {
    auto targets = cluster_map->GetTargets(FanoutTargetMode::kRandom, false);
    EXPECT_EQ(targets.size(), 1);
    selected_nodes.insert(targets[0].node_id);
  }

  // Should have selected different nodes (randomness test)
  EXPECT_GT(selected_nodes.size(), 1);

  // Test replica-only selection
  auto replica_targets =
      cluster_map->GetTargets(FanoutTargetMode::kOneReplicaPerShard, false);
  EXPECT_EQ(replica_targets.size(), 1);
  EXPECT_FALSE(replica_targets[0].is_primary);
}

TEST_F(ClusterMapTest, GetRandomNodeFromShardReplicaOnlyTest) {
  // Create a shard with primary and replicas
  std::vector<SlotRangeConfig> ranges = {
      {.start_slot = 0,
       .end_slot = 5460,
       .primary = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0), {}},
                    NodeConfig{"127.0.0.1", 30005, replica_ids.at(1), {}}}}};

  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));
  ASSERT_NE(cluster_map, nullptr);

  // Test replica-only selection multiple times
  std::set<std::string> selected_replicas;
  for (int i = 0; i < 10; ++i) {
    auto targets =
        cluster_map->GetTargets(FanoutTargetMode::kOneReplicaPerShard, false);
    EXPECT_EQ(targets.size(), 1);
    EXPECT_FALSE(targets[0].is_primary);
    selected_replicas.insert(targets[0].node_id);
  }

  // Should only select from replicas
  for (const auto& node_id : selected_replicas) {
    EXPECT_TRUE(node_id == replica_ids.at(0) || node_id == replica_ids.at(1));
  }
}

TEST_F(ClusterMapTest, GetLocalNodeFromShardWithReplicaOnlyTest) {
  // Create a shard where local node is primary
  std::vector<SlotRangeConfig> ranges = {
      {.start_slot = 0,
       .end_slot = 5460,
       .primary = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0), {}}}}};

  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));
  ASSERT_NE(cluster_map, nullptr);

  // Test replica-only with prefer_local when local is primary
  // Should fall back to random replica since local primary is excluded
  auto targets =
      cluster_map->GetTargets(FanoutTargetMode::kOneReplicaPerShard, true);
  EXPECT_EQ(targets.size(), 1);
  EXPECT_FALSE(targets[0].is_primary);
  EXPECT_FALSE(
      targets[0]
          .is_local);  // Local primary excluded, so remote replica selected

  // Test when local node is replica
  auto cluster_map2 = CreateClusterMapWithConfig(ranges, replica_ids.at(0));
  ASSERT_NE(cluster_map2, nullptr);

  auto targets2 =
      cluster_map2->GetTargets(FanoutTargetMode::kOneReplicaPerShard, true);
  EXPECT_EQ(targets2.size(), 1);
  EXPECT_FALSE(targets2[0].is_primary);
  EXPECT_TRUE(targets2[0].is_local);  // Local replica should be selected
}

TEST_F(ClusterMapTest, GetTargetsWithPreferLocalTest) {
  // Create multiple shards with mixed local/remote nodes
  std::vector<SlotRangeConfig> ranges = {
      {.start_slot = 0,
       .end_slot = 5460,
       .primary = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0), {}}}},
      {.start_slot = 5461,
       .end_slot = 10922,
       .primary = NodeConfig{"127.0.0.1", 30002, primary_ids.at(1), {}},
       .replicas = {NodeConfig{"127.0.0.1", 30005, replica_ids.at(1), {}}}}};

  // Local node is first primary
  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));
  ASSERT_NE(cluster_map, nullptr);

  // Test prefer_local = true
  auto targets_prefer_local =
      cluster_map->GetTargets(FanoutTargetMode::kRandom, true);
  EXPECT_EQ(targets_prefer_local.size(), 2);

  // First shard should select local primary
  bool found_local_primary = false;
  for (const auto& target : targets_prefer_local) {
    if (target.node_id == primary_ids.at(0)) {
      EXPECT_TRUE(target.is_local);
      EXPECT_TRUE(target.is_primary);
      found_local_primary = true;
    }
  }
  EXPECT_TRUE(found_local_primary);

  // Test prefer_local = false (default behavior)
  auto targets_no_preference =
      cluster_map->GetTargets(FanoutTargetMode::kRandom, false);
  EXPECT_EQ(targets_no_preference.size(), 2);
}

}  // namespace

}  // namespace cluster_map
}  // namespace vmsdk
