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

// Helper structure to define a node (master or replica)
struct NodeConfig {
  std::string ip;
  int port;
  std::string node_id;  // 40-character hex string
};

// Helper structure to define a slot range with its nodes
struct SlotRangeConfig {
  int start_slot;
  int end_slot;
  std::optional<NodeConfig> master;
  std::vector<NodeConfig> replicas;
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

  // Helper: Create a node array [ip, port, node_id]
  CallReplyArray CreateNodeArray(const NodeConfig& node) {
    CallReplyArray node_array;
    node_array.push_back(CreateValkeyModuleCallReply(CallReplyString(node.ip)));
    node_array.push_back(
        CreateValkeyModuleCallReply(CallReplyInteger(node.port)));
    node_array.push_back(
        CreateValkeyModuleCallReply(CallReplyString(node.node_id)));
    return node_array;
  }

  // Helper: Create a slot range array [start, end, master, replica1, replica2,
  // ...]
  CallReplyArray CreateSlotRangeArray(const SlotRangeConfig& config) {
    CallReplyArray range;
    range.push_back(
        CreateValkeyModuleCallReply(CallReplyInteger(config.start_slot)));
    range.push_back(
        CreateValkeyModuleCallReply(CallReplyInteger(config.end_slot)));

    if (config.master.has_value()) {
      range.push_back(
          CreateValkeyModuleCallReply(CreateNodeArray(config.master.value())));
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
        .WillRepeatedly(
            testing::Invoke(&TestValkeyModule_CallReplyArrayElementImpl));

    EXPECT_CALL(*kMockValkeyModule, CallReplyType(testing::Ne(reply)))
        .WillRepeatedly(testing::Invoke(&TestValkeyModule_CallReplyTypeImpl));

    EXPECT_CALL(*kMockValkeyModule, CallReplyLength(testing::Ne(reply)))
        .WillRepeatedly(testing::Invoke([](ValkeyModuleCallReply* r) -> size_t {
          if (r && r->type == VALKEYMODULE_REPLY_ARRAY) {
            return std::get<CallReplyArray>(r->val).size();
          }
          return 0;
        }));

    EXPECT_CALL(*kMockValkeyModule, CallReplyInteger(testing::_))
        .WillRepeatedly(
            testing::Invoke(&TestValkeyModule_CallReplyIntegerImpl));

    EXPECT_CALL(*kMockValkeyModule, CallReplyStringPtr(testing::_, testing::_))
        .WillRepeatedly(
            testing::Invoke(&TestValkeyModule_CallReplyStringPtrImpl));
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

  // Helper: Create standard 3-shard configuration
  std::vector<SlotRangeConfig> CreateStandard3ShardConfig() {
    return {{.start_slot = 0,
             .end_slot = 5460,
             .master = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0)},
             .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0)}}},
            {.start_slot = 5461,
             .end_slot = 10922,
             .master = NodeConfig{"127.0.0.1", 30002, primary_ids.at(1)},
             .replicas = {NodeConfig{"127.0.0.1", 30005, replica_ids.at(1)}}},
            {.start_slot = 10923,
             .end_slot = 16383,
             .master = NodeConfig{"127.0.0.1", 30003, primary_ids.at(2)},
             .replicas = {NodeConfig{"127.0.0.1", 30006, replica_ids.at(2)}}}};
  }

  // Helper: Verify target list consistency
  void VerifyTargetListConsistency(const ClusterMap* cluster_map,
                                   size_t expected_primaries,
                                   size_t expected_replicas) {
    const auto& primary_targets = cluster_map->GetPrimaryTargets();
    const auto& replica_targets = cluster_map->GetReplicaTargets();
    const auto& all_targets = cluster_map->GetAllTargets();

    EXPECT_EQ(primary_targets.size(), expected_primaries);
    EXPECT_EQ(replica_targets.size(), expected_replicas);
    EXPECT_EQ(all_targets.size(), expected_primaries + expected_replicas);

    for (const auto& target : primary_targets) {
      EXPECT_EQ(target.role, NodeInfo::NodeRole::kPrimary);
      EXPECT_NE(target.shard, nullptr);
    }

    for (const auto& target : replica_targets) {
      EXPECT_EQ(target.role, NodeInfo::NodeRole::kReplica);
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
      .master = NodeConfig{"127.0.0.1", 6379, primary_ids.at(0)},
      .replicas = {}};

  auto cluster_map =
      CreateClusterMapWithConfig({full_range}, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  EXPECT_TRUE(cluster_map->GetIsClusterMapFull());
  EXPECT_TRUE(cluster_map->IOwnSlot(0));
  EXPECT_TRUE(cluster_map->IOwnSlot(16383));

  VerifyTargetListConsistency(cluster_map.get(), 1, 0);
}

TEST_F(ClusterMapTest, MultipleShards) {
  std::vector<SlotRangeConfig> ranges = {
      {.start_slot = 0,
       .end_slot = 5460,
       .master = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0)},
       .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0)}}},
      {.start_slot = 5461,
       .end_slot = 10922,
       .master = NodeConfig{"127.0.0.1", 30002, primary_ids.at(1)},
       .replicas = {NodeConfig{"127.0.0.1", 30005, replica_ids.at(1)}}},
      {.start_slot = 10923,
       .end_slot = 16383,
       .master = NodeConfig{"127.0.0.1", 30003, primary_ids.at(2)},
       .replicas = {}}};

  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  EXPECT_TRUE(cluster_map->GetIsClusterMapFull());
  EXPECT_TRUE(cluster_map->IOwnSlot(100));
  EXPECT_FALSE(cluster_map->IOwnSlot(10000));

  VerifyTargetListConsistency(cluster_map.get(), 3, 2);
}

TEST_F(ClusterMapTest, PartialCoverage) {
  std::vector<SlotRangeConfig> ranges = {
      {.start_slot = 0,
       .end_slot = 5000,
       .master = NodeConfig{"127.0.0.1", 6379, primary_ids.at(0)},
       .replicas = {}},
      {.start_slot = 10000,  // Gap from 5001-9999
       .end_slot = 16383,
       .master = NodeConfig{"127.0.0.1", 6380, primary_ids.at(1)},
       .replicas = {}}};

  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  EXPECT_FALSE(cluster_map->GetIsClusterMapFull());

  VerifyTargetListConsistency(cluster_map.get(), 2, 0);
}

TEST_F(ClusterMapTest, EmptyClusterSlot) {
  auto cluster_map = CreateClusterMapWithConfig({}, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  EXPECT_FALSE(cluster_map->GetIsClusterMapFull());
  EXPECT_EQ(cluster_map->GetShardBySlot(0), nullptr);
  EXPECT_FALSE(cluster_map->IOwnSlot(5000));

  VerifyTargetListConsistency(cluster_map.get(), 0, 0);
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
       .master = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0)},
       .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0)}}},
      {.start_slot = 5461,
       .end_slot = 10922,
       .master = NodeConfig{"127.0.0.1", 30002, primary_ids.at(1)},
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
       .master = NodeConfig{"127.0.0.1", 6379, primary_ids.at(0)},
       .replicas = {}},
      {.start_slot = 10000,  // Gap from 5001-9999
       .end_slot = 16383,
       .master = NodeConfig{"127.0.0.1", 6380, primary_ids.at(1)},
       .replicas = {}}};

  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  EXPECT_FALSE(cluster_map->GetIsClusterMapFull());

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
       .master = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0)},
       .replicas = {}},
      {.start_slot = 8192,
       .end_slot = 16383,
       .master = NodeConfig{"127.0.0.1", 30002, primary_ids.at(1)},
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
      .master = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0)},
      .replicas = {}};

  auto cluster_map =
      CreateClusterMapWithConfig({single_slot}, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  EXPECT_FALSE(cluster_map->GetIsClusterMapFull());
  EXPECT_TRUE(cluster_map->IOwnSlot(100));
  EXPECT_FALSE(cluster_map->IOwnSlot(99));
  EXPECT_FALSE(cluster_map->IOwnSlot(101));

  const ShardInfo* shard = cluster_map->GetShardBySlot(100);
  ASSERT_NE(shard, nullptr);
  EXPECT_EQ(shard->owned_slots.size(), 1);
  EXPECT_EQ(shard->shard_id, primary_ids.at(0));
}

// ============================================================================
// Replica and Node Location Tests
// ============================================================================

TEST_F(ClusterMapTest, LocalNodeIsReplicaTest) {
  std::vector<SlotRangeConfig> ranges = {
      {.start_slot = 0,
       .end_slot = 8191,
       .master = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0)},
       .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0)}}},
      {.start_slot = 8192,
       .end_slot = 16383,
       .master = NodeConfig{"127.0.0.1", 30002, primary_ids.at(1)},
       .replicas = {NodeConfig{"127.0.0.1", 30005, replica_ids.at(1)}}}};

  auto cluster_map = CreateClusterMapWithConfig(ranges, replica_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  EXPECT_TRUE(cluster_map->GetIsClusterMapFull());

  // Should own slots from the first shard since we're part of it
  EXPECT_TRUE(cluster_map->IOwnSlot(0));
  EXPECT_TRUE(cluster_map->IOwnSlot(4000));
  EXPECT_TRUE(cluster_map->IOwnSlot(8191));
  EXPECT_FALSE(cluster_map->IOwnSlot(8192));
  EXPECT_FALSE(cluster_map->IOwnSlot(16383));

  // Verify both nodes in first shard are marked as local
  const ShardInfo* shard = cluster_map->GetShardById(primary_ids.at(0));
  ASSERT_NE(shard, nullptr);
  EXPECT_EQ(shard->primary->location, NodeInfo::NodeLocation::kLocal);
  EXPECT_EQ(shard->replicas[0].location, NodeInfo::NodeLocation::kLocal);

  // Second shard should be remote
  const ShardInfo* shard2 = cluster_map->GetShardById(primary_ids.at(1));
  ASSERT_NE(shard2, nullptr);
  EXPECT_EQ(shard2->primary->location, NodeInfo::NodeLocation::kRemote);
  EXPECT_EQ(shard2->replicas[0].location, NodeInfo::NodeLocation::kRemote);
}

TEST_F(ClusterMapTest, MultipleReplicasPerShardTest) {
  SlotRangeConfig full_range{
      .start_slot = 0,
      .end_slot = 16383,
      .master = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0)},
      .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0)},
                   NodeConfig{"127.0.0.1", 30005, replica_ids.at(1)},
                   NodeConfig{"127.0.0.1", 30006, replica_ids.at(2)}}};

  auto cluster_map =
      CreateClusterMapWithConfig({full_range}, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);

  const ShardInfo* shard = cluster_map->GetShardById(primary_ids.at(0));
  ASSERT_NE(shard, nullptr);
  EXPECT_EQ(shard->replicas.size(), 3);

  for (const auto& replica : shard->replicas) {
    EXPECT_EQ(replica.shard, shard);
    EXPECT_EQ(replica.role, NodeInfo::NodeRole::kReplica);
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

  auto random_targets = cluster_map->GetRandomTargets();
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
       .master = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0)},
       .replicas = {NodeConfig{"127.0.0.1", 30004, replica_ids.at(0)}}},
      {.start_slot = 8192,
       .end_slot = 16383,
       .master = NodeConfig{"127.0.0.1", 30002, primary_ids.at(1)},
       .replicas = {NodeConfig{"127.0.0.1", 30005, replica_ids.at(1)}}}};

  auto cluster_map = CreateClusterMapWithConfig(ranges, primary_ids.at(0));

  ASSERT_NE(cluster_map, nullptr);
  VerifyTargetListConsistency(cluster_map.get(), 2, 2);
}

// ============================================================================
// Fingerprint and Metadata Tests
// ============================================================================

TEST_F(ClusterMapTest, FingerprintConsistencyTest) {
  SlotRangeConfig range{
      .start_slot = 0,
      .end_slot = 5460,
      .master = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0)},
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
      .master = NodeConfig{"127.0.0.1", 30001, primary_ids.at(0)},
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
      .master = NodeConfig{"127.0.0.1", 6379, primary_ids.at(0)},
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
  // Default is 5000ms, so should be roughly 5 seconds from creation
  auto min_expiration = before + std::chrono::milliseconds(4000);
  auto max_expiration = after + std::chrono::milliseconds(6000);
  EXPECT_GE(expiration, min_expiration);
  EXPECT_LE(expiration, max_expiration);
}

}  // namespace

}  // namespace cluster_map
}  // namespace vmsdk
