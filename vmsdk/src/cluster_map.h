/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_CLUSTER_MAP_H_
#define VMSDK_SRC_CLUSTER_MAP_H_

#include <bitset>
#include <chrono>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "highwayhash/arch_specific.h"
#include "highwayhash/hh_types.h"
#include "highwayhash/highwayhash.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

namespace cluster_map {

static constexpr highwayhash::HHKey kHashKey{
    0x9736bad976c904ea, 0x08f963a1a52eece9, 0x1ea3f3f773f3b510,
    0x9290a6b4e4db3d51};

// Enumeration for fanout target modes
enum class FanoutTargetMode {
  kRandom,        // Default: randomly select one node per shard
  kReplicasOnly,  // Select only replicas, one per shard
  kPrimary,       // Select all primary (master) nodes
  kAll            // Select all nodes (both primary and replica)
};

const size_t k_num_slots = 16384;

// forward declaration to solve circular dependency
struct ShardInfo;

struct NodeInfo {
  std::string node_id;
  bool is_primary;
  bool is_local;
  SocketAddress socket_address;
  // Pointer to the shard this node belongs to
  const ShardInfo* shard = nullptr;

  auto operator<=>(const NodeInfo&) const = default;

  friend std::ostream& operator<<(std::ostream& os, const NodeInfo& target) {
    os << "NodeInfo{role: " << (target.is_primary ? "primary" : "replica")
       << ", location: " << (target.is_local ? "local" : "remote")
       << ", address: " << target.socket_address.ip << ":"
       << target.socket_address.port << "}";
    return os;
  }
};

struct ShardInfo {
  // shard_id is the primary node id
  std::string shard_id;
  // primary node can be empty
  std::optional<NodeInfo> primary;
  std::vector<NodeInfo> replicas;
  // map start slot to end slot
  std::map<uint16_t, uint16_t> owned_slots;
  // Hash of owned_slots
  uint64_t slots_fingerprint;
};

struct SlotRangeInfo {
  uint16_t start_slot;
  uint16_t end_slot;
  std::string shard_id;
};

class ClusterMap {
 public:
  // Create a new cluster map by querying current cluster state
  // Reads would still access the existing cluster map; the new cluster map
  // would replace the existing map once the creation is finished
  static std::shared_ptr<ClusterMap> CreateNewClusterMap(ValkeyModuleCtx* ctx);

  // return pre-generated target vectors
  const std::vector<NodeInfo>& GetPrimaryTargets() const {
    return primary_targets_;
  };
  const std::vector<NodeInfo>& GetReplicaTargets() const {
    return replica_targets_;
  };
  const std::vector<NodeInfo>& GetAllTargets() const { return all_targets_; };

  std::chrono::steady_clock::time_point GetExpirationTime() const {
    return expiration_tp_;
  }

  // are all the slots assigned to some shard
  bool GetIsClusterMapFull() const { return is_cluster_map_full_; }

  // generate a random targets vector from cluster bus
  std::vector<NodeInfo> GetRandomTargets();

  // do I own this slot
  bool IOwnSlot(uint16_t slot) const { return owned_slots_[slot]; }

  // shard lookups, will return nullptr if shard not found
  const ShardInfo* GetShardById(std::string_view shard_id) const;

  // shard lookup by slot, return nullptr if shard not found
  const ShardInfo* GetShardBySlot(uint16_t slot) const;

  // get cluster level slot fingerprint
  uint64_t GetClusterSlotsFingerprint() const {
    return cluster_slots_fingerprint_;
  }

 private:
  std::chrono::steady_clock::time_point expiration_tp_;

  // 1: slot is owned by this cluster, 0: slot is not owned by this cluster
  std::bitset<k_num_slots> owned_slots_;

  absl::flat_hash_map<std::string, ShardInfo> shards_;

  // An ordered map, key is start slot, value is end slot and ShardInfo
  std::map<uint16_t, std::pair<uint16_t, const ShardInfo*>> slot_to_shard_map_;

  // Cluster-level fingerprint (hash of all shard fingerprints)
  uint64_t cluster_slots_fingerprint_;

  bool is_cluster_map_full_;

  // Pre-computed target lists
  std::vector<NodeInfo> primary_targets_;
  std::vector<NodeInfo> replica_targets_;
  std::vector<NodeInfo> all_targets_;

  // helper function to print out cluster map for debug
  static void PrintClusterMap(std::shared_ptr<ClusterMap> map);

  // helper function to create shard fingerprint
  uint64_t ComputeShardFingerprint(
      const std::map<uint16_t, uint16_t>& slot_ranges);

  // helper function to create cluster level fingerprint
  uint64_t ComputeClusterFingerprint();

  // Helper functions for CreateNewClusterMap
  NodeInfo ParseNodeInfo(ValkeyModuleCallReply* node_arr, bool is_local_shard,
                         bool is_primary);

  bool IsLocalShard(ValkeyModuleCallReply* slot_range, const char* my_node_id);

  void ProcessSlotRange(ValkeyModuleCallReply* slot_range,
                        const char* my_node_id,
                        std::vector<SlotRangeInfo>& slot_ranges);

  void BuildSlotToShardMap(const std::vector<SlotRangeInfo>& slot_ranges);

  bool CheckClusterMapFull();
};

}  // namespace cluster_map
}  // namespace vmsdk

#endif  // VMSDK_SRC_CLUSTER_MAP_H_