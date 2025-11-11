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
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "absl/container/btree_map.h"
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
  kRandom,              // Default: randomly select one node per shard
  kOneReplicaPerShard,  // Select only replicas, one per shard
  kPrimary,             // Select all primary nodes
  kReplicas,            // Select all replica nodes
  kAll                  // Select all nodes (both primary and replica)
};

const size_t kNumSlots = 16384;

// forward declaration to solve circular dependency
struct ShardInfo;

struct NodeInfo {
  std::string node_id;
  bool is_primary = false;
  bool is_local = false;
  SocketAddress socket_address;
  // a map containing all additional network metadata(the fourth entry of
  // CLUSTER SLOTS response); can be empty
  absl::flat_hash_map<std::string, std::string> additional_network_metadata;
  // Pointer to the shard this node belongs to
  const ShardInfo* shard = nullptr;

  auto operator<=>(const NodeInfo&) const = default;

  friend std::ostream& operator<<(std::ostream& os, const NodeInfo& target) {
    os << "NodeInfo{role: " << (target.is_primary ? "primary" : "replica")
       << ", location: " << (target.is_local ? "local" : "remote")
       << ", address: " << target.socket_address.primary_endpoint << ":"
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
  absl::btree_map<uint16_t, uint16_t> owned_slots;
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

  // get a vector of node targets based on the mode
  std::vector<NodeInfo> GetTargets(FanoutTargetMode mode,
                                   bool prefer_local = false) const;

  std::chrono::steady_clock::time_point GetExpirationTime() const {
    return expiration_tp_;
  }

  // are all the slots assigned to some shard
  bool IsConsistent() const { return is_consistent_; }

  // do I own this slot
  bool IOwnSlot(uint16_t slot) const { return owned_slots_[slot]; }

  // shard lookups, will return nullptr if shard not found
  const ShardInfo* GetShardById(std::string_view shard_id) const;

  // shard lookup by slot, return nullptr if shard not found
  const ShardInfo* GetShardBySlot(uint16_t slot) const;

  // Get the shard info for the current node
  const ShardInfo* GetCurrentNodeShard() const {
    return current_node_shard_;
  }

  // get cluster level slot fingerprint
  uint64_t GetClusterSlotsFingerprint() const {
    return cluster_slots_fingerprint_;
  }

 private:
  std::chrono::steady_clock::time_point expiration_tp_;

  // a map used for check duplicate socket addresses, SocketAddress -> node_id
  absl::flat_hash_map<SocketAddress, std::string> socket_addr_to_node_map;

  // 1: slot is owned by this cluster, 0: slot is not owned by this cluster
  std::bitset<kNumSlots> owned_slots_;

  absl::btree_map<std::string, ShardInfo> shards_;

  // An ordered map, key is start slot, value is end slot and ShardInfo
  absl::btree_map<uint16_t, std::pair<uint16_t, const ShardInfo*>>
      slot_to_shard_map_;

  // Cluster-level fingerprint (hash of all shard fingerprints)
  uint64_t cluster_slots_fingerprint_;

  bool is_consistent_;

  // Cached pointer to the current node's shard
  const ShardInfo* current_node_shard_;

  // Pre-computed target lists
  std::vector<NodeInfo> primary_targets_;
  std::vector<NodeInfo> replica_targets_;
  std::vector<NodeInfo> all_targets_;

  // get a local node from a shard (prefers local nodes)
  std::optional<NodeInfo> GetLocalNodeFromShard(
      const ShardInfo& shard, bool replica_only = false) const;

  // get a random node from a shard
  NodeInfo GetRandomNodeFromShard(const ShardInfo& shard,
                                  bool replica_only = false,
                                  bool prefer_local = false) const;

  // helper function to print out cluster map for debug
  static void PrintClusterMap(std::shared_ptr<ClusterMap> map);

  // helper function to create shard fingerprint
  uint64_t ComputeShardFingerprint(
      const absl::btree_map<uint16_t, uint16_t>& slot_ranges);

  // helper function to create cluster level fingerprint
  uint64_t ComputeClusterFingerprint();

  // Helper functions for CreateNewClusterMap
  // parse and return a single node info, or return empty if node is invalid
  std::optional<NodeInfo> ParseNodeInfo(ValkeyModuleCallReply* node_arr,
                                        const char* my_node_id,
                                        bool is_primary);

  // check is this a local shard
  bool IsLocalShard(ValkeyModuleCallReply* slot_range, const char* my_node_id);

  // if multiple slot ranges belong to same shard, check is the shard
  // consistent
  bool IsExistingShardConsistent(const ShardInfo& existing_shard,
                                 const NodeInfo& new_primary,
                                 const std::vector<NodeInfo>& new_replicas);

  // process each slot range array, return false if slot range is invalid
  bool ProcessSlotRange(ValkeyModuleCallReply* slot_range,
                        const char* my_node_id,
                        std::vector<SlotRangeInfo>& slot_ranges);

  // build the slot_to_shard_map
  void BuildSlotToShardMap(const std::vector<SlotRangeInfo>& slot_ranges);

  // check if all slots belong to some shard
  bool CheckClusterMapFull();
};

}  // namespace cluster_map
}  // namespace vmsdk

#endif  // VMSDK_SRC_CLUSTER_MAP_H_