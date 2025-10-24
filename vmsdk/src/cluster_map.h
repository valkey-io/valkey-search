/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_CLUSTER_MAP_H_
#define VMSDK_SRC_CLUSTER_MAP_H_

#include <bitset>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
namespace cluster_map {

const size_t k_num_slots = 16384;

// Enumeration for fanout target modes
enum class FanoutTargetMode {
  kRandom,        // Default: randomly select one node per shard
  kReplicasOnly,  // Select only replicas, one per shard
  kPrimary,       // Select all primary (master) nodes
  kAll            // Select all nodes (both primary and replica)
};

struct NodeInfo {
  enum NodeRole { kPrimary, kReplica };
  enum NodeLocation {
    kLocal,
    kRemote,
  };
  std::string node_id;
  NodeRole role;
  NodeLocation location;
  // Empty string if location is kLocal.
  std::string address;

  bool operator==(const NodeInfo& other) const {
    return role == other.role && location == other.location &&
           address == other.address;
  }

  friend std::ostream& operator<<(std::ostream& os, const NodeInfo& target) {
    os << "NodeInfo{role: " << target.role << ", location: " << target.location
       << ", address: " << target.address << "}";
    return os;
  }
};

struct ShardInfo {
  // shard_id is the primary node id
  std::string shard_id;
  // primary node can be empty
  std::optional<NodeInfo> primary;
  std::vector<NodeInfo> replicas;
  std::set<uint16_t> owned_slots;
  // Hash of owned_slots vector
  uint64_t slots_fingerprint;
};

class ClusterMap {
 public:
  // Create a new cluster map by querying current cluster state
  // This builds the map in the background and can be called from any thread
  static std::shared_ptr<ClusterMap> CreateNewClusterMap(ValkeyModuleCtx* ctx);

  // return pre-generated target vectors
  const std::vector<NodeInfo>& GetPrimaryTargets() const;
  const std::vector<NodeInfo>& GetReplicaTargets() const;
  const std::vector<NodeInfo>& GetAllTargets() const;

  bool GetIsClusterMapFull() const;

  // generate a random targets vector from cluster bus
  std::vector<NodeInfo> GetRandomTargets(ValkeyModuleCtx* ctx);

  // slot ownership checks
  bool IsSlotOwned(uint16_t slot) const;

  // shard lookups, will return nullptr if shard does not exist
  const ShardInfo* GetShardById(std::string_view shard_id) const;
  const absl::flat_hash_map<std::string, ShardInfo>& GetAllShards() const;

  // get cluster level slot fingerprint
  uint64_t GetClusterSlotsFingerprint() const;

 private:
  // 1: slot is owned by this cluster, 0: slot is not owned by this cluster
  std::bitset<k_num_slots> owned_slots_;

  absl::flat_hash_map<std::string, ShardInfo> shards_;

  // Cluster-level fingerprint (hash of all shard fingerprints)
  uint64_t cluster_slots_fingerprint_;

  bool is_cluster_map_full_;

  // Pre-computed target lists
  std::vector<NodeInfo> primary_targets_;
  std::vector<NodeInfo> replica_targets_;
  std::vector<NodeInfo> all_targets_;

  // private helper function to refresh targets in CreateNewClusterMap
  static std::vector<NodeInfo> GetTargets(ValkeyModuleCtx* ctx,
                                          FanoutTargetMode target_mode);
};

}  // namespace cluster_map
}  // namespace vmsdk

#endif  // VMSDK_SRC_CLUSTER_MAP_H_
