/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_CLUSTER_MAP_H_
#define VMSDK_SRC_CLUSTER_MAP_H_

#include <bitset>
#include <string>
#include <string_view>
#include <vector>

#include "src/query/fanout_template.h"
#include "src/valkeymodule.h"

namespace vmsdk {
namespace cluster_map {

const size_t NUM_SLOTS = 16384;

// Enumeration for fanout target modes
enum class FanoutTargetMode {
  kRandom,        // Default: randomly select one node per shard
  kReplicasOnly,  // Select only replicas, one per shard
  kPrimary,       // Select all primary (master) nodes
  kAll            // Select all nodes (both primary and replica)
};

struct NodeInfo {
  enum Type {
    kLocal,
    kRemote,
  };
  std::string node_id;
  Type type;
  // Empty string if type is kLocal.
  std::string address;

  bool operator==(const NodeInfo& other) const {
    return type == other.type && address == other.address;
  }

  friend std::ostream& operator<<(std::ostream& os, const NodeInfo& target) {
    os << "NodeInfo{type: " << target.type << ", address: " << target.address
       << "}";
    return os;
  }
};

struct ShardInfo {
  // shard_id is the primary node id
  std::string shard_id;
  std::string primary_address;
  std::vector<std::string> replica_addresses;
  std::vector<uint16_t> owned_slots;
  // Hash of owned_slots vector
  uint64_t slots_fingerprint;
};

class ClusterMap {
 public:
  const std::vector<NodeInfo>& GetPrimaryTargets() const;
  const std::vector<NodeInfo>& GetReplicaTargets() const;
  const std::vector<NodeInfo>& GetRandomTargets() const;
  const std::vector<NodeInfo>& GetAllTargets() const;

  // create a new cluster map in the background
  static std::shared_ptr<ClusterMap> CreateNewClusterMap(ValkeyModuleCtx* ctx);

  // slot ownership checks
  bool IsSlotOwned(uint16_t slot) const;

  // shard lookups
  const ShardInfo* GetShardById(std::string_view shard_id) const;
  const absl::flat_hash_map<std::string, ShardInfo>& GetAllShards() const;

  // get cluster level slot fingerprint
  uint64_t GetClusterSlotsFingerprint() const;

  // get fingerprint for a specific shard
  uint64_t GetShardSlotsFingerprint(std::string_view shard_id) const;

 private:
  // 1: slot is owned by this cluster, 0: slot is not owned by this cluster
  std::bitset<NUM_SLOTS> owned_slots_;

  // slot-to-shard lookup
  std::array<std::string, NUM_SLOTS> slot_to_shard_id_;

  absl::flat_hash_map<std::string, ShardInfo> shards_;

  // Cluster-level fingerprint (hash of all shard fingerprints)
  uint64_t cluster_slots_fingerprint_;

  // Pre-computed target lists
  std::vector<NodeInfo> primary_targets_;
  std::vector<NodeInfo> replica_targets_;
  std::vector<NodeInfo> random_targets_;
  std::vector<NodeInfo> all_targets_;

  // private helper function to refresh targets in CreateNewClusterMap
  std::vector<NodeInfo> GetTargets(ValkeyModuleCtx* ctx,
                                   FanoutTargetMode target_mode);
};

}  // namespace cluster_map
}  // namespace vmsdk

#endif  // VMSDK_SRC_CLUSTER_MAP_H_
