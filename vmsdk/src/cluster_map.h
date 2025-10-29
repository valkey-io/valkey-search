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
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

namespace cluster_map {

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
  enum NodeRole { kPrimary, kReplica };
  enum NodeLocation {
    kLocal,
    kRemote,
  };
  std::string node_id;
  NodeRole role;
  NodeLocation location;
  // Empty string if location is kLocal.
  std::string ip;
  int port;
  std::string address;
  // Pointer to the shard this node belongs to
  const ShardInfo* shard = nullptr;

  auto operator<=>(const NodeInfo&) const = default;

  friend std::ostream& operator<<(std::ostream& os, const NodeInfo& target) {
    os << "NodeInfo{role: " << target.role << ", location: " << target.location
       << ", address: " << target.ip << ":" << target.port << "}";
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
  // Reads would still access the existing cluster map; the new cluster map
  // would replace the existing map once the creation is finished
  static std::shared_ptr<ClusterMap> CreateNewClusterMap(ValkeyModuleCtx* ctx);

  // return pre-generated target vectors
  const std::vector<NodeInfo>& GetPrimaryTargets() const;
  const std::vector<NodeInfo>& GetReplicaTargets() const;
  const std::vector<NodeInfo>& GetAllTargets() const;

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

  static uint64_t compute_shard_fingerprint(const std::set<uint16_t>& slots);

  static uint64_t compute_cluster_fingerprint(
      const absl::flat_hash_map<std::string, ShardInfo>& shards);
};

}  // namespace cluster_map
}  // namespace vmsdk

#endif  // VMSDK_SRC_CLUSTER_MAP_H_