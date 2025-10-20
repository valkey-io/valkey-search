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
#include <vector>

#include "src/query/fanout_template.h"
#include "src/valkeymodule.h"

namespace vmsdk {

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
  // flexible to add other getter methods
 public:
  // create a new cluster map in the background
  static std::shared_ptr<ClusterMap> CreateNewClusterMap(ValkeyModuleCtx* ctx);

  // slot ownership checks
  bool IsSlotOwned(uint16_t slot) const;

  // shard lookups
  const ShardInfo* GetShardById(const std::string& shard_id) const;
  const std::string& GetShardIdBySlot(uint16_t slot) const;
  const absl::flat_hash_map<std::string, ShardInfo>& GetAllShards() const;

  // get cluster level slot fingerprint
  uint64_t GetClusterSlotsFingerprint() const;

  // get fingerprint for a specific shard
  uint64_t GetShardSlotsFingerprint(const std::string& shard_id) const;

 private:
  // 1: slot is owned by this cluster, 0: slot is not owned by this cluster
  std::bitset<16384> owned_slots_;

  // slot-to-shard lookup
  std::array<std::string, 16384> slot_to_shard_id_;

  absl::flat_hash_map<std::string, ShardInfo> shards_;

  // Cluster-level fingerprint (hash of all shard fingerprints)
  uint64_t cluster_slots_fingerprint_;

  // Pre-computed target lists
  std::vector<valkey_search::query::fanout::FanoutSearchTarget>
      primary_targets_;
  std::vector<valkey_search::query::fanout::FanoutSearchTarget>
      replica_targets_;
  std::vector<valkey_search::query::fanout::FanoutSearchTarget> random_targets_;
  std::vector<valkey_search::query::fanout::FanoutSearchTarget> all_targets_;
};

}  // namespace vmsdk

#endif  // VMSDK_SRC_CLUSTER_MAP_H_
