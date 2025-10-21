/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/cluster_map.h"

#include <netinet/in.h>

#include "absl/container/flat_hash_map.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"

namespace vmsdk {
namespace cluster_map {

namespace coordinator {
// Coordinator port offset - same as in src/coordinator/util.h
// This offset results in 26673 for Valkey default port 6379 - which is COORD
// on a telephone keypad.
static constexpr int kCoordinatorPortOffset = 20294;

inline int GetCoordinatorPort(int valkey_port) {
  // TODO Make handling of TLS more robust
  if (valkey_port == 6378) {
    return valkey_port + kCoordinatorPortOffset + 1;
  }
  return valkey_port + kCoordinatorPortOffset;
}
}  // namespace coordinator

// return pre-generated primary targets
const std::vector<NodeInfo>& ClusterMap::GetPrimaryTargets() const {
  return primary_targets_;
}

// return pre-generated replica targets
const std::vector<NodeInfo>& ClusterMap::GetReplicaTargets() const {
  return replica_targets_;
}

// return pre-generated all targets
const std::vector<NodeInfo>& ClusterMap::GetAllTargets() const {
  return all_targets_;
}

// generate a random targets vector from cluster bus (not pre-generated)
std::vector<NodeInfo> ClusterMap::GetRandomTargets(ValkeyModuleCtx* ctx) {
  return GetTargets(ctx, FanoutTargetMode::kRandom);
}

// slot ownership checks
bool ClusterMap::IsSlotOwned(uint16_t slot) const { return owned_slots_[slot]; }

// shard lookups, will return nullptr if shard does not exist
const ShardInfo* ClusterMap::GetShardById(std::string_view shard_id) const {
  auto it = shards_.find(std::string(shard_id));
  if (it != shards_.end()) {
    return &it->second;
  }
  return nullptr;
}

// return all shards as a map
const absl::flat_hash_map<std::string, ShardInfo>& ClusterMap::GetAllShards()
    const {
  return shards_;
}

// get cluster level slot fingerprint
uint64_t ClusterMap::GetClusterSlotsFingerprint() const {
  return cluster_slots_fingerprint_;
}

// private helper function to refresh targets in CreateNewClusterMap
std::vector<NodeInfo> ClusterMap::GetTargets(ValkeyModuleCtx* ctx,
                                             FanoutTargetMode target_mode) {
  size_t num_nodes;
  auto nodes = vmsdk::MakeUniqueValkeyClusterNodesList(ctx, &num_nodes);

  std::vector<NodeInfo> selected_targets;

  if (target_mode == FanoutTargetMode::kPrimary) {
    // Select all primary (master) nodes directly
    for (size_t i = 0; i < num_nodes; ++i) {
      std::string node_id(nodes.get()[i], VALKEYMODULE_NODE_ID_LEN);
      char ip[INET6_ADDRSTRLEN] = "";
      char master_id[VALKEYMODULE_NODE_ID_LEN] = "";
      int port;
      int flags;

      VMSDK_LOG(NOTICE, ctx)
          << "Processing node " << i << ", node_id: " << node_id;

      if (ValkeyModule_GetClusterNodeInfo(ctx, node_id.c_str(), ip, master_id,
                                          &port, &flags) != VALKEYMODULE_OK) {
        VMSDK_LOG_EVERY_N_SEC(DEBUG, ctx, 1)
            << "Failed to get node info for node " << node_id
            << ", skipping node...";
        continue;
      }

      if (flags & (VALKEYMODULE_NODE_PFAIL | VALKEYMODULE_NODE_FAIL)) {
        VMSDK_LOG_EVERY_N_SEC(DEBUG, ctx, 1)
            << "Node " << node_id << " (" << ip
            << ") is failing, skipping for fanout...";
        continue;
      }

      // Only select master nodes
      if (flags & VALKEYMODULE_NODE_MASTER) {
        NodeInfo node_info;
        node_info.node_id = node_id;
        node_info.role = NodeInfo::NodeRole::kPrimary;
        if (flags & VALKEYMODULE_NODE_MYSELF) {
          node_info.location = NodeInfo::NodeLocation::kLocal;
          node_info.address = "";
        } else {
          node_info.location = NodeInfo::NodeLocation::kRemote;
          node_info.address =
              absl::StrCat(ip, ":", coordinator::GetCoordinatorPort(port));
        }
        selected_targets.push_back(std::move(node_info));
      }
    }
  } else if (target_mode == FanoutTargetMode::kAll) {
    // Select all nodes (both primary and replica)
    for (size_t i = 0; i < num_nodes; ++i) {
      std::string node_id(nodes.get()[i], VALKEYMODULE_NODE_ID_LEN);
      char ip[INET6_ADDRSTRLEN] = "";
      char master_id[VALKEYMODULE_NODE_ID_LEN] = "";
      int port;
      int flags;
      if (ValkeyModule_GetClusterNodeInfo(ctx, node_id.c_str(), ip, master_id,
                                          &port, &flags) != VALKEYMODULE_OK) {
        VMSDK_LOG_EVERY_N_SEC(DEBUG, ctx, 1)
            << "Failed to get node info for node " << node_id
            << ", skipping node...";
        continue;
      }

      if (flags & (VALKEYMODULE_NODE_PFAIL | VALKEYMODULE_NODE_FAIL)) {
        VMSDK_LOG_EVERY_N_SEC(DEBUG, ctx, 1)
            << "Node " << node_id << " (" << ip
            << ") is failing, skipping for fanout...";
        continue;
      }

      // Select all nodes (both master and replica)
      NodeInfo node_info;
      node_info.node_id = node_id;
      node_info.role = (flags & VALKEYMODULE_NODE_MASTER)
                           ? NodeInfo::NodeRole::kPrimary
                           : NodeInfo::NodeRole::kReplica;
      if (flags & VALKEYMODULE_NODE_MYSELF) {
        node_info.location = NodeInfo::NodeLocation::kLocal;
        node_info.address = "";
      } else {
        node_info.location = NodeInfo::NodeLocation::kRemote;
        node_info.address =
            absl::StrCat(ip, ":", coordinator::GetCoordinatorPort(port));
      }
      selected_targets.push_back(std::move(node_info));
    }
  } else {
    CHECK(target_mode == FanoutTargetMode::kRandom ||
          target_mode == FanoutTargetMode::kReplicasOnly);
    // Original logic: group master and replica into shards and randomly
    // select one, unless confined to replicas only
    absl::flat_hash_map<std::string, std::vector<size_t>>
        shard_id_to_node_indices;

    for (size_t i = 0; i < num_nodes; ++i) {
      std::string node_id(nodes.get()[i], VALKEYMODULE_NODE_ID_LEN);
      char ip[INET6_ADDRSTRLEN] = "";
      char master_id[VALKEYMODULE_NODE_ID_LEN] = "";
      int port;
      int flags;
      if (ValkeyModule_GetClusterNodeInfo(ctx, node_id.c_str(), ip, master_id,
                                          &port, &flags) != VALKEYMODULE_OK) {
        VMSDK_LOG_EVERY_N_SEC(DEBUG, ctx, 1)
            << "Failed to get node info for node " << node_id
            << ", skipping node...";
        continue;
      }
      auto master_id_str = std::string(master_id, VALKEYMODULE_NODE_ID_LEN);
      if (flags & (VALKEYMODULE_NODE_PFAIL | VALKEYMODULE_NODE_FAIL)) {
        VMSDK_LOG_EVERY_N_SEC(DEBUG, ctx, 1)
            << "Node " << node_id << " (" << ip
            << ") is failing, skipping for fanout...";
        continue;
      }
      if (flags & VALKEYMODULE_NODE_MASTER) {
        master_id_str = node_id;
        if (target_mode == FanoutTargetMode::kReplicasOnly) {
          continue;
        }
      }

      // Store only the node index
      shard_id_to_node_indices[master_id_str].push_back(i);
    }

    // Random selection first, then create only the selected target objects
    absl::BitGen gen;
    for (const auto& [shard_id, node_indices] : shard_id_to_node_indices) {
      size_t index = absl::Uniform(gen, 0u, node_indices.size());
      size_t selected_node_index = node_indices.at(index);

      // Re-fetch node info only for the selected node
      std::string node_id(nodes.get()[selected_node_index],
                          VALKEYMODULE_NODE_ID_LEN);
      char ip[INET6_ADDRSTRLEN] = "";
      char master_id[VALKEYMODULE_NODE_ID_LEN] = "";
      int port;
      int flags;
      if (ValkeyModule_GetClusterNodeInfo(ctx, node_id.c_str(), ip, master_id,
                                          &port, &flags) != VALKEYMODULE_OK) {
        continue;
      }

      // Create target object only for the selected node
      NodeInfo node_info;
      node_info.node_id = node_id;
      node_info.role = (flags & VALKEYMODULE_NODE_MASTER)
                           ? NodeInfo::NodeRole::kPrimary
                           : NodeInfo::NodeRole::kReplica;
      if (flags & VALKEYMODULE_NODE_MYSELF) {
        node_info.location = NodeInfo::NodeLocation::kLocal;
        node_info.address = "";
      } else {
        node_info.location = NodeInfo::NodeLocation::kRemote;
        node_info.address =
            absl::StrCat(ip, ":", coordinator::GetCoordinatorPort(port));
      }
      selected_targets.push_back(std::move(node_info));
    }
  }
  return selected_targets;
}

// create a new cluster map in the background
std::shared_ptr<ClusterMap> ClusterMap::CreateNewClusterMap(
    ValkeyModuleCtx* ctx) {
  auto new_map = std::shared_ptr<ClusterMap>(new ClusterMap());

  // Pre-compute all target lists
  new_map->primary_targets_ = GetTargets(ctx, FanoutTargetMode::kPrimary);
  new_map->replica_targets_ = GetTargets(ctx, FanoutTargetMode::kReplicasOnly);
  new_map->all_targets_ = GetTargets(ctx, FanoutTargetMode::kAll);

  // TODO: Build shards_ map from cluster topology
  // TODO: Build owned_slots_ bitset
  // TODO: Compute cluster_slots_fingerprint_

  return new_map;
}

}  // namespace cluster_map
}  // namespace vmsdk
