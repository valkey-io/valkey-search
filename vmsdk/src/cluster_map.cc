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
#include "vmsdk/src/valkey_module_api/valkey_module.h"

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

bool ClusterMap::GetIsClusterMapFull() const { return is_cluster_map_full_; }

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

void PrintReplyStructure(ValkeyModuleCallReply* reply, int depth = 0) {
  if (!reply) {
    VMSDK_LOG(NOTICE, nullptr) << std::string(depth * 2, ' ') << "NULL";
    return;
  }

  std::string indent(depth * 2, ' ');
  int reply_type = ValkeyModule_CallReplyType(reply);

  switch (reply_type) {
    case VALKEYMODULE_REPLY_ARRAY: {
      size_t len = ValkeyModule_CallReplyLength(reply);
      VMSDK_LOG(NOTICE, nullptr) << indent << "ARRAY[" << len << "] {";
      for (size_t i = 0; i < len; i++) {
        VMSDK_LOG(NOTICE, nullptr) << indent << "  [" << i << "]:";
        ValkeyModuleCallReply* element =
            ValkeyModule_CallReplyArrayElement(reply, i);
        PrintReplyStructure(element, depth + 2);
      }
      VMSDK_LOG(NOTICE, nullptr) << indent << "}";
      break;
    }
    case VALKEYMODULE_REPLY_STRING: {
      size_t len;
      const char* str = ValkeyModule_CallReplyStringPtr(reply, &len);
      VMSDK_LOG(NOTICE, nullptr)
          << indent << "STRING: \"" << std::string(str, len) << "\"";
      break;
    }
    case VALKEYMODULE_REPLY_INTEGER: {
      long long val = ValkeyModule_CallReplyInteger(reply);
      VMSDK_LOG(NOTICE, nullptr) << indent << "INTEGER: " << val;
      break;
    }
    case VALKEYMODULE_REPLY_ERROR: {
      size_t len;
      const char* str = ValkeyModule_CallReplyStringPtr(reply, &len);
      VMSDK_LOG(NOTICE, nullptr)
          << indent << "ERROR: \"" << std::string(str, len) << "\"";
      break;
    }
    default:
      VMSDK_LOG(NOTICE, nullptr) << indent << "UNKNOWN_TYPE: " << reply_type;
      break;
  }
}

// create a new cluster map in the background
std::shared_ptr<ClusterMap> ClusterMap::CreateNewClusterMap(
    ValkeyModuleCtx* ctx) {
  auto new_map = std::shared_ptr<ClusterMap>(new ClusterMap());

  // Pre-compute all target lists
  new_map->primary_targets_ = GetTargets(ctx, FanoutTargetMode::kPrimary);
  new_map->replica_targets_ = GetTargets(ctx, FanoutTargetMode::kReplicasOnly);
  new_map->all_targets_ = GetTargets(ctx, FanoutTargetMode::kAll);

  // call CLUSTER_SLOTS from Valkey Module API
  auto reply = vmsdk::UniquePtrValkeyCallReply(
      ValkeyModule_Call(ctx, "CLUSTER", "c", "SLOTS"));
  if (reply == nullptr) {
    // if Valkey Module API returns nullptr, return an empty map
    VMSDK_IO_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
        << "CLUSTER_MAP_ERROR: CLUSTER SLOTS call returns nullptr";
    new_map->is_cluster_map_full_ = false;
    return new_map;
  }

  auto reply_type = ValkeyModule_CallReplyType(reply.get());
  if (reply_type != VALKEYMODULE_REPLY_ARRAY) {
    // if Valkey Module API returns incorrect type, return an empty map
    VMSDK_IO_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
        << "CLUSTER_MAP_ERROR: CLUSTER SLOTS call returns incorrect type, "
           "expect VALKEYMODULE_REPLY_ARRAY but got "
        << reply_type;
    new_map->is_cluster_map_full_ = false;
    return new_map;
  }

  // // Print the entire reply structure
  // VMSDK_LOG(NOTICE, nullptr) << "=== CLUSTER SLOTS Reply Structure ===";
  // PrintReplyStructure(reply.get());
  // VMSDK_LOG(NOTICE, nullptr) << "=== End of CLUSTER SLOTS Reply ===";

  // Track which slots have been assigned across the entire cluster
  std::bitset<k_num_slots> assigned_slots;

  // Get local node ID to identify which shard we belong to
  char my_node_id[VALKEYMODULE_NODE_ID_LEN];
  size_t len = ValkeyModule_CallReplyLength(reply.get());

  // reply is an array of arrays
  // each array element should contain at least 3 elements
  // (1) start slot range (2) end slot range (3) master info (4)... replica info
  for (size_t i = 0; i < len; ++i) {
    ValkeyModuleCallReply* slot_range =
        ValkeyModule_CallReplyArrayElement(reply.get(), i);
    if (!slot_range ||
        ValkeyModule_CallReplyType(slot_range) != VALKEYMODULE_REPLY_ARRAY) {
      continue;
    }
    size_t slot_len = ValkeyModule_CallReplyLength(slot_range);
    // each array element should have at least 3 elements
    if (slot_len < 3) {
      continue;
    }
    // Get start and end slots
    ValkeyModuleCallReply* start_slot =
        ValkeyModule_CallReplyArrayElement(slot_range, 0);
    ValkeyModuleCallReply* end_slot =
        ValkeyModule_CallReplyArrayElement(slot_range, 1);
    long long start = ValkeyModule_CallReplyInteger(start_slot);
    long long end = ValkeyModule_CallReplyInteger(end_slot);
    // Mark slots as assigned in cluster
    std::set<uint16_t> slot_set;
    for (long long slot = start; slot <= end; slot++) {
      if (slot >= 0 && slot < k_num_slots) {
        assigned_slots[slot] = true;
        slot_set.insert(static_cast<uint16_t>(slot));
      }
    }
    // node info is an array
    // (1) address (2) port number (3) node_id (4) hostname (optional)
    // Get master node ID
    ValkeyModuleCallReply* master_node =
        ValkeyModule_CallReplyArrayElement(slot_range, 2);
    if (!master_node || ValkeyModule_CallReplyLength(master_node) < 3) {
      continue;
    }
    size_t master_id_len;
    const char* master_id_str = ValkeyModule_CallReplyStringPtr(
        ValkeyModule_CallReplyArrayElement(master_node, 2), &master_id_len);
    std::string master_id(master_id_str, master_id_len);

    // Check if any node in this shard is local
    bool is_local_shard = false;
    for (size_t j = 2; j < slot_len; j++) {
      ValkeyModuleCallReply* node =
          ValkeyModule_CallReplyArrayElement(slot_range, j);
      if (!node || ValkeyModule_CallReplyLength(node) < 3) continue;

      size_t node_id_len;
      const char* node_id_str = ValkeyModule_CallReplyStringPtr(
          ValkeyModule_CallReplyArrayElement(node, 2), &node_id_len);
      std::string node_id(node_id_str, node_id_len);

      char ip[INET6_ADDRSTRLEN];
      char master_buf[VALKEYMODULE_NODE_ID_LEN];
      int port, flags;
      if (ValkeyModule_GetClusterNodeInfo(ctx, node_id.c_str(), ip, master_buf,
                                          &port, &flags) == VALKEYMODULE_OK &&
          (flags & VALKEYMODULE_NODE_MYSELF)) {
        is_local_shard = true;
        break;
      }
    }
    // Mark owned slots
    if (is_local_shard) {
      for (long long slot = start; slot <= end; slot++) {
        if (slot >= 0 && slot < k_num_slots) {
          new_map->owned_slots_[slot] = true;
        }
      }
    }
    // Update shards map
    auto it = new_map->shards_.find(master_id);
    if (it == new_map->shards_.end()) {
      ShardInfo shard;
      shard.shard_id = master_id;
      shard.owned_slots = slot_set;
      shard.slots_fingerprint = 0;
      new_map->shards_[master_id] = std::move(shard);
    } else {
      it->second.owned_slots.insert(slot_set.begin(), slot_set.end());
    }
  }
  new_map->is_cluster_map_full_ = assigned_slots.all();

  // Log cluster map state
  VMSDK_LOG(NOTICE, nullptr) << "=== Cluster Map Created ===";
  VMSDK_LOG(NOTICE, nullptr)
      << "is_cluster_map_full_: " << new_map->is_cluster_map_full_;

  // Log owned_slots_
  size_t owned_count = new_map->owned_slots_.count();
  VMSDK_LOG(NOTICE, nullptr) << "owned_slots_ count: " << owned_count;
  if (owned_count > 0) {
    std::string owned_ranges;
    int range_start = -1;
    int range_end = -1;
    for (size_t i = 0; i < k_num_slots; i++) {
      if (new_map->owned_slots_[i]) {
        if (range_start == -1) {
          range_start = i;
          range_end = i;
        } else if (i == range_end + 1) {
          range_end = i;
        } else {
          if (!owned_ranges.empty()) owned_ranges += ", ";
          owned_ranges +=
              std::to_string(range_start) + "-" + std::to_string(range_end);
          range_start = i;
          range_end = i;
        }
      }
    }
    if (range_start != -1) {
      if (!owned_ranges.empty()) owned_ranges += ", ";
      owned_ranges +=
          std::to_string(range_start) + "-" + std::to_string(range_end);
    }
    VMSDK_LOG(NOTICE, nullptr) << "owned_slots_ ranges: " << owned_ranges;
  }

  // Log shards_
  VMSDK_LOG(NOTICE, nullptr) << "shards_ count: " << new_map->shards_.size();
  for (const auto& [shard_id, shard_info] : new_map->shards_) {
    VMSDK_LOG(NOTICE, nullptr) << "Shard ID: " << shard_id;
    VMSDK_LOG(NOTICE, nullptr)
        << "  owned_slots count: " << shard_info.owned_slots.size();
    if (!shard_info.owned_slots.empty()) {
      std::string slot_ranges;
      auto it = shard_info.owned_slots.begin();
      int range_start = *it;
      int range_end = *it;
      ++it;
      for (; it != shard_info.owned_slots.end(); ++it) {
        if (*it == range_end + 1) {
          range_end = *it;
        } else {
          if (!slot_ranges.empty()) slot_ranges += ", ";
          slot_ranges +=
              std::to_string(range_start) + "-" + std::to_string(range_end);
          range_start = *it;
          range_end = *it;
        }
      }
      if (!slot_ranges.empty()) slot_ranges += ", ";
      slot_ranges +=
          std::to_string(range_start) + "-" + std::to_string(range_end);
      VMSDK_LOG(NOTICE, nullptr) << "  slot ranges: " << slot_ranges;
    }
  }

  VMSDK_LOG(NOTICE, nullptr) << "=== End Cluster Map ===";

  return new_map;
}

}  // namespace cluster_map
}  // namespace vmsdk
