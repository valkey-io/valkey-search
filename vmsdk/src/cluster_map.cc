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
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
namespace cluster_map {

// configurable variable for cluster map expiration time
static auto cluster_map_expiration_ms =
    vmsdk::config::Number("cluster-map-expiration-ms",
                          5000,      // default: 5 seconds
                          1000,      // min: 1 second
                          3600000);  // max: 1 hour

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

// shard lookups, will return nullptr if shard does not exist
const ShardInfo* ClusterMap::GetShardById(std::string_view shard_id) const {
  auto it = shards_.find(std::string(shard_id));
  if (it != shards_.end()) {
    return &it->second;
  }
  return nullptr;
}

const ShardInfo* ClusterMap::GetShardBySlot(uint16_t slot) const {
  // Find the first entry with start_slot > slot
  auto it = slot_to_shard_map_.upper_bound(slot);
  // If it's the first entry, slot is before any range
  if (it == slot_to_shard_map_.begin()) {
    return nullptr;
  }
  // Move back to the entry that could contain this slot
  --it;
  // Check if slot is within the range [start_slot, end_slot]
  uint16_t start_slot = it->first;
  uint16_t end_slot = it->second.first;
  if (slot >= start_slot && slot <= end_slot) {
    // Return the shard pointer
    return it->second.second;
  }
  // Slot is in a gap, return nullptr
  return nullptr;
}

// generate a random targets vector with one node from each shard
std::vector<NodeInfo> ClusterMap::GetRandomTargets() {
  std::vector<NodeInfo> random_targets;
  absl::BitGen gen;
  for (const auto& [shard_id, shard_info] : shards_) {
    // Collect all nodes in this shard (primary + replicas)
    std::vector<NodeInfo> shard_nodes;
    if (shard_info.primary.has_value()) {
      shard_nodes.push_back(shard_info.primary.value());
    }
    for (const auto& replica : shard_info.replicas) {
      shard_nodes.push_back(replica);
    }
    // Randomly select one node from this shard
    if (!shard_nodes.empty()) {
      size_t index = absl::Uniform(gen, 0u, shard_nodes.size());
      random_targets.push_back(shard_nodes[index]);
    }
  }
  return random_targets;
}

// For shard fingerprint - just sum all slots
uint64_t ClusterMap::compute_shard_fingerprint(
    const std::set<uint16_t>& slots) {
  uint64_t fingerprint = 0;
  for (uint16_t slot : slots) {
    fingerprint += slot;
  }
  return fingerprint;
}

// For cluster fingerprint - sum all shard fingerprints with shard ID hash
uint64_t ClusterMap::compute_cluster_fingerprint(
    const absl::flat_hash_map<std::string, ShardInfo>& shards) {
  uint64_t fingerprint = 0;

  for (const auto& [shard_id, shard] : shards) {
    // Simple string hash that's deterministic
    uint64_t shard_id_hash = 0;
    for (char c : shard_id) {
      shard_id_hash = shard_id_hash * 31 + c;
    }

    fingerprint += shard.slots_fingerprint + shard_id_hash;
  }

  return fingerprint;
}

// Helper function to parse node info from CLUSTER SLOTS reply
NodeInfo ClusterMap::ParseNodeInfo(ValkeyModuleCallReply* node_arr,
                                   bool is_local_shard,
                                   NodeInfo::NodeRole role) {
  CHECK(node_arr) << "CLUSTER_MAP_ERROR: node subarray returned nullptr";
  CHECK(ValkeyModule_CallReplyLength(node_arr) >= 3)
      << "CLUSTER_MAP_ERROR: node subarray returned invalid length, expected "
         ">= 3";

  // Get node ID
  size_t node_id_len;
  const char* node_id_char = ValkeyModule_CallReplyStringPtr(
      ValkeyModule_CallReplyArrayElement(node_arr, 2), &node_id_len);
  CHECK(node_id_char) << "CLUSTER_MAP_ERROR: node id returned nullptr";

  // Get IP
  size_t node_ip_len;
  const char* node_ip_char = ValkeyModule_CallReplyStringPtr(
      ValkeyModule_CallReplyArrayElement(node_arr, 0), &node_ip_len);
  CHECK(node_ip_char) << "CLUSTER_MAP_ERROR: node ip returned nullptr";

  // Get port
  long long node_port = ValkeyModule_CallReplyInteger(
      ValkeyModule_CallReplyArrayElement(node_arr, 1));
  CHECK(node_port) << "CLUSTER_MAP_ERROR: node port returned nullptr";

  return NodeInfo{.node_id = std::string(node_id_char, node_id_len),
                  .role = role,
                  .location = is_local_shard ? NodeInfo::NodeLocation::kLocal
                                             : NodeInfo::NodeLocation::kRemote,
                  .ip = std::string(node_ip_char, node_ip_len),
                  .port = is_local_shard ? 0 : static_cast<int>(node_port),
                  .shard = nullptr};
}

// Helper function to check if any node in the slot range is local
bool ClusterMap::IsLocalShard(ValkeyModuleCallReply* slot_range,
                              const char* my_node_id) {
  size_t slot_len = ValkeyModule_CallReplyLength(slot_range);

  // Check all nodes (primary at index 2, replicas at index 3+)
  for (size_t i = 2; i < slot_len; i++) {
    ValkeyModuleCallReply* node_arr =
        ValkeyModule_CallReplyArrayElement(slot_range, i);
    if (!node_arr) continue;

    size_t node_id_len;
    const char* node_id_char = ValkeyModule_CallReplyStringPtr(
        ValkeyModule_CallReplyArrayElement(node_arr, 2), &node_id_len);

    if (node_id_char && node_id_len == VALKEYMODULE_NODE_ID_LEN &&
        memcmp(node_id_char, my_node_id, VALKEYMODULE_NODE_ID_LEN) == 0) {
      return true;
    }
  }
  return false;
}

// Helper function to parse slot range and create ShardInfo
void ClusterMap::ProcessSlotRange(
    ValkeyModuleCallReply* slot_range, const char* my_node_id,
    absl::flat_hash_map<std::string, ShardInfo>& shards,
    std::bitset<k_num_slots>& owned_slots,
    std::vector<NodeInfo>& primary_targets,
    std::vector<NodeInfo>& replica_targets, std::vector<NodeInfo>& all_targets,
    std::vector<SlotRangeInfo>& slot_ranges) {
  CHECK(slot_range)
      << "CLUSTER_MAP_ERROR: ValkeyModule_Call CLUSTER SLOTS result returned "
         "invalid slot range sub-array";
  CHECK(ValkeyModule_CallReplyType(slot_range) == VALKEYMODULE_REPLY_ARRAY)
      << "CLUSTER_MAP_ERROR: slot range sub-array has incorrect type";

  size_t slot_len = ValkeyModule_CallReplyLength(slot_range);
  CHECK(slot_len >= 3) << "CLUSTER_MAP_ERROR: slot range sub-array has invalid "
                          "length, should be at least 3";

  // Parse start and end slots
  long long start = ValkeyModule_CallReplyInteger(
      ValkeyModule_CallReplyArrayElement(slot_range, 0));
  long long end = ValkeyModule_CallReplyInteger(
      ValkeyModule_CallReplyArrayElement(slot_range, 1));

  // Build slot set
  std::set<uint16_t> slot_set;
  for (long long slot = start; slot <= end; slot++) {
    if (slot >= 0 && slot < k_num_slots) {
      slot_set.insert(static_cast<uint16_t>(slot));
    }
  }

  // Determine if this is a local shard
  bool is_local_shard = IsLocalShard(slot_range, my_node_id);

  // Parse primary node
  ValkeyModuleCallReply* primary_node_arr =
      ValkeyModule_CallReplyArrayElement(slot_range, 2);
  NodeInfo primary_node = ParseNodeInfo(primary_node_arr, is_local_shard,
                                        NodeInfo::NodeRole::kPrimary);

  // Parse replica nodes
  std::vector<NodeInfo> replicas;
  for (size_t j = 3; j < slot_len; j++) {
    ValkeyModuleCallReply* replica_node_arr =
        ValkeyModule_CallReplyArrayElement(slot_range, j);
    replicas.push_back(ParseNodeInfo(replica_node_arr, is_local_shard,
                                     NodeInfo::NodeRole::kReplica));
  }

  // Mark owned slots if local
  if (is_local_shard) {
    for (long long slot = start; slot <= end; slot++) {
      CHECK(slot >= 0 && slot < k_num_slots)
          << "CLUSTER_MAP_ERROR: invalid slot number";
      owned_slots[slot] = true;
    }
  }

  // Create and insert ShardInfo
  std::string shard_id = primary_node.node_id;
  CHECK(shards.find(shard_id) == shards.end())
      << "CLUSTER_MAP_ERROR: Duplicate shard_id in CLUSTER SLOTS response";

  ShardInfo shard;
  shard.shard_id = shard_id;
  shard.owned_slots = slot_set;
  shard.slots_fingerprint = ClusterMap::compute_shard_fingerprint(slot_set);
  shard.primary = primary_node;
  shard.replicas = std::move(replicas);

  auto [inserted_it, success] = shards.insert({shard_id, std::move(shard)});
  CHECK(success) << "CLUSTER_MAP_ERROR: Failed to insert shard";

  // Store slot range info for later
  slot_ranges.push_back(
      {static_cast<uint16_t>(start), static_cast<uint16_t>(end), shard_id});

  // Fix shard pointers and populate target lists
  if (inserted_it->second.primary.has_value()) {
    inserted_it->second.primary->shard = &(inserted_it->second);
    primary_targets.push_back(inserted_it->second.primary.value());
    all_targets.push_back(inserted_it->second.primary.value());
  }
  for (auto& replica : inserted_it->second.replicas) {
    replica.shard = &(inserted_it->second);
    replica_targets.push_back(replica);
    all_targets.push_back(replica);
  }
}

// Helper function to build slot-to-shard map
void ClusterMap::BuildSlotToShardMap(
    std::map<uint16_t, std::pair<uint16_t, const ShardInfo*>>&
        slot_to_shard_map,
    const absl::flat_hash_map<std::string, ShardInfo>& shards,
    const std::vector<SlotRangeInfo>& slot_ranges) {
  for (const auto& range_info : slot_ranges) {
    auto shard_it = shards.find(range_info.shard_id);
    CHECK(shard_it != shards.end())
        << "CLUSTER_MAP_ERROR: Shard not found when building slot map";
    slot_to_shard_map[range_info.start_slot] =
        std::make_pair(range_info.end_slot, &(shard_it->second));
  }
}

// Helper function to check if cluster map is full
bool ClusterMap::CheckClusterMapFull(
    const std::map<uint16_t, std::pair<uint16_t, const ShardInfo*>>& slot_map) {
  if (slot_map.empty() || slot_map.begin()->first != 0) {
    return false;
  }

  uint16_t expected_next = 0;
  for (const auto& [start_slot, range_and_shard] : slot_map) {
    if (start_slot != expected_next) {
      return false;
    }
    expected_next = range_and_shard.first + 1;
  }

  return expected_next == k_num_slots;
}

// Main function - now much cleaner
std::shared_ptr<ClusterMap> ClusterMap::CreateNewClusterMap(
    ValkeyModuleCtx* ctx) {
  auto new_map = std::shared_ptr<ClusterMap>(new ClusterMap());
  new_map->is_cluster_map_full_ = false;

  // Call CLUSTER SLOTS
  auto reply = vmsdk::UniquePtrValkeyCallReply(
      ValkeyModule_Call(ctx, "CLUSTER", "c", "SLOTS"));
  CHECK(reply)
      << "CLUSTER_MAP_ERROR: ValkeyModule_Call CLUSTER SLOTS returned nullptr";
  CHECK(ValkeyModule_CallReplyType(reply.get()) == VALKEYMODULE_REPLY_ARRAY)
      << "CLUSTER_MAP_ERROR: CLUSTER SLOTS returns incorrect type";

  // Get local node ID
  const char* my_node_id = ValkeyModule_GetMyClusterID();
  CHECK(my_node_id)
      << "CLUSTER_MAP_ERROR: ValkeyModule_GetMyClusterID returned nullptr";

  // Process each slot range
  std::vector<SlotRangeInfo> slot_ranges;
  size_t len = ValkeyModule_CallReplyLength(reply.get());
  for (size_t i = 0; i < len; ++i) {
    ValkeyModuleCallReply* slot_range =
        ValkeyModule_CallReplyArrayElement(reply.get(), i);
    ProcessSlotRange(slot_range, my_node_id, new_map->shards_,
                     new_map->owned_slots_, new_map->primary_targets_,
                     new_map->replica_targets_, new_map->all_targets_,
                     slot_ranges);
  }

  // Build slot-to-shard map
  BuildSlotToShardMap(new_map->slot_to_shard_map_, new_map->shards_,
                      slot_ranges);

  // Check if cluster map is full
  new_map->is_cluster_map_full_ =
      CheckClusterMapFull(new_map->slot_to_shard_map_);

  // Compute cluster-level fingerprint
  new_map->cluster_slots_fingerprint_ =
      compute_cluster_fingerprint(new_map->shards_);

  // Set expiration time
  new_map->expiration_tp_ =
      std::chrono::steady_clock::now() +
      std::chrono::milliseconds(cluster_map_expiration_ms.GetValue());

  return new_map;
}

// debug only, print out cluster map
void ClusterMap::PrintClusterMap(std::shared_ptr<ClusterMap> map) {
  VMSDK_LOG(NOTICE, nullptr) << "=== Cluster Map Created ===";
  VMSDK_LOG(NOTICE, nullptr)
      << "is_cluster_map_full_: " << map->is_cluster_map_full_;
  VMSDK_LOG(NOTICE, nullptr)
      << "cluster_slots_fingerprint_: " << map->cluster_slots_fingerprint_;

  // Print owned slots using slot_to_shard_map_
  size_t owned_count = map->owned_slots_.count();
  VMSDK_LOG(NOTICE, nullptr) << "owned_slots_ count: " << owned_count;
  if (owned_count > 0) {
    std::string owned_ranges;
    for (const auto& [start_slot, range_and_shard] : map->slot_to_shard_map_) {
      uint16_t end_slot = range_and_shard.first;
      const ShardInfo* shard = range_and_shard.second;

      // Only include if this is a local shard
      if (shard->primary.has_value() &&
          shard->primary->location == NodeInfo::NodeLocation::kLocal) {
        if (!owned_ranges.empty()) owned_ranges += ", ";
        owned_ranges +=
            std::to_string(start_slot) + "-" + std::to_string(end_slot);
      } else {
        // Check replicas
        bool is_local = false;
        for (const auto& replica : shard->replicas) {
          if (replica.location == NodeInfo::NodeLocation::kLocal) {
            is_local = true;
            break;
          }
        }
        if (is_local) {
          if (!owned_ranges.empty()) owned_ranges += ", ";
          owned_ranges +=
              std::to_string(start_slot) + "-" + std::to_string(end_slot);
        }
      }
    }
    if (!owned_ranges.empty()) {
      VMSDK_LOG(NOTICE, nullptr) << "owned_slots_ ranges: " << owned_ranges;
    }
  }

  // Print shards
  VMSDK_LOG(NOTICE, nullptr) << "shards_ count: " << map->shards_.size();
  for (const auto& [shard_id, shard_info] : map->shards_) {
    VMSDK_LOG(NOTICE, nullptr) << "Shard ID: " << shard_id;
    VMSDK_LOG(NOTICE, nullptr)
        << "  owned_slots count: " << shard_info.owned_slots.size();
    VMSDK_LOG(NOTICE, nullptr)
        << "  slots_fingerprint: " << shard_info.slots_fingerprint;

    // Simplified slot range printing using slot_to_shard_map_
    if (!shard_info.owned_slots.empty()) {
      std::string slot_ranges;
      for (const auto& [start_slot, range_and_shard] :
           map->slot_to_shard_map_) {
        if (range_and_shard.second == &shard_info) {
          uint16_t end_slot = range_and_shard.first;
          if (!slot_ranges.empty()) slot_ranges += ", ";
          slot_ranges +=
              std::to_string(start_slot) + "-" + std::to_string(end_slot);
        }
      }
      VMSDK_LOG(NOTICE, nullptr) << "  slot ranges: " << slot_ranges;
    }

    // Print primary node info
    if (shard_info.primary.has_value()) {
      const auto& primary = shard_info.primary.value();
      VMSDK_LOG(NOTICE, nullptr) << "  Primary Node:";
      VMSDK_LOG(NOTICE, nullptr) << "    node_id: " << primary.node_id;
      VMSDK_LOG(NOTICE, nullptr)
          << "    role: "
          << (primary.role == NodeInfo::NodeRole::kPrimary ? "Primary"
                                                           : "Replica");
      VMSDK_LOG(NOTICE, nullptr)
          << "    location: "
          << (primary.location == NodeInfo::NodeLocation::kLocal ? "Local"
                                                                 : "Remote");
      VMSDK_LOG(NOTICE, nullptr)
          << "    ip: " << (primary.ip.empty() ? "(local)" : primary.ip);
      VMSDK_LOG(NOTICE, nullptr)
          << "    port: "
          << (primary.port == 0 ? "(local)" : absl::StrCat(primary.port));
    } else {
      VMSDK_LOG(NOTICE, nullptr) << "  Primary Node: (none)";
    }

    // Print replica nodes info
    VMSDK_LOG(NOTICE, nullptr)
        << "  Replicas count: " << shard_info.replicas.size();
    for (size_t i = 0; i < shard_info.replicas.size(); ++i) {
      const auto& replica = shard_info.replicas[i];
      VMSDK_LOG(NOTICE, nullptr) << "  Replica[" << i << "]:";
      VMSDK_LOG(NOTICE, nullptr) << "    node_id: " << replica.node_id;
      VMSDK_LOG(NOTICE, nullptr)
          << "    role: "
          << (replica.role == NodeInfo::NodeRole::kPrimary ? "Primary"
                                                           : "Replica");
      VMSDK_LOG(NOTICE, nullptr)
          << "    location: "
          << (replica.location == NodeInfo::NodeLocation::kLocal ? "Local"
                                                                 : "Remote");
      VMSDK_LOG(NOTICE, nullptr)
          << "    ip: " << (replica.ip.empty() ? "(local)" : replica.ip);
      VMSDK_LOG(NOTICE, nullptr)
          << "    port: "
          << (replica.port == 0 ? "(local)" : absl::StrCat(replica.port));
    }
  }

  // Print pre-computed target lists (unchanged)
  VMSDK_LOG(NOTICE, nullptr)
      << "primary_targets_ count: " << map->primary_targets_.size();
  VMSDK_LOG(NOTICE, nullptr)
      << "replica_targets_ count: " << map->replica_targets_.size();
  VMSDK_LOG(NOTICE, nullptr)
      << "all_targets_ count: " << map->all_targets_.size();

  VMSDK_LOG(NOTICE, nullptr) << "=== End Cluster Map ===";
}

}  // namespace cluster_map
}  // namespace vmsdk