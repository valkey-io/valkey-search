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

const std::string VALKEY_MODULE_CALL_ERROR_MSG =
    "ValkeyModule_Call returned invalid result";

// configurable variable for cluster map expiration time
static auto cluster_map_expiration_ms =
    vmsdk::config::Number("cluster-map-expiration-ms",
                          250,       // default: 0.25 second
                          0,         // min: 0 (no cache)
                          3600000);  // max: 1 hour

// shard lookups, will return nullptr if shard does not exist
const ShardInfo* ClusterMap::GetShardById(std::string_view shard_id) const {
  auto it = shards_.find(std::string(shard_id));
  return (it != shards_.end()) ? &it->second : nullptr;
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

// For shard fingerprint - hash the slot ranges
uint64_t ClusterMap::ComputeShardFingerprint(
    const std::map<uint16_t, uint16_t>& slot_ranges) {
  if (slot_ranges.empty()) {
    return 0;
  }

  // Convert map to vector for contiguous memory
  std::vector<std::pair<uint16_t, uint16_t>> ranges_vec;
  ranges_vec.reserve(slot_ranges.size());
  for (const auto& [start, end] : slot_ranges) {
    ranges_vec.push_back({start, end});
  }

  uint64_t fingerprint;
  highwayhash::HHStateT<HH_TARGET> state(kHashKey);
  highwayhash::HighwayHashT(
      &state, reinterpret_cast<const char*>(ranges_vec.data()),
      ranges_vec.size() * sizeof(std::pair<uint16_t, uint16_t>), &fingerprint);
  return fingerprint;
}

// For cluster fingerprint - hash all shard fingerprints with shard IDs
uint64_t ClusterMap::ComputeClusterFingerprint() {
  if (shards_.empty()) {
    return 0;
  }

  // Create a sorted vector for deterministic ordering
  struct ShardEntry {
    uint64_t shard_id_hash;
    uint64_t slots_fingerprint;
  };
  std::vector<ShardEntry> shard_entries;
  shard_entries.reserve(shards_.size());

  for (const auto& [shard_id, shard] : shards_) {
    // Hash the shard ID
    uint64_t shard_id_hash;
    highwayhash::HHStateT<HH_TARGET> state(kHashKey);
    highwayhash::HighwayHashT(&state, shard_id.c_str(), shard_id.size(),
                              &shard_id_hash);

    shard_entries.push_back({.shard_id_hash = shard_id_hash,
                             .slots_fingerprint = shard.slots_fingerprint});
  }

  // Sort for deterministic ordering
  std::sort(shard_entries.begin(), shard_entries.end(),
            [](const ShardEntry& a, const ShardEntry& b) {
              return a.shard_id_hash < b.shard_id_hash;
            });

  // Hash the sorted entries
  uint64_t fingerprint;
  highwayhash::HHStateT<HH_TARGET> state(kHashKey);
  highwayhash::HighwayHashT(
      &state, reinterpret_cast<const char*>(shard_entries.data()),
      shard_entries.size() * sizeof(ShardEntry), &fingerprint);
  return fingerprint;
}

// Helper function to parse node info from CLUSTER SLOTS reply
NodeInfo ClusterMap::ParseNodeInfo(ValkeyModuleCallReply* node_arr,
                                   bool is_local_shard, bool is_primary) {
  CHECK(node_arr) << VALKEY_MODULE_CALL_ERROR_MSG;
  // each node array should have exactly 4 elements
  CHECK(ValkeyModule_CallReplyLength(node_arr) == 4)
      << VALKEY_MODULE_CALL_ERROR_MSG;

  // Get node ID
  size_t node_id_len;
  const char* node_id_char = ValkeyModule_CallReplyStringPtr(
      ValkeyModule_CallReplyArrayElement(node_arr, 2), &node_id_len);
  CHECK(node_id_char) << VALKEY_MODULE_CALL_ERROR_MSG;

  // Get IP
  size_t node_ip_len;
  const char* node_ip_char = ValkeyModule_CallReplyStringPtr(
      ValkeyModule_CallReplyArrayElement(node_arr, 0), &node_ip_len);
  CHECK(node_ip_char) << VALKEY_MODULE_CALL_ERROR_MSG;

  // Get port
  long long node_port = ValkeyModule_CallReplyInteger(
      ValkeyModule_CallReplyArrayElement(node_arr, 1));
  CHECK(node_port) << VALKEY_MODULE_CALL_ERROR_MSG;

  // Get additional network metadata
  // Depending on the client RESP protocol version, the additional network
  // metadata might be a map(RESP3), or a flattened array(RESP2)
  std::unordered_map<std::string, std::string> additional_network_metadata;
  auto reply_metadata = ValkeyModule_CallReplyArrayElement(node_arr, 3);
  CHECK(reply_metadata) << VALKEY_MODULE_CALL_ERROR_MSG;
  CHECK(ValkeyModule_CallReplyType(reply_metadata) == VALKEYMODULE_REPLY_MAP ||
        ValkeyModule_CallReplyType(reply_metadata) == VALKEYMODULE_REPLY_ARRAY)
      << VALKEY_MODULE_CALL_ERROR_MSG;
  if (ValkeyModule_CallReplyType(reply_metadata) == VALKEYMODULE_REPLY_MAP) {
    ValkeyModuleCallReply* map_key;
    ValkeyModuleCallReply* map_val;
    int idx = 0;
    while (ValkeyModule_CallReplyMapElement(reply_metadata, idx, &map_key,
                                            &map_val) == VALKEYMODULE_OK) {
      size_t key_len = 0;
      const char* key_str = ValkeyModule_CallReplyStringPtr(map_key, &key_len);
      size_t val_len;
      const char* val_str = ValkeyModule_CallReplyStringPtr(map_val, &val_len);
      additional_network_metadata[std::string(key_str, key_len)] =
          std::string(val_str, val_len);
      idx++;
    }
  } else if (ValkeyModule_CallReplyType(reply_metadata) ==
             VALKEYMODULE_REPLY_ARRAY) {
    // format: ARRAY of [key1, val1, key2, val2, ...]
    size_t array_len = ValkeyModule_CallReplyLength(reply_metadata);
    for (size_t i = 0; i + 1 < array_len; i += 2) {
      auto arr_key = ValkeyModule_CallReplyArrayElement(reply_metadata, i);
      auto arr_val = ValkeyModule_CallReplyArrayElement(reply_metadata, i + 1);
      size_t key_len;
      const char* key_str = ValkeyModule_CallReplyStringPtr(arr_key, &key_len);
      size_t val_len = 0;
      const char* val_str = ValkeyModule_CallReplyStringPtr(arr_val, &val_len);
      additional_network_metadata[std::string(key_str, key_len)] =
          std::string(val_str, val_len);
    }
  }

  std::string node_id_str = std::string(node_id_char, node_id_len);
  SocketAddress addr{
      .ip = is_local_shard ? "" : std::string(node_ip_char, node_ip_len),
      .port = is_local_shard ? 0 : static_cast<uint16_t>(node_port)};

  // Check for duplicate socket addresses across different nodes
  // Skip check for local nodes (they all have empty ip and port 0)
  if (!is_local_shard) {
    auto it = node_to_socket_map_.find(node_id_str);
    if (it != node_to_socket_map_.end()) {
      // Node already seen - verify socket address matches
      if (it->second != addr) {
        VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
            << "Node " << node_id_str
            << " has inconsistent socket addresses: " << it->second.ip << ":"
            << it->second.port << " vs " << addr.ip << ":" << addr.port;
        this->is_consistent_ = false;
      }
    } else {
      // New node - check if this socket address is already used by another node
      for (const auto& [existing_node_id, existing_addr] :
           node_to_socket_map_) {
        if (existing_addr == addr) {
          VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
              << "Duplicate socket address " << addr.ip << ":" << addr.port
              << " found for different nodes: " << existing_node_id << " and "
              << node_id_str;
          this->is_consistent_ = false;
          break;
        }
      }
      node_to_socket_map_[node_id_str] = addr;
    }
  }

  return NodeInfo{.node_id = node_id_str,
                  .is_primary = is_primary,
                  .is_local = is_local_shard,
                  .socket_address = addr,
                  .additional_network_metadata = additional_network_metadata,
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

bool ClusterMap::IsExistingShardConsistent(
    const ShardInfo& existing_shard, const NodeInfo& new_primary,
    const std::vector<NodeInfo>& new_replicas) {
  if (existing_shard.primary.has_value()) {
    const auto& existing_primary = existing_shard.primary.value();
    if (existing_primary.node_id != new_primary.node_id) {
      return false;
    }
    if (existing_primary.socket_address != new_primary.socket_address) {
      return false;
    }
    if (existing_primary.is_local != new_primary.is_local) {
      return false;
    }
  }
  if (existing_shard.replicas.size() != new_replicas.size()) {
    return false;
  }
  for (size_t i = 0; i < existing_shard.replicas.size(); ++i) {
    const auto& existing_replica = existing_shard.replicas[i];
    const auto& new_replica = new_replicas[i];
    if (existing_replica.node_id != new_replica.node_id) {
      return false;
    }
    if (existing_replica.socket_address != new_replica.socket_address) {
      return false;
    }
  }
  return true;
}

// Helper function to parse slot range and create ShardInfo
void ClusterMap::ProcessSlotRange(ValkeyModuleCallReply* slot_range,
                                  const char* my_node_id,
                                  std::vector<SlotRangeInfo>& slot_ranges) {
  CHECK(slot_range) << VALKEY_MODULE_CALL_ERROR_MSG;
  CHECK(ValkeyModule_CallReplyType(slot_range) == VALKEYMODULE_REPLY_ARRAY)
      << VALKEY_MODULE_CALL_ERROR_MSG;
  CHECK(ValkeyModule_CallReplyLength(slot_range) >= 3)
      << VALKEY_MODULE_CALL_ERROR_MSG;

  // Parse start and end slots
  long long start = ValkeyModule_CallReplyInteger(
      ValkeyModule_CallReplyArrayElement(slot_range, 0));
  long long end = ValkeyModule_CallReplyInteger(
      ValkeyModule_CallReplyArrayElement(slot_range, 1));

  // Determine if this is a local shard
  bool is_local_shard = IsLocalShard(slot_range, my_node_id);

  // Parse primary node
  ValkeyModuleCallReply* primary_node_arr =
      ValkeyModule_CallReplyArrayElement(slot_range, 2);
  NodeInfo primary_node = ParseNodeInfo(primary_node_arr, is_local_shard, true);

  // Parse replica nodes
  std::vector<NodeInfo> replicas;
  size_t slot_len = ValkeyModule_CallReplyLength(slot_range);
  for (size_t j = 3; j < slot_len; j++) {
    ValkeyModuleCallReply* replica_node_arr =
        ValkeyModule_CallReplyArrayElement(slot_range, j);
    replicas.push_back(ParseNodeInfo(replica_node_arr, is_local_shard, false));
  }

  // Mark owned slots if local
  if (is_local_shard) {
    for (long long slot = start; slot <= end; slot++) {
      CHECK(slot >= 0 && slot < k_num_slots) << "Invalid slot number";
      owned_slots_[slot] = true;
    }
  }

  // Create and insert ShardInfo
  std::string shard_id = primary_node.node_id;
  auto shard_it = shards_.find(shard_id);
  bool is_new_shard = (shard_it == shards_.end());

  if (is_new_shard) {
    ShardInfo shard;
    shard.shard_id = shard_id;
    shard.owned_slots[start] = end;
    // compute shard fingerprint later
    shard.slots_fingerprint = 0;
    shard.primary = primary_node;
    shard.replicas = std::move(replicas);

    auto [inserted_it, success] = shards_.insert({shard_id, std::move(shard)});
    shard_it = inserted_it;
  } else {
    // Existing shard
    // check if the shard info is consistent
    if (!IsExistingShardConsistent(shard_it->second, primary_node, replicas)) {
      VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
          << "Inconsistency shard info found on existing slot ranges!";
      is_consistent_ = false;
    }
    shard_it->second.owned_slots[start] = end;
  }

  // Store slot range info for later
  slot_ranges.push_back(
      {static_cast<uint16_t>(start), static_cast<uint16_t>(end), shard_id});

  // Fix shard pointers and populate target lists
  if (is_new_shard) {
    if (shard_it->second.primary.has_value()) {
      shard_it->second.primary->shard = &(shard_it->second);
      primary_targets_.push_back(shard_it->second.primary.value());
      all_targets_.push_back(shard_it->second.primary.value());
    }
    for (auto& replica : shard_it->second.replicas) {
      replica.shard = &(shard_it->second);
      replica_targets_.push_back(replica);
      all_targets_.push_back(replica);
    }
  }
}

// Helper function to build slot-to-shard map
void ClusterMap::BuildSlotToShardMap(
    const std::vector<SlotRangeInfo>& slot_ranges) {
  for (const auto& range_info : slot_ranges) {
    auto shard_it = shards_.find(range_info.shard_id);
    CHECK(shard_it != shards_.end())
        << "Shard not found when building slot map";
    slot_to_shard_map_[range_info.start_slot] =
        std::make_pair(range_info.end_slot, &(shard_it->second));
  }
}

// Helper function to check if cluster map is full
bool ClusterMap::CheckClusterMapFull() {
  if (slot_to_shard_map_.empty() || slot_to_shard_map_.begin()->first != 0) {
    return false;
  }

  uint16_t expected_next = 0;
  for (const auto& [start_slot, range_and_shard] : slot_to_shard_map_) {
    CHECK(start_slot >= expected_next) << "Duplicate slo ranges found";
    if (start_slot != expected_next) {
      return false;
    }
    expected_next = range_and_shard.first + 1;
  }

  return expected_next == k_num_slots;
}

std::shared_ptr<ClusterMap> ClusterMap::CreateNewClusterMap(
    ValkeyModuleCtx* ctx) {
  auto new_map = std::shared_ptr<ClusterMap>(new ClusterMap());
  new_map->is_consistent_ = true;

  // Call CLUSTER SLOTS
  auto reply = vmsdk::UniquePtrValkeyCallReply(
      ValkeyModule_Call(ctx, "CLUSTER", "c", "SLOTS"));
  CHECK(reply) << VALKEY_MODULE_CALL_ERROR_MSG;
  CHECK(ValkeyModule_CallReplyType(reply.get()) == VALKEYMODULE_REPLY_ARRAY)
      << VALKEY_MODULE_CALL_ERROR_MSG;

  // Get local node ID
  const char* my_node_id = ValkeyModule_GetMyClusterID();
  CHECK(my_node_id) << VALKEY_MODULE_CALL_ERROR_MSG;

  // Process each slot range
  std::vector<SlotRangeInfo> slot_ranges;
  size_t len = ValkeyModule_CallReplyLength(reply.get());
  for (size_t i = 0; i < len; ++i) {
    ValkeyModuleCallReply* slot_range =
        ValkeyModule_CallReplyArrayElement(reply.get(), i);
    new_map->ProcessSlotRange(slot_range, my_node_id, slot_ranges);
  }

  // Build slot-to-shard map
  new_map->BuildSlotToShardMap(slot_ranges);

  // Check if cluster map is full
  new_map->is_consistent_ &= new_map->CheckClusterMapFull();

  // compute fingerprint for each shard
  for (auto& [shard_id, shard] : new_map->shards_) {
    shard.slots_fingerprint =
        new_map->ComputeShardFingerprint(shard.owned_slots);
  }

  // Compute cluster-level fingerprint
  new_map->cluster_slots_fingerprint_ = new_map->ComputeClusterFingerprint();

  // Set expiration time
  new_map->expiration_tp_ =
      std::chrono::steady_clock::now() +
      std::chrono::milliseconds(cluster_map_expiration_ms.GetValue());

  return new_map;
}

// debug only, print out cluster map
void ClusterMap::PrintClusterMap(std::shared_ptr<ClusterMap> map) {
  VMSDK_LOG(NOTICE, nullptr) << "=== Cluster Map Created ===";
  VMSDK_LOG(NOTICE, nullptr) << "is_consistent_: " << map->is_consistent_;
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
      if (shard->primary.has_value() && shard->primary->is_local) {
        if (!owned_ranges.empty()) owned_ranges += ", ";
        owned_ranges +=
            std::to_string(start_slot) + "-" + std::to_string(end_slot);
      } else {
        // Check replicas
        bool is_local = false;
        for (const auto& replica : shard->replicas) {
          if (replica.is_local) {
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
          << "    role: " << (primary.is_primary ? "Primary" : "Replica");
      VMSDK_LOG(NOTICE, nullptr)
          << "    location: " << (primary.is_local ? "Local" : "Remote");
      VMSDK_LOG(NOTICE, nullptr)
          << "    ip: "
          << (primary.socket_address.ip.empty() ? "(local)"
                                                : primary.socket_address.ip);
      VMSDK_LOG(NOTICE, nullptr)
          << "    port: "
          << (primary.socket_address.port == 0
                  ? "(local)"
                  : absl::StrCat(primary.socket_address.port));
      if (!primary.additional_network_metadata.empty()) {
        VMSDK_LOG(NOTICE, nullptr) << "    additional_network_metadata:";
        for (const auto& [key, value] : primary.additional_network_metadata) {
          VMSDK_LOG(NOTICE, nullptr) << "      " << key << ": " << value;
        }
      } else {
        VMSDK_LOG(NOTICE, nullptr) << "additional_network_metadata is empty";
      }
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
          << "    role: " << (replica.is_primary ? "Primary" : "Replica");
      VMSDK_LOG(NOTICE, nullptr)
          << "    location: " << (replica.is_local ? "Local" : "Remote");
      VMSDK_LOG(NOTICE, nullptr)
          << "    ip: "
          << (replica.socket_address.ip.empty() ? "(local)"
                                                : replica.socket_address.ip);
      VMSDK_LOG(NOTICE, nullptr)
          << "    port: "
          << (replica.socket_address.port == 0
                  ? "(local)"
                  : absl::StrCat(replica.socket_address.port));
      if (!replica.additional_network_metadata.empty()) {
        VMSDK_LOG(NOTICE, nullptr) << "    additional_network_metadata:";
        for (const auto& [key, value] : replica.additional_network_metadata) {
          VMSDK_LOG(NOTICE, nullptr) << "      " << key << ": " << value;
        }
      } else {
        VMSDK_LOG(NOTICE, nullptr) << "additional_network_metadata is empty";
      }
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