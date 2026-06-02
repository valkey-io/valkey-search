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
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
namespace cluster_map {

const std::string kValkeyModuleCallErrorMsg =
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

std::optional<NodeInfo> ShardInfo::GetLocalNode(bool replica_only) const {
  // Try to find a local node first
  if (!replica_only && primary.has_value() && primary->is_local) {
    return primary.value();
  }

  // Look for local replicas
  for (const auto& replica : replicas) {
    if (replica.is_local) {
      return replica;
    }
  }

  // Return null if no local nodes found
  return std::nullopt;
}

NodeInfo ShardInfo::GetRandomNode(bool replica_only, bool prefer_local) const {
  if (prefer_local) {
    auto local_node = GetLocalNode(replica_only);
    if (local_node.has_value()) {
      return local_node.value();
    }
    // Fall through to random selection if no local node found
  }

  absl::BitGen gen;

  if (replica_only) {
    size_t replica_index = absl::Uniform(gen, 0u, replicas.size());
    return replicas[replica_index];
  }

  size_t node_count = replicas.size();
  if (primary.has_value()) {
    node_count++;
  }
  CHECK(node_count > 0);
  size_t index = absl::Uniform(gen, 0u, node_count);
  if (index == 0 && primary.has_value()) {
    return primary.value();
  }
  size_t replica_index = primary.has_value() ? index - 1 : index;
  return replicas[replica_index];
}

std::vector<NodeInfo> ClusterMap::GetTargets(FanoutTargetMode mode,
                                             bool prefer_local) const {
  switch (mode) {
    case FanoutTargetMode::kAll:
      return all_targets_;
    case FanoutTargetMode::kPrimary:
      return primary_targets_;
    case FanoutTargetMode::kReplicas:
      return replica_targets_;
    case FanoutTargetMode::kOneReplicaPerShard: {
      std::vector<NodeInfo> random_replicas;
      random_replicas.reserve(shards_.size());
      for (const auto& [shard_id, shard_info] : shards_) {
        if (shard_info.replicas.empty()) {
          continue;
        }
        random_replicas.push_back(shard_info.GetRandomNode(true, prefer_local));
      }
      return random_replicas;
    }
    case FanoutTargetMode::kRandom: {
      std::vector<NodeInfo> random_targets;
      random_targets.reserve(shards_.size());
      for (const auto& [shard_id, shard_info] : shards_) {
        random_targets.push_back(shard_info.GetRandomNode(false, prefer_local));
      }
      return random_targets;
    }
    default:
      CHECK(false);
  }
}

std::vector<NodeInfo> ClusterMap::GetTargetsForSlot(FanoutTargetMode mode,
                                                    bool prefer_local,
                                                    uint16_t slot) const {
  if (slot_to_shard_map_.empty()) {
    return {};
  }
  auto iter = slot_to_shard_map_.lower_bound(slot);
  if (iter == slot_to_shard_map_.end()) {
    iter--;
  }
  const ShardInfo* shard = iter->second.second;
  CHECK(shard);
  if (slot < iter->first || slot >= iter->second.first) {
    return {};  // Slot not in range means no shard has this slot.
  }

  //
  // We have the right shard, pick from it.
  //
  switch (mode) {
    case FanoutTargetMode::kAll:
      CHECK(false);
    case FanoutTargetMode::kPrimary:
      if (shard->primary.has_value()) {
        return {shard->primary.value()};
      } else {
        return {};
      }
    case FanoutTargetMode::kReplicas:
      if (!shard->replicas.empty()) {
        return {shard->replicas[0]};
      } else {
        return {};
      }
    case FanoutTargetMode::kRandom:
      return {shard->GetRandomNode(false, prefer_local)};
    default:
      CHECK(false);
  }
}

// For shard fingerprint - hash the slot ranges
uint64_t ClusterMap::ComputeShardFingerprint(
    const absl::btree_map<uint16_t, uint16_t>& slot_ranges) {
  CHECK(!slot_ranges.empty());
  highwayhash::HighwayHashCatT<HH_TARGET> hasher(kHashKey);
  for (const auto& [start, end] : slot_ranges) {
    std::pair<uint16_t, uint16_t> range = {start, end};
    hasher.Append(reinterpret_cast<const char*>(&range), sizeof(range));
  }
  uint64_t fingerprint;
  hasher.Finalize(&fingerprint);
  return fingerprint;
}

// For cluster fingerprint - hash all shard fingerprints with shard IDs
uint64_t ClusterMap::ComputeClusterFingerprint() {
  if (shards_.empty()) {
    return 0;
  }
  highwayhash::HighwayHashCatT<HH_TARGET> hasher(kHashKey);
  for (const auto& [shard_id, shard] : shards_) {
    hasher.Append(shard_id.c_str(), shard_id.size());
    hasher.Append(reinterpret_cast<const char*>(&shard.slots_fingerprint),
                  sizeof(shard.slots_fingerprint));
  }
  uint64_t fingerprint;
  hasher.Finalize(&fingerprint);
  return fingerprint;
}

// Helper function to parse node info from CLUSTER SLOTS reply
std::optional<NodeInfo> ClusterMap::ParseNodeInfo(
    ValkeyModuleCallReply* node_arr, const char* my_node_id, bool is_primary) {
  CHECK(node_arr) << kValkeyModuleCallErrorMsg;
  // each node array should have exactly 4 elements
  CHECK(ValkeyModule_CallReplyLength(node_arr) == 4)
      << kValkeyModuleCallErrorMsg;

  // Get primary endpoint
  ValkeyModuleCallReply* primary_endpoint_reply =
      ValkeyModule_CallReplyArrayElement(node_arr, 0);
  if (!primary_endpoint_reply) {
    VMSDK_LOG(WARNING, nullptr) << "Invalid node primary endpoint";
    return std::nullopt;
  }

  size_t node_primary_endpoint_len;
  const char* node_primary_endpoint_char = ValkeyModule_CallReplyStringPtr(
      primary_endpoint_reply, &node_primary_endpoint_len);

  // Get port
  long long node_port = ValkeyModule_CallReplyInteger(
      ValkeyModule_CallReplyArrayElement(node_arr, 1));
  CHECK(node_port) << kValkeyModuleCallErrorMsg;

  // Get node ID
  size_t node_id_len;
  const char* node_id_char = ValkeyModule_CallReplyStringPtr(
      ValkeyModule_CallReplyArrayElement(node_arr, 2), &node_id_len);
  CHECK(node_id_char) << kValkeyModuleCallErrorMsg;

  bool is_local_node =
      (node_id_len == VALKEYMODULE_NODE_ID_LEN &&
       memcmp(node_id_char, my_node_id, VALKEYMODULE_NODE_ID_LEN) == 0);

  // Check for invalid endpoint (nullptr, empty, or "?")
  if (!is_local_node &&
      (!node_primary_endpoint_char || node_primary_endpoint_len == 0 ||
       (node_primary_endpoint_len == 1 &&
        node_primary_endpoint_char[0] == '?'))) {
    VMSDK_LOG(WARNING, nullptr) << "Invalid node primary endpoint";
    return std::nullopt;
  }

  // Get additional network metadata
  // Depending on the client RESP protocol version, the additional network
  // metadata might be a map(RESP3), or a flattened array(RESP2)
  absl::flat_hash_map<std::string, std::string> additional_network_metadata;
  auto reply_metadata = ValkeyModule_CallReplyArrayElement(node_arr, 3);
  CHECK(reply_metadata) << kValkeyModuleCallErrorMsg;

  auto insert_metadata = [&additional_network_metadata](
                             ValkeyModuleCallReply* key_reply,
                             ValkeyModuleCallReply* val_reply) {
    size_t key_len;
    const char* key_str = ValkeyModule_CallReplyStringPtr(key_reply, &key_len);
    size_t val_len;
    const char* val_str = ValkeyModule_CallReplyStringPtr(val_reply, &val_len);
    additional_network_metadata[std::string(key_str, key_len)] =
        std::string(val_str, val_len);
  };

  switch (ValkeyModule_CallReplyType(reply_metadata)) {
    case VALKEYMODULE_REPLY_MAP: {
      ValkeyModuleCallReply* map_key;
      ValkeyModuleCallReply* map_val;
      int idx = 0;
      while (ValkeyModule_CallReplyMapElement(reply_metadata, idx, &map_key,
                                              &map_val) == VALKEYMODULE_OK) {
        insert_metadata(map_key, map_val);
        idx++;
      }
      break;
    }
    case VALKEYMODULE_REPLY_ARRAY: {
      // format: ARRAY of [key1, val1, key2, val2, ...]
      size_t array_len = ValkeyModule_CallReplyLength(reply_metadata);
      for (size_t i = 0; i + 1 < array_len; i += 2) {
        auto arr_key = ValkeyModule_CallReplyArrayElement(reply_metadata, i);
        auto arr_val =
            ValkeyModule_CallReplyArrayElement(reply_metadata, i + 1);
        insert_metadata(arr_key, arr_val);
      }
      break;
    }
    default:
      // crash if the reply is not map or array type
      CHECK(false) << kValkeyModuleCallErrorMsg;
  }

  std::string node_id_str = std::string(node_id_char, node_id_len);
  SocketAddress addr{.primary_endpoint = std::string(node_primary_endpoint_char,
                                                     node_primary_endpoint_len),
                     .port = static_cast<uint16_t>(node_port)};

  // Check for duplicate socket addresses across different nodes
  auto it = socket_addr_to_node_map_.find(addr);
  if (it != socket_addr_to_node_map_.end()) {
    // socket address already seen - check if it's the same node
    if (it->second != node_id_str) {
      VMSDK_LOG(WARNING, nullptr)
          << "Socket address " << it->first.primary_endpoint << ":"
          << it->first.port << " collision between nodes " << node_id_str
          << " and " << it->second;
      this->is_consistent_ = false;
    }
  } else {
    socket_addr_to_node_map_[addr] = node_id_str;
  }

  return NodeInfo{.node_id = node_id_str,
                  .is_primary = is_primary,
                  .is_local = is_local_node,
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
// return false if slot range is invalid
bool ClusterMap::ProcessSlotRange(ValkeyModuleCallReply* slot_range,
                                  const char* my_node_id,
                                  std::vector<SlotRangeInfo>& slot_ranges) {
  CHECK(slot_range) << kValkeyModuleCallErrorMsg;
  CHECK(ValkeyModule_CallReplyType(slot_range) == VALKEYMODULE_REPLY_ARRAY)
      << kValkeyModuleCallErrorMsg;
  CHECK(ValkeyModule_CallReplyLength(slot_range) >= 3)
      << kValkeyModuleCallErrorMsg;

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
  auto primary_node_opt = ParseNodeInfo(primary_node_arr, my_node_id, true);
  if (!primary_node_opt.has_value()) {
    VMSDK_LOG(WARNING, nullptr) << "Dropping slot range [" << start << "-"
                                << end << "] due to invalid primary node";
    is_consistent_ = false;
    return false;
  }
  NodeInfo primary_node = primary_node_opt.value();

  // Parse replica nodes
  std::vector<NodeInfo> replicas;
  size_t slot_len = ValkeyModule_CallReplyLength(slot_range);
  for (size_t j = 3; j < slot_len; j++) {
    ValkeyModuleCallReply* replica_node_arr =
        ValkeyModule_CallReplyArrayElement(slot_range, j);
    auto replica_opt = ParseNodeInfo(replica_node_arr, my_node_id, false);
    if (!replica_opt.has_value()) {
      VMSDK_LOG(WARNING, nullptr) << "Skipping invalid replica in slot range ["
                                  << start << "-" << end << "]";
      is_consistent_ = false;
      return false;
    }
    replicas.push_back(replica_opt.value());
  }

  // Mark owned slots if local
  if (is_local_shard) {
    for (long long slot = start; slot <= end; slot++) {
      CHECK(slot >= 0 && slot < kNumSlots) << "Invalid slot number";
      owned_slots_[slot] = true;
    }
  }

  // Create and insert ShardInfo
  std::string shard_id = primary_node.node_id;
  auto shard_it = shards_.find(shard_id);
  bool is_new_shard = (shard_it == shards_.end());

  if (is_new_shard) {
    auto [inserted_it, success] = shards_.insert({shard_id, ShardInfo{}});
    shard_it = inserted_it;
    ShardInfo& shard = shard_it->second;
    shard.shard_id = shard_id;
    shard.owned_slots[start] = end;
    // compute shard fingerprint later
    shard.slots_fingerprint = 0;
    shard.primary = primary_node;
    shard.replicas = std::move(replicas);
  } else {
    // Existing shard, add slot range first
    shard_it->second.owned_slots[start] = end;

    // check shard consistency between existing and new one
    if (!IsExistingShardConsistent(shard_it->second, primary_node, replicas)) {
      VMSDK_LOG(WARNING, nullptr)
          << "Inconsistency shard info found on existing slot ranges!";
      is_consistent_ = false;
    }
  }

  // Store slot range info for later
  slot_ranges.push_back(
      {static_cast<uint16_t>(start), static_cast<uint16_t>(end), shard_id});

  return true;
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
  size_t entries_processed = 0;
  for (const auto& [start_slot, range_and_shard] : slot_to_shard_map_) {
    CHECK(start_slot >= expected_next)
        << "Slot " << start_slot << " overlaps with previous range ending at "
        << (expected_next - 1);
    if (start_slot != expected_next) {
      VMSDK_LOG(WARNING, nullptr)
          << "Slot gap found: slots " << expected_next << " to "
          << (start_slot - 1) << " are not covered";
      return false;
    }
    expected_next = range_and_shard.first + 1;
    entries_processed++;
  }
  // Verify we processed all entries in the map
  if (entries_processed != slot_to_shard_map_.size()) {
    VMSDK_LOG(WARNING, nullptr)
        << "Slot map inconsistency: processed " << entries_processed
        << " entries but map contains " << slot_to_shard_map_.size()
        << " entries";
    return false;
  }
  return expected_next == kNumSlots;
}

std::shared_ptr<ClusterMap> ClusterMap::CreateNewClusterMap(
    ValkeyModuleCtx* ctx) {
  auto new_map = std::shared_ptr<ClusterMap>(new ClusterMap());
  new_map->is_consistent_ = true;

  // Call CLUSTER SLOTS
  auto reply = vmsdk::UniquePtrValkeyCallReply(
      ValkeyModule_Call(ctx, "CLUSTER", "c", "SLOTS"));
  CHECK(reply) << kValkeyModuleCallErrorMsg;
  CHECK(ValkeyModule_CallReplyType(reply.get()) == VALKEYMODULE_REPLY_ARRAY)
      << kValkeyModuleCallErrorMsg;

  // Get local node ID
  const char* my_node_id = ValkeyModule_GetMyClusterID();
  CHECK(my_node_id) << kValkeyModuleCallErrorMsg;

  // Process each slot range
  std::vector<SlotRangeInfo> slot_ranges;
  size_t len = ValkeyModule_CallReplyLength(reply.get());
  for (size_t i = 0; i < len; ++i) {
    ValkeyModuleCallReply* slot_range =
        ValkeyModule_CallReplyArrayElement(reply.get(), i);
    if (!new_map->ProcessSlotRange(slot_range, my_node_id, slot_ranges)) {
      // Slot range was dropped, continue to next one
      continue;
    };
  }

  // fix shard pointers after finished inserting all shards into shards_ map
  // inserting additional shards into shards_ map after this would cause
  // rebalance of btree and invalidate pointers in shards_ map
  for (auto& [shard_id, shard] : new_map->shards_) {
    // Fix primary pointer
    if (shard.primary.has_value()) {
      shard.primary->shard = &shard;
    }
    // Fix replica pointers
    for (auto& replica : shard.replicas) {
      replica.shard = &shard;
    }
  }

  // Cache reference to current node's shard for quick access
  for (const auto& [shard_id, shard_info] : new_map->shards_) {
    // Check if primary is local
    if (shard_info.primary.has_value() && shard_info.primary->is_local) {
      new_map->current_node_shard_ = &shard_info;
      break;
    }
    // Check if any replica is local
    for (const auto& replica : shard_info.replicas) {
      if (replica.is_local) {
        new_map->current_node_shard_ = &shard_info;
        break;
      }
    }
    if (new_map->current_node_shard_ != nullptr) {
      break;
    }
  }

  // Build slot-to-shard map
  new_map->BuildSlotToShardMap(slot_ranges);

  // Populate target lists
  for (auto& [shard_id, shard] : new_map->shards_) {
    if (shard.primary.has_value()) {
      new_map->primary_targets_.push_back(shard.primary.value());
      new_map->all_targets_.push_back(shard.primary.value());
    }
    for (const auto& replica : shard.replicas) {
      new_map->replica_targets_.push_back(replica);
      new_map->all_targets_.push_back(replica);
    }
  }

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
      VMSDK_LOG(NOTICE, nullptr) << "    primary_endpoint: "
                                 << primary.socket_address.primary_endpoint;
      VMSDK_LOG(NOTICE, nullptr) << "    port: " << primary.socket_address.port;
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
      VMSDK_LOG(NOTICE, nullptr) << "    primary_endpoint: "
                                 << replica.socket_address.primary_endpoint;
      VMSDK_LOG(NOTICE, nullptr) << "    port: " << replica.socket_address.port;
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
