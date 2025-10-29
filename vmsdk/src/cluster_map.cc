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

// create a new cluster map
std::shared_ptr<ClusterMap> ClusterMap::CreateNewClusterMap(
    ValkeyModuleCtx* ctx) {
  auto new_map = std::shared_ptr<ClusterMap>(new ClusterMap());
  new_map->is_cluster_map_full_ = false;

  // call CLUSTER_SLOTS from Valkey Module API
  auto reply = vmsdk::UniquePtrValkeyCallReply(
      ValkeyModule_Call(ctx, "CLUSTER", "c", "SLOTS"));
  CHECK(reply)
      << "CLUSTER_MAP_ERROR: ValkeyModule_Call CLUSTER SLOTS returned nullptr";

  auto reply_type = ValkeyModule_CallReplyType(reply.get());
  CHECK(reply_type == VALKEYMODULE_REPLY_ARRAY)
      << "CLUSTER_MAP_ERROR: ValkeyModule_Call CLUSTER SLOTS returns incorrect "
         "type, expect VALKEYMODULE_REPLY_ARRAY but got "
      << reply_type;

  // Get local node ID to identify which shard we belong to
  const char* my_node_id = ValkeyModule_GetMyClusterID();
  CHECK(my_node_id)
      << "CLUSTER_MAP_ERROR:ValkeyModule_GetMyClusterID returned nullptr";

  // reply is an array of arrays
  // each array element should contain at least 3 elements
  // (1) start slot range (2) end slot range (3) master info (4)... replica info
  size_t len = ValkeyModule_CallReplyLength(reply.get());
  for (size_t i = 0; i < len; ++i) {
    ValkeyModuleCallReply* slot_range =
        ValkeyModule_CallReplyArrayElement(reply.get(), i);
    CHECK(slot_range)
        << "CLUSTER_MAP_ERROR: ValkeyModule_Call CLUSTER SLOTS result returned "
           "invalid slot range sub-array";
    auto slot_range_type = ValkeyModule_CallReplyType(slot_range);
    CHECK(slot_range_type == VALKEYMODULE_REPLY_ARRAY)
        << "CLUSTER_MAP_ERROR: ValkeyModule_Call CLUSTER SLOTS result returns "
           "incorrect slot range sub-array type, expect "
           "VALKEYMODULE_REPLY_ARRAY but got "
        << slot_range_type;

    size_t slot_len = ValkeyModule_CallReplyLength(slot_range);
    // each array element should have at least 3 elements
    CHECK(slot_len >= 3)
        << "CLUSTER_MAP_ERROR: ValkeyModule_Call CLUSTER "
           "SLOTS result returned a slot range sub-array with invalid length, "
           "should be at least 3";
    // Get start and end slots
    ValkeyModuleCallReply* start_slot =
        ValkeyModule_CallReplyArrayElement(slot_range, 0);
    CHECK(ValkeyModule_CallReplyType(start_slot) == VALKEYMODULE_REPLY_INTEGER)
        << "CLUSTER_MAP_ERROR: start slot expect INTEGER type, got"
        << ValkeyModule_CallReplyType(start_slot);
    ValkeyModuleCallReply* end_slot =
        ValkeyModule_CallReplyArrayElement(slot_range, 1);
    CHECK(ValkeyModule_CallReplyType(end_slot) == VALKEYMODULE_REPLY_INTEGER)
        << "CLUSTER_MAP_ERROR: end slot expect INTEGER type, got"
        << ValkeyModule_CallReplyType(end_slot);
    long long start = ValkeyModule_CallReplyInteger(start_slot);
    long long end = ValkeyModule_CallReplyInteger(end_slot);
    // Mark slots as assigned in cluster
    std::set<uint16_t> slot_set;
    for (long long slot = start; slot <= end; slot++) {
      if (slot >= 0 && slot < k_num_slots) {
        slot_set.insert(static_cast<uint16_t>(slot));
      }
    }
    // node info is an array
    // (1) address (2) port number (3) node_id (4) hostname (optional)
    // parse primary node
    ValkeyModuleCallReply* primary_node_arr =
        ValkeyModule_CallReplyArrayElement(slot_range, 2);
    CHECK(primary_node_arr)
        << "CLUSTER_MAP_ERROR: primary node subarray returned nullptr";
    CHECK(ValkeyModule_CallReplyLength(primary_node_arr) >= 3)
        << "CLUSTER_MAP_ERROR: primary node subarray returned invalid length, "
           "expected >= 3";
    // get primary node id
    size_t primary_id_len;
    const char* primary_id_char = ValkeyModule_CallReplyStringPtr(
        ValkeyModule_CallReplyArrayElement(primary_node_arr, 2),
        &primary_id_len);
    CHECK(primary_id_char)
        << "CLUSTER_MAP_ERROR: primary node id returned nullptr";
    std::string primary_id(primary_id_char, primary_id_len);

    // get primary IP and port
    size_t primary_ip_len;
    const char* primary_ip_char = ValkeyModule_CallReplyStringPtr(
        ValkeyModule_CallReplyArrayElement(primary_node_arr, 0),
        &primary_ip_len);
    CHECK(primary_ip_char)
        << "CLUSTER_MAP_ERROR: primary node ip returned nullptr";
    std::string primary_ip(primary_ip_char, primary_ip_len);

    long long primary_port = ValkeyModule_CallReplyInteger(
        ValkeyModule_CallReplyArrayElement(primary_node_arr, 1));
    CHECK(primary_port)
        << "CLUSTER_MAP_ERROR: primary node port returned nullptr";

    // First pass: determine if this is a local shard by checking ALL nodes
    bool is_local_shard = false;

    // Check primary
    if (primary_id_len == VALKEYMODULE_NODE_ID_LEN &&
        memcmp(primary_id_char, my_node_id, VALKEYMODULE_NODE_ID_LEN) == 0) {
      is_local_shard = true;
    }

    // Parse all replica node IDs first
    std::vector<std::tuple<std::string, std::string, long long>> replica_data;
    for (size_t j = 3; j < slot_len; j++) {
      ValkeyModuleCallReply* replica_node_arr =
          ValkeyModule_CallReplyArrayElement(slot_range, j);
      CHECK(replica_node_arr)
          << "CLUSTER_MAP_ERROR: replica node subarray returned nullptr";
      CHECK(ValkeyModule_CallReplyLength(replica_node_arr) >= 3)
          << "CLUSTER_MAP_ERROR: replica node subarray returned invalid "
             "length, "
             "should be >= 3";

      size_t replica_id_len;
      const char* replica_id_char = ValkeyModule_CallReplyStringPtr(
          ValkeyModule_CallReplyArrayElement(replica_node_arr, 2),
          &replica_id_len);
      CHECK(replica_id_char)
          << "CLUSTER_MAP_ERROR: replica node id returned nullptr";

      size_t replica_ip_len;
      const char* replica_ip_char = ValkeyModule_CallReplyStringPtr(
          ValkeyModule_CallReplyArrayElement(replica_node_arr, 0),
          &replica_ip_len);
      CHECK(replica_ip_char)
          << "CLUSTER_MAP_ERROR: replica node ip returned nullptr";

      long long replica_port = ValkeyModule_CallReplyInteger(
          ValkeyModule_CallReplyArrayElement(replica_node_arr, 1));
      CHECK(replica_port)
          << "CLUSTER_MAP_ERROR: replica node port returned nullptr";

      replica_data.emplace_back(std::string(replica_id_char, replica_id_len),
                                std::string(replica_ip_char, replica_ip_len),
                                replica_port);

      // Check if this replica is the local node
      if (!is_local_shard && replica_id_len == VALKEYMODULE_NODE_ID_LEN &&
          memcmp(replica_id_char, my_node_id, VALKEYMODULE_NODE_ID_LEN) == 0) {
        is_local_shard = true;
      }
    }

    // Create primary NodeInfo
    NodeInfo primary_node{
        .node_id = primary_id,
        .role = NodeInfo::NodeRole::kPrimary,
        .location = is_local_shard ? NodeInfo::NodeLocation::kLocal
                                   : NodeInfo::NodeLocation::kRemote,
        .ip = primary_ip,
        .port = is_local_shard ? 0
                               : coordinator::GetCoordinatorPort(
                                     static_cast<int>(primary_port)),
        .address =
            is_local_shard
                ? ""
                : absl::StrCat(primary_ip, ":",
                               coordinator::GetCoordinatorPort(primary_port)),
        .shard = nullptr  // Will be set after inserting shard
    };

    // create replica NodeInfo objects using parsed data
    std::vector<NodeInfo> replicas;
    for (const auto& [replica_id, replica_ip, replica_port] : replica_data) {
      NodeInfo replica_node{
          .node_id = replica_id,
          .role = NodeInfo::NodeRole::kReplica,
          .location = is_local_shard ? NodeInfo::NodeLocation::kLocal
                                     : NodeInfo::NodeLocation::kRemote,
          .ip = replica_ip,
          .port = is_local_shard ? 0
                                 : coordinator::GetCoordinatorPort(
                                       static_cast<int>(replica_port)),
          .address =
              is_local_shard
                  ? ""
                  : absl::StrCat(replica_ip, ":",
                                 coordinator::GetCoordinatorPort(replica_port)),
          .shard = nullptr};
      replicas.push_back(replica_node);
    }

    // Mark owned slots
    if (is_local_shard) {
      for (long long slot = start; slot <= end; slot++) {
        CHECK(slot >= 0 && slot < k_num_slots)
            << "CLUSTER_MAP_ERROR: invalid slot number";
        new_map->owned_slots_[slot] = true;
      }
    }
    // Update shards map
    CHECK(new_map->shards_.find(primary_id) == new_map->shards_.end())
        << "CLUSTER_MAP_ERROR: Duplicate master_id in CLUSTER SLOTS response";

    ShardInfo shard;
    shard.shard_id = primary_id;
    shard.owned_slots = slot_set;
    // compute shard-level slot fingerprint
    shard.slots_fingerprint = compute_shard_fingerprint(slot_set);
    shard.primary = primary_node;
    shard.replicas = std::move(replicas);
    // Insert shard into map
    auto [inserted_it, success] =
        new_map->shards_.insert({primary_id, std::move(shard)});
    CHECK(success) << "CLUSTER_MAP_ERROR: Failed to insert shard";

    // Build slot_to_shard_map_ entry
    // The slot range [start, end] maps to this shard
    new_map->slot_to_shard_map_[static_cast<uint16_t>(start)] =
        std::make_pair(static_cast<uint16_t>(end), &(inserted_it->second));

    // Fix shard pointers in NodeInfo
    if (inserted_it->second.primary.has_value()) {
      inserted_it->second.primary->shard = &(inserted_it->second);
      // Add to primary_targets and all_targets
      new_map->primary_targets_.push_back(inserted_it->second.primary.value());
      new_map->all_targets_.push_back(inserted_it->second.primary.value());
    }
    for (auto& replica : inserted_it->second.replicas) {
      replica.shard = &(inserted_it->second);
      // Add to replica_targets and all_targets
      new_map->replica_targets_.push_back(replica);
      new_map->all_targets_.push_back(replica);
    }
  }

  // check is cluster map full using slot to shard map
  if (!new_map->slot_to_shard_map_.empty()) {
    // Check if first range starts at 0
    auto it = new_map->slot_to_shard_map_.begin();
    if (it->first == 0) {
      uint16_t expected_next = 0;
      bool has_gap = false;
      for (const auto& [start_slot, range_and_shard] :
           new_map->slot_to_shard_map_) {
        uint16_t end_slot = range_and_shard.first;
        // Check if there's a gap
        if (start_slot != expected_next) {
          has_gap = true;
          break;
        }
        // Update expected next slot
        expected_next = end_slot + 1;
      }
      // Check if we covered all slots and no gaps
      if (!has_gap && expected_next == k_num_slots) {
        new_map->is_cluster_map_full_ = true;
      }
    }
  }

  // Compute cluster-level fingerprint
  new_map->cluster_slots_fingerprint_ =
      compute_cluster_fingerprint(new_map->shards_);

  // record the creation timestamp
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
          << "    address: "
          << (primary.address.empty() ? "(local)" : primary.address);
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
          << "    address: "
          << (replica.address.empty() ? "(local)" : replica.address);
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