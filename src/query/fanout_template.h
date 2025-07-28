#pragma once

#include <functional>
#include <string>
#include <vector>
#include <ostream>
#include <netinet/in.h> 

#include "absl/status/status.h"
#include "absl/container/flat_hash_map.h"
#include "absl/random/random.h"
#include "absl/strings/str_cat.h"
#include "src/coordinator/util.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search::query::fanout {

struct FanoutSearchTarget {
  enum Type {
    kLocal,
    kRemote,
  };
  Type type;
  // Empty string if type is kLocal.
  std::string address;

  bool operator==(const FanoutSearchTarget& other) const {
    return type == other.type && address == other.address;
  }

  friend std::ostream& operator<<(std::ostream& os,
                                  const FanoutSearchTarget& target) {
    os << "FanoutSearchTarget{type: " << target.type
       << ", address: " << target.address << "}";
    return os;
  }
};

// Template class for fanout operations across cluster nodes
class FanoutTemplate {
 public:
  // Convenience method for FanoutSearchTarget with default lambdas
  static std::vector<FanoutSearchTarget> GetTargets(
      ValkeyModuleCtx *ctx,
      bool primary_only) {
    return GetTargets<FanoutSearchTarget>(
        ctx,
        []() { return FanoutSearchTarget{.type = FanoutSearchTarget::Type::kLocal}; },
        [](const std::string& address) {
          return FanoutSearchTarget{
              .type = FanoutSearchTarget::Type::kRemote,
              .address = address
          };
        },
        primary_only);
  }

  template<typename TargetType>
  static std::vector<TargetType> GetTargets(
      ValkeyModuleCtx *ctx,
      std::function<TargetType()> create_local_target,
      std::function<TargetType(const std::string&)> create_remote_target,
      bool primary_only = false) {
    size_t num_nodes;
    auto nodes = vmsdk::MakeUniqueValkeyClusterNodesList(ctx, &num_nodes);
    
    std::vector<TargetType> selected_targets;
    
    if (primary_only) {
      // When primary_only is true, select all primary (master) nodes directly
      for (size_t i = 0; i < num_nodes; ++i) {
        std::string node_id(nodes.get()[i], VALKEYMODULE_NODE_ID_LEN);
        char ip[INET6_ADDRSTRLEN] = "";
        char master_id[VALKEYMODULE_NODE_ID_LEN] = "";
        int port;
        int flags;
        if (ValkeyModule_GetClusterNodeInfo(ctx, node_id.c_str(), ip, master_id,
                                            &port, &flags) != VALKEYMODULE_OK) {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
              << "Failed to get node info for node " << node_id
              << ", skipping node...";
          continue;
        }
        
        if (flags & VALKEYMODULE_NODE_PFAIL || flags & VALKEYMODULE_NODE_FAIL) {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
              << "Node " << node_id << " (" << ip
              << ") is failing, skipping for fanout...";
          continue;
        }
        
        // Only select master nodes
        if (flags & VALKEYMODULE_NODE_MASTER) {
          if (flags & VALKEYMODULE_NODE_MYSELF) {
            selected_targets.push_back(create_local_target());
          } else {
            selected_targets.push_back(create_remote_target(
                absl::StrCat(ip, ":", coordinator::GetCoordinatorPort(port))));
          }
        }
      }
    } else {
      // Original logic: group master and replica into shards and randomly select one
      absl::flat_hash_map<std::string, std::vector<size_t>> shard_id_to_node_indices;
      
      for (size_t i = 0; i < num_nodes; ++i) {
        std::string node_id(nodes.get()[i], VALKEYMODULE_NODE_ID_LEN);
        char ip[INET6_ADDRSTRLEN] = "";
        char master_id[VALKEYMODULE_NODE_ID_LEN] = "";
        int port;
        int flags;
        if (ValkeyModule_GetClusterNodeInfo(ctx, node_id.c_str(), ip, master_id,
                                            &port, &flags) != VALKEYMODULE_OK) {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
              << "Failed to get node info for node " << node_id
              << ", skipping node...";
          continue;
        }
        auto master_id_str = std::string(master_id, VALKEYMODULE_NODE_ID_LEN);
        if (flags & VALKEYMODULE_NODE_PFAIL || flags & VALKEYMODULE_NODE_FAIL) {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
              << "Node " << node_id << " (" << ip
              << ") is failing, skipping for fanout...";
          continue;
        }
        if (flags & VALKEYMODULE_NODE_MASTER) {
          master_id_str = node_id;
        }
        
        // Store only the node index
        shard_id_to_node_indices[master_id_str].push_back(i);
      }
      
      // Random selection first, then create only the selected target objects
      absl::BitGen gen;
      for (const auto &[shard_id, node_indices] : shard_id_to_node_indices) {
        size_t index = absl::Uniform(gen, 0u, node_indices.size());
        size_t selected_node_index = node_indices.at(index);
        
        // Re-fetch node info only for the selected node
        std::string node_id(nodes.get()[selected_node_index], VALKEYMODULE_NODE_ID_LEN);
        char ip[INET6_ADDRSTRLEN] = "";
        char master_id[VALKEYMODULE_NODE_ID_LEN] = "";
        int port;
        int flags;
        if (ValkeyModule_GetClusterNodeInfo(ctx, node_id.c_str(), ip, master_id,
                                            &port, &flags) != VALKEYMODULE_OK) {
          continue;
        }
        
        // Create target object only for the selected node
        if (flags & VALKEYMODULE_NODE_MYSELF) {
          selected_targets.push_back(create_local_target());
        } else {
          selected_targets.push_back(create_remote_target(
              absl::StrCat(ip, ":", coordinator::GetCoordinatorPort(port))));
        }
      }
    }
    return selected_targets;
  }
};

}  // namespace valkey_search::query::fanout
