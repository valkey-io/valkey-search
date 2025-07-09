#pragma once

#include <functional>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/random/random.h"
#include "absl/strings/str_cat.h"
#include "src/coordinator/util.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search::query::fanout {

// Template class for fanout operations across cluster nodes
class FanoutTemplate {
 public:
  template<typename TargetType>
  static std::vector<TargetType> GetTargets(
      ValkeyModuleCtx *ctx,
      std::function<TargetType()> create_local_target,
      std::function<TargetType(const std::string&)> create_remote_target) {
    size_t num_nodes;
    auto nodes = vmsdk::MakeUniqueValkeyClusterNodesList(ctx, &num_nodes);
    absl::flat_hash_map<std::string, std::vector<TargetType>> shard_id_to_target;
    std::vector<TargetType> selected_targets;
    
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
      if (flags & VALKEYMODULE_NODE_MYSELF) {
        shard_id_to_target[master_id_str].push_back(create_local_target());
      } else {
        shard_id_to_target[master_id_str].push_back(create_remote_target(
            absl::StrCat(ip, ":", coordinator::GetCoordinatorPort(port))));
      }
    }
    
    absl::BitGen gen;
    for (const auto &[shard_id, targets] : shard_id_to_target) {
      size_t index = absl::Uniform(gen, 0u, targets.size());
      selected_targets.push_back(targets.at(index));
    }
    return selected_targets;
  }
};

}  // namespace valkey_search::query::fanout
