#pragma once

#include <netinet/in.h>

#include <algorithm>
#include <functional>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/util.h"
#include "src/valkey_search.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query::fanout {

// Enumeration for fanout target modes
enum class FanoutTargetMode {
  kRandom,        // Default: randomly select one node per shard
  kReplicasOnly,  // Select only replicas, one per shard
  kPrimary,       // Select all primary (master) nodes
  kAll            // Select all nodes (both primary and replica)
};

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
  // Helper function to check if system is under low utilization
  static bool IsSystemUnderLowUtilization() {
    try {
      // Get the configured threshold (queue wait time in milliseconds)
      double threshold = static_cast<double>(
          valkey_search::options::GetLocalFanoutQueueWaitThreshold()
              .GetValue());

      auto& valkey_search_instance = valkey_search::ValkeySearch::Instance();
      auto reader_pool = valkey_search_instance.GetReaderThreadPool();

      if (!reader_pool) {
        return false;
      }

      // Get recent queue wait time (not global average)
      auto queue_wait_result = reader_pool->GetRecentQueueWaitTime();
      if (!queue_wait_result.ok()) {
        // If we can't get queue wait time, assume high utilization for safety
        return false;
      }

      double queue_wait_time = queue_wait_result.value();

      // System is under low utilization if queue wait time is below threshold
      return queue_wait_time < threshold;
    } catch (...) {
      // If anything goes wrong, assume high utilization for safety
      return false;
    }
  }

  // Convenience method for FanoutSearchTarget with default lambdas
  static std::vector<FanoutSearchTarget> GetTargets(
      ValkeyModuleCtx* ctx,
      FanoutTargetMode target_mode = FanoutTargetMode::kRandom) {
    return GetTargets<FanoutSearchTarget>(
        ctx,
        []() {
          return FanoutSearchTarget{.type = FanoutSearchTarget::Type::kLocal};
        },
        [](const std::string& address) {
          return FanoutSearchTarget{.type = FanoutSearchTarget::Type::kRemote,
                                    .address = address};
        },
        target_mode);
  }

  template <typename TargetType>
  static std::vector<TargetType> GetTargets(
      ValkeyModuleCtx* ctx, std::function<TargetType()> create_local_target,
      std::function<TargetType(const std::string&)> create_remote_target,
      FanoutTargetMode target_mode = FanoutTargetMode::kRandom) {
    size_t num_nodes;
    auto nodes = vmsdk::MakeUniqueValkeyClusterNodesList(ctx, &num_nodes);

    std::vector<TargetType> selected_targets;

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
          if (flags & VALKEYMODULE_NODE_MYSELF) {
            selected_targets.push_back(create_local_target());
          } else {
            selected_targets.push_back(create_remote_target(
                absl::StrCat(ip, ":", coordinator::GetCoordinatorPort(port))));
          }
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
        if (flags & VALKEYMODULE_NODE_MYSELF) {
          selected_targets.push_back(create_local_target());
        } else {
          selected_targets.push_back(create_remote_target(
              absl::StrCat(ip, ":", coordinator::GetCoordinatorPort(port))));
        }
      }
    } else {
      CHECK(target_mode == FanoutTargetMode::kRandom ||
            target_mode == FanoutTargetMode::kReplicasOnly);
      // Original logic: group master and replica into shards and randomly
      // select one, unless confined to replicas only
      absl::flat_hash_map<std::string, std::vector<TargetType>>
          shard_id_to_targets;

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

        // Create and store the target directly
        if (flags & VALKEYMODULE_NODE_MYSELF) {
          shard_id_to_targets[master_id_str].push_back(create_local_target());
        } else {
          shard_id_to_targets[master_id_str].push_back(create_remote_target(
              absl::StrCat(ip, ":", coordinator::GetCoordinatorPort(port))));
        }
      }

      // Select targets for each shard based on system utilization
      absl::BitGen gen;
      bool prefer_local = IsSystemUnderLowUtilization();

      for (const auto& [shard_id, shard_targets] : shard_id_to_targets) {
        if (shard_targets.empty()) {
          continue;
        }

        // Prefer local target when system utilization is low
        if (prefer_local && shard_targets.size() > 1) {
          auto local_it = std::find_if(
              shard_targets.begin(), shard_targets.end(),
              [](const TargetType& target) {
                if constexpr (std::is_same_v<TargetType, FanoutSearchTarget>) {
                  return target.type == FanoutSearchTarget::Type::kLocal;
                } else {
                  // For custom target types, fall back to random selection
                  return false;
                }
              });

          if (local_it != shard_targets.end()) {
            selected_targets.push_back(*local_it);
            continue;
          }
        }

        // Random selection (fallback or when system utilization is high)
        size_t index = absl::Uniform(gen, 0u, shard_targets.size());
        selected_targets.push_back(shard_targets[index]);
      }
    }
    return selected_targets;
  }

  template <typename RequestT, typename ResponseT, typename TrackerT>
  void PerformRemoteRequest(
      std::unique_ptr<RequestT> request, const std::string& address,
      coordinator::ClientPool* coordinator_client_pool,
      std::shared_ptr<TrackerT> tracker,
      std::function<void(coordinator::Client*, std::unique_ptr<RequestT>,
                         std::function<void(grpc::Status, ResponseT&)>, int)>
          grpc_invoker,
      std::function<void(const grpc::Status&, ResponseT&,
                         std::shared_ptr<TrackerT>, const std::string&)>
          callback_logic,
      int timeout_ms = -1) {
    auto client = coordinator_client_pool->GetClient(address);

    grpc_invoker(
        client, std::move(request),
        [tracker, address, callback_logic](grpc::Status status,
                                           ResponseT& response) mutable {
          callback_logic(status, response, tracker, address);
        },
        timeout_ms);
  }
};

}  // namespace valkey_search::query::fanout
