/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/fanout.h"

#include <netinet/in.h>

#include <cstddef>
#include <cstring>
#include <deque>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "grpcpp/support/status.h"
#include "src/attribute_data_type.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/coordinator/search_converter.h"
#include "src/coordinator/util.h"
#include "src/indexes/vector_base.h"
#include "src/query/search.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query::fanout {

struct NeighborComparator {
  bool operator()(indexes::Neighbor &a, indexes::Neighbor &b) const {
    // We use a max heap, to pop off the furthest vector during aggregation.
    return a.distance < b.distance;
  }
};

// SearchPartitionResultsTracker is a thread-safe class that tracks the results
// of a query fanout. It aggregates the results from multiple nodes and returns
// the top k results to the callback.
struct SearchPartitionResultsTracker {
  absl::Mutex mutex;
  std::priority_queue<indexes::Neighbor, std::vector<indexes::Neighbor>,
                      NeighborComparator>
      results ABSL_GUARDED_BY(mutex);
  int outstanding_requests ABSL_GUARDED_BY(mutex);
  query::SearchResponseCallback callback;
  std::unique_ptr<VectorSearchParameters> parameters ABSL_GUARDED_BY(mutex);

  SearchPartitionResultsTracker(
      int outstanding_requests, int k, query::SearchResponseCallback callback,
      std::unique_ptr<VectorSearchParameters> parameters)
      : outstanding_requests(outstanding_requests),
        callback(std::move(callback)),
        parameters(std::move(parameters)) {}

  void AddResults(coordinator::SearchIndexPartitionResponse &response) {
    absl::MutexLock lock(&mutex);
    while (response.neighbors_size() > 0) {
      auto neighbor_entry = std::unique_ptr<coordinator::NeighborEntry>(
          response.mutable_neighbors()->ReleaseLast());
      RecordsMap attribute_contents;
      for (const auto &attribute_content :
           neighbor_entry->attribute_contents()) {
        auto identifier =
            vmsdk::MakeUniqueValkeyString(attribute_content.identifier());
        auto identifier_view = vmsdk::ToStringView(identifier.get());
        attribute_contents.emplace(
            identifier_view, RecordsMapValue(std::move(identifier),
                                             vmsdk::MakeUniqueValkeyString(
                                                 attribute_content.content())));
      }
      indexes::Neighbor neighbor{
          std::make_shared<InternedString>(neighbor_entry->key()),
          neighbor_entry->score(), std::move(attribute_contents)};
      AddResult(neighbor);
    }
  }

  void AddResults(std::deque<indexes::Neighbor> &neighbors) {
    absl::MutexLock lock(&mutex);
    for (auto &neighbor : neighbors) {
      AddResult(neighbor);
    }
  }

  void AddResult(indexes::Neighbor &neighbor)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex) {
    // For non-vector queries, we can add the result directly.
    if (parameters->attribute_alias.empty()) {
        results.emplace(std::move(neighbor));
        return;
    }
    if (results.size() < parameters->k) {
      results.emplace(std::move(neighbor));
    } else if (neighbor.distance < results.top().distance) {
      results.emplace(std::move(neighbor));
      results.pop();
    }
  }

  ~SearchPartitionResultsTracker() {
    absl::MutexLock lock(&mutex);
    absl::StatusOr<std::deque<indexes::Neighbor>> result =
        std::deque<indexes::Neighbor>();
    while (!results.empty()) {
      result->push_back(
          std::move(const_cast<indexes::Neighbor &>(results.top())));
      results.pop();
    }
    callback(result, std::move(parameters));
  }
};

void PerformRemoteSearchRequest(
    std::unique_ptr<coordinator::SearchIndexPartitionRequest> request,
    const std::string &address,
    coordinator::ClientPool *coordinator_client_pool,
    std::shared_ptr<SearchPartitionResultsTracker> tracker) {
  auto client = coordinator_client_pool->GetClient(address);

  client->SearchIndexPartition(
      std::move(request),
      [tracker, address = std::string(address)](
          grpc::Status status,
          coordinator::SearchIndexPartitionResponse &response) mutable {
        if (status.ok()) {
          tracker->AddResults(response);
        } else {
          VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
              << "Error during handling of FT.SEARCH on node " << address
              << ": " << status.error_message();
        }
      });
}

void PerformRemoteSearchRequestAsync(
    std::unique_ptr<coordinator::SearchIndexPartitionRequest> request,
    const std::string &address,
    coordinator::ClientPool *coordinator_client_pool,
    std::shared_ptr<SearchPartitionResultsTracker> tracker,
    vmsdk::ThreadPool *thread_pool) {
  thread_pool->Schedule(
      [coordinator_client_pool, address = std::string(address),
       request = std::move(request), tracker]() mutable {
        PerformRemoteSearchRequest(std::move(request), address,
                                   coordinator_client_pool, tracker);
      },
      vmsdk::ThreadPool::Priority::kHigh);
}

absl::Status PerformSearchFanoutAsync(
    ValkeyModuleCtx *ctx, std::vector<FanoutSearchTarget> &search_targets,
    coordinator::ClientPool *coordinator_client_pool,
    std::unique_ptr<VectorSearchParameters> parameters,
    vmsdk::ThreadPool *thread_pool, query::SearchResponseCallback callback) {
  auto request = coordinator::ParametersToGRPCSearchRequest(*parameters);
  // There should be no limit for the fanout search, so put some safe values,
  // so that the default values are not used during the local search.
  request->mutable_limit()->set_first_index(0);
  request->mutable_limit()->set_number(parameters->k);
  auto tracker = std::make_shared<SearchPartitionResultsTracker>(
      search_targets.size(), parameters->k, std::move(callback),
      std::move(parameters));
  bool has_local_target = false;
  for (auto &node : search_targets) {
    auto detached_ctx = vmsdk::MakeUniqueValkeyDetachedThreadSafeContext(ctx);
    if (node.type == FanoutSearchTarget::Type::kLocal) {
      // Defer the local target enqueue, since it will own the parameters from
      // then on.
      has_local_target = true;
      continue;
    }
    auto request_copy =
        std::make_unique<coordinator::SearchIndexPartitionRequest>();
    request_copy->CopyFrom(*request);
    // At 30 requests, it takes ~600 micros to enqueue all the requests.
    // Putting this into the background thread pool will save us time on
    // machines with multiple cores.
    if (search_targets.size() >= 30 && thread_pool->Size() > 1) {
      PerformRemoteSearchRequestAsync(std::move(request_copy), node.address,
                                      coordinator_client_pool, tracker,
                                      thread_pool);
    } else {
      PerformRemoteSearchRequest(std::move(request_copy), node.address,
                                 coordinator_client_pool, tracker);
    }
  }
  if (has_local_target) {
    VMSDK_ASSIGN_OR_RETURN(
        auto local_parameters,
        coordinator::GRPCSearchRequestToParameters(*request, nullptr));
    VMSDK_RETURN_IF_ERROR(query::SearchAsync(
        std::move(local_parameters), thread_pool,
        [tracker](absl::StatusOr<std::deque<indexes::Neighbor>> &neighbors,
                  std::unique_ptr<VectorSearchParameters> parameters) {
          if (neighbors.ok()) {
            tracker->AddResults(*neighbors);
          } else {
            VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
                << "Error during local handling of FT.SEARCH: "
                << neighbors.status().message();
          }
        },
        true))
        << "Failed to handle FT.SEARCH locally during fan-out";
  }
  return absl::OkStatus();
}

// TODO See if caching this improves performance.
std::vector<FanoutSearchTarget> GetSearchTargetsForFanout(
    ValkeyModuleCtx *ctx) {
  size_t num_nodes;
  auto nodes = vmsdk::MakeUniqueValkeyClusterNodesList(ctx, &num_nodes);
  absl::flat_hash_map<std::string, std::vector<FanoutSearchTarget>>
      shard_id_to_target;
  std::vector<FanoutSearchTarget> selected_targets;
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
    // Master ID is not null terminated.
    auto master_id_str = std::string(master_id, VALKEYMODULE_NODE_ID_LEN);
    if (flags & VALKEYMODULE_NODE_PFAIL || flags & VALKEYMODULE_NODE_FAIL) {
      VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
          << "Node " << node_id << " (" << ip
          << ") is failing, skipping for FT.SEARCH...";
      continue;
    }
    if (flags & VALKEYMODULE_NODE_MASTER) {
      master_id_str = node_id;
    }
    if (flags & VALKEYMODULE_NODE_MYSELF) {
      shard_id_to_target[master_id_str].push_back(
          FanoutSearchTarget{.type = FanoutSearchTarget::Type::kLocal});
    } else {
      shard_id_to_target[master_id_str].push_back(FanoutSearchTarget{
          .type = FanoutSearchTarget::Type::kRemote,
          .address =
              absl::StrCat(ip, ":", coordinator::GetCoordinatorPort(port))});
    }
  }
  absl::BitGen gen;
  for (const auto &[shard_id, targets] : shard_id_to_target) {
    size_t index = absl::Uniform(gen, 0u, targets.size());
    selected_targets.push_back(targets.at(index));
  }
  return selected_targets;
}

}  // namespace valkey_search::query::fanout
