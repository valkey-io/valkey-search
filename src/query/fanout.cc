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
#include "src/query/fanout_template.h"
#include "src/query/search.h"
#include "src/utils/string_interning.h"
#include "valkey_search_options.h"
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
  std::unique_ptr<SearchParameters> parameters ABSL_GUARDED_BY(mutex);
  std::atomic_bool reached_oom{false};

  SearchPartitionResultsTracker(int outstanding_requests, int k,
                                query::SearchResponseCallback callback,
                                std::unique_ptr<SearchParameters> parameters)
      : outstanding_requests(outstanding_requests),
        callback(std::move(callback)),
        parameters(std::move(parameters)) {}

  void HandleResponse(coordinator::SearchIndexPartitionResponse &response,
                      const std::string &address, const grpc::Status &status) {
    if (!status.ok()) {
      bool should_cancel = status.error_code() == grpc::RESOURCE_EXHAUSTED ||
                           !options::GetEnablePartialResults().GetValue();
      if (status.error_code() == grpc::RESOURCE_EXHAUSTED) {
        reached_oom.store(true);
      }
      if (should_cancel) {
        parameters->cancellation_token->Cancel();
      }
      if (status.error_code() != grpc::DEADLINE_EXCEEDED &&
          status.error_code() != grpc::RESOURCE_EXHAUSTED) {
        VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
            << "Error during handling of FT.SEARCH on node " << address << ": "
            << status.error_message();
      }
      return;
    }

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
    if (reached_oom) {
      result = absl::ResourceExhaustedError(kOOMMsg);
    } else {
      while (!results.empty()) {
        result->push_back(
            std::move(const_cast<indexes::Neighbor &>(results.top())));
        results.pop();
      }
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
        tracker->HandleResponse(response, address, status);
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
    std::unique_ptr<SearchParameters> parameters,
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
                  std::unique_ptr<SearchParameters> parameters) {
          if (neighbors.ok()) {
            tracker->AddResults(*neighbors);
          } else {
            if (absl::IsResourceExhausted(neighbors.status())) {
              tracker->reached_oom.store(true);
            }
            VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
                << "Error during local handling of FT.SEARCH: "
                << neighbors.status().message();
          }
        },
        SearchMode::kLocal))
        << "Failed to handle FT.SEARCH locally during fan-out";
  }
  return absl::OkStatus();
}

// TODO See if caching this improves performance.
std::vector<fanout::FanoutSearchTarget> GetSearchTargetsForFanout(
    ValkeyModuleCtx *ctx, FanoutTargetMode mode) {
  return fanout::FanoutTemplate::GetTargets(ctx, mode);
}

}  // namespace valkey_search::query::fanout
