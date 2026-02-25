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
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
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
#include "src/valkey_search.h"
#include "valkey_search_options.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query::fanout {

CONTROLLED_BOOLEAN(ForceInvalidSlotFingerprint, false);

struct NeighborComparator {
  bool operator()(const indexes::Neighbor &a,
                  const indexes::Neighbor &b) const {
    // Primary sort: by distance
    // We use a max heap, to pop off the furthest vector during aggregation.
    if (a.distance != b.distance) {
      return a.distance < b.distance;
    }
    // Secondary sort: by key for consistent ordering when distances are equal.
    // Primarily used in non vector queries without scores (distance = 0).
    // The full string compare is required because for external keys there is no
    // guarantee of the stability of the InternedStringPtr across invocations.
    return a.external_id->Str() > b.external_id->Str();
  }
};

// SearchPartitionResultsTracker is a thread-safe class that tracks the results
// of a query fanout. It aggregates the results from multiple nodes and returns
// the top k results to the callback.
struct SearchPartitionResultsTracker {
  absl::Mutex mutex;
  // Holds the LocalResponderSearch after it completes, keeping its
  // SearchParameters fields (return_attributes, sortby_parameter, etc.) alive.
  // Neighbors moved from the local search into `results` contain RecordsMap
  // entries whose string_view keys point into those fields. Without this, the
  // LocalResponderSearch would be destroyed immediately after adding its
  // results to the tracker, leaving dangling string_view keys that are read
  // when the priority queue reallocates (triggering absl::flat_hash_map rehash
  // on move).
  //
  // Since there can only be a single LocalResponder, this doesn't need a lock.
  //
  std::unique_ptr<SearchParameters> local_responder_;
  std::priority_queue<indexes::Neighbor, std::vector<indexes::Neighbor>,
                      NeighborComparator>
      results ABSL_GUARDED_BY(mutex);
  int outstanding_requests ABSL_GUARDED_BY(mutex);
  std::unique_ptr<SearchParameters> parameters ABSL_GUARDED_BY(mutex);
  std::atomic_bool consistency_failed{false};
  std::atomic<size_t> accumulated_total_count{0};

  SearchPartitionResultsTracker(int outstanding_requests, int k,
                                std::unique_ptr<SearchParameters> parameters)
      : outstanding_requests(outstanding_requests),
        parameters(std::move(parameters)) {}

  void HandleResponse(coordinator::SearchIndexPartitionResponse &response,
                      const std::string &address, const grpc::Status &status) {
    if (!status.ok()) {
      if (parameters->enable_consistency &&
          status.error_code() == grpc::FAILED_PRECONDITION) {
        consistency_failed.store(true);
      }
      bool should_cancel = status.error_code() == grpc::RESOURCE_EXHAUSTED ||
                           !parameters->enable_partial_results ||
                           consistency_failed.load();
      if (should_cancel) {
        parameters->cancellation_token->Cancel();
      }
      if (status.error_code() != grpc::DEADLINE_EXCEEDED ||
          status.error_code() != grpc::FAILED_PRECONDITION) {
        VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
            << "Error during handling of FT.SEARCH on node " << address << ": "
            << status.error_message();
      }
      return;
    }

    absl::MutexLock lock(&mutex);
    accumulated_total_count.fetch_add(response.total_count(),
                                      std::memory_order_relaxed);
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
          StringInternStore::Intern(neighbor_entry->key()),
          neighbor_entry->score(), std::move(attribute_contents)};
      AddResult(neighbor);
    }
  }

  void AddResults(std::vector<indexes::Neighbor> &neighbors) {
    absl::MutexLock lock(&mutex);
    for (auto &neighbor : neighbors) {
      AddResult(neighbor);
    }
  }

  void AddTotalCount(size_t count) {
    accumulated_total_count.fetch_add(count, std::memory_order_relaxed);
  }

  void AddResult(indexes::Neighbor &neighbor)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex) {
    // For non-vector queries, we can add the result directly.
    if (parameters->IsNonVectorQuery()) {
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
    absl::Status status;
    if (consistency_failed) {
      status = absl::FailedPreconditionError(kFailedPreconditionMsg);
    } else {
      std::vector<indexes::Neighbor> neighbors;
      neighbors.resize(results.size());
      size_t i = neighbors.size();
      while (!results.empty()) {
        CHECK(i != 0);
        neighbors[--i] =
            std::move(const_cast<indexes::Neighbor &>(results.top()));
        results.pop();
      }
      CHECK(i == 0);
      // Note: We do not sort neighbors here because we do not have the content
      // of the local shard yet. In the SendReply function, we will sort the all
      // neighbors based on the content if sorting is required.
      // SearchResult construction automatically applies trimming based on LIMIT
      // offset count IF the command allows it (ie - it does not require
      // complete results).
      parameters->search_result = SearchResult(
          accumulated_total_count, std::move(neighbors), *parameters);
      status = absl::OkStatus();
    }
    parameters->search_result.status = status;
    // The destructor runs on whichever thread drops the last shared_ptr
    // reference. If remote shards complete first and the local shard (which
    // completes on the main thread via content resolution) drops the last
    // reference, we'll be on the main thread here.
    if (vmsdk::IsMainThread()) {
      parameters->QueryCompleteMainThread(std::move(parameters));
    } else {
      parameters->QueryCompleteBackground(std::move(parameters));
    }
  }
};

// SearchParameters subclass for local responder (local shard in fanout).
// Handles in-flight retry completion by adding results to the tracker.
class LocalResponderSearch : public query::SearchParameters {
 public:
  std::shared_ptr<SearchPartitionResultsTracker> tracker;

  void QueryCompleteMainThread(
      std::unique_ptr<SearchParameters> self) override {
    CHECK(vmsdk::IsMainThread());
    QueryCompleteImpl(std::move(self));
  }

  void QueryCompleteBackground(
      std::unique_ptr<SearchParameters> self) override {
    CHECK(!vmsdk::IsMainThread());
    QueryCompleteImpl(std::move(self));
  }

 private:
  void QueryCompleteImpl(std::unique_ptr<SearchParameters> self) {
    if (search_result.status.ok() || enable_partial_results) {
      tracker->AddResults(search_result.neighbors);
      tracker->AddTotalCount(search_result.total_count);
    } else {
      VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
          << "Error during local handling of FT.SEARCH: "
          << search_result.status.message();
    }
    // Stash `self` (the LocalResponderSearch) in the tracker so that its
    // SearchParameters fields outlive the Neighbor entries moved into
    // `results` above. Those entries' RecordsMaps contain string_view keys
    // that reference data owned by this SearchParameters object.
    //
    // We must break the circular reference first: this LocalResponderSearch
    // holds a shared_ptr to the tracker, so storing `self` in the tracker
    // would create a cycle (tracker -> self -> tracker). Copy the shared_ptr
    // to a local, clear the member, then stash.
    auto tracker_copy = tracker;
    tracker.reset();
    tracker_copy->parameters->local_responder_ = std::move(self);
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
    ValkeyModuleCtx *ctx,
    std::vector<vmsdk::cluster_map::NodeInfo> &search_targets,
    coordinator::ClientPool *coordinator_client_pool,
    std::unique_ptr<SearchParameters> parameters,
    vmsdk::ThreadPool *thread_pool) {
  auto request = coordinator::ParametersToGRPCSearchRequest(*parameters);
  size_t N = search_targets.size();
  uint64_t K = parameters->limit.first_index + parameters->limit.number;
  // The 'fanout-data-uniformity-percent' (U) represents the user's data
  // distribution profile. 100 means data is perfectly balanced (Uniform); 0
  // means data is heavily skewed.
  uint32_t U = options::GetFanoutDataUniformity().GetValue();
  uint64_t index_size = parameters->index_schema->GetSize("");
  uint32_t min_index_size =
      options::GetFanoutUniformityMinIndexSize().GetValue();
  if (parameters->IsNonVectorQuery()) {
    // For non-vector queries, we optimize network traffic by reducing the
    // per-shard fetch limit. Instead of fetching K from every shard, we
    // calculate a limit based on the distribution profile to cover (offset +
    // limit) results across the cluster.

    // For queries requiring complete results (e.g., with SORTBY), we must
    // fetch K results from each shard to guarantee global correctness.
    // Also, for small indices (below the configured threshold), we skip the
    // optimization.
    if (index_size < min_index_size || parameters->RequiresCompleteResults()) {
      request->mutable_limit()->set_first_index(0);
      request->mutable_limit()->set_number(K);
    } else {
      // 1. Calculate the 'fair_share_limit' (The Base).
      // This is the minimum results needed per shard if data is perfectly
      // uniform. We use ceiling division (K + N - 1) / N to ensure the total
      // sum across N shards is at least K.
      uint64_t fair_share_limit = (K + N - 1) / N;
      // 2. Calculate the 'skew_gap' (The Buffer).
      // The extra results needed if data is skewed. The maximum gap is K -
      // fair_share_limit.
      uint64_t skew_gap = K - fair_share_limit;
      // 3. Apply the 'Uniformity' (U) to the gap.
      // - If U = 100 (Uniform): 0% gap added. Limit = fair_share_limit.
      // - If U = 0 (Skewed): 100% gap added. Limit = K.
      uint64_t optimized_limit =
          fair_share_limit + ((100 - U) * skew_gap / 100);
      request->mutable_limit()->set_first_index(0);
      request->mutable_limit()->set_number(optimized_limit);
    }
  } else {
    // Vector searches: Use k as the limit to find top k results. In worst case,
    // all top k results are from a single shard, so no need to fetch more than
    // k.
    request->mutable_limit()->set_first_index(0);
    request->mutable_limit()->set_number(parameters->k);
  }
  auto tracker = std::make_shared<SearchPartitionResultsTracker>(
      search_targets.size(), parameters->k, std::move(parameters));
  bool has_local_target = false;
  for (auto &node : search_targets) {
    if (node.is_local) {
      // Defer the local target enqueue, since it will own the parameters from
      // then on.
      has_local_target = true;
      continue;
    }
    auto request_copy =
        std::make_unique<coordinator::SearchIndexPartitionRequest>();
    request_copy->CopyFrom(*request);

    if (ForceInvalidSlotFingerprint.GetValue()) {
      // test only: set an invalid slot fingerprint and force failure
      request_copy->set_slot_fingerprint(0);
    } else if (node.shard != nullptr) {
      // avoid accessing node.shard if it is not valid in unit tests
      request_copy->set_slot_fingerprint(node.shard->slots_fingerprint);
    }

    // At 30 requests, it takes ~600 micros to enqueue all the requests.
    // Putting this into the background thread pool will save us time on
    // machines with multiple cores.
    std::string target_address =
        absl::StrCat(node.socket_address.primary_endpoint, ":",
                     coordinator::GetCoordinatorPort(node.socket_address.port));
    if (search_targets.size() >=
            valkey_search::options::GetAsyncFanoutThreshold().GetValue() &&
        thread_pool->Size() > 1) {
      PerformRemoteSearchRequestAsync(std::move(request_copy), target_address,
                                      coordinator_client_pool, tracker,
                                      thread_pool);
    } else {
      PerformRemoteSearchRequest(std::move(request_copy), target_address,
                                 coordinator_client_pool, tracker);
    }
  }
  if (has_local_target) {
    auto local_parameters = std::make_unique<LocalResponderSearch>();
    VMSDK_RETURN_IF_ERROR(coordinator::GRPCSearchRequestToParameters(
        *request, nullptr, local_parameters.get()));
    local_parameters->tracker = tracker;
    VMSDK_RETURN_IF_ERROR(query::SearchAsync(std::move(local_parameters),
                                             thread_pool, SearchMode::kLocal))
        << "Failed to handle FT.SEARCH locally during fan-out";
  }
  return absl::OkStatus();
}

bool IsSystemUnderLowUtilization() {
  // Get the configured threshold (queue wait time in milliseconds)
  double threshold = static_cast<double>(
      valkey_search::options::GetLocalFanoutQueueWaitThreshold().GetValue());

  auto &valkey_search_instance = ValkeySearch::Instance();
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
}

}  // namespace valkey_search::query::fanout
