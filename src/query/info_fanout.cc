/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/info_fanout.h"

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/coordinator/info_converter.h"
#include "src/query/fanout.h"
#include "src/schema_manager.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/thread_pool.h"

namespace valkey_search::query::info_fanout {

// InfoPartitionResultsTracker is a thread-safe class that tracks the results
// of an info fanout. It aggregates the results from multiple nodes and returns
// the aggregated result to the callback.
struct InfoPartitionResultsTracker {
  absl::Mutex mutex;
  InfoResult aggregated_result ABSL_GUARDED_BY(mutex);
  int outstanding_requests ABSL_GUARDED_BY(mutex);
  InfoResponseCallback callback;
  std::unique_ptr<InfoParameters> parameters ABSL_GUARDED_BY(mutex);

  InfoPartitionResultsTracker(int outstanding_requests,
                              InfoResponseCallback callback,
                              std::unique_ptr<InfoParameters> parameters)
      : outstanding_requests(outstanding_requests),
        callback(std::move(callback)),
        parameters(std::move(parameters)) {}

  void AddResults(coordinator::InfoIndexPartitionResponse& response) {
    absl::MutexLock lock(&mutex);

    if (response.exists()) {
      aggregated_result.exists = true;
      aggregated_result.index_name = response.index_name();
      aggregated_result.num_docs += response.num_docs();
      aggregated_result.num_records += response.num_records();
      aggregated_result.hash_indexing_failures +=
          response.hash_indexing_failures();
      aggregated_result.backfill_scanned_count +=
          response.backfill_scanned_count();
      aggregated_result.backfill_db_size += response.backfill_db_size();
      aggregated_result.backfill_inqueue_tasks +=
          response.backfill_inqueue_tasks();
      aggregated_result.mutation_queue_size += response.mutation_queue_size();
      aggregated_result.recent_mutations_queue_delay +=
          response.recent_mutations_queue_delay();

      if (response.backfill_in_progress()) {
        aggregated_result.backfill_in_progress = true;
        if (aggregated_result.backfill_complete_percent_max == 0.0f &&
            aggregated_result.backfill_complete_percent_min == 0.0f) {
          aggregated_result.backfill_complete_percent_min =
              response.backfill_complete_percent();
          aggregated_result.backfill_complete_percent_max =
              response.backfill_complete_percent();
        } else {
          aggregated_result.backfill_complete_percent_min =
              std::min(aggregated_result.backfill_complete_percent_min,
                       response.backfill_complete_percent());
          aggregated_result.backfill_complete_percent_max =
              std::max(aggregated_result.backfill_complete_percent_max,
                       response.backfill_complete_percent());
        }
      } else {
        if (aggregated_result.backfill_complete_percent_max == 0.0f &&
            aggregated_result.backfill_complete_percent_min == 0.0f) {
          aggregated_result.backfill_complete_percent_min = 1.0f;
          aggregated_result.backfill_complete_percent_max = 1.0f;
        } else {
          aggregated_result.backfill_complete_percent_min =
              std::min(aggregated_result.backfill_complete_percent_min, 1.0f);
          aggregated_result.backfill_complete_percent_max =
              std::max(aggregated_result.backfill_complete_percent_max, 1.0f);
        }
      }

      if (!response.state().empty()) {
        std::string current_state = response.state();
        if (current_state == "backfill_paused_by_oom") {
          aggregated_result.state = current_state;
        } else if (current_state == "backfill_in_progress" &&
                   aggregated_result.state != "backfill_paused_by_oom") {
          aggregated_result.state = current_state;
        } else if (current_state == "ready" &&
                   aggregated_result.state.empty()) {
          aggregated_result.state = current_state;
        }
      }

      if (!response.error().empty()) {
        if (aggregated_result.error.empty()) {
          aggregated_result.error = response.error();
        } else {
          aggregated_result.error += ";" + response.error();
        }
      }
    }
  }

  // Handle local result (similar to how search handles
  // std::deque<indexes::Neighbor>)
  void AddResults(InfoResult& local_result) {
    absl::MutexLock lock(&mutex);

    if (local_result.exists) {
      aggregated_result.exists = true;
      aggregated_result.index_name = local_result.index_name;
      aggregated_result.num_docs += local_result.num_docs;
      aggregated_result.num_records += local_result.num_records;
      aggregated_result.hash_indexing_failures +=
          local_result.hash_indexing_failures;
      aggregated_result.backfill_scanned_count +=
          local_result.backfill_scanned_count;
      aggregated_result.backfill_db_size += local_result.backfill_db_size;
      aggregated_result.backfill_inqueue_tasks +=
          local_result.backfill_inqueue_tasks;
      aggregated_result.mutation_queue_size += local_result.mutation_queue_size;
      aggregated_result.recent_mutations_queue_delay +=
          local_result.recent_mutations_queue_delay;

      if (local_result.backfill_in_progress) {
        aggregated_result.backfill_in_progress = true;
        if (aggregated_result.backfill_complete_percent_max == 0.0f &&
            aggregated_result.backfill_complete_percent_min == 0.0f) {
          aggregated_result.backfill_complete_percent_min =
              local_result.backfill_complete_percent;
          aggregated_result.backfill_complete_percent_max =
              local_result.backfill_complete_percent;
        } else {
          aggregated_result.backfill_complete_percent_min =
              std::min(aggregated_result.backfill_complete_percent_min,
                       local_result.backfill_complete_percent);
          aggregated_result.backfill_complete_percent_max =
              std::max(aggregated_result.backfill_complete_percent_max,
                       local_result.backfill_complete_percent);
        }
      } else {
        if (aggregated_result.backfill_complete_percent_max == 0.0f &&
            aggregated_result.backfill_complete_percent_min == 0.0f) {
          aggregated_result.backfill_complete_percent_min = 1.0f;
          aggregated_result.backfill_complete_percent_max = 1.0f;
        } else {
          aggregated_result.backfill_complete_percent_min =
              std::min(aggregated_result.backfill_complete_percent_min, 1.0f);
          aggregated_result.backfill_complete_percent_max =
              std::max(aggregated_result.backfill_complete_percent_max, 1.0f);
        }
      }

      if (!local_result.state.empty()) {
        std::string current_state = local_result.state;
        if (current_state == "backfill_paused_by_oom") {
          aggregated_result.state = current_state;
        } else if (current_state == "backfill_in_progress" &&
                   aggregated_result.state != "backfill_paused_by_oom") {
          aggregated_result.state = current_state;
        } else if (current_state == "ready" &&
                   aggregated_result.state.empty()) {
          aggregated_result.state = current_state;
        }
      }

      if (!local_result.error.empty()) {
        if (aggregated_result.error.empty()) {
          aggregated_result.error = local_result.error;
        } else {
          aggregated_result.error += ";" + local_result.error;
        }
      }
    }
  }

  void HandleError(const std::string& error_message) {
    absl::MutexLock lock(&mutex);

    VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
        << "Error during info fanout: " << error_message;

    if (aggregated_result.error.empty()) {
      aggregated_result.error = error_message;
    } else {
      aggregated_result.error += ";" + error_message;
    }
  }

  ~InfoPartitionResultsTracker() {
    absl::MutexLock lock(&mutex);
    absl::StatusOr<InfoResult> result = aggregated_result;
    callback(result, std::move(parameters));
  }
};

void PerformRemoteInfoRequest(
    std::unique_ptr<coordinator::InfoIndexPartitionRequest> request,
    const std::string& address,
    coordinator::ClientPool* coordinator_client_pool,
    std::shared_ptr<InfoPartitionResultsTracker> tracker) {
  auto client = coordinator_client_pool->GetClient(address);

  client->InfoIndexPartition(
      std::move(request),
      [tracker, address = std::string(address)](
          grpc::Status status,
          coordinator::InfoIndexPartitionResponse& response) mutable {
        if (status.ok()) {
          tracker->AddResults(response);
        } else {
          VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
              << "Error during handling of FT.INFO on node " << address << ": "
              << status.error_message();
          tracker->HandleError("gRPC error on node " + address + ": " +
                               status.error_message());
        }
      });
}

void PerformRemoteInfoRequestAsync(
    std::unique_ptr<coordinator::InfoIndexPartitionRequest> request,
    const std::string& address,
    coordinator::ClientPool* coordinator_client_pool,
    std::shared_ptr<InfoPartitionResultsTracker> tracker,
    vmsdk::ThreadPool* thread_pool) {
  thread_pool->Schedule(
      [coordinator_client_pool, address = std::string(address),
       request = std::move(request), tracker]() mutable {
        PerformRemoteInfoRequest(std::move(request), address,
                                 coordinator_client_pool, tracker);
      },
      vmsdk::ThreadPool::Priority::kHigh);
}

InfoResult GetLocalInfoResult(ValkeyModuleCtx* ctx,
                              const std::string& index_name) {
  InfoResult result;

  // Get local index schema and extract info
  auto index_schema_result = SchemaManager::Instance().GetIndexSchema(
      ValkeyModule_GetSelectedDb(ctx), index_name);

  if (index_schema_result.ok()) {
    auto index_schema = index_schema_result.value();
    IndexSchema::InfoIndexPartitionData data =
        index_schema->GetInfoIndexPartitionData();
    result.exists = true;
    result.index_name = index_name;
    result.num_docs = data.num_docs;
    result.num_records = data.num_records;
    result.hash_indexing_failures = data.hash_indexing_failures;
    result.backfill_scanned_count = data.backfill_scanned_count;
    result.backfill_db_size = data.backfill_db_size;
    result.backfill_inqueue_tasks = data.backfill_inqueue_tasks;
    result.mutation_queue_size = data.mutation_queue_size;
    result.recent_mutations_queue_delay = data.recent_mutations_queue_delay;
    result.backfill_in_progress = data.backfill_in_progress;
    result.backfill_complete_percent = data.backfill_complete_percent;
    result.state = data.state;
    result.error = "";
  } else {
    result.exists = false;
    result.index_name = index_name;
    result.error = std::string("Index not found: ") +
                   std::string(index_schema_result.status().message());
  }

  return result;
}

absl::Status PerformInfoFanoutAsync(
    ValkeyModuleCtx* ctx, std::vector<fanout::FanoutSearchTarget>& info_targets,
    coordinator::ClientPool* coordinator_client_pool,
    std::unique_ptr<InfoParameters> parameters, vmsdk::ThreadPool* thread_pool,
    InfoResponseCallback callback) {
  auto request =
      coordinator::CreateInfoIndexPartitionRequest(parameters->index_name);
  auto tracker = std::make_shared<InfoPartitionResultsTracker>(
      info_targets.size(), std::move(callback), std::move(parameters));

  bool has_local_target = false;

  for (auto& node : info_targets) {
    if (node.type == fanout::FanoutSearchTarget::Type::kLocal) {
      has_local_target = true;
      continue;
    }

    auto request_copy =
        std::make_unique<coordinator::InfoIndexPartitionRequest>();
    request_copy->CopyFrom(*request);

    // Use async scheduling for better performance with many nodes
    if (info_targets.size() >= 30 && thread_pool->Size() > 1) {
      PerformRemoteInfoRequestAsync(std::move(request_copy), node.address,
                                    coordinator_client_pool, tracker,
                                    thread_pool);
    } else {
      PerformRemoteInfoRequest(std::move(request_copy), node.address,
                               coordinator_client_pool, tracker);
    }
  }

  if (has_local_target) {
    vmsdk::RunByMain(
        [ctx, index_name = tracker->parameters->index_name, tracker]() {
          auto local_result = GetLocalInfoResult(ctx, index_name);
          tracker->AddResults(local_result);
        });
  }

  return absl::OkStatus();
}

std::vector<fanout::FanoutSearchTarget> GetInfoTargetsForFanout(
    ValkeyModuleCtx* ctx) {
  return fanout::GetSearchTargetsForFanout(ctx, true);
}

}  // namespace valkey_search::query::info_fanout
