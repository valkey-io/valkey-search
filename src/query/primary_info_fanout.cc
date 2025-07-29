/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/primary_info_fanout.h"

#include <algorithm>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/coordinator/info_converter.h"
#include "src/coordinator/metadata_manager.h"
#include "src/query/fanout.h"
#include "src/schema_manager.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/thread_pool.h"

namespace valkey_search::query::primary_info_fanout {

// InfoPartitionResultsTracker is a thread-safe class that tracks the results
// of an info fanout. It aggregates the results from multiple nodes and returns
// the aggregated result to the callback.
struct PrimaryInfoPartitionResultsTracker {
  absl::Mutex mutex;
  PrimaryInfoResult aggregated_result ABSL_GUARDED_BY(mutex);
  int outstanding_requests ABSL_GUARDED_BY(mutex);
  PrimaryInfoResponseCallback callback;
  std::unique_ptr<PrimaryInfoParameters> parameters ABSL_GUARDED_BY(mutex);

  PrimaryInfoPartitionResultsTracker(
      int outstanding_requests, PrimaryInfoResponseCallback callback,
      std::unique_ptr<PrimaryInfoParameters> parameters)
      : outstanding_requests(outstanding_requests),
        callback(std::move(callback)),
        parameters(std::move(parameters)) {}

  void AddResults(coordinator::InfoIndexPartitionResponse& response) {
    absl::MutexLock lock(&mutex);

    if (response.exists()) {
      // Check fingerprint consistency first
      if (!aggregated_result.schema_fingerprint.has_value()) {
        aggregated_result.schema_fingerprint = response.schema_fingerprint();
      } else if (aggregated_result.schema_fingerprint.value() !=
                 response.schema_fingerprint()) {
        VMSDK_LOG(WARNING, nullptr)
            << "Schema fingerprint mismatch detected! Reference: "
            << aggregated_result.schema_fingerprint.value()
            << ", Remote node: " << response.schema_fingerprint();
        aggregated_result.has_schema_mismatch = true;
        aggregated_result.error =
            "found index schema inconsistency in the cluster";
        return;
      }

      // Check version consistency
      if (!aggregated_result.encoding_version.has_value()) {
        aggregated_result.encoding_version = response.encoding_version();
      } else if (aggregated_result.encoding_version.value() !=
                 response.encoding_version()) {
        VMSDK_LOG(WARNING, nullptr)
            << "Encoding version mismatch detected! Reference: "
            << aggregated_result.encoding_version.value()
            << ", Remote node: " << response.encoding_version();
        aggregated_result.has_version_mismatch = true;
        aggregated_result.error =
            "found index schema version inconsistency in the cluster";
        return;
      }

      aggregated_result.exists = true;
      aggregated_result.index_name = response.index_name();
      aggregated_result.num_docs += response.num_docs();
      aggregated_result.num_records += response.num_records();
      aggregated_result.hash_indexing_failures +=
          response.hash_indexing_failures();

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
  void AddResults(valkey_search::query::primary_info_fanout::PrimaryInfoResult&
                      local_result) {
    absl::MutexLock lock(&mutex);

    if (local_result.exists) {
      // Check fingerprint consistency first
      if (!aggregated_result.schema_fingerprint.has_value()) {
        aggregated_result.schema_fingerprint = local_result.schema_fingerprint;
      } else if (local_result.schema_fingerprint.has_value() &&
                 aggregated_result.schema_fingerprint.value() !=
                     local_result.schema_fingerprint.value()) {
        VMSDK_LOG(WARNING, nullptr)
            << "Schema fingerprint mismatch detected! Reference: "
            << aggregated_result.schema_fingerprint.value()
            << ", Local node: " << local_result.schema_fingerprint.value();
        aggregated_result.has_schema_mismatch = true;
        aggregated_result.error =
            "found index schema inconsistency in the cluster";
        return;
      }

      // Check version consistency
      if (!aggregated_result.encoding_version.has_value()) {
        aggregated_result.encoding_version = local_result.encoding_version;
      } else if (local_result.encoding_version.has_value() &&
                 aggregated_result.encoding_version.value() !=
                     local_result.encoding_version.value()) {
        VMSDK_LOG(WARNING, nullptr)
            << "Encoding version mismatch detected! Reference: "
            << aggregated_result.encoding_version.value()
            << ", Local node: " << local_result.encoding_version.value();
        aggregated_result.has_version_mismatch = true;
        aggregated_result.error =
            "found index schema version inconsistency in the cluster";
        return;
      }

      aggregated_result.exists = true;
      aggregated_result.index_name = local_result.index_name;
      aggregated_result.num_docs += local_result.num_docs;
      aggregated_result.num_records += local_result.num_records;
      aggregated_result.hash_indexing_failures +=
          local_result.hash_indexing_failures;

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

  ~PrimaryInfoPartitionResultsTracker() {
    absl::MutexLock lock(&mutex);
    absl::StatusOr<PrimaryInfoResult> result = aggregated_result;
    callback(result, std::move(parameters));
  }
};

void PerformRemotePrimaryInfoRequest(
    std::unique_ptr<coordinator::InfoIndexPartitionRequest> request,
    const std::string& address,
    coordinator::ClientPool* coordinator_client_pool,
    std::shared_ptr<PrimaryInfoPartitionResultsTracker> tracker) {
  auto client = coordinator_client_pool->GetClient(address);

  int timeout_ms;
  {
    absl::MutexLock lock(&tracker->mutex);
    timeout_ms = tracker->parameters->timeout_ms;
  }

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
      },
      timeout_ms);
}

void PerformRemotePrimaryInfoRequestAsync(
    std::unique_ptr<coordinator::InfoIndexPartitionRequest> request,
    const std::string& address,
    coordinator::ClientPool* coordinator_client_pool,
    std::shared_ptr<PrimaryInfoPartitionResultsTracker> tracker,
    vmsdk::ThreadPool* thread_pool) {
  thread_pool->Schedule(
      [coordinator_client_pool, address = std::string(address),
       request = std::move(request), tracker]() mutable {
        PerformRemotePrimaryInfoRequest(std::move(request), address,
                                        coordinator_client_pool, tracker);
      },
      vmsdk::ThreadPool::Priority::kHigh);
}

PrimaryInfoResult GetLocalPrimaryInfoResult(ValkeyModuleCtx* ctx,
                                            const std::string& index_name) {
  PrimaryInfoResult result;

  auto index_schema_result = SchemaManager::Instance().GetIndexSchema(
      ValkeyModule_GetSelectedDb(ctx), index_name);

  if (index_schema_result.ok()) {
    auto index_schema = index_schema_result.value();
    IndexSchema::InfoIndexPartitionData data =
        index_schema->GetInfoIndexPartitionData();

    uint64_t fingerprint = 0;
    uint32_t encoding_version = 0;

    // Get the full metadata to access both fingerprint and encoding_version
    auto global_metadata = coordinator::MetadataManager::Instance().GetGlobalMetadata();
    if (global_metadata->type_namespace_map().contains(kSchemaManagerMetadataTypeName)) {
      const auto& entry_map = global_metadata->type_namespace_map().at(kSchemaManagerMetadataTypeName);
      if (entry_map.entries().contains(index_name)) {
        const auto& entry = entry_map.entries().at(index_name);
        fingerprint = entry.fingerprint();
        encoding_version = entry.encoding_version();
      }
    }

    result.exists = true;
    result.index_name = index_name;
    result.num_docs = data.num_docs;
    result.num_records = data.num_records;
    result.hash_indexing_failures = data.hash_indexing_failures;
    result.error = "";
    result.schema_fingerprint = fingerprint;
    result.encoding_version = encoding_version;
  } else {
    result.exists = false;
    result.index_name = index_name;
    result.schema_fingerprint = std::nullopt;
    result.encoding_version = std::nullopt;
    result.error = std::string("Index not found: ") +
                   std::string(index_schema_result.status().message());
  }
  return result;
}

absl::Status PerformPrimaryInfoFanoutAsync(
    ValkeyModuleCtx* ctx, std::vector<fanout::FanoutSearchTarget>& info_targets,
    coordinator::ClientPool* coordinator_client_pool,
    std::unique_ptr<PrimaryInfoParameters> parameters,
    vmsdk::ThreadPool* thread_pool, PrimaryInfoResponseCallback callback) {
  auto request = coordinator::CreateInfoIndexPartitionRequest(
      parameters->index_name, parameters->timeout_ms);
  auto tracker = std::make_shared<PrimaryInfoPartitionResultsTracker>(
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
      PerformRemotePrimaryInfoRequestAsync(
          std::move(request_copy), node.address, coordinator_client_pool,
          tracker, thread_pool);
    } else {
      PerformRemotePrimaryInfoRequest(std::move(request_copy), node.address,
                                      coordinator_client_pool, tracker);
    }
  }

  if (has_local_target) {
    vmsdk::RunByMain(
        [ctx, index_name = tracker->parameters->index_name, tracker]() {
          auto local_result = GetLocalPrimaryInfoResult(ctx, index_name);
          tracker->AddResults(local_result);
        });
  }

  return absl::OkStatus();
}

}  // namespace valkey_search::query::primary_info_fanout
