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
#include "src/query/partition_results_tracker_base.h"
#include "src/schema_manager.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/thread_pool.h"

namespace valkey_search::query::primary_info_fanout {

class PrimaryInfoPartitionResultsTracker
  : public fanout::PartitionResultsTrackerBase<
        PrimaryInfoResult,
        coordinator::InfoIndexPartitionResponse,
        PrimaryInfoResult,
        PrimaryInfoParameters> {
 public:
  PrimaryInfoPartitionResultsTracker(
      PrimaryInfoResponseCallback callback,
      std::unique_ptr<PrimaryInfoParameters> params)
    : fanout::PartitionResultsTrackerBase<
          PrimaryInfoResult,
          coordinator::InfoIndexPartitionResponse,
          PrimaryInfoResult,
          PrimaryInfoParameters>(std::move(callback), std::move(params)) {}

 protected:
  void AggregateFromResponse(
      const coordinator::InfoIndexPartitionResponse& resp,
      PrimaryInfoResult& result) override {
    // copy old logic:
    if (!resp.error().empty()) {
      if (result.error.empty()) {
        result.error = resp.error();
      } else {
        result.error += ";" + resp.error();
      }
      return;
    }
    if (resp.exists()) {
      // fingerprint
      if (!result.schema_fingerprint.has_value()) {
        result.schema_fingerprint = resp.schema_fingerprint();
      } else if (result.schema_fingerprint.value() != resp.schema_fingerprint()) {
        result.has_schema_mismatch = true;
        result.error = "found index schema inconsistency in the cluster";
        return;
      }
      // version
      if (!result.encoding_version.has_value()) {
        result.encoding_version = resp.encoding_version();
      } else if (result.encoding_version.value() != resp.encoding_version()) {
        result.has_version_mismatch = true;
        result.error = "found index schema version inconsistency in the cluster";
        return;
      }
      result.exists = true;
      result.index_name = resp.index_name();
      result.num_docs += resp.num_docs();
      result.num_records += resp.num_records();
      result.hash_indexing_failures += resp.hash_indexing_failures();
    }
  }

  void AggregateFromLocal(
      const PrimaryInfoResult& local,
      PrimaryInfoResult& result) override {
    // same as AddResults(local)
    if (!local.error.empty()) {
      if (result.error.empty()) {
        result.error = local.error;
      } else {
        result.error += ";" + local.error;
      }
      return;
    }
    if (local.exists) {
      if (!result.schema_fingerprint.has_value()) {
        result.schema_fingerprint = local.schema_fingerprint;
      } else if (local.schema_fingerprint.has_value() &&
                 result.schema_fingerprint.value() != local.schema_fingerprint.value()) {
        result.has_schema_mismatch = true;
        result.error = "found index schema inconsistency in the cluster";
        return;
      }
      if (!result.encoding_version.has_value()) {
        result.encoding_version = local.encoding_version;
      } else if (local.encoding_version.has_value() &&
                 result.encoding_version.value() != local.encoding_version.value()) {
        result.has_version_mismatch = true;
        result.error = "found index schema version inconsistency in the cluster";
        return;
      }
      result.exists = true;
      result.index_name = local.index_name;
      result.num_docs += local.num_docs;
      result.num_records += local.num_records;
      result.hash_indexing_failures += local.hash_indexing_failures;
    }
  }

  void AggregateError(
      const std::string& err,
      PrimaryInfoResult& result) override {
    if (result.error.empty()) {
      result.error = err;
    } else {
      result.error += ";" + err;
    }
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
    absl::MutexLock lock(&tracker->GetMutex());
    timeout_ms = tracker->GetParameters()->timeout_ms;
  }

  client->InfoIndexPartition(
      std::move(request),
      [tracker, address = std::string(address)](
          grpc::Status status,
          coordinator::InfoIndexPartitionResponse& response) mutable {
        if (!status.ok()) {
          tracker->HandleError("gRPC error on node " + address + ": " +
                               status.error_message());
        } else {
          tracker->AddResults(response);
        }
      },
      timeout_ms);
}

PrimaryInfoResult GetLocalPrimaryInfoResult(ValkeyModuleCtx* ctx,
                                            const std::string& index_name) {
  PrimaryInfoResult result;

  auto index_schema_result = SchemaManager::Instance().GetIndexSchema(
      ValkeyModule_GetSelectedDb(ctx), index_name);

  if (!index_schema_result.ok()) {
    result.exists = false;
    result.index_name = index_name;
    result.schema_fingerprint = std::nullopt;
    result.encoding_version = std::nullopt;
    result.error = std::string("Index not found: ") +
                   std::string(index_schema_result.status().message());
  } else {
    auto index_schema = index_schema_result.value();
    IndexSchema::InfoIndexPartitionData data =
        index_schema->GetInfoIndexPartitionData();

    uint64_t fingerprint = 0;
    uint32_t encoding_version = 0;

    // Get the full metadata to access both fingerprint and encoding_version
    auto global_metadata =
        coordinator::MetadataManager::Instance().GetGlobalMetadata();
    if (global_metadata->type_namespace_map().contains(
            kSchemaManagerMetadataTypeName)) {
      const auto& entry_map = global_metadata->type_namespace_map().at(
          kSchemaManagerMetadataTypeName);
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
      std::move(callback), std::move(parameters));

  bool has_local_target = false;

  for (auto& node : info_targets) {
    if (node.type == fanout::FanoutSearchTarget::Type::kLocal) {
      has_local_target = true;
      continue;
    }

    auto request_copy =
        std::make_unique<coordinator::InfoIndexPartitionRequest>();
    request_copy->CopyFrom(*request);

    PerformRemotePrimaryInfoRequest(std::move(request_copy), node.address,
                                    coordinator_client_pool, tracker);
  }

  if (has_local_target) {
    vmsdk::RunByMain(
        [ctx, index_name = tracker->GetParameters()->index_name, tracker]() {
          auto local_result = GetLocalPrimaryInfoResult(ctx, index_name);
          tracker->AddResults(local_result);
        });
  }

  return absl::OkStatus();
}

}  // namespace valkey_search::query::primary_info_fanout
