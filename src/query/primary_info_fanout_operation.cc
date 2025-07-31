/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/primary_info_fanout_operation.h"

#include "src/coordinator/metadata_manager.h"
#include "src/schema_manager.h"

namespace valkey_search::query::primary_info_fanout {

PrimaryInfoFanoutOperation::PrimaryInfoFanoutOperation(
    ValkeyModuleCtx* ctx, coordinator::ClientPool* client_pool,
    std::unique_ptr<PrimaryInfoParameters> params,
    PrimaryInfoResponseCallback callback)
    : fanout::FanoutOperationBase<coordinator::InfoIndexPartitionRequest,
                                  coordinator::InfoIndexPartitionResponse,
                                  fanout::FanoutTargetMode::kPrimary>(ctx),
      client_pool_(client_pool),
      parameters_(std::move(params)),
      callback_(std::move(callback)),
      index_name_(parameters_ ? parameters_->index_name : "") {}

int PrimaryInfoFanoutOperation::GetTimeoutMs() const {
  return parameters_ ? parameters_->timeout_ms : 5000;
}

coordinator::InfoIndexPartitionRequest
PrimaryInfoFanoutOperation::GenerateRequest(const fanout::FanoutSearchTarget&,
                                            int timeout_ms) {
  coordinator::InfoIndexPartitionRequest req;
  if (parameters_) {
    req.set_index_name(parameters_->index_name);
    req.set_timeout_ms(timeout_ms);
  }
  return req;
}

void PrimaryInfoFanoutOperation::OnResponse(
    const coordinator::InfoIndexPartitionResponse& /*resp*/,
    const fanout::FanoutSearchTarget& /*target*/) {
  // Stub: add aggregation logic later
}

void PrimaryInfoFanoutOperation::OnError(
    const std::string& /*error*/,
    const fanout::FanoutSearchTarget& /*target*/) {
  // Stub: add error aggregation logic later
}

void PrimaryInfoFanoutOperation::OnCompletion() {
  // Stub: call the callback with dummy result for now
  if (callback_) {
    PrimaryInfoResult result;
    result.index_name = index_name_;
    callback_(absl::StatusOr<PrimaryInfoResult>(result),
              std::move(parameters_));
  }
}

void PrimaryInfoFanoutOperation::PerformLocalCall(
    const fanout::FanoutSearchTarget& target,
    const coordinator::InfoIndexPartitionRequest& request, int /*timeout_ms*/) {
  vmsdk::RunByMain([this, target, request]() {
    PrimaryInfoResult local_result;

    // 1. Look up the local index schema.
    auto index_schema_result = SchemaManager::Instance().GetIndexSchema(
        ValkeyModule_GetSelectedDb(ctx_), request.index_name());

    if (!index_schema_result.ok()) {
      // Index not found locally.
      local_result.exists = false;
      local_result.index_name = request.index_name();
      local_result.error = std::string("Index not found: ") +
                           std::string(index_schema_result.status().message());
      // Aggregate as an error.
      this->OnError(local_result.error, target);
      this->RpcDone();
      return;
    }

    // 2. Gather info/stats from schema and global metadata.
    auto index_schema = index_schema_result.value();
    IndexSchema::InfoIndexPartitionData data =
        index_schema->GetInfoIndexPartitionData();

    uint64_t fingerprint = 0;
    uint32_t encoding_version = 0;

    auto global_metadata =
        coordinator::MetadataManager::Instance().GetGlobalMetadata();
    if (global_metadata->type_namespace_map().contains(
            kSchemaManagerMetadataTypeName)) {
      const auto& entry_map = global_metadata->type_namespace_map().at(
          kSchemaManagerMetadataTypeName);
      if (entry_map.entries().contains(request.index_name())) {
        const auto& entry = entry_map.entries().at(request.index_name());
        fingerprint = entry.fingerprint();
        encoding_version = entry.encoding_version();
      }
    }

    local_result.exists = true;
    local_result.index_name = request.index_name();
    local_result.num_docs = data.num_docs;
    local_result.num_records = data.num_records;
    local_result.hash_indexing_failures = data.hash_indexing_failures;
    local_result.error = "";
    local_result.schema_fingerprint = fingerprint;
    local_result.encoding_version = encoding_version;

    // 3. Convert to proto for aggregation.
    coordinator::InfoIndexPartitionResponse resp;
    resp.set_exists(local_result.exists);
    resp.set_index_name(local_result.index_name);
    resp.set_num_docs(local_result.num_docs);
    resp.set_num_records(local_result.num_records);
    resp.set_hash_indexing_failures(local_result.hash_indexing_failures);
    if (local_result.schema_fingerprint) {
      resp.set_schema_fingerprint(*local_result.schema_fingerprint);
    }
    if (local_result.encoding_version) {
      resp.set_encoding_version(*local_result.encoding_version);
    }
    resp.set_error(local_result.error);

    // 4. Aggregate into operation (even if error is empty).
    this->OnResponse(resp, target);
    this->RpcDone();
  });
}

void PrimaryInfoFanoutOperation::PerformRemoteCall(
    const fanout::FanoutSearchTarget& target,
    const coordinator::InfoIndexPartitionRequest& request, int timeout_ms) {
  // Get the remote client for the target address
  auto client = client_pool_->GetClient(target.address);

  // Create a unique_ptr copy of the request for the RPC call
  auto request_ptr =
      std::make_unique<coordinator::InfoIndexPartitionRequest>(request);

  // Issue the async RPC call
  client->InfoIndexPartition(
      std::move(request_ptr),
      [this, target](grpc::Status status,
                     coordinator::InfoIndexPartitionResponse& response) {
        if (status.ok()) {
          this->OnResponse(response, target);
        } else {
          this->OnError("gRPC error on node " + target.address + ": " +
                            status.error_message(),
                        target);
        }
        this->RpcDone();
      },
      timeout_ms);
}

}  // namespace valkey_search::query::primary_info_fanout
