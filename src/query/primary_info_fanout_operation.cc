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
    std::string index_name, int timeout_ms,
    coordinator::ClientPool* client_pool, PrimaryInfoResponseCallback callback)
    : fanout::FanoutOperationBase<coordinator::InfoIndexPartitionRequest,
                                  coordinator::InfoIndexPartitionResponse,
                                  fanout::FanoutTargetMode::kPrimary>(),
      index_name_(index_name),
      timeout_ms_(timeout_ms),
      client_pool_(client_pool),
      callback_(std::move(callback)) {}

int PrimaryInfoFanoutOperation::GetTimeoutMs() const {
  return timeout_ms_.value_or(5000);
}

coordinator::InfoIndexPartitionRequest
PrimaryInfoFanoutOperation::GenerateRequest(const fanout::FanoutSearchTarget&,
                                            int timeout_ms) {
  coordinator::InfoIndexPartitionRequest req;
  req.set_index_name(index_name_);
  req.set_timeout_ms(timeout_ms);
  return req;
}

void PrimaryInfoFanoutOperation::OnResponse(
    const coordinator::InfoIndexPartitionResponse& resp,
    const fanout::FanoutSearchTarget& /*target*/) {
  absl::MutexLock lock(&mutex_);

  if (!resp.error().empty()) {
    if (error_.empty()) {
      error_ = resp.error();
    } else {
      error_ += ";" + resp.error();
    }
    return;
  }

  if (resp.exists()) {
    if (!schema_fingerprint_.has_value()) {
      schema_fingerprint_ = resp.schema_fingerprint();
    } else if (schema_fingerprint_.value() != resp.schema_fingerprint()) {
      has_schema_mismatch_ = true;
      error_ = "found index schema inconsistency in the cluster";
      return;
    }
    if (!encoding_version_.has_value()) {
      encoding_version_ = resp.encoding_version();
    } else if (encoding_version_.value() != resp.encoding_version()) {
      has_version_mismatch_ = true;
      error_ = "found index schema version inconsistency in the cluster";
      return;
    }
    exists_ = true;
    index_name_ = resp.index_name();
    num_docs_ += resp.num_docs();
    num_records_ += resp.num_records();
    hash_indexing_failures_ += resp.hash_indexing_failures();
  }
}

void PrimaryInfoFanoutOperation::OnError(
    const std::string& error, const fanout::FanoutSearchTarget& /*target*/) {
  absl::MutexLock lock(&mutex_);
  if (error_.empty()) {
    error_ = error;
  } else {
    error_ += ";" + error;
  }
}

void PrimaryInfoFanoutOperation::OnCompletion() {
  absl::MutexLock lock(&mutex_);

  PrimaryInfoResult result;
  result.exists = exists_;
  result.index_name = index_name_;
  result.num_docs = num_docs_;
  result.num_records = num_records_;
  result.hash_indexing_failures = hash_indexing_failures_;
  result.error = error_;
  result.schema_fingerprint = schema_fingerprint_;
  result.has_schema_mismatch = has_schema_mismatch_;
  result.encoding_version = encoding_version_;
  result.has_version_mismatch = has_version_mismatch_;

  if (callback_) {
    std::unique_ptr<PrimaryInfoParameters> parameters =
        std::make_unique<PrimaryInfoParameters>();
    parameters->index_name = index_name_;
    parameters->timeout_ms = timeout_ms_.value_or(5000);
    callback_(absl::StatusOr<PrimaryInfoResult>(result), std::move(parameters));
  }
  delete this;
}

void PrimaryInfoFanoutOperation::FillLocalResponse(
    ValkeyModuleCtx* ctx, const coordinator::InfoIndexPartitionRequest& request,
    coordinator::InfoIndexPartitionResponse& resp,
    const fanout::FanoutSearchTarget& /*target*/) {
  auto index_schema_result = SchemaManager::Instance().GetIndexSchema(
      ValkeyModule_GetSelectedDb(ctx), request.index_name());

  if (!index_schema_result.ok()) {
    resp.set_exists(false);
    resp.set_index_name(request.index_name());
    resp.set_error(std::string("Index not found: ") +
                   std::string(index_schema_result.status().message()));
    return;
  }

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

  resp.set_exists(true);
  resp.set_index_name(request.index_name());
  resp.set_num_docs(data.num_docs);
  resp.set_num_records(data.num_records);
  resp.set_hash_indexing_failures(data.hash_indexing_failures);
  if (fingerprint) {
    resp.set_schema_fingerprint(fingerprint);
  }
  if (encoding_version) {
    resp.set_encoding_version(encoding_version);
  }
  resp.set_error("");
}

void PrimaryInfoFanoutOperation::InvokeRemoteRpc(
    coordinator::Client* client,
    const coordinator::InfoIndexPartitionRequest& request,
    std::function<void(grpc::Status, coordinator::InfoIndexPartitionResponse&)>
        callback,
    int timeout_ms) {
  std::unique_ptr<coordinator::InfoIndexPartitionRequest> request_ptr =
      std::make_unique<coordinator::InfoIndexPartitionRequest>(request);
  client->InfoIndexPartition(std::move(request_ptr), std::move(callback),
                             timeout_ms);
}

}  // namespace valkey_search::query::primary_info_fanout
