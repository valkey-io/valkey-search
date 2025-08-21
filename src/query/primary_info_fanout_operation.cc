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

PrimaryInfoFanoutOperation::PrimaryInfoFanoutOperation(std::string index_name,
                                                       unsigned timeout_ms)
    : fanout::FanoutOperationBase<coordinator::InfoIndexPartitionRequest,
                                  coordinator::InfoIndexPartitionResponse,
                                  fanout::FanoutTargetMode::kPrimary>(),
      index_name_(index_name),
      timeout_ms_(timeout_ms),
      exists_(false),
      num_docs_(0),
      num_records_(0),
      hash_indexing_failures_(0) {}

unsigned PrimaryInfoFanoutOperation::GetTimeoutMs() const {
  return timeout_ms_;
}

coordinator::InfoIndexPartitionRequest
PrimaryInfoFanoutOperation::GenerateRequest(const fanout::FanoutSearchTarget&,
                                            unsigned timeout_ms) {
  coordinator::InfoIndexPartitionRequest req;
  req.set_index_name(index_name_);
  req.set_timeout_ms(timeout_ms);
  return req;
}

void PrimaryInfoFanoutOperation::OnResponse(
    const coordinator::InfoIndexPartitionResponse& resp,
    [[maybe_unused]] const fanout::FanoutSearchTarget& target) {
  absl::MutexLock lock(&mutex_);
  if (!resp.error().empty()) {
    grpc::Status status =
        grpc::Status(grpc::StatusCode::INTERNAL, resp.error());
    OnError(status, resp.error_type(), target);
    return;
  }
  if (!resp.exists()) {
    grpc::Status status =
        grpc::Status(grpc::StatusCode::INTERNAL, "Index does not exists");
    OnError(status, coordinator::FanoutErrorType::INDEX_NAME_ERROR, target);
    return;
  }
  if (!schema_fingerprint_.has_value()) {
    schema_fingerprint_ = resp.schema_fingerprint();
  } else if (schema_fingerprint_.value() != resp.schema_fingerprint()) {
    grpc::Status status =
        grpc::Status(grpc::StatusCode::INTERNAL,
                     "Cluster not in a consistent state, please retry.");
    OnError(status, coordinator::FanoutErrorType::INCONSISTENT_STATE_ERROR,
            target);
    return;
  }
  if (!version_.has_value()) {
    version_ = resp.version();
  } else if (version_.value() != resp.version()) {
    grpc::Status status =
        grpc::Status(grpc::StatusCode::INTERNAL,
                     "Cluster not in a consistent state, please retry.");
    OnError(status, coordinator::FanoutErrorType::INCONSISTENT_STATE_ERROR,
            target);
    return;
  }
  if (resp.index_name() != index_name_) {
    grpc::Status status =
        grpc::Status(grpc::StatusCode::INTERNAL,
                     "Cluster not in a consistent state, please retry.");
    OnError(status, coordinator::FanoutErrorType::INCONSISTENT_STATE_ERROR,
            target);
    return;
  }
  exists_ = true;
  num_docs_ += resp.num_docs();
  num_records_ += resp.num_records();
  hash_indexing_failures_ += resp.hash_indexing_failures();
}

coordinator::InfoIndexPartitionResponse
PrimaryInfoFanoutOperation::GetLocalResponse(
    int db_id, const coordinator::InfoIndexPartitionRequest& request,
    [[maybe_unused]] const fanout::FanoutSearchTarget& target) {
  auto index_schema_result =
      SchemaManager::Instance().GetIndexSchema(db_id, request.index_name());

  coordinator::InfoIndexPartitionResponse resp;

  if (!index_schema_result.ok()) {
    resp.set_exists(false);
    resp.set_index_name(request.index_name());
    resp.set_error_type(coordinator::FanoutErrorType::INDEX_NAME_ERROR);
    return resp;
  }

  auto index_schema = index_schema_result.value();
  IndexSchema::InfoIndexPartitionData data =
      index_schema->GetInfoIndexPartitionData();

  std::optional<uint64_t> fingerprint;
  std::optional<uint32_t> version;

  auto global_metadata =
      coordinator::MetadataManager::Instance().GetGlobalMetadata();
  if (global_metadata->type_namespace_map().contains(
          kSchemaManagerMetadataTypeName)) {
    const auto& entry_map = global_metadata->type_namespace_map().at(
        kSchemaManagerMetadataTypeName);
    if (entry_map.entries().contains(request.index_name())) {
      const auto& entry = entry_map.entries().at(request.index_name());
      fingerprint = entry.fingerprint();
      version = entry.version();
    }
  }

  if (!fingerprint.has_value() || !version.has_value()) {
    resp.set_exists(false);
    resp.set_index_name(request.index_name());
    resp.set_error_type(coordinator::FanoutErrorType::INCONSISTENT_STATE_ERROR);
    return resp;
  }
  resp.set_exists(true);
  resp.set_index_name(request.index_name());
  resp.set_num_docs(data.num_docs);
  resp.set_num_records(data.num_records);
  resp.set_hash_indexing_failures(data.hash_indexing_failures);
  resp.set_schema_fingerprint(fingerprint.value());
  resp.set_version(version.value());
  resp.set_error("");
  return resp;
}

void PrimaryInfoFanoutOperation::InvokeRemoteRpc(
    coordinator::Client* client,
    const coordinator::InfoIndexPartitionRequest& request,
    std::function<void(grpc::Status, coordinator::InfoIndexPartitionResponse&)>
        callback,
    unsigned timeout_ms) {
  std::unique_ptr<coordinator::InfoIndexPartitionRequest> request_ptr =
      std::make_unique<coordinator::InfoIndexPartitionRequest>(request);
  client->InfoIndexPartition(std::move(request_ptr), std::move(callback),
                             timeout_ms);
}

int PrimaryInfoFanoutOperation::GenerateReply(ValkeyModuleCtx* ctx,
                                              ValkeyModuleString** argv,
                                              int argc) {
  if (!index_name_error_nodes.empty() || !communication_error_nodes.empty() ||
      !inconsistent_state_error_nodes.empty()) {
    return FanoutOperationBase::GenerateErrorReply(ctx);
  }
  ValkeyModule_ReplyWithArray(ctx, 10);
  ValkeyModule_ReplyWithSimpleString(ctx, "mode");
  ValkeyModule_ReplyWithSimpleString(ctx, "primary");
  ValkeyModule_ReplyWithSimpleString(ctx, "index_name");
  ValkeyModule_ReplyWithSimpleString(ctx, index_name_.c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "num_docs");
  ValkeyModule_ReplyWithCString(ctx, std::to_string(num_docs_).c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "num_records");
  ValkeyModule_ReplyWithCString(ctx, std::to_string(num_records_).c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "hash_indexing_failures");
  ValkeyModule_ReplyWithCString(
      ctx, std::to_string(hash_indexing_failures_).c_str());
  return VALKEYMODULE_OK;
}

void PrimaryInfoFanoutOperation::ResetForRetry() {
  exists_ = false;
  schema_fingerprint_.reset();
  version_.reset();
  num_docs_ = 0;
  num_records_ = 0;
  hash_indexing_failures_ = 0;
}

bool PrimaryInfoFanoutOperation::ShouldRetry() {
  return !inconsistent_state_error_nodes.empty();
}

}  // namespace valkey_search::query::primary_info_fanout
