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
    uint32_t db_num, const std::string& index_name, unsigned timeout_ms)
    : fanout::FanoutOperationBase<coordinator::InfoIndexPartitionRequest,
                                  coordinator::InfoIndexPartitionResponse,
                                  fanout::FanoutTargetMode::kPrimary>(),
      db_num_(db_num),
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
PrimaryInfoFanoutOperation::GenerateRequest(const fanout::FanoutSearchTarget&) {
  coordinator::InfoIndexPartitionRequest req;
  req.set_db_num(db_num_);
  req.set_index_name(index_name_);
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

std::pair<grpc::Status, coordinator::InfoIndexPartitionResponse>
PrimaryInfoFanoutOperation::GetLocalResponse(
    const coordinator::InfoIndexPartitionRequest& request,
    [[maybe_unused]] const fanout::FanoutSearchTarget& target) {
  return coordinator::Service::GenerateInfoResponse(request);
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

// retry condition: (1) inconsistent state (2) network error
bool PrimaryInfoFanoutOperation::ShouldRetry() {
  return !inconsistent_state_error_nodes.empty() ||
         !communication_error_nodes.empty();
}

}  // namespace valkey_search::query::primary_info_fanout
