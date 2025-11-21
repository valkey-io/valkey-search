/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/primary_info_fanout_operation.h"

#include "src/coordinator/metadata_manager.h"
#include "src/schema_manager.h"
#include "vmsdk/src/info.h"

namespace valkey_search::query::primary_info_fanout {

CONTROLLED_BOOLEAN(ForceInfoInvalidIndexFingerprint, false);

PrimaryInfoFanoutOperation::PrimaryInfoFanoutOperation(
    uint32_t db_num, const std::string& index_name, unsigned timeout_ms,
    bool enable_partial_results, bool require_consistency)
    : fanout::FanoutOperationBase<
          coordinator::InfoIndexPartitionRequest,
          coordinator::InfoIndexPartitionResponse,
          vmsdk::cluster_map::FanoutTargetMode::kPrimary>(
          enable_partial_results, require_consistency),
      db_num_(db_num),
      index_name_(index_name),
      timeout_ms_(timeout_ms),
      exists_(false),
      num_docs_(0),
      num_records_(0),
      hash_indexing_failures_(0) {
  if (require_consistency_) {
    // test only: force invalid index fingerprint and version
    if (ForceInfoInvalidIndexFingerprint.GetValue()) {
      expected_fingerprint_version_.set_fingerprint(404);
      expected_fingerprint_version_.set_version(404);
    } else {
      // Get expected fingerprint/version from local metadata
      auto global_metadata =
          coordinator::MetadataManager::Instance().GetGlobalMetadata();
      if (global_metadata->type_namespace_map().contains(
              kSchemaManagerMetadataTypeName)) {
        const auto& entry_map = global_metadata->type_namespace_map().at(
            kSchemaManagerMetadataTypeName);
        if (entry_map.entries().contains(index_name_)) {
          const auto& entry = entry_map.entries().at(index_name_);
          expected_fingerprint_version_.set_fingerprint(entry.fingerprint());
          expected_fingerprint_version_.set_version(entry.version());
        }
      }
    }
  }
}

std::vector<vmsdk::cluster_map::NodeInfo>
PrimaryInfoFanoutOperation::GetTargets() const {
  return ValkeySearch::Instance().GetClusterMap()->GetTargets(
      vmsdk::cluster_map::FanoutTargetMode::kPrimary);
}

unsigned PrimaryInfoFanoutOperation::GetTimeoutMs() const {
  return timeout_ms_;
}

coordinator::InfoIndexPartitionRequest
PrimaryInfoFanoutOperation::GenerateRequest(
    const vmsdk::cluster_map::NodeInfo&) {
  coordinator::InfoIndexPartitionRequest req;
  req.set_db_num(db_num_);
  req.set_index_name(index_name_);

  if (require_consistency_) {
    req.set_require_consistency(true);
    *req.mutable_index_fingerprint_version() = expected_fingerprint_version_;
  }

  return req;
}

void PrimaryInfoFanoutOperation::OnResponse(
    const coordinator::InfoIndexPartitionResponse& resp,
    [[maybe_unused]] const vmsdk::cluster_map::NodeInfo& target) {
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

  absl::MutexLock lock(&mutex_);
  exists_ = true;
  num_docs_ += resp.num_docs();
  num_records_ += resp.num_records();
  hash_indexing_failures_ += resp.hash_indexing_failures();
}

std::pair<grpc::Status, coordinator::InfoIndexPartitionResponse>
PrimaryInfoFanoutOperation::GetLocalResponse(
    const coordinator::InfoIndexPartitionRequest& request,
    [[maybe_unused]] const vmsdk::cluster_map::NodeInfo& target) {
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
  if (!index_name_error_nodes.empty() ||
      (!enable_partial_results_ && !communication_error_nodes.empty()) ||
      !inconsistent_state_error_nodes.empty()) {
    return FanoutOperationBase::GenerateErrorReply(ctx);
  }
  size_t reply_size = 10;
  if (vmsdk::info_field::GetShowDeveloper()) {
    reply_size += 4;
  }
  ValkeyModule_ReplyWithArray(ctx, reply_size);
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

  if (vmsdk::info_field::GetShowDeveloper()) {
    auto status_or_schema =
        SchemaManager::Instance().GetIndexSchema(db_num_, index_name_);
    auto schema = std::move(status_or_schema.value());
    ValkeyModule_ReplyWithSimpleString(ctx, "index_fingerprint");
    int64_t fingerprint = static_cast<int64_t>(schema->GetFingerprint());
    ValkeyModule_ReplyWithLongLong(ctx, fingerprint);
    ValkeyModule_ReplyWithSimpleString(ctx, "index_version");
    ValkeyModule_ReplyWithLongLong(ctx, schema->GetVersion());
  }

  return VALKEYMODULE_OK;
}

void PrimaryInfoFanoutOperation::ResetForRetry() {
  exists_ = false;
  num_docs_ = 0;
  num_records_ = 0;
  hash_indexing_failures_ = 0;
}

// retry condition: (1) inconsistent state (2) network error
bool PrimaryInfoFanoutOperation::ShouldRetry() {
  return !inconsistent_state_error_nodes.empty() ||
         !communication_error_nodes.empty() || !index_name_error_nodes.empty();
}

}  // namespace valkey_search::query::primary_info_fanout
