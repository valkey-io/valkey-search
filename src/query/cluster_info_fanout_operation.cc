/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/cluster_info_fanout_operation.h"

#include "src/coordinator/metadata_manager.h"
#include "src/schema_manager.h"

namespace valkey_search::query::cluster_info_fanout {

ClusterInfoFanoutOperation::ClusterInfoFanoutOperation(
    uint32_t db_num, const std::string& index_name, unsigned timeout_ms,
    bool enable_partial_results, bool enable_consistency)
    : fanout::FanoutOperationBase<coordinator::InfoIndexPartitionRequest,
                                  coordinator::InfoIndexPartitionResponse,
                                  vmsdk::cluster_map::FanoutTargetMode::kAll>(
          enable_partial_results, enable_consistency),
      db_num_(db_num),
      index_name_(index_name),
      timeout_ms_(timeout_ms),
      exists_(false),
      backfill_complete_percent_max_(0.0f),
      backfill_complete_percent_min_(0.0f),
      backfill_in_progress_(false) {
  // Get expected fingerprint/version from local metadata
  auto global_metadata =
      coordinator::MetadataManager::Instance().GetGlobalMetadata();
  if (global_metadata->type_namespace_map().contains(
          kSchemaManagerMetadataTypeName)) {
    const auto& entry_map = global_metadata->type_namespace_map().at(
        kSchemaManagerMetadataTypeName);
    if (entry_map.entries().contains(index_name_)) {
      const auto& entry = entry_map.entries().at(index_name_);
      expected_fingerprint_version_.emplace();
      expected_fingerprint_version_->set_fingerprint(entry.fingerprint());
      expected_fingerprint_version_->set_version(entry.version());
    }
  }
}

std::vector<vmsdk::cluster_map::NodeInfo>
ClusterInfoFanoutOperation::GetTargets() const {
  return ValkeySearch::Instance().GetClusterMap()->GetTargets(
      vmsdk::cluster_map::FanoutTargetMode::kAll);
}

unsigned ClusterInfoFanoutOperation::GetTimeoutMs() const {
  return timeout_ms_;
}

coordinator::InfoIndexPartitionRequest
ClusterInfoFanoutOperation::GenerateRequest(
    const vmsdk::cluster_map::NodeInfo&) {
  coordinator::InfoIndexPartitionRequest req;
  req.set_db_num(db_num_);
  req.set_index_name(index_name_);

  if (expected_fingerprint_version_.has_value()) {
    *req.mutable_index_fingerprint_version() =
        expected_fingerprint_version_.value();
  }

  return req;
}

void ClusterInfoFanoutOperation::OnResponse(
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

  exists_ = true;
  float node_percent = resp.backfill_complete_percent();
  if (backfill_complete_percent_max_ < node_percent) {
    backfill_complete_percent_max_ = node_percent;
  }
  if (backfill_complete_percent_min_ == 0.0f ||
      backfill_complete_percent_min_ > node_percent) {
    backfill_complete_percent_min_ = node_percent;
  }
  backfill_in_progress_ = backfill_in_progress_ || resp.backfill_in_progress();
  std::string current_state = resp.state();
  if (current_state == "backfill_paused_by_oom") {
    state_ = current_state;
  } else if (current_state == "backfill_in_progress" &&
             state_ != "backfill_paused_by_oom") {
    state_ = current_state;
  } else if (current_state == "ready" && state_.empty()) {
    state_ = current_state;
  }
}

std::pair<grpc::Status, coordinator::InfoIndexPartitionResponse>
ClusterInfoFanoutOperation::GetLocalResponse(
    const coordinator::InfoIndexPartitionRequest& request,
    [[maybe_unused]] const vmsdk::cluster_map::NodeInfo& target) {
  return coordinator::Service::GenerateInfoResponse(request);
}

void ClusterInfoFanoutOperation::InvokeRemoteRpc(
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

int ClusterInfoFanoutOperation::GenerateReply(ValkeyModuleCtx* ctx,
                                              ValkeyModuleString** argv,
                                              int argc) {
  if (!index_name_error_nodes.empty() || !communication_error_nodes.empty() ||
      !inconsistent_state_error_nodes.empty()) {
    return FanoutOperationBase::GenerateErrorReply(ctx);
  }
  ValkeyModule_ReplyWithArray(ctx, 12);
  ValkeyModule_ReplyWithSimpleString(ctx, "mode");
  ValkeyModule_ReplyWithSimpleString(ctx, "cluster");
  ValkeyModule_ReplyWithSimpleString(ctx, "index_name");
  ValkeyModule_ReplyWithSimpleString(ctx, index_name_.c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "backfill_in_progress");
  ValkeyModule_ReplyWithCString(ctx, backfill_in_progress_ ? "1" : "0");
  ValkeyModule_ReplyWithSimpleString(ctx, "backfill_complete_percent_max");
  ValkeyModule_ReplyWithCString(
      ctx, std::to_string(backfill_complete_percent_max_).c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "backfill_complete_percent_min");
  ValkeyModule_ReplyWithCString(
      ctx, std::to_string(backfill_complete_percent_min_).c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "state");
  ValkeyModule_ReplyWithSimpleString(ctx, state_.c_str());
  return VALKEYMODULE_OK;
}

void ClusterInfoFanoutOperation::ResetForRetry() {
  exists_ = false;
  backfill_complete_percent_max_ = 0.0f;
  backfill_complete_percent_min_ = 0.0f;
  backfill_in_progress_ = false;
  state_ = "";
}

// retry condition: (1) inconsistent state (2) network error (3) index name
// error
bool ClusterInfoFanoutOperation::ShouldRetry() {
  return !inconsistent_state_error_nodes.empty() ||
         !communication_error_nodes.empty() || !index_name_error_nodes.empty();
}

}  // namespace valkey_search::query::cluster_info_fanout
