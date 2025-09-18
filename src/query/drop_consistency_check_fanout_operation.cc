/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/drop_consistency_check_fanout_operation.h"

#include "src/coordinator/metadata_manager.h"
#include "src/schema_manager.h"

namespace valkey_search::query::drop_consistency_check_fanout {

DropConsistencyCheckFanoutOperation::DropConsistencyCheckFanoutOperation(
    uint32_t db_num, const std::string& index_name, unsigned timeout_ms)
    : fanout::FanoutOperationBase<coordinator::InfoIndexPartitionRequest,
                                  coordinator::InfoIndexPartitionResponse,
                                  fanout::FanoutTargetMode::kAll>(),
      db_num_(db_num),
      index_name_(index_name),
      timeout_ms_(timeout_ms) {}

unsigned DropConsistencyCheckFanoutOperation::GetTimeoutMs() const {
  return timeout_ms_;
}

coordinator::InfoIndexPartitionRequest
DropConsistencyCheckFanoutOperation::GenerateRequest(
    const fanout::FanoutSearchTarget&) {
  coordinator::InfoIndexPartitionRequest req;
  req.set_db_num(db_num_);
  req.set_index_name(index_name_);
  return req;
}

void DropConsistencyCheckFanoutOperation::OnResponse(
    const coordinator::InfoIndexPartitionResponse& resp,
    [[maybe_unused]] const fanout::FanoutSearchTarget& target) {
  // if the index exist on some node and returns a valid response, treat it as
  // inconsistent error
  inconsistent_state_error_nodes.push_back(target);
}

std::pair<grpc::Status, coordinator::InfoIndexPartitionResponse>
DropConsistencyCheckFanoutOperation::GetLocalResponse(
    const coordinator::InfoIndexPartitionRequest& request,
    [[maybe_unused]] const fanout::FanoutSearchTarget& target) {
  return coordinator::Service::GenerateInfoResponse(request);
}

void DropConsistencyCheckFanoutOperation::InvokeRemoteRpc(
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

int DropConsistencyCheckFanoutOperation::GenerateReply(
    ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc) {
  return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

void DropConsistencyCheckFanoutOperation::ResetForRetry() {}

// retry condition: (1) inconsistent state (2) network error (3) index still
// exists on some nodes
bool DropConsistencyCheckFanoutOperation::ShouldRetry() {
  VMSDK_LOG(NOTICE, nullptr)
      << "index name node error number is " << index_name_error_nodes.size();
  return !inconsistent_state_error_nodes.empty() ||
         !communication_error_nodes.empty() ||
         index_name_error_nodes.size() != targets_.size();
}

}  // namespace valkey_search::query::drop_consistency_check_fanout