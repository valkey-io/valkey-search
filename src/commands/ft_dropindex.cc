/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "src/acl.h"
#include "src/commands/commands.h"
#include "src/query/fanout_operation_base.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/cluster_map.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

class DropConsistencyCheckFanoutOperation
    : public query::fanout::FanoutOperationBase<
          coordinator::InfoIndexPartitionRequest,
          coordinator::InfoIndexPartitionResponse,
          fanout::FanoutTargetMode::kAll> {
 public:
  DropConsistencyCheckFanoutOperation(uint32_t db_num,
                                      const std::string& index_name,
                                      unsigned timeout_ms)
      : query::fanout::FanoutOperationBase<
            coordinator::InfoIndexPartitionRequest,
            coordinator::InfoIndexPartitionResponse,
            fanout::FanoutTargetMode::kAll>(),
        db_num_(db_num),
        index_name_(index_name),
        timeout_ms_(timeout_ms){};

  unsigned GetTimeoutMs() const override { return timeout_ms_; }

  coordinator::InfoIndexPartitionRequest GenerateRequest(
      const vmsdk::cluster_map::NodeInfo&) override {
    coordinator::InfoIndexPartitionRequest req;
    req.set_db_num(db_num_);
    req.set_index_name(index_name_);
    return req;
  }

  void OnResponse(
      const coordinator::InfoIndexPartitionResponse& resp,
      [[maybe_unused]] const vmsdk::cluster_map::NodeInfo& target) override {
    // if the index exist on some node and returns a valid response, treat it as
    // inconsistent error
    absl::MutexLock lock(&mutex_);
    inconsistent_state_error_nodes.push_back(target);
  }

  std::pair<grpc::Status, coordinator::InfoIndexPartitionResponse>
  GetLocalResponse(
      const coordinator::InfoIndexPartitionRequest& request,
      [[maybe_unused]] const vmsdk::cluster_map::NodeInfo&) override {
    return coordinator::Service::GenerateInfoResponse(request);
  }

  void InvokeRemoteRpc(
      coordinator::Client* client,
      const coordinator::InfoIndexPartitionRequest& request,
      std::function<void(grpc::Status,
                         coordinator::InfoIndexPartitionResponse&)>
          callback,
      unsigned timeout_ms) override {
    std::unique_ptr<coordinator::InfoIndexPartitionRequest> request_ptr =
        std::make_unique<coordinator::InfoIndexPartitionRequest>(request);
    client->InfoIndexPartition(std::move(request_ptr), std::move(callback),
                               timeout_ms);
  }

  int GenerateReply(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                    int argc) override {
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  }

  // reset and clean the fields for new round of retry
  void ResetForRetry() override {};

  // decide which condition to run retry
  bool ShouldRetry() override {
    return !inconsistent_state_error_nodes.empty() ||
           !communication_error_nodes.empty() ||
           index_name_error_nodes.size() != targets_.size();
  }

 private:
  uint32_t db_num_;
  std::string index_name_;
  unsigned timeout_ms_;
};

absl::Status FTDropIndexCmd(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                            int argc) {
  if (argc != 2) {
    return absl::InvalidArgumentError(vmsdk::WrongArity(kDropIndexCommand));
  }
  auto index_schema_name = vmsdk::ToStringView(argv[1]);

  VMSDK_ASSIGN_OR_RETURN(
      auto index_schema,
      SchemaManager::Instance().GetIndexSchema(ValkeyModule_GetSelectedDb(ctx),
                                               index_schema_name));
  static const auto permissions =
      PrefixACLPermissions(kDropIndexCmdPermissions, kDropIndexCommand);
  VMSDK_RETURN_IF_ERROR(
      AclPrefixCheck(ctx, permissions, index_schema->GetKeyPrefixes()));

  VMSDK_RETURN_IF_ERROR(SchemaManager::Instance().RemoveIndexSchema(
      ValkeyModule_GetSelectedDb(ctx), index_schema_name));

  // directly handle reply in standalone mode
  // let fanout operation handle reply in cluster mode
  if (ValkeySearch::Instance().IsCluster() &&
      ValkeySearch::Instance().UsingCoordinator()) {
    unsigned timeout_ms = options::GetFTInfoTimeoutMs().GetValue();
    auto op = new DropConsistencyCheckFanoutOperation(
        ValkeyModule_GetSelectedDb(ctx), std::string(index_schema_name),
        timeout_ms);
    op->StartOperation(ctx);
  } else {
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  }
  ValkeyModule_ReplicateVerbatim(ctx);
  return absl::OkStatus();
}
}  // namespace valkey_search
