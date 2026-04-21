/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/query/cluster_info_fanout_operation.h"
#include "src/query/fanout_operation_base.h"
#include "src/valkey_search.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

// Waits until an alias resolves to the expected index on all cluster nodes.
// Used by ALIASADD and ALIASUPDATE.
class AliasExistsConsistencyCheckFanoutOperation
    : public query::cluster_info_fanout::ClusterInfoFanoutOperation {
 public:
  AliasExistsConsistencyCheckFanoutOperation(
      uint32_t db_num, const std::string &alias_name, unsigned timeout_ms,
      coordinator::IndexFingerprintVersion expected_ifv)
      : ClusterInfoFanoutOperation(db_num, alias_name, timeout_ms, false,
                                   false),
        expected_ifv_(expected_ifv) {}

  coordinator::InfoIndexPartitionRequest GenerateRequest(
      const vmsdk::cluster_map::NodeInfo &) override {
    coordinator::InfoIndexPartitionRequest req;
    req.set_db_num(db_num_);
    req.set_index_name(index_name_);
    *req.mutable_index_fingerprint_version() = expected_ifv_;
    return req;
  }

  int GenerateReply(ValkeyModuleCtx *ctx, ValkeyModuleString **, int) override {
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  }

 private:
  coordinator::IndexFingerprintVersion expected_ifv_;
};

// Waits until an alias is no longer resolvable on any cluster node.
// Used by ALIASDEL.
class AliasRemovedConsistencyCheckFanoutOperation
    : public query::fanout::FanoutOperationBase<
          coordinator::InfoIndexPartitionRequest,
          coordinator::InfoIndexPartitionResponse,
          vmsdk::cluster_map::FanoutTargetMode::kAll> {
 public:
  AliasRemovedConsistencyCheckFanoutOperation(uint32_t db_num,
                                              const std::string &alias_name,
                                              unsigned timeout_ms)
      : FanoutOperationBase(false, false),
        db_num_(db_num),
        alias_name_(alias_name),
        timeout_ms_(timeout_ms) {}

  std::vector<vmsdk::cluster_map::NodeInfo> GetTargets() const override {
    return ValkeySearch::Instance().GetClusterMap()->GetTargets(
        vmsdk::cluster_map::FanoutTargetMode::kAll);
  }

  unsigned GetTimeoutMs() const override { return timeout_ms_; }

  coordinator::InfoIndexPartitionRequest GenerateRequest(
      const vmsdk::cluster_map::NodeInfo &) override {
    coordinator::InfoIndexPartitionRequest req;
    req.set_db_num(db_num_);
    req.set_index_name(alias_name_);
    return req;
  }

  void OnResponse(const coordinator::InfoIndexPartitionResponse &,
                  const vmsdk::cluster_map::NodeInfo &target) override {
    absl::MutexLock lock(&mutex_);
    inconsistent_state_error_nodes.push_back(target);
  }

  std::pair<grpc::Status, coordinator::InfoIndexPartitionResponse>
  GetLocalResponse(const coordinator::InfoIndexPartitionRequest &request,
                   const vmsdk::cluster_map::NodeInfo &) override {
    return coordinator::Service::GenerateInfoResponse(request);
  }

  void InvokeRemoteRpc(
      coordinator::Client *client,
      const coordinator::InfoIndexPartitionRequest &request,
      std::function<void(grpc::Status,
                         coordinator::InfoIndexPartitionResponse &)>
          callback,
      unsigned timeout_ms) override {
    client->InfoIndexPartition(
        std::make_unique<coordinator::InfoIndexPartitionRequest>(request),
        std::move(callback), timeout_ms);
  }

  int GenerateReply(ValkeyModuleCtx *ctx, ValkeyModuleString **, int) override {
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  }

  void ResetForRetry() override {}

  bool ShouldRetry() override {
    return !inconsistent_state_error_nodes.empty() ||
           !communication_error_nodes.empty() ||
           index_name_error_nodes.size() != targets_.size();
  }

 private:
  uint32_t db_num_;
  std::string alias_name_;
  unsigned timeout_ms_;
};

}  // namespace valkey_search
