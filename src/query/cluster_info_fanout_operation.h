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
#include <optional>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "grpcpp/support/status.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/query/fanout_operation_base.h"

namespace valkey_search::query::cluster_info_fanout {

class ClusterInfoFanoutOperation
    : public fanout::FanoutOperationBase<
          coordinator::InfoIndexPartitionRequest,
          coordinator::InfoIndexPartitionResponse,
          vmsdk::cluster_map::FanoutTargetMode::kAll> {
 public:
  ClusterInfoFanoutOperation(uint32_t db_num, const std::string& index_name,
                             unsigned timeout_ms, bool enable_partial_results,
                             bool require_consistency);

  std::vector<vmsdk::cluster_map::NodeInfo> GetTargets() const override;

  unsigned GetTimeoutMs() const override;

  coordinator::InfoIndexPartitionRequest GenerateRequest(
      const vmsdk::cluster_map::NodeInfo&) override;

  void OnResponse(
      const coordinator::InfoIndexPartitionResponse& resp,
      [[maybe_unused]] const vmsdk::cluster_map::NodeInfo&) override;

  std::pair<grpc::Status, coordinator::InfoIndexPartitionResponse>
  GetLocalResponse(
      const coordinator::InfoIndexPartitionRequest& request,
      [[maybe_unused]] const vmsdk::cluster_map::NodeInfo&) override;

  void InvokeRemoteRpc(
      coordinator::Client* client,
      const coordinator::InfoIndexPartitionRequest& request,
      std::function<void(grpc::Status,
                         coordinator::InfoIndexPartitionResponse&)>
          callback,
      unsigned timeout_ms) override;

  int GenerateReply(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                    int argc) override;

  // reset and clean the fields for new round of retry
  void ResetForRetry() override;

  // decide which condition to run retry
  bool ShouldRetry() override;

 protected:
  bool exists_;
  uint32_t db_num_;
  std::string index_name_;
  unsigned timeout_ms_;
  float backfill_complete_percent_max_;
  float backfill_complete_percent_min_;
  bool backfill_in_progress_;
  std::string state_;
  coordinator::IndexFingerprintVersion expected_fingerprint_version_;
  struct AttributeData {
    std::string identifier;
    uint64_t user_indexed_memory{0};
  };
  absl::flat_hash_map<std::string, AttributeData> attribute_data_;
};

}  // namespace valkey_search::query::cluster_info_fanout
