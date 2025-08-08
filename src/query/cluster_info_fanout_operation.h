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

#include "grpcpp/support/status.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/query/fanout_operation_base.h"
#include "src/query/fanout_template.h"

namespace valkey_search::query::cluster_info_fanout {

class ClusterInfoFanoutOperation : public fanout::FanoutOperationBase<
                                       coordinator::InfoIndexPartitionRequest,
                                       coordinator::InfoIndexPartitionResponse,
                                       fanout::FanoutTargetMode::kAll> {
 public:
  ClusterInfoFanoutOperation(std::string index_name, unsigned timeout_ms);

  unsigned GetTimeoutMs() const override;

  coordinator::InfoIndexPartitionRequest GenerateRequest(
      const fanout::FanoutSearchTarget&, unsigned timeout_ms) override;

  void OnResponse(const coordinator::InfoIndexPartitionResponse& resp,
                  [[maybe_unused]] const fanout::FanoutSearchTarget&) override;

  void OnError(grpc::Status status,
               [[maybe_unused]] const fanout::FanoutSearchTarget&) override;

  coordinator::InfoIndexPartitionResponse GetLocalResponse(
      ValkeyModuleCtx* ctx,
      const coordinator::InfoIndexPartitionRequest& request,
      [[maybe_unused]] const fanout::FanoutSearchTarget&) override;

  void InvokeRemoteRpc(
      coordinator::Client* client,
      const coordinator::InfoIndexPartitionRequest& request,
      std::function<void(grpc::Status,
                         coordinator::InfoIndexPartitionResponse&)>
          callback,
      unsigned timeout_ms) override;

  int GenerateReply(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                    int argc) override;

 private:
  bool exists_;
  std::optional<uint64_t> schema_fingerprint_;
  std::optional<uint32_t> encoding_version_;
  std::vector<std::string> errors_;
  std::string index_name_;
  std::optional<unsigned> timeout_ms_;
  float backfill_complete_percent_max_;
  float backfill_complete_percent_min_;
  bool backfill_in_progress_;
  std::string state_;
};

}  // namespace valkey_search::query::cluster_info_fanout
