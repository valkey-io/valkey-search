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

namespace valkey_search::query::primary_info_fanout {

class PrimaryInfoFanoutOperation : public fanout::FanoutOperationBase<
                                       coordinator::InfoIndexPartitionRequest,
                                       coordinator::InfoIndexPartitionResponse,
                                       fanout::FanoutTargetMode::kPrimary> {
 public:
  PrimaryInfoFanoutOperation(std::string index_name, unsigned timeout_ms);

  unsigned GetTimeoutMs() const override;

  coordinator::InfoIndexPartitionRequest GenerateRequest(
      const fanout::FanoutSearchTarget&, unsigned timeout_ms) override;

  void OnResponse(const coordinator::InfoIndexPartitionResponse& resp,
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
  std::optional<uint32_t> version_;
  std::string index_name_;
  std::optional<unsigned> timeout_ms_;
  uint64_t num_docs_;
  uint64_t num_records_;
  uint64_t hash_indexing_failures_;
};

}  // namespace valkey_search::query::primary_info_fanout
