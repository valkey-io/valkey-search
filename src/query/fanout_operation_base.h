/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#pragma once

#include <grpcpp/grpcpp.h>

#include "absl/synchronization/mutex.h"
#include "src/coordinator/client_pool.h"
#include "src/query/fanout_template.h"
#include "src/valkey_search.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query::fanout {

template <typename Request, typename Response, FanoutTargetMode kTargetMode>
class FanoutOperationBase {
 public:
  explicit FanoutOperationBase() = default;

  virtual ~FanoutOperationBase() = default;

  void StartOperation(ValkeyModuleCtx* ctx) {
    auto targets = GetTargets(ctx);
    outstanding_ = targets.size();
    int timeout_ms = GetTimeoutMs();
    for (const auto& target : targets) {
      auto req = GenerateRequest(target, timeout_ms);
      IssueRpc(ctx, target, req, timeout_ms);
    }
  }

 protected:
  std::vector<FanoutSearchTarget> GetTargets(ValkeyModuleCtx* ctx) const {
    return query::fanout::FanoutTemplate::GetTargets(ctx, kTargetMode);
  }

  virtual void FillLocalResponse(ValkeyModuleCtx* ctx, const Request&,
                                 Response&, const FanoutSearchTarget&) = 0;

  virtual void InvokeRemoteRpc(coordinator::Client*, const Request&,
                               std::function<void(grpc::Status, Response&)>,
                               int timeout_ms) = 0;
  virtual int GetTimeoutMs() const = 0;
  virtual Request GenerateRequest([[maybe_unused]] const FanoutSearchTarget&,
                                  int timeout_ms) = 0;
  virtual void OnResponse(const Response&,
                          [[maybe_unused]] const FanoutSearchTarget&) = 0;
  virtual void OnError(const std::string&,
                       [[maybe_unused]] const FanoutSearchTarget&) = 0;
  virtual void OnCompletion() = 0;

  void IssueRpc(ValkeyModuleCtx* ctx, const FanoutSearchTarget& target,
                const Request& request, int timeout_ms) {
    coordinator::ClientPool* client_pool_ =
        ValkeySearch::Instance().GetCoordinatorClientPool();
    if (target.type == FanoutSearchTarget::Type::kLocal) {
      vmsdk::RunByMain([this, ctx, target, request]() {
        Response resp;
        this->FillLocalResponse(ctx, request, resp, target);
        if (!resp.error().empty()) {
          this->OnError(resp.error(), target);
        } else {
          this->OnResponse(resp, target);
        }
        this->RpcDone();
      });
    } else {
      auto client = client_pool_->GetClient(target.address);
      if (!client) {
        this->OnError("No client found for " + target.address, target);
        this->RpcDone();
        return;
      }
      this->InvokeRemoteRpc(
          client.get(), request,
          [this, target](grpc::Status status, Response& resp) {
            if (status.ok()) {
              this->OnResponse(resp, target);
            } else {
              this->OnError("gRPC error on node " + target.address + ": " +
                                status.error_message(),
                            target);
            }
            this->RpcDone();
          },
          timeout_ms);
    }
  }

  void RpcDone() {
    if (--outstanding_ == 0) {
      OnCompletion();
    }
  }

  unsigned outstanding_{0};
  absl::Mutex mutex_;
};

}  // namespace valkey_search::query::fanout
