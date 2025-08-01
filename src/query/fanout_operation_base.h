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
  explicit FanoutOperationBase(ValkeyModuleCtx* ctx) : ctx_(ctx) {}

  virtual ~FanoutOperationBase() = default;

  void StartOperation() {
    auto targets = GetTargets();
    outstanding_ = targets.size();
    int timeout_ms = GetTimeoutMs();
    for (const auto& target : targets) {
      auto req = GenerateRequest(target, timeout_ms);
      IssueRpc(target, req, timeout_ms);
    }
  }

 protected:
  std::vector<FanoutSearchTarget> GetTargets() const {
    return query::fanout::FanoutTemplate::GetTargets(ctx_, kTargetMode);
  }

  virtual bool GetLocalResponse(const Request& request, Response& response) {
    return false;
  }

  virtual void FillLocalResponse(const Request&, Response&,
                                 const FanoutSearchTarget&) = 0;

  virtual void InvokeRemoteRpc(coordinator::Client*, std::unique_ptr<Request>,
                               std::function<void(grpc::Status, Response&)>,
                               int timeout_ms) = 0;

  virtual int GetTimeoutMs() const = 0;
  virtual Request GenerateRequest(const FanoutSearchTarget&,
                                  int timeout_ms) = 0;
  virtual void OnResponse(const Response&, const FanoutSearchTarget&) = 0;
  virtual void OnError(const std::string&, const FanoutSearchTarget&) = 0;
  virtual void OnCompletion() = 0;

  void IssueRpc(const FanoutSearchTarget& target, const Request& request,
                int timeout_ms) {
    coordinator::ClientPool* client_pool_ =
        ValkeySearch::Instance().GetCoordinatorClientPool();
    if (target.type == FanoutSearchTarget::Type::kLocal) {
      vmsdk::RunByMain([this, target, request]() {
        Response resp;
        this->FillLocalResponse(request, resp, target);
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
      auto req_ptr = std::make_unique<Request>(request);
      this->InvokeRemoteRpc(
          client.get(), std::move(req_ptr),
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

  ValkeyModuleCtx* ctx_;
  int outstanding_{0};
  absl::Mutex mutex_;
};

}  // namespace valkey_search::query::fanout
