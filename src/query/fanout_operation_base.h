/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#pragma once

#include <grpcpp/grpcpp.h>

#include "absl/synchronization/mutex.h"
#include "grpcpp/support/status.h"
#include "src/coordinator/client_pool.h"
#include "src/query/fanout_template.h"
#include "src/valkey_search.h"
#include "vmsdk/src/blocked_client.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query::fanout {

template <typename Request, typename Response, FanoutTargetMode kTargetMode>
class FanoutOperationBase {
 public:
  explicit FanoutOperationBase() = default;

  virtual ~FanoutOperationBase() = default;

  void StartOperation(ValkeyModuleCtx* ctx) {
    blocked_client_ = std::make_unique<vmsdk::BlockedClient>(
        ctx, &Reply, &Timeout, &Free, GetTimeoutMs());
    blocked_client_->MeasureTimeStart();

    auto targets = GetTargets(ctx);
    outstanding_ = targets.size();
    unsigned timeout_ms = GetTimeoutMs();
    for (const auto& target : targets) {
      auto req = GenerateRequest(target, timeout_ms);
      IssueRpc(ctx, target, req, timeout_ms);
    }
  }

 protected:
  static int Reply(ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc) {
    auto* op = static_cast<FanoutOperationBase*>(
        ValkeyModule_GetBlockedClientPrivateData(ctx));
    if (!op) {
      return ValkeyModule_ReplyWithError(ctx, "No reply data");
    }
    return op->GenerateReply(ctx, argv, argc);
  }

  static int Timeout(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                     int argc) {
    return ValkeyModule_ReplyWithError(ctx, "Request timed out");
  }

  static void Free(ValkeyModuleCtx* ctx, void* privdata) {
    delete static_cast<FanoutOperationBase*>(privdata);
  }

  std::vector<FanoutSearchTarget> GetTargets(ValkeyModuleCtx* ctx) const {
    return query::fanout::FanoutTemplate::GetTargets(ctx, kTargetMode);
  }

  virtual Response GetLocalResponse(
      ValkeyModuleCtx* ctx, const Request&,
      [[maybe_unused]] const FanoutSearchTarget&) = 0;

  virtual void InvokeRemoteRpc(coordinator::Client*, const Request&,
                               std::function<void(grpc::Status, Response&)>,
                               unsigned timeout_ms) = 0;

  virtual unsigned GetTimeoutMs() const = 0;

  virtual Request GenerateRequest([[maybe_unused]] const FanoutSearchTarget&,
                                  unsigned timeout_ms) = 0;

  virtual void OnResponse(const Response&,
                          [[maybe_unused]] const FanoutSearchTarget&) = 0;

  virtual void OnError(grpc::Status status,
                       [[maybe_unused]] const FanoutSearchTarget&) = 0;

  virtual int GenerateReply(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                            int argc) = 0;

  void IssueRpc(ValkeyModuleCtx* ctx, const FanoutSearchTarget& target,
                const Request& request, unsigned timeout_ms) {
    coordinator::ClientPool* client_pool_ =
        ValkeySearch::Instance().GetCoordinatorClientPool();
    if (target.type == FanoutSearchTarget::Type::kLocal) {
      vmsdk::RunByMain([this, ctx, target, request]() {
        Response resp = this->GetLocalResponse(ctx, request, target);
        if (!resp.error().empty()) {
          this->OnError(grpc::Status(grpc::StatusCode::INTERNAL, resp.error()),
                        target);
        } else {
          this->OnResponse(resp, target);
        }
        this->RpcDone();
      });
    } else {
      auto client = client_pool_->GetClient(target.address);
      if (!client) {
        this->OnError(grpc::Status(grpc::StatusCode::INTERNAL,
                                   "No client found for " + target.address),
                      target);
        this->RpcDone();
        return;
      }
      this->InvokeRemoteRpc(
          client.get(), request,
          [this, target](grpc::Status status, Response& resp) {
            if (status.ok()) {
              this->OnResponse(resp, target);
            } else {
              this->OnError(status, target);
            }
            this->RpcDone();
          },
          timeout_ms);
    }
  }

  void RpcDone() {
    absl::MutexLock lock(&mutex_);
    if (--outstanding_ == 0) {
      OnCompletion();
    }
  }

  virtual void OnCompletion() {
    CHECK(blocked_client_);
    blocked_client_->SetReplyPrivateData(this);
    blocked_client_->UnblockClient();
  }

  unsigned outstanding_{0};
  absl::Mutex mutex_;
  std::unique_ptr<vmsdk::BlockedClient> blocked_client_;
};

}  // namespace valkey_search::query::fanout
