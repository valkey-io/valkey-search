/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#pragma once

#include <grpcpp/grpcpp.h>

#include "src/query/fanout_template.h"
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

  virtual void PerformLocalCall(const FanoutSearchTarget& target,
                                const Request& request, int timeout_ms) = 0;

  virtual void PerformRemoteCall(const FanoutSearchTarget& target,
                                 const Request& request, int timeout_ms) = 0;

  virtual int GetTimeoutMs() const = 0;
  virtual Request GenerateRequest(const FanoutSearchTarget&,
                                  int timeout_ms) = 0;
  virtual void OnResponse(const Response&, const FanoutSearchTarget&) = 0;
  virtual void OnError(const std::string&, const FanoutSearchTarget&) = 0;
  virtual void OnCompletion() = 0;

  void IssueRpc(const FanoutSearchTarget& target, const Request& request,
                int timeout_ms) {
    if (target.type == FanoutSearchTarget::Type::kLocal) {
      PerformLocalCall(target, request, timeout_ms);
    } else {
      PerformRemoteCall(target, request, timeout_ms);
    }
  }

  void RpcDone() {
    if (--outstanding_ == 0) {
      OnCompletion();
    }
  }

  ValkeyModuleCtx* ctx_;
  int outstanding_{0};
};

}  // namespace valkey_search::query::fanout
