/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#pragma once

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <source_location>
#include <thread>

#include "absl/synchronization/mutex.h"
#include "grpcpp/support/status.h"
#include "src/coordinator/client_pool.h"
#include "src/metrics.h"
#include "src/query/fanout_template.h"
#include "src/utils/cancel.h"
#include "src/valkey_search.h"
#include "vmsdk/src/blocked_client.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/module_config.h"
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
    blocked_client_->SetReplyPrivateData(this);
    blocked_client_->MeasureTimeStart();
    deadline_tp_ = std::chrono::steady_clock::now() +
                   std::chrono::milliseconds(GetTimeoutMs());
    targets_ = GetTargets(ctx);
    Metrics::GetStats().fanout_retry_cnt.store(0);
    StartFanoutRound();
  }

 protected:
  const std::string INDEX_NAME_ERROR_LOG_PREFIX =
      "FT.INFO FAILURE: Index name error on node with address ";
  const std::string INCONSISTENT_STATE_ERROR_LOG_PREFIX =
      "FT.INFO FAILURE: Inconsistent state error on node with address ";
  const std::string COMMUNICATION_ERROR_LOG_PREFIX =
      "FT.INFO FAILURE: Communication error on node with address ";

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

  void StartFanoutRound() {
    outstanding_ = targets_.size();
    unsigned timeout_ms = GetTimeoutMs();
    for (const auto& target : targets_) {
      auto req = GenerateRequest(target);
      IssueRpc(target, req, timeout_ms);
    }
  }

  std::vector<FanoutSearchTarget> GetTargets(ValkeyModuleCtx* ctx) const {
    return query::fanout::FanoutTemplate::GetTargets(ctx, kTargetMode);
  }

  void IssueRpc(const FanoutSearchTarget& target, const Request& request,
                unsigned timeout_ms) {
    coordinator::ClientPool* client_pool_ =
        ValkeySearch::Instance().GetCoordinatorClientPool();

    if (target.type == FanoutSearchTarget::Type::kLocal) {
      vmsdk::RunByMain([this, target, request]() {
        auto [status, resp] = this->GetLocalResponse(request, target);
        if (status.ok()) {
          this->OnResponse(resp, target);
        } else {
          this->OnError(status, resp.error_type(), target);
        }
        this->RpcDone();
      });
    } else {
      // force the remote to fail 10 times for testing only
      if (valkey_search::cancel::GetForceTimeoutValue() &&
          Metrics::GetStats().fanout_retry_cnt < 10) {
        std::thread([this, target]() {
          this->OnError(grpc::Status(grpc::StatusCode::INTERNAL,
                                     "Forced remote failure for testing"),
                        coordinator::FanoutErrorType::COMMUNICATION_ERROR,
                        target);
          this->RpcDone();
        }).detach();
        return;
      }
      auto client = client_pool_->GetClient(target.address);
      if (!client) {
        VMSDK_LOG(WARNING, nullptr) << "Found invalid client!";
        this->OnError(grpc::Status(grpc::StatusCode::INTERNAL, ""),
                      coordinator::FanoutErrorType::COMMUNICATION_ERROR,
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
              VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
                  << "FANOUT_DEBUG: InvokeRemoteRpc error on target "
                  << target.address << ", status code: " << status.error_code()
                  << ", error message: " << status.error_message();
              this->OnError(status, resp.error_type(), target);
            }
            this->RpcDone();
          },
          timeout_ms);
    }
  }

  virtual std::pair<grpc::Status, Response> GetLocalResponse(
      const Request&, [[maybe_unused]] const FanoutSearchTarget&) = 0;

  virtual void InvokeRemoteRpc(coordinator::Client*, const Request&,
                               std::function<void(grpc::Status, Response&)>,
                               unsigned timeout_ms) = 0;

  virtual unsigned GetTimeoutMs() const = 0;

  virtual Request GenerateRequest(
      [[maybe_unused]] const FanoutSearchTarget&) = 0;

  virtual void OnResponse(const Response&,
                          [[maybe_unused]] const FanoutSearchTarget&) = 0;

  virtual void OnError(grpc::Status status,
                       coordinator::FanoutErrorType error_type,
                       const FanoutSearchTarget& target) {
    absl::MutexLock lock(&mutex_);
    if (error_type == coordinator::FanoutErrorType::INDEX_NAME_ERROR) {
      index_name_error_nodes.push_back(target);
    } else if (error_type ==
               coordinator::FanoutErrorType::INCONSISTENT_STATE_ERROR) {
      inconsistent_state_error_nodes.push_back(target);
    } else {
      communication_error_nodes.push_back(target);
    }
  }

  virtual bool ShouldRetry() = 0;

  void ResetBaseForRetry() {
    index_name_error_nodes.clear();
    inconsistent_state_error_nodes.clear();
    communication_error_nodes.clear();
  };

  virtual void ResetForRetry() = 0;

  virtual int GenerateReply(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                            int argc) = 0;

  virtual int GenerateErrorReply(ValkeyModuleCtx* ctx) {
    absl::MutexLock lock(&mutex_);
    std::string error_message;
    // Log index name errors
    if (!index_name_error_nodes.empty()) {
      error_message = "Index name not found.";
      for (const FanoutSearchTarget& target : index_name_error_nodes) {
        if (target.type == FanoutSearchTarget::Type::kLocal) {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
              << INDEX_NAME_ERROR_LOG_PREFIX << "LOCAL NODE";
        } else {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
              << INDEX_NAME_ERROR_LOG_PREFIX << target.address;
        }
      }
    }
    // Log communication errors
    if (!communication_error_nodes.empty()) {
      error_message = "Communication error between nodes found.";
      for (const FanoutSearchTarget& target : communication_error_nodes) {
        if (target.type == FanoutSearchTarget::Type::kLocal) {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
              << COMMUNICATION_ERROR_LOG_PREFIX << "LOCAL NODE";
        } else {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
              << COMMUNICATION_ERROR_LOG_PREFIX << target.address;
        }
      }
    }
    // Log inconsistent state errors
    if (!inconsistent_state_error_nodes.empty()) {
      error_message = "Inconsistent index state error found.";
      for (const FanoutSearchTarget& target : inconsistent_state_error_nodes) {
        if (target.type == FanoutSearchTarget::Type::kLocal) {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
              << INCONSISTENT_STATE_ERROR_LOG_PREFIX << "LOCAL NODE";
        } else {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
              << INCONSISTENT_STATE_ERROR_LOG_PREFIX << target.address;
        }
      }
    }
    return ValkeyModule_ReplyWithError(ctx, error_message.c_str());
  }

  bool IsOperationTimedOut() const {
    using namespace std::chrono;
    return steady_clock::now() >= deadline_tp_;
  }

  void RpcDone() {
    bool done = false;
    {
      absl::MutexLock lock(&mutex_);
      if (--outstanding_ == 0) {
        done = true;
      }
    }
    if (done) {
      if (!IsOperationTimedOut() && ShouldRetry()) {
        ++Metrics::GetStats().fanout_retry_cnt;
        ResetBaseForRetry();
        ResetForRetry();
        StartFanoutRound();
      } else {
        OnCompletion();
      }
    }
  }

  virtual void OnCompletion() {
    CHECK(blocked_client_);
    blocked_client_->UnblockClient();
  }

  unsigned outstanding_{0};
  absl::Mutex mutex_;
  std::unique_ptr<vmsdk::BlockedClient> blocked_client_;
  std::vector<FanoutSearchTarget> index_name_error_nodes;
  std::vector<FanoutSearchTarget> inconsistent_state_error_nodes;
  std::vector<FanoutSearchTarget> communication_error_nodes;
  std::chrono::steady_clock::time_point deadline_tp_;
  std::vector<FanoutSearchTarget> targets_;
};

}  // namespace valkey_search::query::fanout
