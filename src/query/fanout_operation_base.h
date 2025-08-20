/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#pragma once

#include <grpcpp/grpcpp.h>

#include <chrono>

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
  explicit FanoutOperationBase(bool retry_enabled = false)
      : retry_enabled_(retry_enabled) {};

  virtual ~FanoutOperationBase() = default;

  void StartOperation(ValkeyModuleCtx* ctx) {
    blocked_client_ = std::make_unique<vmsdk::BlockedClient>(
        ctx, &Reply, &Timeout, &Free, GetTimeoutMs());
    blocked_client_->MeasureTimeStart();
    start_tp_ = std::chrono::steady_clock::now();
    db_id_ = ValkeyModule_GetSelectedDb(ctx);
    StartFanoutRound(ctx);
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

  void StartFanoutRound(ValkeyModuleCtx* ctx) {
    auto targets = GetTargets(ctx);
    {
      absl::MutexLock lock(&mutex_);
      outstanding_ = targets.size();
    }
    unsigned timeout_ms = GetTimeoutMs();
    for (const auto& target : targets) {
      auto req = GenerateRequest(target, timeout_ms);
      IssueRpc(ctx, target, req, timeout_ms);
    }
  }

  std::vector<FanoutSearchTarget> GetTargets(ValkeyModuleCtx* ctx) const {
    return query::fanout::FanoutTemplate::GetTargets(ctx, kTargetMode);
  }

  void IssueRpc(ValkeyModuleCtx* ctx, const FanoutSearchTarget& target,
                const Request& request, unsigned timeout_ms) {
    coordinator::ClientPool* client_pool_ =
        ValkeySearch::Instance().GetCoordinatorClientPool();

    if (target.type == FanoutSearchTarget::Type::kLocal) {
      vmsdk::RunByMain([this, ctx, target, request]() {
        Response resp = this->GetLocalResponse(db_id_, request, target);
        switch (resp.error_type()) {
          // no error, continue to aggregate response
          case coordinator::FanoutErrorType::OK:
            this->OnResponse(resp, target);
            break;
          case coordinator::FanoutErrorType::INDEX_NAME_ERROR:
            this->OnError(grpc::Status(grpc::StatusCode::INTERNAL, ""),
                          coordinator::FanoutErrorType::INDEX_NAME_ERROR,
                          target);
            break;
          case coordinator::FanoutErrorType::INCONSISTENT_STATE_ERROR:
            this->OnError(
                grpc::Status(grpc::StatusCode::INTERNAL, ""),
                coordinator::FanoutErrorType::INCONSISTENT_STATE_ERROR, target);
            break;
          case coordinator::FanoutErrorType::COMMUNICATION_ERROR:
            this->OnError(grpc::Status(grpc::StatusCode::INTERNAL, ""),
                          coordinator::FanoutErrorType::COMMUNICATION_ERROR,
                          target);
            break;
          default:
            CHECK(false);
            break;
        }
        this->RpcDone(ctx);
      });
    } else {
      auto client = client_pool_->GetClient(target.address);
      if (!client) {
        this->OnError(grpc::Status(grpc::StatusCode::INTERNAL, ""),
                      coordinator::FanoutErrorType::COMMUNICATION_ERROR,
                      target);
        this->RpcDone(ctx);
        return;
      }
      this->InvokeRemoteRpc(
          client.get(), request,
          [this, ctx, target](grpc::Status status, Response& resp) {
            if (status.ok()) {
              this->OnResponse(resp, target);
            } else {
              this->OnError(status, resp.error_type(), target);
            }
            this->RpcDone(ctx);
          },
          timeout_ms);
    }
  }

  virtual Response GetLocalResponse(
      int db_id, const Request&,
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

  virtual void ResetForRetry() {
    index_name_error_nodes.clear();
    inconsistent_state_error_nodes.clear();
    communication_error_nodes.clear();
  };

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
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 5)
              << INDEX_NAME_ERROR_LOG_PREFIX << "LOCAL NODE";
        } else {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 5)
              << INDEX_NAME_ERROR_LOG_PREFIX << target.address;
        }
      }
    }
    // Log communication errors
    if (!communication_error_nodes.empty()) {
      error_message = "Communication error between nodes found.";
      for (const FanoutSearchTarget& target : communication_error_nodes) {
        if (target.type == FanoutSearchTarget::Type::kLocal) {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 5)
              << COMMUNICATION_ERROR_LOG_PREFIX << "LOCAL NODE";
        } else {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 5)
              << COMMUNICATION_ERROR_LOG_PREFIX << target.address;
        }
      }
    }
    // Log inconsistent state errors
    if (!inconsistent_state_error_nodes.empty()) {
      error_message = "Inconsistent index state error found.";
      for (const FanoutSearchTarget& target : inconsistent_state_error_nodes) {
        if (target.type == FanoutSearchTarget::Type::kLocal) {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 5)
              << INCONSISTENT_STATE_ERROR_LOG_PREFIX << "LOCAL NODE";
        } else {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 5)
              << INCONSISTENT_STATE_ERROR_LOG_PREFIX << target.address;
        }
      }
    }
    return ValkeyModule_ReplyWithError(ctx, error_message.c_str());
  }

  unsigned GetCurrentTimeoutMs() const {
    using namespace std::chrono;
    auto elapsed_ms =
        duration_cast<milliseconds>(steady_clock::now() - start_tp_).count();
    if (elapsed_ms >= GetTimeoutMs()) {
      return 0;
    }
    return static_cast<unsigned>(GetTimeoutMs() - elapsed_ms);
  }

  void RpcDone(ValkeyModuleCtx* ctx) {
    bool done = false;
    {
      absl::MutexLock lock(&mutex_);
      if (--outstanding_ == 0) {
        done = true;
      }
    }
    if (done) {
      if (retry_enabled_ && (GetCurrentTimeoutMs() > 0) && ShouldRetry()) {
        ResetForRetry();
        StartFanoutRound(ctx);
      } else {
        OnCompletion();
      }
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
  std::vector<FanoutSearchTarget> index_name_error_nodes;
  std::vector<FanoutSearchTarget> inconsistent_state_error_nodes;
  std::vector<FanoutSearchTarget> communication_error_nodes;
  bool retry_enabled_;
  std::chrono::steady_clock::time_point start_tp_;
  int db_id_;
};

}  // namespace valkey_search::query::fanout
