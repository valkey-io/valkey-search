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
#include "cluster_map.h"
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

constexpr unsigned kNoValkeyTimeout = 86400000;

template <typename Request, typename Response,
          vmsdk::cluster_map::FanoutTargetMode kTargetMode>
class FanoutOperationBase {
 public:
  explicit FanoutOperationBase() = default;

  virtual ~FanoutOperationBase() = default;

  void StartOperation(ValkeyModuleCtx* ctx) {
    blocked_client_ = std::make_unique<vmsdk::BlockedClient>(
        ctx, &Reply, &Timeout, &Free, kNoValkeyTimeout);
    blocked_client_->MeasureTimeStart();
    deadline_tp_ = std::chrono::steady_clock::now() +
                   std::chrono::milliseconds(GetTimeoutMs());
    // if current cluster map is not complete or the current cluster map
    // expires, refresh cluster map
    if (!ValkeySearch::Instance().GetClusterMap()->GetIsClusterMapFull() ||
        std::chrono::steady_clock::now() >
            ValkeySearch::Instance().GetClusterMap()->GetExpirationTime()) {
      ValkeySearch::Instance().RefreshClusterMap(ctx);
    }
    targets_ = GetTargets();
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
    if (op->timeout_occurred_) {
      return op->GenerateTimeoutReply(ctx);
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

  virtual std::vector<vmsdk::cluster_map::NodeInfo> GetTargets() const = 0;

  void IssueRpc(const vmsdk::cluster_map::NodeInfo& target,
                const Request& request, unsigned timeout_ms) {
    coordinator::ClientPool* client_pool_ =
        ValkeySearch::Instance().GetCoordinatorClientPool();

    if (target.location == vmsdk::cluster_map::NodeInfo::NodeLocation::kLocal) {
      vmsdk::RunByMain([this, target, request]() {
        auto [status, resp] = this->GetLocalResponse(request, target);
        if (status.ok()) {
          this->OnResponse(resp, target);
        } else {
          ++Metrics::GetStats().info_fanout_fail_cnt;
          VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
              << "FANOUT_DEBUG: Local node error, status code: "
              << status.error_code()
              << ", error message: " << status.error_message();
          this->OnError(status, resp.error_type(), target);
        }
        this->RpcDone();
      });
    } else {
      std::string client_ip_port = absl::StrCat(
          target.ip, ":", coordinator::GetCoordinatorPort(target.port));
      auto client = client_pool_->GetClient(client_ip_port);
      if (!client) {
        ++Metrics::GetStats().info_fanout_fail_cnt;
        VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
            << "FANOUT_DEBUG: Found invalid client on target "
            << client_ip_port;
        this->OnError(grpc::Status(grpc::StatusCode::INTERNAL, ""),
                      coordinator::FanoutErrorType::COMMUNICATION_ERROR,
                      target);
        this->RpcDone();
        return;
      }
      this->InvokeRemoteRpc(
          client.get(), request,
          [this, target, client_ip_port](grpc::Status status, Response& resp) {
            if (status.ok()) {
              this->OnResponse(resp, target);
            } else {
              ++Metrics::GetStats().info_fanout_fail_cnt;
              VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
                  << "FANOUT_DEBUG: InvokeRemoteRpc error on target "
                  << client_ip_port << ", status code: " << status.error_code()
                  << ", error message: " << status.error_message();
              // if grpc failed, the response is invalid, so we need to manually
              // set the error type
              if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
                resp.set_error_type(
                    coordinator::FanoutErrorType::INDEX_NAME_ERROR);
              } else {
                resp.set_error_type(
                    coordinator::FanoutErrorType::COMMUNICATION_ERROR);
              }
              this->OnError(status, resp.error_type(), target);
            }
            this->RpcDone();
          },
          timeout_ms);
    }
  }

  virtual std::pair<grpc::Status, Response> GetLocalResponse(
      const Request&, [[maybe_unused]] const vmsdk::cluster_map::NodeInfo&) = 0;

  virtual void InvokeRemoteRpc(coordinator::Client*, const Request&,
                               std::function<void(grpc::Status, Response&)>,
                               unsigned timeout_ms) = 0;

  virtual unsigned GetTimeoutMs() const = 0;

  virtual Request GenerateRequest(
      [[maybe_unused]] const vmsdk::cluster_map::NodeInfo&) = 0;

  virtual void OnResponse(
      const Response&,
      [[maybe_unused]] const vmsdk::cluster_map::NodeInfo&) = 0;

  virtual void OnError(grpc::Status status,
                       coordinator::FanoutErrorType error_type,
                       const vmsdk::cluster_map::NodeInfo& target) {
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

  // decide which condition to run retry
  virtual bool ShouldRetry() = 0;

  void ResetBaseForRetry() {
    index_name_error_nodes.clear();
    inconsistent_state_error_nodes.clear();
    communication_error_nodes.clear();
  };

  // reset and clean the fields for new round of retry
  virtual void ResetForRetry() = 0;

  virtual int GenerateReply(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                            int argc) = 0;

  virtual int GenerateTimeoutReply(ValkeyModuleCtx* ctx) {
    return ValkeyModule_ReplyWithError(ctx,
                                       "Unable to contact all cluster members");
  }

  virtual int GenerateErrorReply(ValkeyModuleCtx* ctx) {
    absl::MutexLock lock(&mutex_);
    std::string error_message;
    // Log index name errors
    if (!index_name_error_nodes.empty()) {
      error_message = "Index name not found.";
      for (const vmsdk::cluster_map::NodeInfo& target :
           index_name_error_nodes) {
        if (target.location ==
            vmsdk::cluster_map::NodeInfo::NodeLocation::kLocal) {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
              << INDEX_NAME_ERROR_LOG_PREFIX << "LOCAL NODE";
        } else {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
              << INDEX_NAME_ERROR_LOG_PREFIX
              << absl::StrCat(target.ip, ":",
                              coordinator::GetCoordinatorPort(target.port));
        }
      }
    }
    // Log communication errors
    if (!communication_error_nodes.empty()) {
      error_message = "Communication error between nodes found.";
      for (const vmsdk::cluster_map::NodeInfo& target :
           communication_error_nodes) {
        if (target.location ==
            vmsdk::cluster_map::NodeInfo::NodeLocation::kLocal) {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
              << COMMUNICATION_ERROR_LOG_PREFIX << "LOCAL NODE";
        } else {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
              << COMMUNICATION_ERROR_LOG_PREFIX
              << absl::StrCat(target.ip, ":",
                              coordinator::GetCoordinatorPort(target.port));
        }
      }
    }
    // Log inconsistent state errors
    if (!inconsistent_state_error_nodes.empty()) {
      error_message = "Inconsistent index state error found.";
      for (const vmsdk::cluster_map::NodeInfo& target :
           inconsistent_state_error_nodes) {
        if (target.location ==
            vmsdk::cluster_map::NodeInfo::NodeLocation::kLocal) {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
              << INCONSISTENT_STATE_ERROR_LOG_PREFIX << "LOCAL NODE";
        } else {
          VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 1)
              << INCONSISTENT_STATE_ERROR_LOG_PREFIX
              << absl::StrCat(target.ip, ":",
                              coordinator::GetCoordinatorPort(target.port));
        }
      }
    }
    return ValkeyModule_ReplyWithError(ctx, error_message.c_str());
  }

  bool IsOperationTimedOut() const {
    return std::chrono::steady_clock::now() >= deadline_tp_;
  }

  virtual void OnTimeout() {
    timeout_occurred_ = true;
    OnCompletion();
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
      if (IsOperationTimedOut()) {
        OnTimeout();
        return;
      }
      if (ShouldRetry()) {
        ++Metrics::GetStats().info_fanout_retry_cnt;
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
    blocked_client_->SetReplyPrivateData(this);
    blocked_client_->UnblockClient();
  }

  unsigned outstanding_{0};
  absl::Mutex mutex_;
  std::unique_ptr<vmsdk::BlockedClient> blocked_client_;
  std::vector<vmsdk::cluster_map::NodeInfo> index_name_error_nodes;
  std::vector<vmsdk::cluster_map::NodeInfo> inconsistent_state_error_nodes;
  std::vector<vmsdk::cluster_map::NodeInfo> communication_error_nodes;
  std::vector<vmsdk::cluster_map::NodeInfo> targets_;
  std::chrono::steady_clock::time_point deadline_tp_;
  bool timeout_occurred_ = false;
};

}  // namespace valkey_search::query::fanout
