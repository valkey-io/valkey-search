/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/commands/commands.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/query/cluster_info_fanout_operation.h"
#include "src/query/fanout_operation_base.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

// ─── Cluster consistency fanout operations ──────────────────────────────────

// Waits until an alias resolves to the expected index on all cluster nodes.
// Used by ALIASADD and ALIASUPDATE.
class AliasExistsConsistencyCheckFanoutOperation
    : public query::cluster_info_fanout::ClusterInfoFanoutOperation {
 public:
  AliasExistsConsistencyCheckFanoutOperation(
      uint32_t db_num, const std::string &alias_name, unsigned timeout_ms,
      coordinator::IndexFingerprintVersion expected_ifv)
      : ClusterInfoFanoutOperation(db_num, alias_name, timeout_ms, false,
                                   false),
        expected_ifv_(expected_ifv) {}

  coordinator::InfoIndexPartitionRequest GenerateRequest(
      const vmsdk::cluster_map::NodeInfo &) override {
    coordinator::InfoIndexPartitionRequest req;
    req.set_db_num(db_num_);
    req.set_index_name(index_name_);
    *req.mutable_index_fingerprint_version() = expected_ifv_;
    return req;
  }

  int GenerateReply(ValkeyModuleCtx *ctx, ValkeyModuleString **, int) override {
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  }

 private:
  coordinator::IndexFingerprintVersion expected_ifv_;
};

// Waits until an alias is no longer resolvable on any cluster node.
// Used by ALIASDEL.
class AliasRemovedConsistencyCheckFanoutOperation
    : public query::fanout::FanoutOperationBase<
          coordinator::InfoIndexPartitionRequest,
          coordinator::InfoIndexPartitionResponse,
          vmsdk::cluster_map::FanoutTargetMode::kAll> {
 public:
  AliasRemovedConsistencyCheckFanoutOperation(uint32_t db_num,
                                              const std::string &alias_name,
                                              unsigned timeout_ms)
      : FanoutOperationBase(false, false),
        db_num_(db_num),
        alias_name_(alias_name),
        timeout_ms_(timeout_ms) {}

  std::vector<vmsdk::cluster_map::NodeInfo> GetTargets() const override {
    return ValkeySearch::Instance().GetClusterMap()->GetTargets(
        vmsdk::cluster_map::FanoutTargetMode::kAll);
  }

  unsigned GetTimeoutMs() const override { return timeout_ms_; }

  coordinator::InfoIndexPartitionRequest GenerateRequest(
      const vmsdk::cluster_map::NodeInfo &) override {
    coordinator::InfoIndexPartitionRequest req;
    req.set_db_num(db_num_);
    req.set_index_name(alias_name_);
    return req;
  }

  void OnResponse(const coordinator::InfoIndexPartitionResponse &response,
                  const vmsdk::cluster_map::NodeInfo &target) override {
    absl::MutexLock lock(&mutex_);
    if (response.exists()) {
      // Alias still resolves on this node — needs retry.
      inconsistent_state_error_nodes.push_back(target);
    } else {
      // Alias confirmed gone on this node.
      index_name_error_nodes.push_back(target);
    }
  }

  std::pair<grpc::Status, coordinator::InfoIndexPartitionResponse>
  GetLocalResponse(const coordinator::InfoIndexPartitionRequest &request,
                   const vmsdk::cluster_map::NodeInfo &) override {
    return coordinator::Service::GenerateInfoResponse(request);
  }

  void InvokeRemoteRpc(
      coordinator::Client *client,
      const coordinator::InfoIndexPartitionRequest &request,
      std::function<void(grpc::Status,
                         coordinator::InfoIndexPartitionResponse &)>
          callback,
      unsigned timeout_ms) override {
    client->InfoIndexPartition(
        std::make_unique<coordinator::InfoIndexPartitionRequest>(request),
        std::move(callback), timeout_ms);
  }

  int GenerateReply(ValkeyModuleCtx *ctx, ValkeyModuleString **, int) override {
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  }

  void ResetForRetry() override {}

  bool ShouldRetry() override {
    return !inconsistent_state_error_nodes.empty() ||
           !communication_error_nodes.empty() ||
           index_name_error_nodes.size() != targets_.size();
  }

 private:
  uint32_t db_num_;
  std::string alias_name_;
  unsigned timeout_ms_;
};
namespace {

// Rejects commands issued inside MULTI/EXEC or Lua in CME mode, where local
// mutation without fanout would cause inconsistency.
absl::Status RejectIfMultiExecInCme(ValkeyModuleCtx *ctx) {
  if (ABSL_PREDICT_FALSE(vmsdk::MultiOrLua(ctx) &&
                         ValkeySearch::Instance().IsCluster() &&
                         ValkeySearch::Instance().UsingCoordinator())) {
    return absl::InvalidArgumentError(
        "MULTI/EXEC or Lua script are not supported in CME mode.");
  }
  return absl::OkStatus();
}

// Performs the cluster consistency fanout for alias add/update operations.
// The primary check is alias resolution: the fanout retries until every node
// can resolve the alias name to an index via GetIndexSchema. The IFV
// (fingerprint/version) check is secondary — it guards against stale index
// state but is not the mechanism that ensures alias visibility.
void FanoutAliasExists(ValkeyModuleCtx *ctx, absl::string_view alias) {
  const bool is_loading =
      ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_LOADING;
  if (ValkeySearch::Instance().IsCluster() &&
      ValkeySearch::Instance().UsingCoordinator()) {
    if (!is_loading) {
      // NOTE: We look up the IFV after the alias is created rather than
      // passing it from CreateEntry because the index already exists and
      // concurrent modifications could bump its version, causing the
      // exact-match consistency check to retry until timeout.
      auto schema_or = SchemaManager::Instance().GetIndexSchema(
          ValkeyModule_GetSelectedDb(ctx), alias);
      if (schema_or.ok()) {
        coordinator::IndexFingerprintVersion ifv;
        ifv.set_fingerprint((*schema_or)->GetFingerprint());
        ifv.set_version((*schema_or)->GetVersion());
        unsigned timeout_ms = options::GetFTInfoTimeoutMs().GetValue();
        auto op = new AliasExistsConsistencyCheckFanoutOperation(
            ValkeyModule_GetSelectedDb(ctx), std::string(alias), timeout_ms,
            ifv);
        op->StartOperation(ctx);
        return;
      }
      VMSDK_LOG(WARNING, ctx)
          << "FanoutAliasExists: failed to resolve alias '" << alias
          << "' locally, skipping cluster consistency fanout: "
          << schema_or.status().message();
    }
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  } else {
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  }
}

// Performs the cluster consistency fanout for alias delete operations,
// waiting until the alias is no longer resolvable on any node.
void FanoutAliasRemoved(ValkeyModuleCtx *ctx, absl::string_view alias) {
  const bool is_loading =
      ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_LOADING;
  if (ValkeySearch::Instance().IsCluster() &&
      ValkeySearch::Instance().UsingCoordinator()) {
    if (!is_loading) {
      unsigned timeout_ms = options::GetFTInfoTimeoutMs().GetValue();
      auto op = new AliasRemovedConsistencyCheckFanoutOperation(
          ValkeyModule_GetSelectedDb(ctx), std::string(alias), timeout_ms);
      op->StartOperation(ctx);
      return;
    }
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  } else {
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  }
}

// Replicates the command on non-coordinator (CMD) clusters.
void ReplicateIfNeeded(ValkeyModuleCtx *ctx) {
  if (!options::GetUseCoordinator().GetValue()) {
    ValkeyModule_ReplicateVerbatim(ctx);
  }
}

}  // namespace

absl::Status FTAliasAddCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                           int argc) {
  VMSDK_RETURN_IF_ERROR(RejectIfMultiExecInCme(ctx));

  auto alias = vmsdk::ToStringView(argv[1]);
  if (alias.empty()) {
    return absl::InvalidArgumentError("Alias name cannot be empty");
  }
  auto index_name = vmsdk::ToStringView(argv[2]);

  VMSDK_RETURN_IF_ERROR(SchemaManager::Instance().AddAlias(
      ValkeyModule_GetSelectedDb(ctx), alias, index_name));

  FanoutAliasExists(ctx, alias);
  ReplicateIfNeeded(ctx);
  return absl::OkStatus();
}

absl::Status FTAliasDelCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                           int argc) {
  VMSDK_RETURN_IF_ERROR(RejectIfMultiExecInCme(ctx));

  auto alias = vmsdk::ToStringView(argv[1]);
  if (alias.empty()) {
    return absl::InvalidArgumentError("Alias name cannot be empty");
  }

  VMSDK_RETURN_IF_ERROR(SchemaManager::Instance().RemoveAlias(
      ValkeyModule_GetSelectedDb(ctx), alias));

  FanoutAliasRemoved(ctx, alias);
  ReplicateIfNeeded(ctx);
  return absl::OkStatus();
}

absl::Status FTAliasUpdateCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                              int argc) {
  VMSDK_RETURN_IF_ERROR(RejectIfMultiExecInCme(ctx));

  auto alias = vmsdk::ToStringView(argv[1]);
  if (alias.empty()) {
    return absl::InvalidArgumentError("Alias name cannot be empty");
  }
  auto index_name = vmsdk::ToStringView(argv[2]);

  VMSDK_RETURN_IF_ERROR(SchemaManager::Instance().UpdateAlias(
      ValkeyModule_GetSelectedDb(ctx), alias, index_name));

  FanoutAliasExists(ctx, alias);
  ReplicateIfNeeded(ctx);
  return absl::OkStatus();
}

absl::Status FTAliasListCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                            int argc) {
  auto aliases =
      SchemaManager::Instance().GetAllAliases(ValkeyModule_GetSelectedDb(ctx));
  ValkeyModule_ReplyWithArray(ctx, aliases.size() * 2);
  for (const auto &[alias, index_name] : aliases) {
    ValkeyModule_ReplyWithStringBuffer(ctx, alias.data(), alias.size());
    ValkeyModule_ReplyWithStringBuffer(ctx, index_name.data(),
                                       index_name.size());
  }
  return absl::OkStatus();
}

}  // namespace valkey_search
