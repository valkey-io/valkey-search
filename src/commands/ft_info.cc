/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "src/acl.h"
#include "src/commands/commands.h"
#include "src/query/cluster_info_fanout.h"
#include "src/query/primary_info_fanout_operation.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

constexpr absl::string_view kFTInfoTimeoutMsConfig{"ft-info-timeout-ms"};
constexpr uint32_t kDefaultFTInfoTimeoutMs{5000};
constexpr uint32_t kMinimumFTInfoTimeoutMs{100};
constexpr uint32_t kMaximumFTInfoTimeoutMs{300000};  // 5 minutes max

namespace options {

/// Register the "--ft-info-timeout-ms" flag. Controls the timeout for FT.INFO
/// operations
static auto ft_info_timeout_ms =
    vmsdk::config::NumberBuilder(
        kFTInfoTimeoutMsConfig,   // name
        kDefaultFTInfoTimeoutMs,  // default timeout (5 seconds)
        kMinimumFTInfoTimeoutMs,  // min timeout (100ms)
        kMaximumFTInfoTimeoutMs)  // max timeout (5 minutes)
        .Build();

vmsdk::config::Number &GetFTInfoTimeoutMs() {
  return dynamic_cast<vmsdk::config::Number &>(*ft_info_timeout_ms);
}

}  // namespace options

namespace cluster_info_async {

struct ClusterInfoAsyncResult {
  absl::StatusOr<query::cluster_info_fanout::ClusterInfoResult> info;
  std::unique_ptr<query::cluster_info_fanout::ClusterInfoParameters> parameters;
  ClusterInfoAsyncResult(
      absl::StatusOr<query::cluster_info_fanout::ClusterInfoResult> i,
      std::unique_ptr<query::cluster_info_fanout::ClusterInfoParameters> p)
      : info(std::move(i)), parameters(std::move(p)) {}
};

int Reply(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
  auto *res = static_cast<ClusterInfoAsyncResult *>(
      ValkeyModule_GetBlockedClientPrivateData(ctx));
  if (!res->info.ok()) {
    return ValkeyModule_ReplyWithError(ctx,
                                       res->info.status().message().data());
  }
  const auto &info = res->info.value();
  const auto index_name = res->parameters->index_name.c_str();

  if (!info.exists) {
    std::string error_msg =
        absl::StrFormat("Cluster index with name '%s' not found", index_name);
    return ValkeyModule_ReplyWithError(ctx, error_msg.c_str());
  }

  if (info.has_schema_mismatch) {
    return ValkeyModule_ReplyWithError(
        ctx, "ERR found cluster index schema inconsistency in the cluster");
  }

  if (info.has_version_mismatch) {
    return ValkeyModule_ReplyWithError(
        ctx,
        "ERR found cluster index schema version inconsistency in the cluster");
  }

  ValkeyModule_ReplyWithArray(ctx, 12);
  ValkeyModule_ReplyWithSimpleString(ctx, "mode");
  ValkeyModule_ReplyWithSimpleString(ctx, "cluster");
  ValkeyModule_ReplyWithSimpleString(ctx, "index_name");
  ValkeyModule_ReplyWithSimpleString(ctx, index_name);
  ValkeyModule_ReplyWithSimpleString(ctx, "backfill_in_progress");
  ValkeyModule_ReplyWithCString(ctx, info.backfill_in_progress ? "1" : "0");
  ValkeyModule_ReplyWithSimpleString(ctx, "backfill_complete_percent_max");
  ValkeyModule_ReplyWithCString(
      ctx, absl::StrFormat("%f", info.backfill_complete_percent_max).c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "backfill_complete_percent_min");
  ValkeyModule_ReplyWithCString(
      ctx, absl::StrFormat("%f", info.backfill_complete_percent_min).c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "state");
  ValkeyModule_ReplyWithSimpleString(ctx, info.state.c_str());

  return VALKEYMODULE_OK;
}

void Free(ValkeyModuleCtx *ctx, void *privdata) {
  delete static_cast<ClusterInfoAsyncResult *>(privdata);
}

int Timeout(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
  return ValkeyModule_ReplyWithError(ctx, "Cluster info request timed out");
}

}  // namespace cluster_info_async

absl::Status FTInfoCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                       int argc) {
  if (argc < 2) {
    ValkeyModule_ReplyWithError(ctx, vmsdk::WrongArity(kInfoCommand).c_str());
    return absl::OkStatus();
  }

  vmsdk::ArgsIterator itr{argv, argc};
  itr.Next();
  VMSDK_ASSIGN_OR_RETURN(auto itr_arg, itr.Get());
  auto index_schema_name = vmsdk::ToStringView(itr_arg);

  bool is_global = false;
  bool is_primary = false;
  bool is_cluster = false;
  unsigned timeout_ms = options::GetFTInfoTimeoutMs().GetValue();

  if (argc == 2) {
    is_global = false;
  } else if (argc == 3) {
    itr.Next();
    VMSDK_ASSIGN_OR_RETURN(auto scope_arg, itr.Get());
    auto scope = vmsdk::ToStringView(scope_arg);

    if (absl::EqualsIgnoreCase(scope, "LOCAL")) {
      is_global = false;
    } else if (absl::EqualsIgnoreCase(scope, "PRIMARY")) {
      if (!ValkeySearch::Instance().IsCluster() ||
          !ValkeySearch::Instance().UsingCoordinator()) {
        ValkeyModule_ReplyWithError(
            ctx, "ERR PRIMARY option is not valid in this configuration");
        return absl::OkStatus();
      }
      is_primary = true;
    } else if (absl::EqualsIgnoreCase(scope, "CLUSTER")) {
      if (!ValkeySearch::Instance().IsCluster() ||
          !ValkeySearch::Instance().UsingCoordinator()) {
        ValkeyModule_ReplyWithError(
            ctx, "ERR CLUSTER option is not valid in this configuration");
        return absl::OkStatus();
      }
      is_cluster = true;
    } else {
      ValkeyModule_ReplyWithError(
          ctx,
          "ERR Invalid scope parameter. Must be LOCAL, PRIMARY or CLUSTER");
      return absl::OkStatus();
    }
  } else {
    // Invalid number of parameters
    ValkeyModule_ReplyWithError(ctx, vmsdk::WrongArity(kInfoCommand).c_str());
    return absl::OkStatus();
  }

  // ACL check
  VMSDK_ASSIGN_OR_RETURN(
      auto index_schema,
      SchemaManager::Instance().GetIndexSchema(ValkeyModule_GetSelectedDb(ctx),
                                               index_schema_name));
  static const auto permissions =
      PrefixACLPermissions(kInfoCmdPermissions, kInfoCommand);
  VMSDK_RETURN_IF_ERROR(
      AclPrefixCheck(ctx, permissions, index_schema->GetKeyPrefixes()));

  if (is_primary) {
    auto op = new query::primary_info_fanout::PrimaryInfoFanoutOperation(
        std::string(index_schema_name), timeout_ms);
    op->StartOperation(ctx);
    return absl::OkStatus();
  } else if (is_cluster) {
    VMSDK_LOG(DEBUG, ctx) << "==========Using Cluster Scope==========";
    auto parameters =
        std::make_unique<query::cluster_info_fanout::ClusterInfoParameters>();
    parameters->index_name = std::string(index_schema_name);
    parameters->timeout_ms = timeout_ms;
    auto targets = query::fanout::FanoutTemplate::GetTargets(
        ctx, query::fanout::FanoutTargetMode::kAll);
    VMSDK_LOG(DEBUG, ctx) << "Found " << targets.size() << " fanout targets:";

    for (const auto &target : targets) {
      VMSDK_LOG(DEBUG, ctx)
          << "  Target type: "
          << (target.type == query::fanout::FanoutSearchTarget::Type::kLocal
                  ? "LOCAL"
                  : "REMOTE")
          << ", address: " << target.address;
    }
    vmsdk::BlockedClient blocked_client(
        ctx, cluster_info_async::Reply, cluster_info_async::Timeout,
        cluster_info_async::Free, parameters->timeout_ms);
    blocked_client.MeasureTimeStart();
    auto on_done =
        [blocked_client = std::move(blocked_client)](
            absl::StatusOr<query::cluster_info_fanout::ClusterInfoResult>
                result,
            std::unique_ptr<query::cluster_info_fanout::ClusterInfoParameters>
                params) mutable {
          auto payload =
              std::make_unique<cluster_info_async::ClusterInfoAsyncResult>(
                  std::move(result), std::move(params));
          blocked_client.SetReplyPrivateData(payload.release());
        };
    return query::cluster_info_fanout::PerformClusterInfoFanoutAsync(
        ctx, targets, ValkeySearch::Instance().GetCoordinatorClientPool(),
        std::move(parameters), ValkeySearch::Instance().GetReaderThreadPool(),
        std::move(on_done));

  } else {
    VMSDK_LOG(DEBUG, ctx) << "==========Using Local Scope==========";
    index_schema->RespondWithInfo(ctx);
  }

  return absl::OkStatus();
}

}  // namespace valkey_search
