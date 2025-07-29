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
#include "src/query/info_fanout.h"
#include "src/query/primary_info_fanout.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace info_async {

struct InfoAsyncResult {
  absl::StatusOr<query::info_fanout::InfoResult> info;
  std::unique_ptr<query::info_fanout::InfoParameters> parameters;
  InfoAsyncResult(absl::StatusOr<query::info_fanout::InfoResult> i,
                  std::unique_ptr<query::info_fanout::InfoParameters> p)
      : info(std::move(i)), parameters(std::move(p)) {}
};

int Reply(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
  auto *res = static_cast<InfoAsyncResult *>(
      ValkeyModule_GetBlockedClientPrivateData(ctx));
  if (!res->info.ok()) {
    return ValkeyModule_ReplyWithError(ctx,
                                       res->info.status().message().data());
  }
  const auto &info = res->info.value();
  const auto index_name = res->parameters->index_name.c_str();

  if (!info.exists) {
    std::string error_msg =
        absl::StrFormat("Index with name '%s' not found", index_name);
    return ValkeyModule_ReplyWithError(ctx, error_msg.c_str());
  }

  if (info.has_schema_mismatch) {
    return ValkeyModule_ReplyWithError(
        ctx, "ERR found index schema inconsistency in the cluster");
  }

  ValkeyModule_ReplyWithArray(ctx, 18);
  ValkeyModule_ReplyWithSimpleString(ctx, "global_info");
  ValkeyModule_ReplyWithSimpleString(ctx, "true");
  ValkeyModule_ReplyWithSimpleString(ctx, "index_name");
  ValkeyModule_ReplyWithSimpleString(ctx, index_name);
  ValkeyModule_ReplyWithSimpleString(ctx, "num_docs");
  ValkeyModule_ReplyWithCString(ctx, std::to_string(info.num_docs).c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "num_records");
  ValkeyModule_ReplyWithCString(ctx, std::to_string(info.num_records).c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "hash_indexing_failures");
  ValkeyModule_ReplyWithCString(
      ctx, std::to_string(info.hash_indexing_failures).c_str());
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
  delete static_cast<InfoAsyncResult *>(privdata);
}

int Timeout(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
  return ValkeyModule_ReplyWithError(ctx, "Request timed out");
}

}  // namespace info_async

namespace primary_info_async {

struct PrimaryInfoAsyncResult {
  absl::StatusOr<query::primary_info_fanout::PrimaryInfoResult> info;
  std::unique_ptr<query::primary_info_fanout::PrimaryInfoParameters> parameters;
  PrimaryInfoAsyncResult(
      absl::StatusOr<query::primary_info_fanout::PrimaryInfoResult> i,
      std::unique_ptr<query::primary_info_fanout::PrimaryInfoParameters> p)
      : info(std::move(i)), parameters(std::move(p)) {}
};

int Reply(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
  auto *res = static_cast<PrimaryInfoAsyncResult *>(
      ValkeyModule_GetBlockedClientPrivateData(ctx));
  if (!res->info.ok()) {
    return ValkeyModule_ReplyWithError(ctx,
                                       res->info.status().message().data());
  }
  const auto &info = res->info.value();
  const auto index_name = res->parameters->index_name.c_str();

  if (!info.exists) {
    std::string error_msg =
        absl::StrFormat("Primary index with name '%s' not found", index_name);
    return ValkeyModule_ReplyWithError(ctx, error_msg.c_str());
  }

  if (info.has_schema_mismatch) {
    return ValkeyModule_ReplyWithError(
        ctx, "ERR found primary index schema inconsistency in the cluster");
  }

  if (info.has_version_mismatch) {
    return ValkeyModule_ReplyWithError(
        ctx, "ERR found index schema version inconsistency in the cluster");
  }

  ValkeyModule_ReplyWithArray(ctx, 10);
  ValkeyModule_ReplyWithSimpleString(ctx, "global");
  ValkeyModule_ReplyWithSimpleString(ctx, "true");
  ValkeyModule_ReplyWithSimpleString(ctx, "index_name");
  ValkeyModule_ReplyWithSimpleString(ctx, index_name);
  ValkeyModule_ReplyWithSimpleString(ctx, "num_docs");
  ValkeyModule_ReplyWithCString(ctx, std::to_string(info.num_docs).c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "num_records");
  ValkeyModule_ReplyWithCString(ctx, std::to_string(info.num_records).c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "hash_indexing_failures");
  ValkeyModule_ReplyWithCString(
      ctx, std::to_string(info.hash_indexing_failures).c_str());

  return VALKEYMODULE_OK;
}

void Free(ValkeyModuleCtx *ctx, void *privdata) {
  delete static_cast<PrimaryInfoAsyncResult *>(privdata);
}

int Timeout(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
  return ValkeyModule_ReplyWithError(ctx, "Primary info request timed out");
}

}  // namespace primary_info_async

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
  if (argc == 2) {
    is_global = false;
  } else if (argc == 3) {
    itr.Next();
    VMSDK_ASSIGN_OR_RETURN(auto scope_arg, itr.Get());
    auto scope = vmsdk::ToStringView(scope_arg);

    if (absl::EqualsIgnoreCase(scope, "LOCAL")) {
      is_global = false;
    } else if (absl::EqualsIgnoreCase(scope, "GLOBAL")) {
      if (!ValkeySearch::Instance().IsCluster() ||
          !ValkeySearch::Instance().UsingCoordinator()) {
        ValkeyModule_ReplyWithError(
            ctx,
            "ERR GLOBAL scope requires cluster mode with coordinator enabled");
        return absl::OkStatus();
      }
      is_global = true;
    } else if (absl::EqualsIgnoreCase(scope, "PRIMARY")) {
      if (!ValkeySearch::Instance().IsCluster() ||
          !ValkeySearch::Instance().UsingCoordinator()) {
        ValkeyModule_ReplyWithError(ctx,
                                    "ERR PRIMARY keyword requires cluster mode "
                                    "with coordinator enabled");
        return absl::OkStatus();
      }
      is_primary = true;
    } else {
      ValkeyModule_ReplyWithError(
          ctx, "ERR Invalid scope parameter. Must be LOCAL or GLOBAL");
      return absl::OkStatus();
    }
  } else {
    // More than 2 parameters provided
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

  if (is_global) {
    VMSDK_LOG(DEBUG, ctx) << "==========Using Global Scope==========";
    // Global info fanout - aggregate info from all cluster nodes
    auto parameters = std::make_unique<query::info_fanout::InfoParameters>();
    parameters->index_name = std::string(index_schema_name);

    auto targets = query::info_fanout::GetInfoTargetsForFanout(ctx);
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
        ctx, info_async::Reply, info_async::Timeout, info_async::Free, 5000);
    blocked_client.MeasureTimeStart();

    auto on_done = [blocked_client = std::move(blocked_client)](
                       absl::StatusOr<query::info_fanout::InfoResult> result,
                       std::unique_ptr<query::info_fanout::InfoParameters>
                           params) mutable {
      auto payload = std::make_unique<info_async::InfoAsyncResult>(
          std::move(result), std::move(params));
      blocked_client.SetReplyPrivateData(payload.release());
    };

    return query::info_fanout::PerformInfoFanoutAsync(
        ctx, targets, ValkeySearch::Instance().GetCoordinatorClientPool(),
        std::move(parameters), ValkeySearch::Instance().GetReaderThreadPool(),
        std::move(on_done));

  } else if (is_primary) {
    VMSDK_LOG(DEBUG, ctx) << "==========Using Primary Scope==========";
    auto parameters =
        std::make_unique<query::primary_info_fanout::PrimaryInfoParameters>();
    parameters->index_name = std::string(index_schema_name);
    auto targets =
        query::primary_info_fanout::GetPrimaryInfoTargetsForFanout(ctx);
    VMSDK_LOG(DEBUG, ctx) << "Found " << targets.size() << " fanout targets:";

    for (const auto &target : targets) {
      VMSDK_LOG(DEBUG, ctx)
          << "  Target type: "
          << (target.type == query::fanout::FanoutSearchTarget::Type::kLocal
                  ? "LOCAL"
                  : "REMOTE")
          << ", address: " << target.address;
    }
    vmsdk::BlockedClient blocked_client(ctx, primary_info_async::Reply,
                                        primary_info_async::Timeout,
                                        primary_info_async::Free, 5000);
    blocked_client.MeasureTimeStart();
    auto on_done =
        [blocked_client = std::move(blocked_client)](
            absl::StatusOr<query::primary_info_fanout::PrimaryInfoResult>
                result,
            std::unique_ptr<query::primary_info_fanout::PrimaryInfoParameters>
                params) mutable {
          auto payload =
              std::make_unique<primary_info_async::PrimaryInfoAsyncResult>(
                  std::move(result), std::move(params));
          blocked_client.SetReplyPrivateData(payload.release());
        };
    return query::primary_info_fanout::PerformPrimaryInfoFanoutAsync(
        ctx, targets, ValkeySearch::Instance().GetCoordinatorClientPool(),
        std::move(parameters), ValkeySearch::Instance().GetReaderThreadPool(),
        std::move(on_done));

  } else {
    VMSDK_LOG(DEBUG, ctx) << "==========Using Local Scope==========";
    // index_schema already retrieved above for ACL check
    index_schema->RespondWithInfo(ctx);
  }

  return absl::OkStatus();
}

}  // namespace valkey_search
