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
#include "src/query/cluster_info_fanout_operation.h"
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
constexpr uint32_t kDefaultFTInfoTimeoutMs{1000};
constexpr uint32_t kMinimumFTInfoTimeoutMs{100};
constexpr uint32_t kMaximumFTInfoTimeoutMs{10000};  // 10s max

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
  } else if (is_cluster) {
    auto op = new query::cluster_info_fanout::ClusterInfoFanoutOperation(
        std::string(index_schema_name), timeout_ms);
    op->StartOperation(ctx);
  } else {
    VMSDK_LOG(DEBUG, ctx) << "==========Using Local Scope==========";
    index_schema->RespondWithInfo(ctx);
  }
  return absl::OkStatus();
}

}  // namespace valkey_search
