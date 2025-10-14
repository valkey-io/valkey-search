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
#include "src/valkey_search_options.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

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
  itr.Next();

  bool is_cluster_and_using_coordinator =
      ValkeySearch::Instance().IsCluster() &&
      ValkeySearch::Instance().UsingCoordinator();
  bool is_primary = false;
  bool is_cluster = false;
  bool allshards_required =
      options::GetFTInfoDefaultAllshardsRequired().GetValue();
  bool consistency_required =
      options::GetFTInfoDefaultConsistencyRequired().GetValue();
  unsigned timeout_ms = options::GetFTInfoTimeoutMs().GetValue();

  while (itr.HasNext()) {
    VMSDK_ASSIGN_OR_RETURN(auto arg, itr.Get());
    auto arg_str = vmsdk::ToStringView(arg);
    itr.Next();

    if (absl::EqualsIgnoreCase(arg_str, "PRIMARY")) {
      if (!is_cluster_and_using_coordinator) {
        ValkeyModule_ReplyWithError(
            ctx, "ERR PRIMARY option is not valid in this configuration");
        return absl::OkStatus();
      }
      is_primary = true;
    } else if (absl::EqualsIgnoreCase(arg_str, "CLUSTER")) {
      if (!is_cluster_and_using_coordinator) {
        ValkeyModule_ReplyWithError(
            ctx, "ERR CLUSTER option is not valid in this configuration");
        return absl::OkStatus();
      }
      is_cluster = true;
    } else if (absl::EqualsIgnoreCase(arg_str, "ALLSHARDS")) {
      if (!is_cluster_and_using_coordinator) {
        ValkeyModule_ReplyWithError(
            ctx, "ERR ALLSHARDS option is not valid in this configuration");
        return absl::OkStatus();
      }
      allshards_required = true;
    } else if (absl::EqualsIgnoreCase(arg_str, "SOMESHARDS")) {
      if (!is_cluster_and_using_coordinator) {
        ValkeyModule_ReplyWithError(
            ctx, "ERR SOMESHARDS option is not valid in this configuration");
        return absl::OkStatus();
      }
      allshards_required = false;
    } else if (absl::EqualsIgnoreCase(arg_str, "CONSISTENT")) {
      if (!is_cluster_and_using_coordinator) {
        ValkeyModule_ReplyWithError(
            ctx, "ERR CONSISTENT option is not valid in this configuration");
        return absl::OkStatus();
      }
      consistency_required = true;
    } else if (absl::EqualsIgnoreCase(arg_str, "INCONSISTENT")) {
      if (!is_cluster_and_using_coordinator) {
        ValkeyModule_ReplyWithError(
            ctx, "ERR INCONSISTENT option is not valid in this configuration");
        return absl::OkStatus();
      }
      consistency_required = false;
    } else {
      ValkeyModule_ReplyWithError(
          ctx, absl::StrFormat("ERR Unknown argument: %s", arg_str).c_str());
      return absl::OkStatus();
    }
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

  // operation(db_num, index_name, timeout)
  if (is_primary) {
    auto op = new query::primary_info_fanout::PrimaryInfoFanoutOperation(
        ValkeyModule_GetSelectedDb(ctx), std::string(index_schema_name),
        timeout_ms, allshards_required, consistency_required);
    op->StartOperation(ctx);
  } else if (is_cluster) {
    auto op = new query::cluster_info_fanout::ClusterInfoFanoutOperation(
        ValkeyModule_GetSelectedDb(ctx), std::string(index_schema_name),
        timeout_ms, allshards_required, consistency_required);
    op->StartOperation(ctx);
  } else {
    VMSDK_LOG(DEBUG, ctx) << "==========Using Local Scope==========";
    index_schema->RespondWithInfo(ctx);
  }
  return absl::OkStatus();
}

}  // namespace valkey_search
