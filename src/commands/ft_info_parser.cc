/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/ft_info_parser.h"

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "src/acl.h"
#include "src/commands/commands.h"
#include "src/query/cluster_info_fanout_operation.h"
#include "src/query/primary_info_fanout_operation.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search {

namespace {

const absl::flat_hash_map<absl::string_view, InfoScope> kScopeByStr = {
    {"LOCAL", InfoScope::kLocal},
    {"PRIMARY", InfoScope::kPrimary},
    {"CLUSTER", InfoScope::kCluster}};

vmsdk::KeyValueParser<InfoCommand> CreateInfoParser() {
  vmsdk::KeyValueParser<InfoCommand> parser;

  parser.AddParamParser(
      "LOCAL",
      std::make_unique<vmsdk::ParamParser<InfoCommand>>(
          [](InfoCommand &cmd, vmsdk::ArgsIterator &itr) -> absl::Status {
            cmd.scope = InfoScope::kLocal;
            return absl::OkStatus();
          }));

  parser.AddParamParser(
      "PRIMARY",
      std::make_unique<vmsdk::ParamParser<InfoCommand>>(
          [](InfoCommand &cmd, vmsdk::ArgsIterator &itr) -> absl::Status {
            cmd.scope = InfoScope::kPrimary;
            return absl::OkStatus();
          }));

  parser.AddParamParser(
      "CLUSTER",
      std::make_unique<vmsdk::ParamParser<InfoCommand>>(
          [](InfoCommand &cmd, vmsdk::ArgsIterator &itr) -> absl::Status {
            cmd.scope = InfoScope::kCluster;
            return absl::OkStatus();
          }));

  return parser;
}

static vmsdk::KeyValueParser<InfoCommand> InfoParser = CreateInfoParser();

}  // namespace

absl::Status InfoCommand::ParseCommand(ValkeyModuleCtx *ctx,
                                       vmsdk::ArgsIterator &itr) {
  // Parse index name
  VMSDK_ASSIGN_OR_RETURN(auto index_name_rs, itr.Get());
  index_schema_name = vmsdk::ToStringView(index_name_rs);
  itr.Next();

  // Get index schema
  VMSDK_ASSIGN_OR_RETURN(
      index_schema, SchemaManager::Instance().GetIndexSchema(
                        ValkeyModule_GetSelectedDb(ctx), index_schema_name));

  // Parse optional parameters
  VMSDK_RETURN_IF_ERROR(InfoParser.Parse(*this, itr));

  // Validate no extra arguments
  if (itr.DistanceEnd() > 0) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Unexpected parameter: ", vmsdk::ToStringView(itr.Get().value())));
  }

  // Validate scope
  if (scope == InfoScope::kPrimary) {
    if (!ValkeySearch::Instance().IsCluster() ||
        !ValkeySearch::Instance().UsingCoordinator()) {
      return absl::InvalidArgumentError(
          "ERR PRIMARY option is not valid in this configuration");
    }
  } else if (scope == InfoScope::kCluster) {
    if (!ValkeySearch::Instance().IsCluster() ||
        !ValkeySearch::Instance().UsingCoordinator()) {
      return absl::InvalidArgumentError(
          "ERR CLUSTER option is not valid in this configuration");
    }
  }

  timeout_ms = options::GetFTInfoTimeoutMs().GetValue();
  return absl::OkStatus();
}

absl::Status InfoCommand::Execute(ValkeyModuleCtx *ctx) {
  // ACL check
  static const auto permissions =
      PrefixACLPermissions(kInfoCmdPermissions, kInfoCommand);
  VMSDK_RETURN_IF_ERROR(
      AclPrefixCheck(ctx, permissions, index_schema->GetKeyPrefixes()));

  // Execute based on scope
  switch (scope) {
    case InfoScope::kPrimary: {
      auto op = new query::primary_info_fanout::PrimaryInfoFanoutOperation(
          ValkeyModule_GetSelectedDb(ctx), index_schema_name, timeout_ms);
      op->StartOperation(ctx);
      break;
    }
    case InfoScope::kCluster: {
      auto op = new query::cluster_info_fanout::ClusterInfoFanoutOperation(
          ValkeyModule_GetSelectedDb(ctx), index_schema_name, timeout_ms);
      op->StartOperation(ctx);
      break;
    }
    case InfoScope::kLocal:
    default:
      VMSDK_LOG(DEBUG, ctx) << "Using Local Scope";
      index_schema->RespondWithInfo(ctx);
      break;
  }

  return absl::OkStatus();
}

}  // namespace valkey_search
