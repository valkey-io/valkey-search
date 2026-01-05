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

  parser.AddParamParser("ALLSHARDS", GENERATE_NEGATED_FLAG_PARSER(
                                         InfoCommand, enable_partial_results));

  parser.AddParamParser(
      "SOMESHARDS", GENERATE_FLAG_PARSER(InfoCommand, enable_partial_results));

  parser.AddParamParser("CONSISTENT",
                        GENERATE_FLAG_PARSER(InfoCommand, require_consistency));

  parser.AddParamParser("INCONSISTENT", GENERATE_NEGATED_FLAG_PARSER(
                                            InfoCommand, require_consistency));

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
  if (!ValkeySearch::Instance().IsCluster() ||
      !ValkeySearch::Instance().UsingCoordinator()) {
    switch (scope) {
      case InfoScope::kPrimary: {
        return absl::InvalidArgumentError(
            "ERR PRIMARY option is not valid in this configuration");
      }
      case InfoScope::kCluster: {
        return absl::InvalidArgumentError(
            "ERR CLUSTER option is not valid in this configuration");
      }
      case InfoScope::kLocal:
        break;
      default:
        CHECK(false);
    }
  }

  timeout_ms = options::GetFTInfoTimeoutMs().GetValue();
  return absl::OkStatus();
}

absl::Status InfoCommand::Execute(ValkeyModuleCtx *ctx) {
  // ACL check
  VMSDK_RETURN_IF_ERROR(AclPrefixCheck(ctx, acl::KeyAccess::kRead,
                                       index_schema->GetKeyPrefixes()));

  const bool is_loading =
      ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_LOADING;
  const bool inside_multi_exec = vmsdk::MultiOrLua(ctx);

  // Execute based on scope
  switch (scope) {
    case InfoScope::kPrimary: {
      if (is_loading || inside_multi_exec) {
        VMSDK_LOG(NOTICE, nullptr) << "The server is loading AOF or inside "
                                      "multi/exec or lua script, skip "
                                      "fanout operation";
        index_schema->RespondWithInfo(ctx);
      } else {
        auto op = new query::primary_info_fanout::PrimaryInfoFanoutOperation(
            ValkeyModule_GetSelectedDb(ctx), index_schema_name, timeout_ms,
            enable_partial_results, require_consistency);
        op->StartOperation(ctx);
      }
      break;
    }
    case InfoScope::kCluster: {
      if (is_loading || inside_multi_exec) {
        VMSDK_LOG(NOTICE, nullptr) << "The server is loading AOF or inside "
                                      "multi/exec or lua script, skip "
                                      "fanout operation";
        index_schema->RespondWithInfo(ctx);
      } else {
        auto op = new query::cluster_info_fanout::ClusterInfoFanoutOperation(
            ValkeyModule_GetSelectedDb(ctx), index_schema_name, timeout_ms,
            enable_partial_results, require_consistency);
        op->StartOperation(ctx);
      }
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
