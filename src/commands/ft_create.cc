/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "absl/status/status.h"
#include "src/acl.h"
#include "src/commands/commands.h"
#include "src/commands/ft_create_parser.h"
#include "src/query/cluster_info_fanout_operation.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

class CreateConsistencyCheckFanoutOperation
    : public query::cluster_info_fanout::ClusterInfoFanoutOperation {
 public:
  CreateConsistencyCheckFanoutOperation(uint32_t db_num,
                                        const std::string &index_name,
                                        unsigned timeout_ms,
                                        uint64_t new_entry_fingerprint,
                                        uint32_t new_netry_version)
      : ClusterInfoFanoutOperation(db_num, index_name, timeout_ms),
        new_entry_fingerprint_(new_entry_fingerprint),
        new_entry_version_(new_netry_version) {}

  void OnResponse(const coordinator::InfoIndexPartitionResponse &resp,
                  [[maybe_unused]] const query::fanout::FanoutSearchTarget
                      &target) override {
    if (!resp.error().empty()) {
      grpc::Status status =
          grpc::Status(grpc::StatusCode::INTERNAL, resp.error());
      OnError(status, resp.error_type(), target);
      return;
    }
    if (!resp.exists()) {
      grpc::Status status =
          grpc::Status(grpc::StatusCode::INTERNAL, "Index does not exists");
      OnError(status, coordinator::FanoutErrorType::INDEX_NAME_ERROR, target);
      return;
    }
    // if the received fingerprint is not equal to the exact fingerprint
    // created in the ft.create command, report an error
    if (!schema_fingerprint_.has_value()) {
      schema_fingerprint_ = resp.schema_fingerprint();
    } else if (schema_fingerprint_.value() != resp.schema_fingerprint() ||
               resp.schema_fingerprint() != new_entry_fingerprint_) {
      grpc::Status status =
          grpc::Status(grpc::StatusCode::INTERNAL,
                       "Cluster not in a consistent state, please retry.");
      OnError(status, coordinator::FanoutErrorType::INCONSISTENT_STATE_ERROR,
              target);
      return;
    }
    // if the received version is not equal to the exact version
    // created in the ft.create command, report an error
    if (!version_.has_value()) {
      version_ = resp.version();
    } else if (version_.value() != resp.version() ||
               resp.version() != new_entry_version_) {
      grpc::Status status =
          grpc::Status(grpc::StatusCode::INTERNAL,
                       "Cluster not in a consistent state, please retry.");
      OnError(status, coordinator::FanoutErrorType::INCONSISTENT_STATE_ERROR,
              target);
      return;
    }
    if (resp.index_name() != index_name_) {
      grpc::Status status =
          grpc::Status(grpc::StatusCode::INTERNAL,
                       "Cluster not in a consistent state, please retry.");
      OnError(status, coordinator::FanoutErrorType::INCONSISTENT_STATE_ERROR,
              target);
      return;
    }
    exists_ = true;
  }

  int GenerateReply(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                    int argc) override {
    if (!index_name_error_nodes.empty() || !communication_error_nodes.empty() ||
        !inconsistent_state_error_nodes.empty()) {
      return FanoutOperationBase::GenerateErrorReply(ctx);
    }
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  }

  int GenerateTimeoutReply(ValkeyModuleCtx *ctx) override {
    if (schema_fingerprint_.has_value() &&
        schema_fingerprint_.value() != new_entry_fingerprint_) {
      return ValkeyModule_ReplyWithError(
          ctx,
          absl::StrFormat("Index %s already exists.", index_name_).c_str());
    }
    return ValkeyModule_ReplyWithError(ctx,
                                       "Unable to contact all cluster members");
  }

 private:
  uint64_t new_entry_fingerprint_;
  uint32_t new_entry_version_;
};

absl::Status FTCreateCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                         int argc) {
  VMSDK_ASSIGN_OR_RETURN(auto index_schema_proto,
                         ParseFTCreateArgs(ctx, argv + 1, argc - 1));
  index_schema_proto.set_db_num(ValkeyModule_GetSelectedDb(ctx));
  static const auto permissions =
      PrefixACLPermissions(kCreateCmdPermissions, kCreateCommand);
  VMSDK_RETURN_IF_ERROR(AclPrefixCheck(ctx, permissions, index_schema_proto));
  VMSDK_ASSIGN_OR_RETURN(
      auto create_result,
      SchemaManager::Instance().CreateIndexSchema(ctx, index_schema_proto));

  // directly handle reply in standalone mode
  // let fanout operation handle reply in cluster mode
  if (ValkeySearch::Instance().IsCluster() &&
      ValkeySearch::Instance().UsingCoordinator()) {
    // ft.create consistency check
    unsigned timeout_ms = options::GetFTInfoTimeoutMs().GetValue();
    auto op = new CreateConsistencyCheckFanoutOperation(
        ValkeyModule_GetSelectedDb(ctx), index_schema_proto.name(), timeout_ms,
        create_result.fingerprint, create_result.version);
    op->StartOperation(ctx);
  } else {
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  }
  ValkeyModule_ReplicateVerbatim(ctx);
  return absl::OkStatus();
}
}  // namespace valkey_search
