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
  CreateConsistencyCheckFanoutOperation(
      uint32_t db_num, const std::string &index_name, unsigned timeout_ms,
      coordinator::IndexFingerprintVersion new_entry_fingerprint_version,
      bool allshards_required, bool consistency_required)
      : ClusterInfoFanoutOperation(db_num, index_name, timeout_ms,
                                   allshards_required, consistency_required),
        new_entry_fingerprint_version_(new_entry_fingerprint_version) {}

  int GenerateReply(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                    int argc) override {
    // if the received fingerprint is not equal to the exact fingerprint
    // created in the ft.create command, report an error
    if (index_fingerprint_version_->fingerprint() !=
            new_entry_fingerprint_version_.fingerprint() ||
        index_fingerprint_version_->version() !=
            new_entry_fingerprint_version_.version()) {
      return ValkeyModule_ReplyWithError(
          ctx,
          absl::StrFormat("Index %s already exists.", index_name_).c_str());
    }
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  }

 private:
  coordinator::IndexFingerprintVersion new_entry_fingerprint_version_;
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
      auto new_entry_fingerprint_version,
      SchemaManager::Instance().CreateIndexSchema(ctx, index_schema_proto));

  // directly handle reply in standalone mode
  // let fanout operation handle reply in cluster mode
  if (ValkeySearch::Instance().IsCluster() &&
      ValkeySearch::Instance().UsingCoordinator()) {
    // ft.create consistency check
    unsigned timeout_ms = options::GetFTInfoTimeoutMs().GetValue();
    auto op = new CreateConsistencyCheckFanoutOperation(
        ValkeyModule_GetSelectedDb(ctx), index_schema_proto.name(), timeout_ms,
        new_entry_fingerprint_version, true, true);
    op->StartOperation(ctx);
  } else {
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  }
  ValkeyModule_ReplicateVerbatim(ctx);
  return absl::OkStatus();
}
}  // namespace valkey_search
