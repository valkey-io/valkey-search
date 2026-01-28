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
      coordinator::IndexFingerprintVersion new_entry_fingerprint_version)
      : ClusterInfoFanoutOperation(db_num, index_name, timeout_ms, false,
                                   false),
        new_entry_fingerprint_version_(new_entry_fingerprint_version) {}

  coordinator::InfoIndexPartitionRequest GenerateRequest(
      const vmsdk::cluster_map::NodeInfo &) override {
    coordinator::InfoIndexPartitionRequest req;
    req.set_db_num(db_num_);
    req.set_index_name(index_name_);
    // Use the newly created fingerprint/version
    auto *expected_ifv = req.mutable_index_fingerprint_version();
    expected_ifv->set_fingerprint(new_entry_fingerprint_version_.fingerprint());
    expected_ifv->set_version(new_entry_fingerprint_version_.version());
    return req;
  }

  int GenerateReply(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                    int argc) override {
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
  VMSDK_RETURN_IF_ERROR(
      AclPrefixCheck(ctx, acl::KeyAccess::kWrite, index_schema_proto));
  VMSDK_ASSIGN_OR_RETURN(
      auto new_entry_fingerprint_version,
      SchemaManager::Instance().CreateIndexSchema(ctx, index_schema_proto));

  // directly handle reply in standalone mode
  // let fanout operation handle reply in cluster mode
  const bool is_loading =
      ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_LOADING;
  const bool inside_multi_exec = vmsdk::MultiOrLua(ctx);
  if (ValkeySearch::Instance().IsCluster() &&
      ValkeySearch::Instance().UsingCoordinator() && !is_loading &&
      !inside_multi_exec) {
    // ft.create consistency check
    unsigned timeout_ms = options::GetFTInfoTimeoutMs().GetValue();
    auto op = new CreateConsistencyCheckFanoutOperation(
        ValkeyModule_GetSelectedDb(ctx), index_schema_proto.name(), timeout_ms,
        new_entry_fingerprint_version);
    op->StartOperation(ctx);
  } else {
    if (is_loading || inside_multi_exec) {
      VMSDK_LOG(NOTICE, nullptr) << "The server is loading AOF or inside "
                                    "multi/exec or lua script, skip "
                                    "fanout operation";
    }
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  }

  // Replicate FT.CREATE only for CMD clusters (no coordinator),
  // CME clusters (with coordinator) use FT.INTERNAL_UPDATE for replication with
  // metadata versioning
  if (!options::GetUseCoordinator().GetValue()) {
    ValkeyModule_ReplicateVerbatim(ctx);
  }
  return absl::OkStatus();
}
}  // namespace valkey_search
