/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/string_view.h"
#include "src/acl.h"
#include "src/commands/commands.h"
#include "src/query/info_fanout.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace async {

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

  // Emit the same fields as LOCAL FT.INFO, postponing array length:
  ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_LEN);
  ValkeyModule_ReplyWithSimpleString(ctx, "index_name");
  ValkeyModule_ReplyWithSimpleString(ctx, info.index_name.c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "exists");
  ValkeyModule_ReplyWithSimpleString(ctx, info.exists ? "true" : "false");
  ValkeyModule_ReplyWithSimpleString(ctx, "num_docs");
  ValkeyModule_ReplyWithLongLong(ctx, info.num_docs);
  ValkeyModule_ReplyWithSimpleString(ctx, "num_records");
  ValkeyModule_ReplyWithLongLong(ctx, info.num_records);
  ValkeyModule_ReplyWithSimpleString(ctx, "hash_indexing_failures");
  ValkeyModule_ReplyWithLongLong(ctx, info.hash_indexing_failures);
  ValkeyModule_ReplyWithSimpleString(ctx, "backfill_scanned_count");
  ValkeyModule_ReplyWithLongLong(ctx, info.backfill_scanned_count);
  ValkeyModule_ReplyWithSimpleString(ctx, "backfill_db_size");
  ValkeyModule_ReplyWithLongLong(ctx, info.backfill_db_size);
  ValkeyModule_ReplyWithSimpleString(ctx, "backfill_inqueue_tasks");
  ValkeyModule_ReplyWithLongLong(ctx, info.backfill_inqueue_tasks);
  ValkeyModule_ReplyWithSimpleString(ctx, "mutation_queue_size");
  ValkeyModule_ReplyWithLongLong(ctx, info.mutation_queue_size);
  ValkeyModule_ReplyWithSimpleString(ctx, "recent_mutations_queue_delay");
  ValkeyModule_ReplyWithLongLong(ctx, info.recent_mutations_queue_delay);
  ValkeyModule_ReplyWithSimpleString(ctx, "backfill_in_progress");
  ValkeyModule_ReplyWithSimpleString(
      ctx, info.backfill_in_progress ? "true" : "false");
  ValkeyModule_ReplyWithSimpleString(ctx, "backfill_complete_percent");
  ValkeyModule_ReplyWithDouble(ctx, info.backfill_complete_percent);
  ValkeyModule_ReplyWithSimpleString(ctx, "state");
  ValkeyModule_ReplyWithSimpleString(ctx, info.state.c_str());
  if (!info.error.empty()) {
    ValkeyModule_ReplyWithSimpleString(ctx, "error");
    ValkeyModule_ReplyWithSimpleString(ctx, info.error.c_str());
  }
  // Now set the correct length (13 fields×2, plus optional error)
  ValkeyModule_ReplySetArrayLength(ctx, info.error.empty() ? 26 : 28);
  return VALKEYMODULE_OK;
}

void Free(ValkeyModuleCtx *ctx, void *privdata) {
  delete static_cast<InfoAsyncResult *>(privdata);
}

int Timeout(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
  return ValkeyModule_ReplyWithSimpleString(ctx, "Request timed out");
}

}  // namespace async

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

  // Parse optional LOCAL/GLOBAL parameter
  bool is_global = false;
  if (argc == 2) {
    // Only 1 parameter provided (index name), default to LOCAL
    is_global = false;
    VMSDK_LOG(NOTICE, ctx) << "==========Using Local Scope==========";
  } else if (argc == 3) {
    // 2 parameters provided, validate the second parameter
    itr.Next();
    VMSDK_ASSIGN_OR_RETURN(auto scope_arg, itr.Get());
    auto scope = vmsdk::ToStringView(scope_arg);

    if (absl::EqualsIgnoreCase(scope, "LOCAL")) {
      VMSDK_LOG(NOTICE, ctx) << "==========Using Local Scope==========";
      is_global = false;
    } else if (absl::EqualsIgnoreCase(scope, "GLOBAL")) {
      // Check if we're in cluster mode and using coordinator
      if (!ValkeySearch::Instance().IsCluster() ||
          !ValkeySearch::Instance().UsingCoordinator()) {
        ValkeyModule_ReplyWithError(
            ctx,
            "ERR GLOBAL scope requires cluster mode with coordinator enabled");
        return absl::OkStatus();
      }
      is_global = true;

      VMSDK_LOG(NOTICE, ctx) << "==========Using Global Scope==========";

      // Get and print fanout targets
      auto targets = query::info_fanout::GetInfoTargetsForFanout(ctx);
      VMSDK_LOG(NOTICE, ctx)
          << "Found " << targets.size() << " fanout targets:";

      for (const auto &target : targets) {
        VMSDK_LOG(NOTICE, ctx)
            << "  Target type: "
            << (target.type == query::fanout::FanoutSearchTarget::Type::kLocal
                    ? "LOCAL"
                    : "REMOTE")
            << ", address: " << target.address;
      }
    } else {
      // Invalid scope parameter
      ValkeyModule_ReplyWithError(
          ctx, "ERR Invalid scope parameter. Must be LOCAL or GLOBAL");
      return absl::OkStatus();
    }
  } else {
    // More than 2 parameters provided
    ValkeyModule_ReplyWithError(ctx, vmsdk::WrongArity(kInfoCommand).c_str());
    return absl::OkStatus();
  }

  if (is_global) {
    // Global info fanout - aggregate info from all cluster nodes
    auto parameters = std::make_unique<query::info_fanout::InfoParameters>();
    parameters->index_name = std::string(index_schema_name);

    auto targets = query::info_fanout::GetInfoTargetsForFanout(ctx);

    // Wrap the ctx in a BlockedClient so it stays alive until we explicitly
    // reply:
    vmsdk::BlockedClient blocked_client(ctx, async::Reply, async::Timeout,
                                        async::Free, 5000);
    blocked_client.MeasureTimeStart();

    auto on_done = [blocked_client = std::move(blocked_client)](
                       absl::StatusOr<query::info_fanout::InfoResult> result,
                       std::unique_ptr<query::info_fanout::InfoParameters>
                           params) mutable {
      auto payload = std::make_unique<async::InfoAsyncResult>(
          std::move(result), std::move(params));
      blocked_client.SetReplyPrivateData(payload.release());
    };

    // // 3) STUB: build a dummy InfoResult right now
    // query::info_fanout::InfoResult dummy;
    // dummy.index_name = parameters->index_name;
    // dummy.exists = true;
    // dummy.num_docs = 42;
    // dummy.num_records = 100;
    // dummy.hash_indexing_failures = 0;
    // dummy.backfill_scanned_count = 0;
    // dummy.backfill_db_size = 0;
    // dummy.backfill_inqueue_tasks = 0;
    // dummy.mutation_queue_size = 0;
    // dummy.recent_mutations_queue_delay = 0;
    // dummy.backfill_in_progress = false;
    // dummy.backfill_complete_percent = 0.0;
    // dummy.state = "READY";
    // // no error

    // // 4) Immediately invoke the callback with our dummy
    // on_done(absl::StatusOr<query::info_fanout::InfoResult>(std::move(dummy)),
    //         std::move(parameters));

    return query::info_fanout::PerformInfoFanoutAsync(
        ctx, targets, ValkeySearch::Instance().GetCoordinatorClientPool(),
        std::move(parameters), ValkeySearch::Instance().GetReaderThreadPool(),
        std::move(on_done));

  } else {
    // Local info - existing implementation
    VMSDK_ASSIGN_OR_RETURN(
        auto index_schema,
        SchemaManager::Instance().GetIndexSchema(
            ValkeyModule_GetSelectedDb(ctx), index_schema_name));
    static const auto permissions =
        PrefixACLPermissions(kInfoCmdPermissions, kInfoCommand);
    VMSDK_RETURN_IF_ERROR(
        AclPrefixCheck(ctx, permissions, index_schema->GetKeyPrefixes()));
    index_schema->RespondWithInfo(ctx);
  }

  return absl::OkStatus();
}

}  // namespace valkey_search
