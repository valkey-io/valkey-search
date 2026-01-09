/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/commands.h"

#include "fanout.h"
#include "ft_create_parser.h"
#include "src/acl.h"
#include "src/commands/ft_search.h"
#include "src/coordinator/metadata_manager.h"
#include "src/query/fanout.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "valkey_search_options.h"
#include "vmsdk/src/cluster_map.h"
#include "vmsdk/src/debug.h"

namespace valkey_search {
namespace async {

struct Result {
  cancel::Token cancellation_token;
<<<<<<< HEAD
  absl::StatusOr<std::vector<indexes::Neighbor>> neighbors;
=======
  absl::StatusOr<query::SearchResult> search_result;
>>>>>>> upstream/main
  std::unique_ptr<QueryCommand> parameters;
};

int Timeout(ValkeyModuleCtx *ctx, [[maybe_unused]] ValkeyModuleString **argv,
            [[maybe_unused]] int argc) {
  return ValkeyModule_ReplyWithError(
      ctx, "Search operation cancelled due to timeout");
}

int Reply(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
  auto *res =
      static_cast<Result *>(ValkeyModule_GetBlockedClientPrivateData(ctx));
  CHECK(res != nullptr);

  // Check if operation was cancelled and partial results are disabled
  if (!res->parameters->enable_partial_results &&
      res->parameters->cancellation_token->IsCancelled()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    return ValkeyModule_ReplyWithError(
        ctx, "Search operation cancelled due to timeout");
  }

  if (!res->search_result.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    return ValkeyModule_ReplyWithError(
        ctx, res->search_result.status().message().data());
  }
  res->parameters->SendReply(ctx, res->search_result.value());
  return VALKEYMODULE_OK;
}

void Free([[maybe_unused]] ValkeyModuleCtx *ctx, void *privdata) {
  auto *result = static_cast<Result *>(privdata);
  // Some things in the Result can only be cleaned up on the main thread.
  // We need to do this here.
  result->parameters->index_schema = nullptr;
  ValkeySearch::Instance().ScheduleSearchResultCleanup(
      [result]() { delete result; });
}

}  // namespace async

CONTROLLED_BOOLEAN(ForceReplicasOnly, false);
CONTROLLED_BOOLEAN(ForceInvalidIndexFingerprint, false);

//
// Common Class for FT.SEARCH and FT.AGGREGATE command
//
absl::Status QueryCommand::Execute(ValkeyModuleCtx *ctx,
                                   ValkeyModuleString **argv, int argc,
                                   std::unique_ptr<QueryCommand> parameters) {
  auto status = [&]() -> absl::Status {
    auto &schema_manager = SchemaManager::Instance();
    vmsdk::ArgsIterator itr{argv + 1, argc - 1};
    parameters->timeout_ms = options::GetDefaultTimeoutMs().GetValue();
    VMSDK_RETURN_IF_ERROR(
        vmsdk::ParseParamValue(itr, parameters->index_schema_name));

    uint32_t db_num = ValkeyModule_GetSelectedDb(ctx);
    parameters->db_num = db_num;

    VMSDK_ASSIGN_OR_RETURN(parameters->index_schema,
                           SchemaManager::Instance().GetIndexSchema(
                               db_num, parameters->index_schema_name));
    VMSDK_RETURN_IF_ERROR(
        vmsdk::ParseParamValue(itr, parameters->parse_vars.query_string));
    VMSDK_RETURN_IF_ERROR(parameters->ParseCommand(itr));
    parameters->parse_vars.ClearAtEndOfParse();
    parameters->cancellation_token =
        cancel::Make(parameters->timeout_ms, nullptr);
    VMSDK_RETURN_IF_ERROR(
        AclPrefixCheck(ctx, acl::KeyAccess::kRead,
                       parameters->index_schema->GetKeyPrefixes()));

    parameters->index_schema->ProcessMultiQueue();

    const bool inside_multi_exec = vmsdk::MultiOrLua(ctx);
    if (ABSL_PREDICT_FALSE(!ValkeySearch::Instance().SupportParallelQueries() ||
                           inside_multi_exec)) {
      VMSDK_ASSIGN_OR_RETURN(
          auto search_result,
          query::Search(*parameters, query::SearchMode::kLocal));
      if (!parameters->enable_partial_results &&
          parameters->cancellation_token->IsCancelled()) {
        ValkeyModule_ReplyWithError(
            ctx, "Search operation cancelled due to timeout");
        ++Metrics::GetStats().query_failed_requests_cnt;
        return absl::OkStatus();
      }
      parameters->SendReply(ctx, search_result);
      ValkeySearch::Instance().ScheduleSearchResultCleanup(
          [neighbors = std::move(search_result.neighbors)]() mutable {
            // neighbors destructor runs automatically when lambda completes
          });
      return absl::OkStatus();
    }

    vmsdk::BlockedClient blocked_client(ctx, async::Reply, async::Timeout,
                                        async::Free, parameters->timeout_ms);
    blocked_client.MeasureTimeStart();
    auto on_done_callback = [blocked_client = std::move(blocked_client)](
                                auto &neighbors, auto parameters) mutable {
      std::unique_ptr<QueryCommand> upcast_parameters(
          dynamic_cast<QueryCommand *>(parameters.release()));
      CHECK(upcast_parameters != nullptr);
      auto result = std::make_unique<async::Result>(async::Result{
          .search_result = std::move(neighbors),
          .parameters = std::move(upcast_parameters),
      });
      blocked_client.SetReplyPrivateData(result.release());
    };

    if (ValkeySearch::Instance().UsingCoordinator() &&
        ValkeySearch::Instance().IsCluster() && !parameters->local_only) {
      auto mode = /* !vmsdk::IsReadOnly(ctx) ? query::fanout::kPrimaries ? */
          ForceReplicasOnly.GetValue()
              ? vmsdk::cluster_map::FanoutTargetMode::kOneReplicaPerShard
              : vmsdk::cluster_map::FanoutTargetMode::kRandom;
      // refresh cluster map if needed
      auto search_targets =
          ValkeySearch::Instance().GetOrRefreshClusterMap(ctx)->GetTargets(
              mode, query::fanout::IsSystemUnderLowUtilization());

      // get index fingerprint and version
      if (ForceInvalidIndexFingerprint.GetValue()) {
        // test only: simulate invalid index fingerprint and version
        parameters->index_fingerprint_version.set_fingerprint(404);
        parameters->index_fingerprint_version.set_version(404);
      } else {
        // get fingerprint/version from IndexSchema
        parameters->index_fingerprint_version.set_fingerprint(
            parameters->index_schema->GetFingerprint());
        parameters->index_fingerprint_version.set_version(
            parameters->index_schema->GetVersion());
      }

      return query::fanout::PerformSearchFanoutAsync(
          ctx, search_targets,
          ValkeySearch::Instance().GetCoordinatorClientPool(),
          std::move(parameters), ValkeySearch::Instance().GetReaderThreadPool(),
          std::move(on_done_callback));
    }
    return query::SearchAsync(
        std::move(parameters), ValkeySearch::Instance().GetReaderThreadPool(),
        std::move(on_done_callback), query::SearchMode::kLocal);
  }();
  if (!status.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
  }
  return status;
}

}  // namespace valkey_search
