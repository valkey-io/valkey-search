/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/commands.h"

#include "fanout.h"
#include "src/acl.h"
#include "src/commands/ft_search.h"
#include "src/query/search.h"
#include "src/valkey_search.h"
#include "valkey_search_options.h"
#include "vmsdk/src/debug.h"

namespace valkey_search {
namespace async {

struct Result {
  cancel::Token cancellation_token;
  absl::StatusOr<std::deque<indexes::Neighbor>> neighbors;
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
  if (!res->neighbors.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    return ValkeyModule_ReplyWithError(
        ctx, res->neighbors.status().message().data());
  }
  res->parameters->SendReply(ctx, res->neighbors.value());
  return VALKEYMODULE_OK;
}

void Free([[maybe_unused]] ValkeyModuleCtx *ctx, void *privdata) {
  auto *result = static_cast<Result *>(privdata);
  delete result;
}

}  // namespace async

CONTROLLED_BOOLEAN(ForceReplicasOnly, false);

//
// Common Class for FT.SEARCH and FT.AGGREGATE command
//
absl::Status QueryCommand::Execute(
    ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc,
    absl::StatusOr<std::unique_ptr<QueryCommand>> (*parse_command)(
        ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc,
        const SchemaManager &manager)) {
  auto status = [&]() -> absl::Status {
    auto &schema_manager = SchemaManager::Instance();
    VMSDK_ASSIGN_OR_RETURN(
        auto parameters,
        (*parse_command)(ctx, argv + 1, argc - 1, schema_manager));
    parameters->cancellation_token =
        cancel::Make(parameters->timeout_ms, nullptr);
    static const auto permissions =
        PrefixACLPermissions(kSearchCmdPermissions, kSearchCommand);
    VMSDK_RETURN_IF_ERROR(AclPrefixCheck(
        ctx, permissions, parameters->index_schema->GetKeyPrefixes()));

    parameters->index_schema->ProcessMultiQueue();

    const bool inside_multi_exec = vmsdk::MultiOrLua(ctx);
    if (ABSL_PREDICT_FALSE(!ValkeySearch::Instance().SupportParallelQueries() ||
                           inside_multi_exec)) {
      VMSDK_ASSIGN_OR_RETURN(
          auto neighbors,
          query::Search(*parameters, query::SearchMode::kLocal));
      if (!options::GetEnablePartialResults().GetValue() &&
          parameters->cancellation_token->IsCancelled()) {
        ValkeyModule_ReplyWithError(
            ctx, "Search operation cancelled due to timeout");
        ++Metrics::GetStats().query_failed_requests_cnt;
        return absl::OkStatus();
      }
      parameters->SendReply(ctx, neighbors);
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
          .neighbors = std::move(neighbors),
          .parameters = std::move(upcast_parameters),
      });
      blocked_client.SetReplyPrivateData(result.release());
    };

    if (ValkeySearch::Instance().UsingCoordinator() &&
        ValkeySearch::Instance().IsCluster() && !parameters->local_only) {
      auto mode = /* !vmsdk::IsReadOnly(ctx) ? query::fanout::kPrimaries ? */
          ForceReplicasOnly.GetValue()
              ? query::fanout::FanoutTargetMode::kReplicasOnly
              : query::fanout::FanoutTargetMode::kRandom;
      auto search_targets = query::fanout::GetSearchTargetsForFanout(ctx, mode);
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
