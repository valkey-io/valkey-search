/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/commands.h"

#include <memory>
#include <vector>

#include "fanout.h"
#include "ft_create_parser.h"
#include "src/acl.h"
#include "src/commands/ft_search.h"
#include "src/coordinator/metadata_manager.h"
#include "src/metrics.h"
#include "src/query/fanout.h"
#include "src/query/inflight_retry.h"
#include "src/query/response_generator.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "valkey_search_options.h"
#include "vmsdk/src/blocked_client.h"
#include "vmsdk/src/cluster_map.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/utils.h"

namespace valkey_search {
namespace async {

struct Result {
  cancel::Token cancellation_token;
  absl::StatusOr<std::vector<indexes::Neighbor>> neighbors;
  std::unique_ptr<QueryCommand> parameters;
};

// Context for timer-based retry when waiting for in-flight keys
struct InFlightRetryContext {
  vmsdk::BlockedClient blocked_client;
  std::unique_ptr<Result> result;
  std::vector<InternedStringPtr>
      neighbor_keys;  // Cached to avoid O(n) copy on each retry

  InFlightRetryContext(vmsdk::BlockedClient bc, std::unique_ptr<Result> res,
                       std::vector<InternedStringPtr> keys)
      : blocked_client(std::move(bc)),
        result(std::move(res)),
        neighbor_keys(std::move(keys)) {}
};

void InFlightRetryTimerCallback(ValkeyModuleCtx *ctx, void *data);

void CheckAndHandleInFlightConflicts(ValkeyModuleCtx *ctx,
                                     InFlightRetryContext *retry_ctx) {
  auto &result = retry_ctx->result;

  if (result->parameters->cancellation_token->IsCancelled()) {
    retry_ctx->blocked_client.SetReplyPrivateData(result.release());
    delete retry_ctx;
    return;
  }

  if (query::CheckInFlightAndScheduleRetry(
          ctx, retry_ctx, retry_ctx->neighbor_keys,
          result->parameters->index_schema, InFlightRetryTimerCallback,
          "Full-text query")) {
    return;
  }

  retry_ctx->blocked_client.SetReplyPrivateData(result.release());
  delete retry_ctx;
}

void InFlightRetryTimerCallback(ValkeyModuleCtx *ctx, void *data) {
  auto *retry_ctx = static_cast<InFlightRetryContext *>(data);
  CheckAndHandleInFlightConflicts(ctx, retry_ctx);
}

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
          auto neighbors,
          query::Search(*parameters, query::SearchMode::kLocal));
      if (!parameters->enable_partial_results &&
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

      // For pure full-text queries with content, check for in-flight key
      // conflicts. Skip if no_content since we only return key names without
      // fetching/evaluating data.
      if (!result->parameters->no_content &&
          query::IsPureFullTextQuery(*result->parameters)) {
        auto neighbor_keys =
            query::CollectNeighborKeys(result->neighbors.value());
        if (result->parameters->index_schema->HasAnyConflictingInFlightKeys(
                neighbor_keys)) {
          ++Metrics::GetStats().fulltext_query_blocked_cnt;
          auto *retry_ctx = new async::InFlightRetryContext(
              std::move(blocked_client), std::move(result),
              std::move(neighbor_keys));

          query::ScheduleInFlightRetryOnMain(retry_ctx,
                                             async::InFlightRetryTimerCallback);
          return;
        }
      }

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
