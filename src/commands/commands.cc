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
#include "ft_search_parser.h"
#include "src/acl.h"
#include "src/commands/ft_search.h"
#include "src/metrics.h"
#include "src/query/fanout.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "vmsdk/src/blocked_client.h"
#include "vmsdk/src/cluster_map.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/utils.h"

namespace valkey_search {
namespace async {

struct Result {
  cancel::Token cancellation_token;
  absl::Status status;
  std::unique_ptr<QueryCommand> parameters;
};

int Timeout(ValkeyModuleCtx *ctx, [[maybe_unused]] ValkeyModuleString **argv,
            [[maybe_unused]] int argc) {
  return ValkeyModule_ReplyWithError(
      ctx, "Search operation cancelled due to timeout");
}

int Reply(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
  auto *parameters = static_cast<QueryCommand *>(
      ValkeyModule_GetBlockedClientPrivateData(ctx));
  CHECK(parameters != nullptr);

  // Check if operation was cancelled and partial results are disabled
  if (!parameters->enable_partial_results &&
      parameters->cancellation_token->IsCancelled()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    return ValkeyModule_ReplyWithError(
        ctx, "Search operation cancelled due to timeout");
  }

  if (!parameters->search_result.status.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    return ValkeyModule_ReplyWithError(
        ctx, parameters->search_result.status.message().data());
  }
  parameters->SendReply(ctx, parameters->search_result);
  return VALKEYMODULE_OK;
}

void Free([[maybe_unused]] ValkeyModuleCtx *ctx, void *privdata) {
  auto *parameters = static_cast<QueryCommand *>(privdata);
  // Some things can only be cleaned up on the main thread.
  // We need to do this here.
  parameters->index_schema = nullptr;
  ValkeySearch::Instance().ScheduleSearchResultCleanup(
      [parameters]() { delete parameters; });
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

    VMSDK_ASSIGN_OR_RETURN(
        parameters->index_schema,
        schema_manager.GetIndexSchema(db_num, parameters->index_schema_name));
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
      VMSDK_RETURN_IF_ERROR(
          query::Search(*parameters, query::SearchMode::kLocal));
      if (!parameters->enable_partial_results &&
          parameters->cancellation_token->IsCancelled()) {
        ValkeyModule_ReplyWithError(
            ctx, "Search operation cancelled due to timeout");
        ++Metrics::GetStats().query_failed_requests_cnt;
        return absl::OkStatus();
      }
      parameters->SendReply(ctx, parameters->search_result);
      ValkeySearch::Instance().ScheduleSearchResultCleanup(
          [neighbors =
               std::move(parameters->search_result.neighbors)]() mutable {
            // neighbors destructor runs automatically when lambda completes
          });
      return absl::OkStatus();
    }

    parameters->blocked_client = vmsdk::BlockedClient(
        ctx, async::Reply, async::Timeout, async::Free, parameters->timeout_ms);
    parameters->blocked_client->MeasureTimeStart();
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
          std::move(parameters), ValkeySearch::Instance().GetReaderThreadPool());
    }
    return query::SearchAsync(std::move(parameters),
                              ValkeySearch::Instance().GetReaderThreadPool(),
                              query::SearchMode::kLocal);
  }();
  if (!status.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
  }
  return status;
}

void QueryCommand::QueryCompleteBackground(
    std::unique_ptr<SearchParameters> parameters) {
  blocked_client->SetReplyPrivateData(parameters.release());
  blocked_client->UnblockClient();
}

void QueryCommand::QueryCompleteMainThread(
    std::unique_ptr<SearchParameters> parameters) {
  QueryCompleteBackground(std::move(parameters));
}

}  // namespace valkey_search
