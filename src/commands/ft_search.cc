/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/ft_search.h"

#include <strings.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "src/acl.h"
#include "src/commands/commands.h"
#include "src/commands/ft_aggregate.h"
#include "src/commands/ft_search_parser.h"
#include "src/indexes/vector_base.h"
#include "src/metrics.h"
#include "src/query/fanout.h"
#include "src/query/response_generator.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/blocked_client.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {
// FT.SEARCH idx "*=>[KNN 10 @vec $BLOB AS score]" PARAMS 2 BLOB
// "\x12\xa9\xf5\x6c" DIALECT 2
void ReplyAvailNeighbors(ValkeyModuleCtx *ctx,
                         const std::deque<indexes::Neighbor> &neighbors,
                         const query::VectorSearchParameters &parameters) {
  ValkeyModule_ReplyWithLongLong(
      ctx, std::min(neighbors.size(), static_cast<size_t>(parameters.k)));
}

size_t CalcEndIndex(const std::deque<indexes::Neighbor> &neighbors,
                    const query::VectorSearchParameters &parameters) {
  return std::min(
      static_cast<size_t>(parameters.k),
      std::min(static_cast<size_t>(parameters.limit.number), neighbors.size()));
}

size_t CalcStartIndex(const std::deque<indexes::Neighbor> &neighbors,
                      const query::VectorSearchParameters &parameters) {
  CHECK_GT(parameters.k, parameters.limit.first_index);
  if (neighbors.size() <= parameters.limit.first_index) {
    return neighbors.size();
  }
  return parameters.limit.first_index;
}

void SendReplyNoContent(ValkeyModuleCtx *ctx,
                        const std::deque<indexes::Neighbor> &neighbors,
                        const query::VectorSearchParameters &parameters) {
  const size_t start_index = CalcStartIndex(neighbors, parameters);
  const size_t end_index = start_index + CalcEndIndex(neighbors, parameters);
  ValkeyModule_ReplyWithArray(ctx, end_index - start_index + 1);
  ReplyAvailNeighbors(ctx, neighbors, parameters);
  for (auto i = start_index; i < end_index; ++i) {
    ValkeyModule_ReplyWithString(
        ctx, vmsdk::MakeUniqueValkeyString(*neighbors[i].external_id).get());
  }
}

void ReplyScore(ValkeyModuleCtx *ctx, ValkeyModuleString &score_as,
                const indexes::Neighbor &neighbor) {
  ValkeyModule_ReplyWithString(ctx, &score_as);
  auto score_value = absl::StrFormat("%.12g", neighbor.distance);
  ValkeyModule_ReplyWithString(
      ctx, vmsdk::MakeUniqueValkeyString(score_value).get());
}

void SerializeNeighbors(ValkeyModuleCtx *ctx,
                        const std::deque<indexes::Neighbor> &neighbors,
                        const query::VectorSearchParameters &parameters) {
  CHECK_GT(static_cast<size_t>(parameters.k), parameters.limit.first_index);
  const size_t start_index = CalcStartIndex(neighbors, parameters);
  const size_t end_index = start_index + CalcEndIndex(neighbors, parameters);
  ValkeyModule_ReplyWithArray(ctx, 2 * (end_index - start_index) + 1);
  ReplyAvailNeighbors(ctx, neighbors, parameters);

  for (auto i = start_index; i < end_index; ++i) {
    ValkeyModule_ReplyWithString(
        ctx, vmsdk::MakeUniqueValkeyString(*neighbors[i].external_id).get());
    if (parameters.return_attributes.empty()) {
      ValkeyModule_ReplyWithArray(
          ctx, 2 * neighbors[i].attribute_contents.value().size() + 2);
      ReplyScore(ctx, *parameters.score_as, neighbors[i]);
      for (auto &attribute_content : neighbors[i].attribute_contents.value()) {
        ValkeyModule_ReplyWithString(ctx,
                                     attribute_content.second.GetIdentifier());
        ValkeyModule_ReplyWithString(ctx, attribute_content.second.value.get());
      }
    } else {
      ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_LEN);
      size_t cnt = 0;
      for (const auto &return_attribute : parameters.return_attributes) {
        if (vmsdk::ToStringView(parameters.score_as.get()) ==
            vmsdk::ToStringView(return_attribute.identifier.get())) {
          ReplyScore(ctx, *parameters.score_as, neighbors[i]);
          ++cnt;
          continue;
        }
        auto it = neighbors[i].attribute_contents.value().find(
            vmsdk::ToStringView(return_attribute.identifier.get()));
        if (it != neighbors[i].attribute_contents.value().end()) {
          ValkeyModule_ReplyWithString(ctx, return_attribute.alias.get());
          ValkeyModule_ReplyWithString(ctx, it->second.value.get());
          ++cnt;
        }
      }
      ValkeyModule_ReplySetArrayLength(ctx, 2 * cnt);
    }
  }
}

// Handle non-vector queries by processing the neighbors and replying with the
// attribute contents.
void SerializeNonVectorNeighbors(
    ValkeyModuleCtx *ctx, const std::deque<indexes::Neighbor> &neighbors,
    const query::VectorSearchParameters &parameters) {
  const size_t available_results = neighbors.size();
  ValkeyModule_ReplyWithArray(ctx, 2 * available_results + 1);
  // First element is the count of available results.
  ValkeyModule_ReplyWithLongLong(ctx, available_results);
  for (const auto &neighbor : neighbors) {
    // Document ID
    ValkeyModule_ReplyWithString(
        ctx, vmsdk::MakeUniqueValkeyString(*neighbor.external_id).get());
    const auto &contents = neighbor.attribute_contents.value();
    // Fields and values as a flat array
    ValkeyModule_ReplyWithArray(ctx, 2 * contents.size());
    for (const auto &attribute_content : contents) {
      ValkeyModule_ReplyWithString(ctx,
                                   attribute_content.second.GetIdentifier());
      ValkeyModule_ReplyWithString(ctx, attribute_content.second.value.get());
    }
  }
}

}  // namespace
// The reply structure is an array which consists of:
// 1. The amount of response elements
// 2. Per response entry:
//   1. The cache entry Hash key
//   2. An array with the following entries:
//      1. Key value: [$score_as] score_value
//      2. Distance value
//      3. Attribute name
//      4. The vector value
// SendReply respects the Limit, see https://valkey.io/commands/ft.search/
void SendReply(ValkeyModuleCtx *ctx, std::deque<indexes::Neighbor> &neighbors,
               query::VectorSearchParameters &parameters) {
  if (!options::GetEnablePartialResults().GetValue() &&
      parameters.cancellation_token->IsCancelled()) {
    ValkeyModule_ReplyWithError(ctx,
                                "Search operation cancelled due to timeout");
    ++Metrics::GetStats().query_failed_requests_cnt;
    return;
  }
  if (auto agg = dynamic_cast<aggregate::AggregateParameters *>(&parameters)) {
    SendAggReply(ctx, neighbors, *agg);
    return;
  }
  // Increment success counter.
  ++Metrics::GetStats().query_successful_requests_cnt;

  // Support non-vector queries: no attribute_alias and k == 0
  if (parameters.IsNonVectorQuery()) {
    query::ProcessNonVectorNeighborsForReply(
        ctx, parameters.index_schema->GetAttributeDataType(), neighbors,
        parameters);
    SerializeNonVectorNeighbors(ctx, neighbors, parameters);
    return;
  }

  if (parameters.limit.first_index >= static_cast<uint64_t>(parameters.k) ||
      parameters.limit.number == 0) {
    ValkeyModule_ReplyWithArray(ctx, 1);
    ValkeyModule_ReplyWithLongLong(ctx, neighbors.size());
    return;
  }
  if (parameters.no_content) {
    SendReplyNoContent(ctx, neighbors, parameters);
    return;
  }
  auto identifier =
      parameters.index_schema->GetIdentifier(parameters.attribute_alias);
  if (!identifier.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    ValkeyModule_ReplyWithError(ctx, identifier.status().message().data());
    return;
  }
  query::ProcessNeighborsForReply(
      ctx, parameters.index_schema->GetAttributeDataType(), neighbors,
      parameters, identifier.value());

  SerializeNeighbors(ctx, neighbors, parameters);
}

namespace async {

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
  SendReply(ctx, res->neighbors.value(), *res->parameters);
  return VALKEYMODULE_OK;
}

void Free([[maybe_unused]] ValkeyModuleCtx *ctx, void *privdata) {
  auto *result = static_cast<Result *>(privdata);
  delete result;
}

}  // namespace async

CONTROLLED_BOOLEAN(ForceReplicasOnly, false);

absl::Status FTSearchCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                         int argc) {
  auto status = [&]() -> absl::Status {
    auto &schema_manager = SchemaManager::Instance();
    VMSDK_ASSIGN_OR_RETURN(
        auto parameters,
        ParseVectorSearchParameters(ctx, argv + 1, argc - 1, schema_manager));
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
      SendReply(ctx, neighbors, *parameters);
      return absl::OkStatus();
    }

    vmsdk::BlockedClient blocked_client(ctx, async::Reply, async::Timeout,
                                        async::Free, parameters->timeout_ms);
    blocked_client.MeasureTimeStart();
    auto on_done_callback = [blocked_client = std::move(blocked_client)](
                                auto &neighbors, auto parameters) mutable {
      auto result = std::make_unique<async::Result>(async::Result{
          .neighbors = std::move(neighbors),
          .parameters = std::move(parameters),
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
