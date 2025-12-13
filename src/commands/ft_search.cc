/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <strings.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "src/commands/commands.h"
#include "src/commands/ft_search_parser.h"
#include "src/indexes/vector_base.h"
#include "src/metrics.h"
#include "src/query/response_generator.h"
#include "src/query/search.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {
// FT.SEARCH idx "*=>[KNN 10 @vec $BLOB AS score]" PARAMS 2 BLOB
// "\x12\xa9\xf5\x6c" DIALECT 2

void ReplyAvailNeighbors(ValkeyModuleCtx *ctx,
                         const query::SearchResult &search_result,
                         const query::SearchParameters &parameters) {
  if (parameters.IsNonVectorQuery()) {
    ValkeyModule_ReplyWithLongLong(ctx, search_result.total_count);
  } else {
    ValkeyModule_ReplyWithLongLong(
        ctx,
        std::min(search_result.total_count, static_cast<size_t>(parameters.k)));
  }
}

void SendReplyNoContent(ValkeyModuleCtx *ctx,
                        const query::SearchResult &search_result,
                        const query::SearchParameters &parameters) {
  const auto &neighbors = search_result.neighbors;
  auto range = search_result.GetSerializationRange(parameters);

  ValkeyModule_ReplyWithArray(ctx, range.count() + 1);
  ReplyAvailNeighbors(ctx, search_result, parameters);
  for (auto i = range.start_index; i < range.end_index; ++i) {
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
                        const query::SearchResult &search_result,
                        const query::SearchParameters &parameters) {
  const auto &neighbors = search_result.neighbors;
  CHECK_GT(static_cast<size_t>(parameters.k), parameters.limit.first_index);
  auto range = search_result.GetSerializationRange(parameters);

  ValkeyModule_ReplyWithArray(ctx, 2 * range.count() + 1);
  ReplyAvailNeighbors(ctx, search_result, parameters);

  for (auto i = range.start_index; i < range.end_index; ++i) {
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

void SerializeNonVectorNeighbors(ValkeyModuleCtx *ctx,
                                 const query::SearchResult &search_result,
                                 const query::SearchParameters &parameters) {
  const auto &neighbors = search_result.neighbors;
  auto range = search_result.GetSerializationRange(parameters);

  ValkeyModule_ReplyWithArray(ctx, 2 * range.count() + 1);
  ReplyAvailNeighbors(ctx, search_result, parameters);
  for (size_t i = range.start_index; i < range.end_index; ++i) {
    // Document ID
    ValkeyModule_ReplyWithString(
        ctx, vmsdk::MakeUniqueValkeyString(*neighbors[i].external_id).get());
    const auto &contents = neighbors[i].attribute_contents.value();
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
void SearchCommand::SendReply(ValkeyModuleCtx *ctx,
                              query::SearchResult &search_result) {
  // Increment success counter.
  ++Metrics::GetStats().query_successful_requests_cnt;
  auto &neighbors = search_result.neighbors;
  // This handles two cases:
  // 1. Any query with limit number == 0
  // 2. Vector queries with limit first_index >= k
  if ((!IsNonVectorQuery() &&
       (limit.first_index >= static_cast<uint64_t>(k))) ||
      limit.number == 0) {
    ValkeyModule_ReplyWithArray(ctx, 1);
    ValkeyModule_ReplyWithLongLong(ctx, search_result.total_count);
    return;
  }
  if (no_content) {
    SendReplyNoContent(ctx, search_result, *this);
    return;
  }
  size_t original_size = neighbors.size();
  // Support non-vector queries
  if (IsNonVectorQuery()) {
    query::ProcessNonVectorNeighborsForReply(
        ctx, index_schema->GetAttributeDataType(), neighbors, *this);
    search_result.total_count -= (original_size - neighbors.size());
    SerializeNonVectorNeighbors(ctx, search_result, *this);
    return;
  }
  auto identifier = index_schema->GetIdentifier(attribute_alias);
  if (!identifier.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    ValkeyModule_ReplyWithError(ctx, identifier.status().message().data());
    return;
  }
  query::ProcessNeighborsForReply(ctx, index_schema->GetAttributeDataType(),
                                  neighbors, *this, identifier.value());
  search_result.total_count -= (original_size - neighbors.size());

  SerializeNeighbors(ctx, search_result, *this);
}

absl::Status FTSearchCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                         int argc) {
  return QueryCommand::Execute(ctx, argv, argc,
                               std::unique_ptr<QueryCommand>(new SearchCommand(
                                   ValkeyModule_GetSelectedDb(ctx))));
}

}  // namespace valkey_search
