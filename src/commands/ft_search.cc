/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <strings.h>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "src/commands/commands.h"
#include "src/commands/ft_search_parser.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"
#include "src/metrics.h"
#include "src/query/response_generator.h"
#include "src/query/search.h"
#include "value.h"
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

// Replies with the field/value pairs of a result and returns the number of
// pairs. The caller wraps them in a RESP2 array or a RESP3 map. A map keeps
// only one value per name, so with unique_names a field named like the score
// is omitted and the score wins, as in Redis Search.
size_t ReplyAttributes(ValkeyModuleCtx *ctx, const indexes::Neighbor &neighbor,
                       const query::SearchParameters &parameters,
                       bool unique_names = false) {
  const auto &contents = neighbor.attribute_contents.value();
  size_t cnt = 0;
  if (parameters.return_attributes.empty()) {
    absl::string_view score_as;
    if (parameters.IsVectorQuery()) {
      ReplyScore(ctx, *parameters.score_as, neighbor);
      ++cnt;
      if (unique_names) {
        score_as = vmsdk::ToStringView(parameters.score_as.get());
      }
    }
    for (const auto &attribute_content : contents) {
      if (!score_as.empty() && attribute_content.first == score_as) {
        continue;
      }
      ValkeyModule_ReplyWithString(ctx,
                                   attribute_content.second.GetIdentifier());
      ValkeyModule_ReplyWithString(ctx, attribute_content.second.value.get());
      ++cnt;
    }
    return cnt;
  }
  for (const auto &return_attribute : parameters.return_attributes) {
    auto identifier = vmsdk::ToStringView(return_attribute.identifier.get());
    if (parameters.IsVectorQuery() &&
        identifier == vmsdk::ToStringView(parameters.score_as.get())) {
      ReplyScore(ctx, *parameters.score_as, neighbor);
      ++cnt;
    } else if (auto it = contents.find(identifier); it != contents.end()) {
      ValkeyModule_ReplyWithString(ctx, return_attribute.alias.get());
      ValkeyModule_ReplyWithString(ctx, it->second.value.get());
      ++cnt;
    }
  }
  return cnt;
}

void ReplyAttributesArray(ValkeyModuleCtx *ctx,
                          const indexes::Neighbor &neighbor,
                          const query::SearchParameters &parameters) {
  ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_LEN);
  ValkeyModule_ReplySetArrayLength(
      ctx, 2 * ReplyAttributes(ctx, neighbor, parameters));
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
    ReplyAttributesArray(ctx, neighbors[i], parameters);
  }
}

// Helper function to get the sort key value for a neighbor
std::string GetSortKeyValue(const indexes::Neighbor &neighbor,
                            const SearchCommand &command) {
  if (!command.sortby_parameter.has_value() ||
      !neighbor.attribute_contents.has_value()) {
    return "";
  }

  auto it = neighbor.attribute_contents->find(command.sortby_parameter->field);
  if (it == neighbor.attribute_contents->end()) {
    return "";
  }

  return std::string(vmsdk::ToStringView(it->second.value.get()));
}

// Handle non-vector queries by processing the neighbors and replying with the
// attribute contents.
void SerializeNonVectorNeighbors(ValkeyModuleCtx *ctx,
                                 const query::SearchResult &search_result,
                                 const SearchCommand &command) {
  const auto &neighbors = search_result.neighbors;
  auto range = search_result.GetSerializationRange(command);

  // When with_sort_keys is true, we add an extra element per result (the sort
  // key)
  size_t elements_per_result = command.with_sort_keys ? 3 : 2;
  ValkeyModule_ReplyWithArray(ctx, elements_per_result * range.count() + 1);
  ReplyAvailNeighbors(ctx, search_result, command);
  for (size_t i = range.start_index; i < range.end_index; ++i) {
    // Document ID
    ValkeyModule_ReplyWithString(
        ctx, vmsdk::MakeUniqueValkeyString(*neighbors[i].external_id).get());

    // Sort key value (prefixed with #) when WITHSORTKEYS is specified
    if (command.with_sort_keys) {
      std::string sort_key_value = GetSortKeyValue(neighbors[i], command);
      std::string prefixed_value = "#" + sort_key_value;
      ValkeyModule_ReplyWithString(
          ctx, vmsdk::MakeUniqueValkeyString(prefixed_value).get());
    }

    ReplyAttributesArray(ctx, neighbors[i], command);
  }
}

void ReplySortKeyResp3(ValkeyModuleCtx *ctx, const indexes::Neighbor &neighbor,
                       const SearchCommand &command) {
  ValkeyModule_ReplyWithSimpleString(ctx, "sortkey");
  if (!command.sortby_parameter || !neighbor.attribute_contents) {
    ValkeyModule_ReplyWithNull(ctx);
    return;
  }
  const auto &field = command.sortby_parameter->field;
  const auto &contents = *neighbor.attribute_contents;
  auto it = contents.find(field);
  if (it == contents.end()) {
    auto identifier = command.index_schema->GetIdentifier(field);
    if (identifier.ok()) {
      it = contents.find(*identifier);
    }
  }
  if (it == contents.end()) {
    ValkeyModule_ReplyWithNull(ctx);
    return;
  }
  auto index = command.index_schema->GetIndex(field);
  bool numeric = index.ok() &&
                 (*index)->GetIndexerType() == indexes::IndexerType::kNumeric;
  std::string value = numeric ? "#" : "$";
  value.append(vmsdk::ToStringView(it->second.value.get()));
  ValkeyModule_ReplyWithStringBuffer(ctx, value.data(), value.size());
}

void SerializeNeighborsResp3(ValkeyModuleCtx *ctx,
                             const query::SearchResult &search_result,
                             const SearchCommand &command) {
  const auto total =
      command.IsNonVectorQuery()
          ? search_result.total_count
          : std::min(search_result.total_count, static_cast<size_t>(command.k));
  if (query::ShouldReturnNoResults(command)) {
    query::ReplyWithSearchEnvelope(ctx, total, 0);
    return;
  }
  const auto range = search_result.GetSerializationRange(command);
  query::ReplyWithSearchEnvelope(ctx, total, range.count());
  for (auto i = range.start_index; i < range.end_index; ++i) {
    const auto &neighbor = search_result.neighbors[i];
    ValkeyModule_ReplyWithMap(ctx, 2 + (command.with_sort_keys ? 1 : 0) +
                                       (command.no_content ? 0 : 1));
    ValkeyModule_ReplyWithSimpleString(ctx, "id");
    auto id = neighbor.external_id->Str();
    ValkeyModule_ReplyWithStringBuffer(ctx, id.data(), id.size());
    if (command.with_sort_keys) {
      ReplySortKeyResp3(ctx, neighbor, command);
    }
    if (command.no_content) {
      ValkeyModule_ReplyWithSimpleString(ctx, "values");
      ValkeyModule_ReplyWithArray(ctx, 0);
      continue;
    }
    ValkeyModule_ReplyWithSimpleString(ctx, "extra_attributes");
    ValkeyModule_ReplyWithMap(ctx, VALKEYMODULE_POSTPONED_LEN);
    ValkeyModule_ReplySetMapLength(
        ctx, ReplyAttributes(ctx, neighbor, command, /*unique_names=*/true));
    ValkeyModule_ReplyWithSimpleString(ctx, "values");
    ValkeyModule_ReplyWithArray(ctx, 0);
  }
}

}  // namespace
// Apply sorting to neighbors based on attribute values in attribute_contents
void ApplySorting(std::vector<indexes::Neighbor> &neighbors,
                  const SearchCommand &parameters) {
  if (!parameters.sortby_parameter.has_value() || neighbors.empty()) {
    return;
  }

  auto sortby = parameters.sortby_parameter.value();

  // Check if field is a declared numeric attribute
  auto index_result = parameters.index_schema->GetIndex(sortby.field);
  bool is_numeric =
      index_result.ok() &&
      index_result.value()->GetIndexerType() == indexes::IndexerType::kNumeric;
  auto compare = [&](const indexes::Neighbor &a,
                     const indexes::Neighbor &b) -> bool {
    if (!a.attribute_contents.has_value() ||
        !b.attribute_contents.has_value()) {
      return false;
    }

    auto it_a = a.attribute_contents->find(sortby.field);
    auto it_b = b.attribute_contents->find(sortby.field);

    if (it_a == a.attribute_contents->end()) {
      return false;
    }
    if (it_b == b.attribute_contents->end()) {
      return true;
    }

    auto str_a = vmsdk::ToStringView(it_a->second.value.get());
    auto str_b = vmsdk::ToStringView(it_b->second.value.get());

    expr::Value val_a, val_b;
    if (is_numeric) {
      auto num_a = vmsdk::To<double>(str_a).value_or(0.0);
      auto num_b = vmsdk::To<double>(str_b).value_or(0.0);
      val_a = expr::Value(num_a);
      val_b = expr::Value(num_b);
    } else {
      val_a = expr::Value(str_a);
      val_b = expr::Value(str_b);
    }

    auto cmp = expr::Compare(val_a, val_b);
    if (cmp == expr::Ordering::kLESS) {
      return sortby.order == query::SortOrder::kAscending;
    }
    if (cmp == expr::Ordering::kGREATER) {
      return sortby.order == query::SortOrder::kDescending;
    }
    return false;
  };

  auto amountToKeep = parameters.limit.first_index + parameters.limit.number;
  if (amountToKeep >= neighbors.size()) {
    std::stable_sort(neighbors.begin(), neighbors.end(), compare);
  } else {
    std::partial_sort(neighbors.begin(), neighbors.begin() + amountToKeep,
                      neighbors.end(), compare);
  }
}

// Check for scenarios that require sending an early reply.
// Returns true if an early reply was sent and processing should stop.
bool HandleEarlyReplyScenarios(ValkeyModuleCtx *ctx,
                               query::SearchResult &search_result,
                               const SearchCommand &command) {
  // Check if no results should be returned based on query parameters.
  if (query::ShouldReturnNoResults(command)) {
    ValkeyModule_ReplyWithArray(ctx, 1);
    ValkeyModule_ReplyWithLongLong(ctx, search_result.total_count);
    return true;  // Early reply sent, stop processing
  }

  if (command.no_content) {
    SendReplyNoContent(ctx, search_result, command);
    return true;  // Early reply sent, stop processing
  }

  return false;  // No early reply needed, continue processing
}

// Process neighbors for both vector and non-vector queries
absl::Status ProcessNeighborsForQuery(ValkeyModuleCtx *ctx,
                                      query::SearchResult &search_result,
                                      SearchCommand &command) {
  size_t original_size = search_result.neighbors.size();

  std::optional<std::string> vector_identifier = std::nullopt;

  if (command.IsVectorQuery()) {
    VMSDK_ASSIGN_OR_RETURN(
        vector_identifier,
        command.index_schema->GetIdentifier(command.attribute_alias));
  }
  // Handle vector queries

  query::ProcessNeighborsForReply(
      ctx, command.index_schema->GetAttributeDataType(),
      search_result.neighbors, command, vector_identifier);
  // Adjust total count based on neighbors removed during processing
  // due to filtering or missing attributes.
  search_result.total_count -= (original_size - search_result.neighbors.size());

  return absl::OkStatus();
}

// RESP3 uses the Redis Search map envelope. The RESP2 reply is an array of:
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

  const bool resp3 =
      ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_RESP3;
  if (resp3 && (query::ShouldReturnNoResults(*this) ||
                (no_content && !sortby_parameter))) {
    SerializeNeighborsResp3(ctx, search_result, *this);
    return;
  }

  // 1. Handle early reply scenarios
  if (!resp3 && HandleEarlyReplyScenarios(ctx, search_result, *this)) {
    return;
  }

  // 2. Process neighbors for the query
  auto status = ProcessNeighborsForQuery(ctx, search_result, *this);
  if (!status.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    ValkeyModule_ReplyWithError(ctx, status.message().data());
    return;
  }

  ApplySorting(search_result.neighbors, *this);

  // 3. Serialize neighbors based on query type
  if (resp3) {
    SerializeNeighborsResp3(ctx, search_result, *this);
  } else if (IsNonVectorQuery()) {
    SerializeNonVectorNeighbors(ctx, search_result, *this);
  } else {
    SerializeNeighbors(ctx, search_result, *this);
  }
}

absl::Status FTSearchCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                         int argc) {
  return QueryCommand::Execute(ctx, argv, argc,
                               std::unique_ptr<QueryCommand>(new SearchCommand(
                                   ValkeyModule_GetSelectedDb(ctx))));
}

}  // namespace valkey_search
