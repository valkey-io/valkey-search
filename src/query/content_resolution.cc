/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/content_resolution.h"

#include <algorithm>
#include <cstddef>
#include <optional>
#include <string>
#include <utility>

#include "absl/types/span.h"
#include "src/commands/ft_search.h"
#include "src/index_schema.h"
#include "src/query/response_generator.h"
#include "src/query/search.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search::query {

void SeedContentFetch(SearchParameters &params,
                      std::vector<indexes::Neighbor> candidates) {
  // Every candidate is a global winner: resolve content for all of them.
  params.content_limit.reset();
  params.search_result.neighbors = std::move(candidates);
  params.search_result.total_count = params.search_result.neighbors.size();
}

void ResolveContent(std::unique_ptr<SearchParameters> params) {
  vmsdk::VerifyMainThread();
  // 1. Check cancellation
  if (params->cancellation_token->IsCancelled()) {
    params->QueryCompleteMainThread(std::move(params));
    return;
  }

  // 2. Check if index is dropped
  if (params->index_schema->IsMarkedDestructing()) {
    params->search_result.status = GenerateIndexNotFoundError(
        params->index_schema->GetDBNum(), params->index_schema->GetName());
    params->QueryCompleteMainThread(std::move(params));
    return;
  }

  // Bounded content fan-out: only the first `content_limit` results (the head)
  // are resolved here; the rest go back to the coordinator as key and score,
  // and are resolved on demand if they win (see query/fanout_content_limit.h).
  // Without a content limit the head is every result.
  const size_t result_count = params->search_result.neighbors.size();
  const size_t head = std::min<size_t>(
      params->content_limit.value_or(result_count), result_count);

  // 3. If kContentionCheckRequired, check for in-flight mutations. Only the
  // head is read below, so only the head needs its mutations applied first.
  if (params->GetContentProcessing() == kContentionCheckRequired) {
    if (params->index_schema->PerformKeyContentionCheck(
            absl::MakeConstSpan(params->search_result.neighbors).first(head),
            std::move(params))) {
      // Contention found — params has been moved into the mutation queue.
      // ConsumeTrackedMutatedAttribute will call ResolveContent again
      // via RunByMain when the mutation completes.
      return;
    }
    // If PerformKeyContentionCheck returns false, params is still valid (not
    // moved). Fall through to content fetch.
  }

  // 4. Content fetch + filter via ProcessNeighborsForReply.
  //
  // A caller that wants no content still comes through here for the checks
  // above -- FT.HYBRID runs the contention check whatever its LOAD clause
  // asked for -- but there is nothing to fetch for it.
  //
  // NOCONTENT alone is not enough to skip the fetch: a SORTBY has to read the
  // sort field off each document even when the reply carries only keys, which
  // is what NoProcessingRequired() adds. Testing `no_content` here instead
  // left the sort nothing to compare, so a sorted NOCONTENT search came back
  // in arbitrary order -- the defect #1217 fixed, reintroduced from this side.
  if (params->NoProcessingRequired()) {
    params->QueryCompleteMainThread(std::move(params));
    return;
  }
  auto ctx = vmsdk::MakeUniqueValkeyThreadSafeContext(nullptr);
  const auto &attribute_data_type =
      params->index_schema->GetAttributeDataType();
  size_t original_size = params->search_result.neighbors.size();

  std::optional<std::string> vector_identifier = std::nullopt;
  if (!params->attribute_alias.empty()) {
    auto id = params->index_schema->GetIdentifier(params->attribute_alias);
    if (id.ok()) {
      vector_identifier = *id;
    }
  }

  query::ProcessNeighborsForReply(ctx.get(), attribute_data_type,
                                  params->search_result.neighbors, *params,
                                  vector_identifier, head);

  // 5. Adjust search_result.total_count for removed neighbors
  size_t removed = original_size - params->search_result.neighbors.size();
  if (params->search_result.total_count > removed) {
    params->search_result.total_count -= removed;
  } else {
    params->search_result.total_count = 0;
  }

  // 6. Apply INKEYS post-filter (shard-local in cluster mode)
  if (params->inkeys.has_value()) {
    ApplyInkeysFilter(params->search_result, *params->inkeys);
  }

  // 7. Call QueryCompleteMainThread
  params->QueryCompleteMainThread(std::move(params));
}

}  // namespace valkey_search::query
