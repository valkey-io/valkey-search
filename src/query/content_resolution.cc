/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/content_resolution.h"

#include <optional>
#include <string>
#include <utility>

#include "src/commands/ft_search.h"
#include "src/index_schema.h"
#include "src/query/response_generator.h"
#include "src/query/search.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search::query {

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

  // 3. If kContentionCheckRequired, check for in-flight mutations
  if (params->GetContentProcessing() == kContentionCheckRequired) {
    if (params->index_schema->PerformKeyContentionCheck(
            params->search_result.neighbors, std::move(params))) {
      // Contention found — params has been moved into the mutation queue.
      // ConsumeTrackedMutatedAttribute will call ResolveContent again
      // via RunByMain when the mutation completes.
      return;
    }
    // If PerformKeyContentionCheck returns false, params is still valid (not
    // moved). Fall through to content fetch.
  }

  // 4. Content fetch + filter via ProcessNeighborsForReply
  auto ctx = vmsdk::MakeUniqueValkeyThreadSafeContext(nullptr);
  const auto& attribute_data_type =
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
                                  vector_identifier, params->sortby_parameter);

  // 5. Adjust search_result.total_count for removed neighbors
  size_t removed = original_size - params->search_result.neighbors.size();
  if (params->search_result.total_count > removed) {
    params->search_result.total_count -= removed;
  } else {
    params->search_result.total_count = 0;
  }

  // 6. Apply INKEYS post-filter (shard-local in cluster mode)
  if (!params->inkeys.empty()) {
    ApplyInkeysFilter(params->search_result.neighbors,
                      params->search_result.total_count, params->inkeys);
  }

  // 7. Call QueryCompleteMainThread
  params->QueryCompleteMainThread(std::move(params));
}

}  // namespace valkey_search::query
