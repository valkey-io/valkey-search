/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COORDINATOR_SEARCH_CONVERTER_H_
#define VALKEYSEARCH_SRC_COORDINATOR_SEARCH_CONVERTER_H_

#include <memory>
#include <optional>

#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "grpcpp/server_context.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/query/search.h"

namespace valkey_search::coordinator {

absl::Status GRPCSearchRequestToParameters(
    const SearchIndexPartitionRequest& request,
    grpc::CallbackServerContext* context, query::SearchParameters* parameters);

std::unique_ptr<SearchIndexPartitionRequest> ParametersToGRPCSearchRequest(
    const query::SearchParameters& parameters);

std::optional<query::SortByParameter> SortByFromGRPC(
    const SearchIndexPartitionRequest& request);

void SortByToGRPC(const std::optional<query::SortByParameter>& sortby,
                  SearchIndexPartitionRequest* request);

// Decodes the infields repeated field from a proto request into an optional
// set. Returns nullopt when no infields are present on the wire (same semantics
// as an unset optional in SearchParameters).
std::optional<absl::flat_hash_set<std::string>> InfieldsFromGRPC(
    const SearchIndexPartitionRequest& request);

}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_SRC_COORDINATOR_SEARCH_CONVERTER_H_
