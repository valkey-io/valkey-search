/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COORDINATOR_SEARCH_CONVERTER_H_
#define VALKEYSEARCH_SRC_COORDINATOR_SEARCH_CONVERTER_H_

#include <memory>

#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "grpcpp/server_context.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/index_schema.h"
#include "src/query/predicate.h"
#include "src/query/search.h"

namespace valkey_search::coordinator {

absl::Status GRPCSearchRequestToParameters(
    const SearchIndexPartitionRequest& request,
    grpc::CallbackServerContext* context, query::SearchParameters* parameters);

// Converts a single gRPC predicate (and its children, recursively) into a
// query::Predicate. Exposed for testing: this is the inter-node entry point
// that must defensively reject malformed UTF-8 before it reaches decoders that
// CHECK-fail on it (e.g. FuzzySearch::Search).
absl::StatusOr<std::unique_ptr<query::Predicate>> GRPCPredicateToPredicate(
    const Predicate& predicate, std::shared_ptr<IndexSchema> index_schema,
    absl::flat_hash_set<std::string>& attribute_identifiers);

std::unique_ptr<SearchIndexPartitionRequest> ParametersToGRPCSearchRequest(
    const query::SearchParameters& parameters);

std::optional<query::SortByParameter> SortByFromGRPC(
    const SearchIndexPartitionRequest& request);

void SortByToGRPC(const std::optional<query::SortByParameter>& sortby,
                  SearchIndexPartitionRequest* request);

}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_SRC_COORDINATOR_SEARCH_CONVERTER_H_
