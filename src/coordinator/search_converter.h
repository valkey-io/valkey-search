/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COORDINATOR_SEARCH_CONVERTER_H_
#define VALKEYSEARCH_SRC_COORDINATOR_SEARCH_CONVERTER_H_

#include <memory>
#include <string>

#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "grpcpp/server_context.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/index_schema.h"
#include "src/query/predicate.h"
#include "src/query/search.h"

namespace valkey_search::coordinator {

// Converts a wire predicate tree back into an in-process predicate tree,
// accumulating into `attribute_identifiers` every attribute identifier the tree
// reads. That set becomes SearchParameters::filter_parse_results
// ::filter_identifiers on the shard, which gates the post-search
// re-verification in response_generator.cc -- an empty set disables it.
absl::StatusOr<std::unique_ptr<query::Predicate>> GRPCPredicateToPredicate(
    const Predicate& predicate, std::shared_ptr<IndexSchema> index_schema,
    absl::flat_hash_set<std::string>& attribute_identifiers);

// Inverse of GRPCPredicateToPredicate. Returns nullptr for a predicate kind
// that has no wire representation.
std::unique_ptr<Predicate> PredicateToGRPCPredicate(
    const query::Predicate& predicate);

absl::Status GRPCSearchRequestToParameters(
    const SearchIndexPartitionRequest& request,
    grpc::CallbackServerContext* context, query::SearchParameters* parameters);

std::unique_ptr<SearchIndexPartitionRequest> ParametersToGRPCSearchRequest(
    const query::SearchParameters& parameters);

std::optional<query::SortByParameter> SortByFromGRPC(
    const SearchIndexPartitionRequest& request);

void SortByToGRPC(const std::optional<query::SortByParameter>& sortby,
                  SearchIndexPartitionRequest* request);

coordinator::Scorer ScorerToGRPC(indexes::scoring::ScorerType scorer);

indexes::scoring::ScorerType ScorerFromGRPC(coordinator::Scorer scorer);

}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_SRC_COORDINATOR_SEARCH_CONVERTER_H_
