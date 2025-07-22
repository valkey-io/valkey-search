/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COORDINATOR_SEARCH_CONVERTER_H_
#define VALKEYSEARCH_SRC_COORDINATOR_SEARCH_CONVERTER_H_

#include <memory>

#include "absl/status/statusor.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/query/search.h"
#include "grpcpp/server_context.h"

namespace valkey_search::coordinator {

absl::StatusOr<std::unique_ptr<query::VectorSearchParameters>>
GRPCSearchRequestToParameters(const SearchIndexPartitionRequest& request, grpc::CallbackServerContext *context);

std::unique_ptr<SearchIndexPartitionRequest> ParametersToGRPCSearchRequest(
    const query::VectorSearchParameters& parameters);

}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_SRC_COORDINATOR_SEARCH_CONVERTER_H_
