/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_RESPONSE_GENERATOR_H_
#define VALKEYSEARCH_SRC_QUERY_RESPONSE_GENERATOR_H_

#include <string>
#include <vector>

#include "src/attribute_data_type.h"
#include "src/indexes/vector_base.h"
#include "src/query/search.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::options {

/// Return the configuration entry that allows the caller to control the
/// maximum content size for a record in the search response
vmsdk::config::Number &GetMaxSearchResultRecordSize();

/// Return the configuration entry that allows the caller to control the
/// maximum number of fields in the content of the search response
vmsdk::config::Number &GetMaxSearchResultFieldsCount();

}  // namespace valkey_search::options
namespace valkey_search::query {

// Adds all local content for neighbors to the list of neighbors.
// Skipping neighbors if one of the following:
// Neighbor already contained in the attribute content map.
// Neighbor without any attribute content.
// Neighbor not comply to the pre-filter expression.
void ProcessNeighborsForReply(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    std::vector<indexes::Neighbor> &neighbors,
    const query::SearchParameters &parameters,
    const std::optional<std::string> &vector_identifier);

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_RESPONSE_GENERATOR_H_
