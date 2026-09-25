/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_PLANNER_H_
#define VALKEYSEARCH_SRC_QUERY_PLANNER_H_

#include "src/indexes/vector_base.h"

namespace valkey_search::query {

enum class HybridPolicy;
struct SearchParameters;

// Returns whether to use pre-filtering as opposed to inline filtering based on
// explicit query policy and planner heuristics.
bool UsePreFiltering(size_t estimated_num_of_keys,
                     indexes::VectorBase *vector_index,
                     const SearchParameters &parameters);
}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_PLANNER_H_
