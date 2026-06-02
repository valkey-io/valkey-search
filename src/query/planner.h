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

// Returns whether to use pre-filtering as opposed to inline filtering based on
// heuristics.
bool UsePreFiltering(size_t estimated_num_of_keys,
                     indexes::VectorBase *vector_index);
}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_PLANNER_H_
