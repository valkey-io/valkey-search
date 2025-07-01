/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/planner.h"

#include <cstddef>

#include "absl/log/check.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"

namespace valkey_search::query {
// TODO: Tune this parameter.
constexpr double kPreFilteringThresholdRatio = 0.001;  // 0.1%
// The query planner decides whether to use pre or inline filtering based on
// heuristics.
bool UsePreFiltering(size_t estimated_num_of_keys,
                     indexes::VectorBase *vector_index) {
  if (vector_index->GetIndexerType() == indexes::IndexerType::kFlat) {
    /* With a flat index, the search needs to go through all the vectors,
    taking O(N*log(k)). With pre-filtering, we can do the same search on the
    reduced space, taking O(n*log(k)). Therefore we should always choose
    pre-filtering */
    return true;
  }
  if (vector_index->GetIndexerType() == indexes::IndexerType::kHNSW) {
    // TODO: Come up with a formulation accounting for various
    // other factors like ef_construction, M, size of vectors, ef_runtime, k
    // etc. Also benchmark various combinations to tune the hyperparameters.
    size_t N = vector_index->GetCapacity();
    // We choose pre-filtering if the size of the filtered space is below a
    // certain threshold (relative to the total size).
    return estimated_num_of_keys <= kPreFilteringThresholdRatio * N;
  }
  CHECK(false) << "Unsupported indexer type: "
               << (int)vector_index->GetIndexerType();
}
}  // namespace valkey_search::query
