/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
