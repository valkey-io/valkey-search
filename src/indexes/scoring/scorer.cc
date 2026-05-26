/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/scoring/scorer.h"

#include "absl/types/span.h"

namespace valkey_search::indexes::scoring {

float Scorer::CombineGroup(absl::Span<const float> child_scores,
                           float group_weight) const {
  float sum = 0.0f;
  for (float s : child_scores) sum += s;
  return group_weight * sum;
}

}  // namespace valkey_search::indexes::scoring
