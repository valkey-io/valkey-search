/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/scoring/scorer.h"

#include "absl/types/span.h"

namespace valkey_search::indexes::scoring {

double Scorer::CombineGroup(absl::Span<const double> child_scores,
                            double group_weight) const {
  double sum = 0.0;
  for (double s : child_scores) sum += s;
  return group_weight * sum;
}

}  // namespace valkey_search::indexes::scoring
