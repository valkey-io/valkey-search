/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/scoring/slop_calculator.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/types/span.h"

namespace valkey_search::indexes::scoring {

namespace {

// Minimum absolute gap between two sorted, non-empty position lists.
// Two-pointer scan, O(|left| + |right|).
SlopPosition MinGap(const std::vector<SlopPosition>& left,
                    const std::vector<SlopPosition>& right) {
  size_t i = 0;
  size_t j = 0;
  SlopPosition best = std::numeric_limits<SlopPosition>::max();
  while (i < left.size() && j < right.size()) {
    SlopPosition l = left[i];
    SlopPosition r = right[j];
    best = std::min(best, l > r ? l - r : r - l);
    if (l < r) {
      ++i;
    } else {
      ++j;
    }
  }
  return best;
}

// floor(sqrt(n)) computed in integer space. Avoids depending on the exact
// rounding of std::sqrt under -ffast-math: seed from a double then correct.
uint32_t IntSqrt(uint64_t n) {
  if (n == 0) return 0;
  uint64_t x = static_cast<uint64_t>(std::sqrt(static_cast<double>(n)));
  while (x * x > n) --x;
  while ((x + 1) * (x + 1) <= n) ++x;
  return static_cast<uint32_t>(x);
}

}  // namespace

void SlopCalculator::EmitAnchor(Anchor anchor) {
  if (group_stack_.empty()) {
    anchors_.push_back(std::move(anchor));
    return;
  }
  // Fold into the enclosing group's union.
  Anchor& parent = group_stack_.back();
  parent.insert(parent.end(), anchor.begin(), anchor.end());
}

void SlopCalculator::OnTerm(absl::Span<const SlopPosition> positions) {
  // A term absent in this doc contributes no anchor. For an outermost term
  // this only happens when admission did not require it (e.g. an OR branch).
  if (positions.empty()) {
    return;
  }
  EmitAnchor(Anchor(positions.begin(), positions.end()));
}

void SlopCalculator::EnterGroup() { group_stack_.emplace_back(); }

void SlopCalculator::ExitGroup() {
  CHECK(!group_stack_.empty());
  Anchor group = std::move(group_stack_.back());
  group_stack_.pop_back();
  // An all-absent group has an empty union: drop it rather than emit an
  // empty anchor.
  if (group.empty()) {
    return;
  }
  EmitAnchor(std::move(group));
}

uint32_t SlopCalculator::Finalize() {
  CHECK(group_stack_.empty());
  if (anchors_.size() <= 1) {
    return 1;
  }
  // Sort positions *within* each anchor so MinGap's two-pointer scan holds.
  // The order of anchors_ itself is the query order and is left untouched.
  for (auto& anchor : anchors_) {
    std::sort(anchor.begin(), anchor.end());
  }
  uint64_t sum_squares = 0;
  for (size_t i = 0; i + 1 < anchors_.size(); ++i) {
    uint64_t gap = MinGap(anchors_[i], anchors_[i + 1]);
    sum_squares += gap * gap;
  }
  // The min-1 guard is applied only here, to the final slop, to avoid a
  // divide-by-zero when slop later divides the TFIDF numerator.
  return std::max(1u, IntSqrt(sum_squares));
}

}  // namespace valkey_search::indexes::scoring
