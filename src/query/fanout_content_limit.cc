/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/fanout_content_limit.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>

#include "absl/container/inlined_vector.h"
#include "absl/types/span.h"
#include "src/query/search.h"
#include "vmsdk/src/cluster_map.h"
#include "vmsdk/src/log.h"

namespace valkey_search::query::fanout {

namespace {

// The smallest e >= ceil(K * p) with P(X > e) <= tolerance, X ~ Bin(K, p).
//
// Two passes over the binomial terms from ceil(K * p) + 1 upward, each built
// from the previous by the ratio pmf(x+1) / pmf(x) = (K-x)/(x+1) * p/(1-p):
// the first sums the tail above ceil(K * p), the second subtracts terms until
// the tail is within tolerance. Past the mean the terms only shrink, so the
// first pass stops once they no longer change the sum -- about nine standard
// deviations in. No allocation; one lgamma-based term per call.
uint64_t SmallestSafeLimit(uint64_t k, double p, double tolerance) {
  if (p <= 0.0) {
    return 0;
  }
  const double kd = static_cast<double>(k);
  const uint64_t mean_share =
      std::min(k, static_cast<uint64_t>(std::ceil(kd * p - 1e-9)));
  if (p >= 1.0 || tolerance <= 0.0 || mean_share >= k) {
    return k;
  }
  const double odds = p / (1.0 - p);
  const double x0 = static_cast<double>(mean_share + 1);
  const double first_term =
      std::exp(std::lgamma(kd + 1.0) - std::lgamma(x0 + 1.0) -
               std::lgamma(kd - x0 + 1.0) + x0 * std::log(p) +
               (kd - x0) * std::log1p(-p));
  double tail = 0.0;
  double term = first_term;
  for (uint64_t x = mean_share + 1; x <= k && term > 0.0; ++x) {
    tail += term;
    if (term < tail * std::numeric_limits<double>::epsilon()) {
      break;
    }
    term *= static_cast<double>(k - x) / static_cast<double>(x + 1) * odds;
  }
  uint64_t limit = mean_share;
  term = first_term;
  while (tail > tolerance && limit < k) {
    ++limit;
    tail -= term;
    term *=
        static_cast<double>(k - limit) / static_cast<double>(limit + 1) * odds;
  }
  return limit;
}

// Each target's share of the slots the targets own. Equal shares when a
// target's slot ownership is unknown.
absl::InlinedVector<double, 16> ShardWeights(
    absl::Span<const vmsdk::cluster_map::NodeInfo> targets) {
  absl::InlinedVector<double, 16> weights(targets.size(), 0.0);
  double total = 0.0;
  for (size_t i = 0; i < targets.size(); ++i) {
    if (targets[i].shard == nullptr) {
      total = 0.0;
      break;
    }
    for (const auto &[start, end] : targets[i].shard->owned_slots) {
      weights[i] += static_cast<double>(end) - static_cast<double>(start) + 1;
    }
    total += weights[i];
  }
  if (total <= 0.0) {
    std::fill(weights.begin(), weights.end(),
              1.0 / static_cast<double>(targets.size()));
    return weights;
  }
  for (auto &weight : weights) {
    weight /= total;
  }
  return weights;
}

}  // namespace

RefetchRateEstimator::RefetchRateEstimator(double target_refetch_rate)
    : target_refetch_rate_(std::clamp(target_refetch_rate, 0.0, 1.0)) {}

void RefetchRateEstimator::Estimate(const EstimateInput &input,
                                    absl::Span<uint64_t> limits) const {
  CHECK_EQ(limits.size(), input.shard_weights.size());
  if (limits.empty()) {
    return;
  }
  const double per_shard_tolerance =
      target_refetch_rate_ / static_cast<double>(limits.size());
  // Shards usually own equal shares; compute each distinct share once.
  std::optional<double> last_weight;
  uint64_t last_limit = 0;
  for (size_t i = 0; i < limits.size(); ++i) {
    const double weight = input.shard_weights[i];
    if (last_weight != weight) {
      last_limit =
          SmallestSafeLimit(input.candidate_limit, weight, per_shard_tolerance);
      last_weight = weight;
    }
    limits[i] = last_limit;
  }
}

bool FanoutContentPlanner::IsEligible(const SearchParameters &parameters) {
  // NOCONTENT moves no content, so there is nothing to bound.
  // SORTBY / INKEYS order or filter on content the coordinator does not have
  // for a key-only candidate, so every candidate needs its content.
  return !parameters.no_content && !parameters.RequiresCompleteResults() &&
         parameters.SupportsBoundedContent() &&
         !ShouldReturnNoResults(parameters) && CandidateLimit(parameters) > 0;
}

uint64_t FanoutContentPlanner::CandidateLimit(
    const SearchParameters &parameters) {
  if (parameters.IsVectorQuery()) {
    return parameters.k > 0 ? static_cast<uint64_t>(parameters.k) : 0;
  }
  const uint64_t offset = parameters.limit.first_index;
  const uint64_t count = parameters.limit.number;
  return count > std::numeric_limits<uint64_t>::max() - offset
             ? std::numeric_limits<uint64_t>::max()
             : offset + count;
}

std::optional<BoundedContentPlan> FanoutContentPlanner::Plan(
    const SearchParameters &parameters,
    absl::Span<const vmsdk::cluster_map::NodeInfo> targets) const {
  if (targets.size() < 2 || !IsEligible(parameters)) {
    return std::nullopt;
  }
  BoundedContentPlan plan{CandidateLimit(parameters),
                          ShardLimits(targets.size())};
  const auto weights = ShardWeights(targets);
  estimator_.Estimate({plan.candidate_limit, weights},
                      absl::MakeSpan(plan.content_limits));
  const bool bounds_any =
      std::any_of(plan.content_limits.begin(), plan.content_limits.end(),
                  [&](uint64_t limit) { return limit < plan.candidate_limit; });
  if (!bounds_any) {
    // Nothing to save: the single-round path moves the same content and
    // never needs a second round.
    return std::nullopt;
  }
  return plan;
}

}  // namespace valkey_search::query::fanout
