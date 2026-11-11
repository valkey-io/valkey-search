/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

// Bounded content fan-out: planning (valkey-search#1324).
//
// A fanned-out query bounded to K results (KNN K, or LIMIT offset+count) needs
// all K from one shard in the worst case, so today every shard returns K
// results with content: N * K records moved to keep K. Bounded content asks
// every shard for K candidates (keys and scores) but content for only its
// first E_i. The coordinator still ranks every candidate, so the result is
// exact; a winner that arrived without content is fetched from its shard in a
// second round. A low E_i therefore costs latency on some queries, never
// correctness.
//
// How E_i is chosen. Keys hash to slots, so a global winner lands on shard i
// with probability p_i, the shard's share of the slots, and the number of
// winners on that shard is Binomial(K, p_i). A query needs the second round
// when any shard holds more winners than its E_i. The estimator picks the
// smallest E_i that keeps the chance of that at or below a target refetch
// rate alpha, split evenly over the shards (alpha / N each, so the sum over
// shards is at most alpha). Small clusters get E_i close to K, where a second
// round would cost more than it saves; large clusters get E_i close to K * p_i.
//
// This file is policy only: no I/O, unit-tested directly.

#ifndef VALKEYSEARCH_SRC_QUERY_FANOUT_CONTENT_LIMIT_H_
#define VALKEYSEARCH_SRC_QUERY_FANOUT_CONTENT_LIMIT_H_

#include <cstdint>
#include <optional>

#include "absl/container/inlined_vector.h"
#include "absl/types/span.h"
#include "src/query/search.h"
#include "vmsdk/src/cluster_map.h"

namespace valkey_search::query::fanout {

struct EstimateInput {
  uint64_t candidate_limit;                // K
  absl::Span<const double> shard_weights;  // p_i: each target's share
};

// Chooses each shard's content limit E_i. Implementations write one value per
// shard weight into `limits`, each in [ceil(K * p_i), K].
//
// The extension point for other policies; an adaptive one would wrap a base
// estimator and correct its input or output per index.
class ContentLimitEstimator {
 public:
  virtual ~ContentLimitEstimator() = default;
  virtual void Estimate(const EstimateInput &input,
                        absl::Span<uint64_t> limits) const = 0;
};

// The smallest E_i per shard that keeps the query-level chance of a refetch
// round at or below `target_refetch_rate` (see the file comment). 0 never
// refetches (every E_i = K); 1 is the most aggressive setting, still at or
// above each shard's mean share.
class RefetchRateEstimator final : public ContentLimitEstimator {
 public:
  explicit RefetchRateEstimator(double target_refetch_rate);
  void Estimate(const EstimateInput &input,
                absl::Span<uint64_t> limits) const override;

 private:
  double target_refetch_rate_;
};

using ShardLimits = absl::InlinedVector<uint64_t, 16>;

struct BoundedContentPlan {
  uint64_t candidate_limit;    // K: candidates (keys and scores) per shard.
  ShardLimits content_limits;  // E_i per target, in target order.
};

// Decides whether a fan-out bounds per-shard content, and with what limits.
class FanoutContentPlanner {
 public:
  explicit FanoutContentPlanner(const ContentLimitEstimator &estimator)
      : estimator_(estimator) {}

  // A plan for fanning `parameters` out to `targets`, or nullopt when the
  // query takes the single-round path: it is not eligible, or no shard's
  // content limit would be below K.
  std::optional<BoundedContentPlan> Plan(
      const SearchParameters &parameters,
      absl::Span<const vmsdk::cluster_map::NodeInfo> targets) const;

  // Whether the query's shape allows bounding content at all.
  static bool IsEligible(const SearchParameters &parameters);

  // K for an eligible query: KNN K, or LIMIT offset+count.
  static uint64_t CandidateLimit(const SearchParameters &parameters);

 private:
  const ContentLimitEstimator &estimator_;
};

}  // namespace valkey_search::query::fanout

#endif  // VALKEYSEARCH_SRC_QUERY_FANOUT_CONTENT_LIMIT_H_
