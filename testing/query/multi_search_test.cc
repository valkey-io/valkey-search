/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/multi_search.h"

#include <cstddef>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "gtest/gtest.h"
#include "src/indexes/vector_base.h"
#include "src/query/search.h"
#include "src/utils/string_interning.h"

namespace valkey_search::query {
namespace {

// Construct a MultiSearchParameters with N arm shims. The shims' SearchResult
// is left empty; the test directly invokes the tracker's OnArmComplete to
// inject the per-arm result.
std::unique_ptr<MultiSearchParameters> MakeParams(size_t arm_count) {
  auto params = MakeMultiSearchParameters();
  for (size_t i = 0; i < arm_count; ++i) {
    params->arms.push_back(std::make_unique<MultiArmShim>());
  }
  return params;
}

// Build a SearchResult containing a single neighbor with the given key and
// score. Useful for asserting per-arm result aggregation.
SearchResult ResultWithOne(absl::string_view key, float score) {
  SearchResult r;
  r.total_count = 1;
  r.neighbors.push_back(indexes::Neighbor{
      StringInternStore::Intern(std::string(key)), score, std::nullopt});
  return r;
}

TEST(MultiSearchTrackerTest, FinalizeCalledAfterAllArmsComplete) {
  auto params = MakeParams(2);
  bool finalize_called = false;
  std::vector<size_t> per_arm_neighbor_counts;
  params->on_all_arms_complete =
      [&](std::unique_ptr<MultiSearchParameters> p) {
        finalize_called = true;
        for (auto& result : p->per_arm_results) {
          per_arm_neighbor_counts.push_back(result.neighbors.size());
        }
      };

  // Reset arms vector to size N (PerformMultiSearchLocalAsync's pattern); we
  // are bypassing dispatch and constructing the tracker directly.
  auto arms = std::move(params->arms);
  params->arms.clear();
  params->arms.resize(arms.size());
  auto tracker = std::make_shared<MultiSearchTracker>(std::move(params));

  // Inject completions for each arm (out of order, to test).
  tracker->OnArmComplete(1, ResultWithOne("doc:b", 0.5f), nullptr);
  EXPECT_FALSE(finalize_called);
  tracker->OnArmComplete(0, ResultWithOne("doc:a", 0.1f), nullptr);
  EXPECT_TRUE(finalize_called);

  ASSERT_EQ(per_arm_neighbor_counts.size(), 2u);
  EXPECT_EQ(per_arm_neighbor_counts[0], 1u);
  EXPECT_EQ(per_arm_neighbor_counts[1], 1u);
}

TEST(MultiSearchTrackerTest, ArmErrorPropagatedAsSearchResultStatus) {
  auto params = MakeParams(2);
  bool finalize_called = false;
  absl::Status final_status = absl::OkStatus();
  // enable_partial_results is false by default; first arm error should
  // surface as the fused result's status.
  params->on_all_arms_complete =
      [&](std::unique_ptr<MultiSearchParameters> p) {
        finalize_called = true;
        final_status = p->search_result.status;
      };
  auto arms = std::move(params->arms);
  params->arms.clear();
  params->arms.resize(arms.size());
  auto tracker = std::make_shared<MultiSearchTracker>(std::move(params));

  // Arm 0 errors; arm 1 succeeds. Tracker should still run finalize after
  // both report; because enable_partial_results is false, final_status should
  // carry the first error.
  SearchResult err;
  err.status = absl::CancelledError("simulated arm 0 cancel");
  tracker->OnArmComplete(0, std::move(err), nullptr);
  tracker->OnArmComplete(1, ResultWithOne("doc:b", 0.5f), nullptr);

  EXPECT_TRUE(finalize_called);
  EXPECT_TRUE(absl::IsCancelled(final_status));
}

TEST(MultiSearchTrackerTest, PartialResultsModePreservesSurvivingArms) {
  auto params = MakeParams(2);
  params->enable_partial_results = true;
  bool finalize_called = false;
  size_t arm0_count = 0;
  size_t arm1_count = 0;
  params->on_all_arms_complete =
      [&](std::unique_ptr<MultiSearchParameters> p) {
        finalize_called = true;
        arm0_count = p->per_arm_results[0].neighbors.size();
        arm1_count = p->per_arm_results[1].neighbors.size();
      };
  auto arms = std::move(params->arms);
  params->arms.clear();
  params->arms.resize(arms.size());
  auto tracker = std::make_shared<MultiSearchTracker>(std::move(params));

  SearchResult err;
  err.status = absl::CancelledError("arm 0 cancel");
  tracker->OnArmComplete(0, std::move(err), nullptr);
  tracker->OnArmComplete(1, ResultWithOne("doc:b", 0.5f), nullptr);

  EXPECT_TRUE(finalize_called);
  // Surviving arm's result remains visible.
  EXPECT_EQ(arm0_count, 0u);
  EXPECT_EQ(arm1_count, 1u);
}

TEST(MultiSearchTrackerTest, ConcurrentCompletions) {
  // Stress: N=8 arms reporting concurrently from N threads. Verify finalize
  // fires exactly once and per_arm_results contains all N entries.
  constexpr size_t kArmCount = 8;
  auto params = MakeParams(kArmCount);
  std::atomic<int> finalize_count{0};
  params->on_all_arms_complete =
      [&](std::unique_ptr<MultiSearchParameters> p) {
        finalize_count.fetch_add(1, std::memory_order_relaxed);
        EXPECT_EQ(p->per_arm_results.size(), kArmCount);
        for (size_t i = 0; i < kArmCount; ++i) {
          EXPECT_EQ(p->per_arm_results[i].neighbors.size(), 1u);
        }
      };
  auto arms = std::move(params->arms);
  params->arms.clear();
  params->arms.resize(arms.size());
  auto tracker = std::make_shared<MultiSearchTracker>(std::move(params));

  std::vector<std::thread> threads;
  threads.reserve(kArmCount);
  for (size_t i = 0; i < kArmCount; ++i) {
    threads.emplace_back([tracker, i]() {
      tracker->OnArmComplete(
          i, ResultWithOne(absl::StrCat("doc:", i), 0.1f * float(i)), nullptr);
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  EXPECT_EQ(finalize_count.load(), 1);
}

}  // namespace
}  // namespace valkey_search::query
