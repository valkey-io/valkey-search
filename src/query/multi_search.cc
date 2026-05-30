/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/multi_search.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "src/commands/ft_aggregate_parser.h"  // for ~AggregateParameters
#include "src/expr/expr.h"                     // for ~Expression
#include "src/query/search.h"
#include "vmsdk/src/thread_pool.h"

namespace valkey_search::query {

MultiSearchParameters::MultiSearchParameters() = default;
MultiSearchParameters::~MultiSearchParameters() = default;

std::unique_ptr<MultiSearchParameters> MakeMultiSearchParameters() {
  // Direct std::make_unique would inline the destructor (and the inner
  // unique_ptr<AggregateParameters>'s deleter) at the call site, which
  // requires AggregateParameters to be complete. Construct via new instead.
  return std::unique_ptr<MultiSearchParameters>(new MultiSearchParameters());
}

void MultiArmShim::QueryCompleteBackground(
    std::unique_ptr<SearchParameters> self) {
  CHECK(!vmsdk::IsMainThread());
  CHECK(no_content);
  // Hand off to the meta-tracker. We move `self` into the tracker so the
  // arm's SearchParameters (which owns string_view-backed Neighbor entries)
  // outlives the fused result.
  auto tracker_copy = tracker;
  tracker.reset();
  auto result = std::move(search_result);
  tracker_copy->OnArmComplete(arm_index, std::move(result), std::move(self));
}

void MultiArmShim::QueryCompleteMainThread(
    std::unique_ptr<SearchParameters> self) {
  CHECK(vmsdk::IsMainThread());
  CHECK(!no_content);
  auto tracker_copy = tracker;
  tracker.reset();
  auto result = std::move(search_result);
  tracker_copy->OnArmComplete(arm_index, std::move(result), std::move(self));
}

MultiSearchTracker::MultiSearchTracker(
    std::unique_ptr<MultiSearchParameters> params)
    : parameters_(std::move(params)), outstanding_(parameters_->arms.size()) {
  // SearchResult is move-only; resize() default-constructs each slot.
  parameters_->per_arm_results.resize(parameters_->arms.size());
}

void MultiSearchTracker::OnArmComplete(
    size_t arm_index, SearchResult&& result,
    std::unique_ptr<SearchParameters> arm_self) {
  bool finalize_now = false;
  {
    absl::MutexLock lock(&mu_);
    CHECK(arm_index < parameters_->per_arm_results.size());
    if (!result.status.ok() && !any_arm_failed_.exchange(true)) {
      first_error_ = result.status;
      // Cancel in-flight arms (best-effort; subsequent arms may already be
      // in their content-fetch stage and won't observe this immediately).
      // The token is null only in unit tests that bypass dispatch; production
      // callers always populate it via cancel::Make.
      if (!parameters_->enable_partial_results &&
          parameters_->cancellation_token) {
        parameters_->cancellation_token->Cancel();
      }
    }
    parameters_->per_arm_results[arm_index] = std::move(result);
    arm_owners_.emplace_back(std::move(arm_self));
    CHECK(outstanding_ > 0);
    --outstanding_;
    finalize_now = (outstanding_ == 0);
  }
  if (finalize_now) {
    Finalize();
  }
}

void MultiSearchTracker::SetOuterReaderLock(vmsdk::TimeSlicedMRMWMutex* mutex,
                                            bool may_prolong) {
  absl::MutexLock lock(&mu_);
  outer_mutex_ = mutex;
  outer_may_prolong_ = may_prolong;
}

void MultiSearchTracker::ReleaseOuterReaderLock() {
  vmsdk::TimeSlicedMRMWMutex* mutex = nullptr;
  bool may_prolong = false;
  {
    absl::MutexLock lock(&mu_);
    mutex = outer_mutex_;
    may_prolong = outer_may_prolong_;
    outer_mutex_ = nullptr;
  }
  if (mutex != nullptr) {
    mutex->Unlock(may_prolong, /*ignore_time_quota=*/false);
  }
}

void MultiSearchTracker::Finalize() {
  std::unique_ptr<MultiSearchParameters> params;
  {
    absl::MutexLock lock(&mu_);
    params = std::move(parameters_);
    // Transfer arm ownership into the envelope so the per-arm SearchParameters
    // (and their local-responder chains) outlive the async reply path. The
    // per-arm Neighbor entries may hold string_view keys into these objects.
    for (auto& owner : arm_owners_) {
      params->retained_arm_owners.push_back(std::move(owner));
    }
    arm_owners_.clear();
  }
  if (any_arm_failed_.load() && !params->enable_partial_results) {
    params->search_result.status = first_error_;
  }
  // Release the outer reader lock — every arm has observed a consistent index
  // snapshot by this point, so writers may now switch in. (Done BEFORE the
  // user-supplied completion so a slow aggregate pipeline doesn't starve
  // writers; the post-fusion ResolveContent acquires its own short-lived
  // contention check independently.)
  ReleaseOuterReaderLock();
  // Hand off to the user-supplied completion. Production code (Phase 4) runs
  // fusion + the aggregate pipeline + unblocks the client. Tests inspect
  // per_arm_results from inside this callback.
  if (params->on_all_arms_complete) {
    auto cb = std::move(params->on_all_arms_complete);
    std::move(cb)(std::move(params));
  }
}

absl::Status PerformMultiSearchLocalAsync(
    std::unique_ptr<MultiSearchParameters> parameters,
    vmsdk::ThreadPool* reader_pool) {
  CHECK(parameters);
  CHECK(!parameters->arms.empty()) << "MultiSearchParameters with no arms";

  // Capture the now-populated envelope cancellation_token before moving
  // parameters into the tracker. ExecuteCommand sets the envelope token AFTER
  // ParseAfterIndex returns, so the arms parsed during ParseFtHybridCommand
  // observed a null token; this is the first opportunity to fix that up
  // before any arm executes.
  cancel::Token shared_token = parameters->cancellation_token;
  // Move arms out of parameters before constructing the tracker. After this
  // point, parameters->arms is left at the same size (tracker reads
  // parameters_->arms.size() to size per_arm_results) but the unique_ptrs
  // are nulled out — the live shim objects travel through SearchAsync.
  const size_t arm_count = parameters->arms.size();
  std::vector<std::unique_ptr<MultiArmShim>> arms = std::move(parameters->arms);
  parameters->arms.clear();
  parameters->arms.resize(arm_count);  // keep size() == N for tracker init
  // Acquire ONE reader lock on the index's time-sliced mutex BEFORE
  // scheduling any arm. Each arm's own Search() will additionally take its
  // own reader lock (recursive readers are fine — the mutex just counts), but
  // this outer lock is what guarantees the mutex stays in read mode for the
  // entire multi-arm operation. Without it, a writer could time-slice in
  // between arm A releasing its inner lock and arm B acquiring its own,
  // producing the "split-arm" inconsistency the user prohibited.
  // Released in MultiSearchTracker::Finalize after every arm has reported.
  vmsdk::TimeSlicedMRMWMutex* index_mutex = nullptr;
  bool index_mutex_may_prolong = false;
  if (parameters->index_schema != nullptr) {
    index_mutex = &parameters->index_schema->GetTimeSlicedMutex();
    index_mutex->ReaderLock(index_mutex_may_prolong,
                            /*ignore_time_quota=*/false);
  }
  auto tracker = std::make_shared<MultiSearchTracker>(std::move(parameters));
  if (index_mutex != nullptr) {
    tracker->SetOuterReaderLock(index_mutex, index_mutex_may_prolong);
  }

  for (size_t i = 0; i < arm_count; ++i) {
    arms[i]->tracker = tracker;
    arms[i]->arm_index = i;
    arms[i]->cancellation_token = shared_token;
    // Run the index search only; the database content fetch + mutation check
    // is deferred until after fusion so the multi-arm result is validated
    // atomically as a unit (see FuseThenResolveLocal). With no_content set,
    // GetContentProcessing() returns kNoContent and the arm completes on the
    // background thread without a per-arm ResolveContent.
    arms[i]->no_content = true;
    // Uncap each arm pre-fusion: fusion needs the full per-arm match set so
    // a doc matched by both arms is contributed by both (the "both arms or
    // neither" guarantee — see TestFtHybridParallelArmConsistency). The
    // aggregate-pipeline LIMIT (post-fusion) bounds the final reply size.
    arms[i]->limit.first_index = 0;
    arms[i]->limit.number = std::numeric_limits<uint64_t>::max();
    auto status =
        SearchAsync(std::move(arms[i]), reader_pool, SearchMode::kLocal);
    if (!status.ok()) {
      // Synthesize a per-arm completion with the failure status so the tracker
      // can converge.
      SearchResult err_result;
      err_result.status = status;
      tracker->OnArmComplete(i, std::move(err_result), nullptr);
    }
  }
  return absl::OkStatus();
}

}  // namespace valkey_search::query
