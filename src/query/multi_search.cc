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
    std::unique_ptr<SearchParameters> arm_self, bool consistency_failed) {
  bool finalize_now = false;
  if (consistency_failed) {
    consistency_failed_.store(true);
  }
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

void MultiSearchTracker::Finalize() {
  std::unique_ptr<MultiSearchParameters> params;
  absl::Status first_error;
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
    // Read first_error_ under mu_ (it is ABSL_GUARDED_BY(mu_)); use the local
    // copy below, after the lock is released.
    first_error = first_error_;
  }
  if (consistency_failed_.load()) {
    // Not a shard we can pretend didn't exist: the reply would be assembled
    // from a cluster map we no longer trust. Fails regardless of
    // enable-partial-results, matching FT.SEARCH, whose reply path checks
    // search_result.status before the partial-results gate.
    params->search_result.status =
        absl::FailedPreconditionError(kFailedPreconditionMsg);
  } else if (any_arm_failed_.load()) {
    // Deliberately NOT gated on enable_partial_results. Partial-results
    // tolerance is applied one level down, per shard, by
    // SearchPartitionResultsTracker: it only lets a non-OK status through when
    // partial results are refused, or when not a single shard answered this
    // arm. (On the local-only path there is no shard to tolerate the loss of
    // at all.) So an arm still carrying a non-OK status here is an arm nothing
    // answered, and fusing it as empty would report a total failure as "no
    // matches". FT.SEARCH errors in exactly this situation -- see the
    // !has_successful_node branch in fanout.cc.
    params->search_result.status = first_error;
  }
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
  // No lock is taken here. Each arm takes its own reader lock on the index's
  // time-sliced mutex for the duration of its own search, and nothing spans
  // the arms.
  //
  // Cross-arm correctness does not come from a shared index snapshot; it comes
  // from the post-search validation, after every arm has reported:
  //   1. ArmGate runs ONE contention check over every (arm, key) probe and
  //      parks the whole envelope until the mutation queue is quiescent.
  //   2. RevalidateArmsBeforeFusion then re-checks each neighbor's sequence
  //      number against the index and drops, re-verifies or rescores it.
  //   3. Only then does fusion run, followed by a single content fetch.
  // So an arm's stale entry is corrected, not merely inherited. The one case
  // this does not cover is a document that became newly-matching between two
  // arms' searches, which then appears in one arm rather than neither -- and
  // RevalidateArmsBeforeFusion is explicitly one-directional about exactly
  // that case already (see its comment: it "deliberately does not do ...
  // recover a document the mutation made newly matching").
  //
  // Do not re-add an outer reader lock spanning the arms. It would deadlock:
  // TimeSlicedMRMWMutex::Lock is not reentrant, so an arm's inner ReaderLock
  // enters SwitchWithWait once the read quota has expired with a writer
  // waiting, and the switch it waits for requires ShouldSwitch(), i.e.
  // active_lock_count_ == 0 -- which an outer holder blocked on that very arm
  // guarantees never happens.
  auto tracker = std::make_shared<MultiSearchTracker>(std::move(parameters));

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
    // Uncap each arm pre-fusion. Not because fusion needs the whole match set:
    // it reads only each arm's top `window` entries (see the `cap` in
    // rank_fusion.cc), and the "both arms or neither" guarantee comes from
    // ArmGate and RevalidateArmsBeforeFusion re-checking every arm together
    // after they report, not from the size of an arm. The cap is simply not
    // applied here yet. Capping to `window` is a pending optimization and
    // needs some care, because revalidation can drop entries, so a cap of
    // exactly `window` could leave an arm short of the candidates fusion is
    // entitled to. The aggregate-pipeline LIMIT (post-fusion) bounds the final
    // reply size either way.
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
