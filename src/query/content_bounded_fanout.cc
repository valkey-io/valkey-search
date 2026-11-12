/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/content_bounded_fanout.h"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "grpcpp/support/status.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/coordinator/search_converter.h"
#include "src/coordinator/util.h"
#include "src/indexes/vector_base.h"
#include "src/metrics.h"
#include "src/query/content_resolution.h"
#include "src/query/fanout_content_limit.h"
#include "src/query/fanout_merge.h"
#include "src/query/search.h"
#include "src/utils/cancel.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/cluster_map.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::query::fanout {

namespace {

struct ShardTarget {
  std::string address;
  uint64_t slot_fingerprint;
  uint64_t content_limit;
  bool is_local;
};

// The single-round fan-out's error rules:
//   - with consistency enabled, a FAILED_PRECONDITION from any shard fails the
//     query, whatever partial results allows;
//   - without partial results, any failure fails the query (first error);
//   - with partial results, the query fails only if no shard succeeded.
class ShardOutcomes {
 public:
  ShardOutcomes(bool enable_partial_results, bool enable_consistency)
      : enable_partial_results_(enable_partial_results),
        enable_consistency_(enable_consistency) {}

  void RecordSuccess() { has_success_ = true; }

  // Returns true once the query has failed, so in-flight shards can stop.
  bool RecordFailure(const absl::Status &status) {
    if (first_error_.ok()) {
      first_error_ = status;
    }
    if (enable_consistency_ &&
        status.code() == absl::StatusCode::kFailedPrecondition) {
      consistency_failed_ = true;
    }
    return Failed();
  }

  bool Failed() const {
    return consistency_failed_ ||
           (!first_error_.ok() && !enable_partial_results_);
  }

  absl::Status Resolve() const {
    if (consistency_failed_) {
      return absl::FailedPreconditionError(kFailedPreconditionMsg);
    }
    if (!first_error_.ok() && (!enable_partial_results_ || !has_success_)) {
      return first_error_;
    }
    return absl::OkStatus();
  }

 private:
  const bool enable_partial_results_;
  const bool enable_consistency_;
  bool has_success_{false};
  bool consistency_failed_{false};
  absl::Status first_error_;
};

struct Candidate {
  indexes::Neighbor neighbor;
  size_t shard;
  // Its content was asked for in a fetch round.
  bool requested{false};
};

// The single-round merge order (fanout_merge.h), then the reply's relevance
// order, as SearchResult::TrimResults applies it.
bool MergesBefore(const Candidate &a, const Candidate &b) {
  return NeighborComparator()(a.neighbor, b.neighbor);
}

bool RanksHigherByScore(const Candidate &a, const Candidate &b) {
  if (a.neighbor.score != b.neighbor.score) {
    return a.neighbor.score > b.neighbor.score;
  }
  return a.neighbor.external_id->Str() < b.neighbor.external_id->Str();
}

void CompleteQuery(std::unique_ptr<SearchParameters> parameters) {
  if (vmsdk::IsMainThread()) {
    parameters->QueryCompleteMainThread(std::move(parameters));
  } else {
    parameters->QueryCompleteBackground(std::move(parameters));
  }
}

// One bounded content fan-out, from dispatch to completion. Shared by its
// in-flight requests; the last reply of each round advances it.
//
// Round 1 searches every shard. Then, while the reply window holds a
// candidate without content, a fetch round asks the owning shards for those
// candidates' content. A requested candidate that comes back without content
// no longer matches and is removed, which moves the window on to the next
// candidates; so is every candidate of a shard whose fetch failed. Each
// candidate is fetched at most once, so the rounds end.
class BoundedContentFanout
    : public std::enable_shared_from_this<BoundedContentFanout> {
 public:
  BoundedContentFanout(
      std::unique_ptr<SearchParameters> parameters,
      std::unique_ptr<coordinator::SearchIndexPartitionRequest> request,
      std::vector<ShardTarget> targets, coordinator::ClientPool *client_pool,
      vmsdk::ThreadPool *thread_pool)
      : request_(std::move(request)),
        targets_(std::move(targets)),
        client_pool_(client_pool),
        thread_pool_(thread_pool),
        cancellation_token_(parameters->cancellation_token),
        deadline_(absl::Now() + absl::Milliseconds(parameters->timeout_ms)),
        outcomes_(parameters->enable_partial_results,
                  parameters->enable_consistency),
        parameters_(std::move(parameters)),
        failed_shards_(targets_.size(), false),
        outstanding_(targets_.size()) {}

  // Round 1: every shard, remote requests first so they run while the local
  // shard searches.
  void Start();

  void OnSearchReply(size_t shard, const grpc::Status &status,
                     coordinator::SearchIndexPartitionResponse &response);
  void OnLocalSearchDone(size_t shard, std::unique_ptr<SearchParameters> local);
  void OnFetchReply(size_t shard, const grpc::Status &status,
                    coordinator::SearchIndexPartitionResponse &response);
  void OnLocalFetchDone(size_t shard, std::unique_ptr<SearchParameters> local);

 private:
  bool DispatchAsync(size_t request_count) const;
  void SendSearch(size_t shard);
  void SearchLocally(size_t shard);
  void SendFetch(size_t fetch);
  // The local shard's fetch: resolved in this process, as its round-1 search
  // is, rather than through an RPC to itself.
  void FetchLocally(size_t shard,
                    std::unique_ptr<coordinator::FetchContentRequest> request);

  // Records one shard's round-1 result. `local` keeps the local search alive:
  // its neighbors refer into it. Returns true for the round's last shard.
  bool RecordSearchResult(size_t shard, const absl::Status &status,
                          std::vector<indexes::Neighbor> neighbors,
                          size_t total_count,
                          std::unique_ptr<SearchParameters> local);
  void OnSearchRoundComplete();
  // Records one shard's fetch result; `local` as for RecordSearchResult.
  // Returns true for the round's last fetch.
  bool RecordFetchResult(size_t shard, const absl::Status &status,
                         std::vector<indexes::Neighbor> fetched,
                         std::unique_ptr<SearchParameters> local);
  // Keeps a local search or fetch alive until the reply: its neighbors'
  // content refers into it.
  void KeepAlive(std::unique_ptr<SearchParameters> local)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // Starts the next fetch round, or completes the query when none is needed.
  void ContinueRefetch();
  void RankCandidates() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // The leading candidates a reply may draw from: the k nearest for a vector
  // query (the rest are kept to replace winners that drop out), all of them
  // otherwise.
  size_t EligibleCount() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // Returns true if any candidate was removed.
  bool RemoveUnresolvableCandidates() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // Builds the next fetch round; returns its request count (0: none needed).
  size_t PlanFetchRound() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // Removes the candidates the reply cannot use: key-only ones past the
  // window, or every key-only one once the query is cancelled.
  void DropKeyOnlyCandidates() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  size_t WindowEnd() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void Complete();

  // Immutable after construction.
  const std::unique_ptr<const coordinator::SearchIndexPartitionRequest>
      request_;
  const std::vector<ShardTarget> targets_;
  coordinator::ClientPool *const client_pool_;
  vmsdk::ThreadPool *const thread_pool_;
  const cancel::Token cancellation_token_;
  // The query's deadline. A fetch carries the time left until it, not the
  // query's full timeout: the shard starts its timeout when the fetch
  // arrives.
  const absl::Time deadline_;

  absl::Mutex mutex_;
  ShardOutcomes outcomes_ ABSL_GUARDED_BY(mutex_);
  std::unique_ptr<SearchParameters> parameters_ ABSL_GUARDED_BY(mutex_);
  // The local searches and fetches, chained through local_responder_.
  std::unique_ptr<SearchParameters> local_search_ ABSL_GUARDED_BY(mutex_);
  std::vector<Candidate> candidates_ ABSL_GUARDED_BY(mutex_);
  std::vector<bool> failed_shards_ ABSL_GUARDED_BY(mutex_);
  size_t total_count_ ABSL_GUARDED_BY(mutex_){0};
  size_t outstanding_ ABSL_GUARDED_BY(mutex_);
  bool refetched_ ABSL_GUARDED_BY(mutex_){false};
  // A fetch returned a recomputed score or distance, so the ranking is stale.
  bool ranking_stale_ ABSL_GUARDED_BY(mutex_){false};
  // The current fetch round: the candidate each requested key belongs to.
  absl::flat_hash_map<absl::string_view, size_t> fetched_candidate_
      ABSL_GUARDED_BY(mutex_);
  // The current fetch round's requests, built under the lock. Each is taken
  // by exactly one SendFetch before its reply can end the round, and the
  // vector is rebuilt only by the next round, so sending needs no lock.
  struct PendingFetch {
    size_t shard;
    std::unique_ptr<coordinator::FetchContentRequest> request;
  };
  std::vector<PendingFetch> pending_fetches_;
};

// The local shard's round-1 search; reports to the fan-out, not a client.
class LocalBoundedSearch : public SearchParameters {
 public:
  LocalBoundedSearch(std::shared_ptr<BoundedContentFanout> fanout, size_t shard)
      : fanout_(std::move(fanout)), shard_(shard) {}

  void QueryCompleteMainThread(
      std::unique_ptr<SearchParameters> self) override {
    Report(std::move(self));
  }
  void QueryCompleteBackground(
      std::unique_ptr<SearchParameters> self) override {
    Report(std::move(self));
  }

 private:
  void Report(std::unique_ptr<SearchParameters> self) {
    // The fan-out keeps `self` alive afterwards; release our reference to it
    // first so the two do not form a cycle.
    auto fanout = std::move(fanout_);
    fanout->OnLocalSearchDone(shard_, std::move(self));
  }

  std::shared_ptr<BoundedContentFanout> fanout_;
  size_t shard_;
};

// The local shard's fetch round; reports to the fan-out, not a client.
class LocalContentFetch : public SearchParameters {
 public:
  LocalContentFetch(std::shared_ptr<BoundedContentFanout> fanout, size_t shard)
      : fanout_(std::move(fanout)), shard_(shard) {}

  void QueryCompleteMainThread(
      std::unique_ptr<SearchParameters> self) override {
    Report(std::move(self));
  }
  void QueryCompleteBackground(
      std::unique_ptr<SearchParameters> self) override {
    Report(std::move(self));
  }

 private:
  void Report(std::unique_ptr<SearchParameters> self) {
    auto fanout = std::move(fanout_);
    fanout->OnLocalFetchDone(shard_, std::move(self));
  }

  std::shared_ptr<BoundedContentFanout> fanout_;
  size_t shard_;
};

bool BoundedContentFanout::DispatchAsync(size_t request_count) const {
  // Enqueueing many RPCs is worth moving off this thread, by the same rule as
  // the single-round fan-out.
  return request_count >= options::GetAsyncFanoutThreshold().GetValue() &&
         thread_pool_->Size() > 1;
}

void BoundedContentFanout::Start() {
  const bool async = DispatchAsync(targets_.size());
  std::optional<size_t> local_shard;
  for (size_t shard = 0; shard < targets_.size(); ++shard) {
    if (targets_[shard].is_local) {
      local_shard = shard;
      continue;
    }
    // A pool that refuses the task (stopping) must not lose the request.
    if (!async || !thread_pool_->Schedule([self = shared_from_this(),
                                           shard] { self->SendSearch(shard); },
                                          vmsdk::ThreadPool::Priority::kHigh)) {
      SendSearch(shard);
    }
  }
  if (local_shard.has_value()) {
    SearchLocally(*local_shard);
  }
}

void BoundedContentFanout::SendSearch(size_t shard) {
  auto request = std::make_unique<coordinator::SearchIndexPartitionRequest>();
  request->CopyFrom(*request_);
  request->set_slot_fingerprint(targets_[shard].slot_fingerprint);
  request->set_content_limit(targets_[shard].content_limit);
  client_pool_->GetClient(targets_[shard].address)
      ->SearchIndexPartition(
          std::move(request),
          [self = shared_from_this(), shard](
              grpc::Status status,
              coordinator::SearchIndexPartitionResponse &response) {
            self->OnSearchReply(shard, status, response);
          });
}

void BoundedContentFanout::SearchLocally(size_t shard) {
  auto local = std::make_unique<LocalBoundedSearch>(shared_from_this(), shard);
  coordinator::SearchIndexPartitionRequest request;
  request.CopyFrom(*request_);
  request.set_content_limit(targets_[shard].content_limit);
  auto status =
      coordinator::GRPCSearchRequestToParameters(request, nullptr, local.get());
  if (status.ok()) {
    status = SearchAsync(std::move(local), thread_pool_, SearchMode::kLocal);
  }
  if (!status.ok()) {
    // The search will never report, so report its failure in its place.
    VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
        << "Failed to run FT.SEARCH locally during bounded content fan-out: "
        << status.message();
    if (RecordSearchResult(shard, status, {}, 0, nullptr)) {
      OnSearchRoundComplete();
    }
  }
}

void BoundedContentFanout::OnSearchReply(
    size_t shard, const grpc::Status &status,
    coordinator::SearchIndexPartitionResponse &response) {
  // Decode before taking the lock; it is the bulk of the work.
  std::vector<indexes::Neighbor> neighbors;
  if (status.ok()) {
    neighbors.reserve(response.neighbors_size());
    for (auto &entry : *response.mutable_neighbors()) {
      neighbors.push_back(DecodeNeighborEntry(entry));
    }
  } else {
    VMSDK_LOG_EVERY_N_SEC(DEBUG, nullptr, 1)
        << "Error during bounded content FT.SEARCH on node "
        << targets_[shard].address << ", Error code: " << status.error_code();
  }
  if (RecordSearchResult(shard, ToAbslStatus(status), std::move(neighbors),
                         response.total_count(), nullptr)) {
    OnSearchRoundComplete();
  }
}

void BoundedContentFanout::OnLocalSearchDone(
    size_t shard, std::unique_ptr<SearchParameters> local) {
  auto &result = local->search_result;
  const absl::Status status = result.status;
  auto neighbors = std::move(result.neighbors);
  const size_t total_count = result.total_count;
  if (RecordSearchResult(shard, status, std::move(neighbors), total_count,
                         std::move(local))) {
    OnSearchRoundComplete();
  }
}

bool BoundedContentFanout::RecordSearchResult(
    size_t shard, const absl::Status &status,
    std::vector<indexes::Neighbor> neighbors, size_t total_count,
    std::unique_ptr<SearchParameters> local) {
  absl::MutexLock lock(&mutex_);
  KeepAlive(std::move(local));
  if (status.ok()) {
    outcomes_.RecordSuccess();
  } else if (outcomes_.RecordFailure(status)) {
    cancellation_token_->Cancel();
  }
  // A failed search's partial result is kept only under partial results.
  if (status.ok() || parameters_->enable_partial_results) {
    total_count_ += total_count;
    candidates_.reserve(candidates_.size() + neighbors.size());
    for (auto &neighbor : neighbors) {
      candidates_.push_back(Candidate{std::move(neighbor), shard});
    }
  }
  CHECK_GT(outstanding_, 0u);
  return --outstanding_ == 0;
}

void BoundedContentFanout::OnSearchRoundComplete() {
  {
    absl::MutexLock lock(&mutex_);
    RankCandidates();
  }
  ContinueRefetch();
}

void BoundedContentFanout::RankCandidates() {
  std::sort(candidates_.begin(), candidates_.end(), MergesBefore);
  if (parameters_->ScoreIsRelevance()) {
    std::stable_sort(candidates_.begin(), candidates_.begin() + EligibleCount(),
                     RanksHigherByScore);
  }
  ranking_stale_ = false;
}

size_t BoundedContentFanout::EligibleCount() const {
  return parameters_->IsVectorQuery()
             ? std::min(candidates_.size(), static_cast<size_t>(parameters_->k))
             : candidates_.size();
}

size_t BoundedContentFanout::WindowEnd() const {
  // A default SearchResult is not yet offset, so this is the end of
  // [offset, offset + count) over the eligible candidates: a vector query's
  // window ends at k, not among the candidates kept past it.
  return SearchResult()
      .GetSerializationRange(*parameters_, EligibleCount())
      .end_index;
}

bool BoundedContentFanout::RemoveUnresolvableCandidates() {
  // A requested candidate still without content no longer matches (or its
  // shard failed): like a result the reply path cannot resolve, it is dropped
  // and no longer counted. A failed shard's other key-only candidates cannot
  // be resolved either; they were never returned, so they are not counted.
  const size_t before = candidates_.size();
  size_t dropped = 0;
  candidates_.erase(
      std::remove_if(candidates_.begin(), candidates_.end(),
                     [&](const Candidate &candidate) {
                       if (candidate.neighbor.attribute_contents.has_value()) {
                         return false;
                       }
                       dropped += candidate.requested;
                       return candidate.requested ||
                              failed_shards_[candidate.shard];
                     }),
      candidates_.end());
  total_count_ -= std::min(dropped, total_count_);
  return candidates_.size() != before;
}

size_t BoundedContentFanout::PlanFetchRound() {
  // A removal moves the next candidates up; for a vector query it can also
  // change which ones are eligible. A recomputed score can reorder them.
  if (RemoveUnresolvableCandidates() || ranking_stale_) {
    RankCandidates();
  }
  pending_fetches_.clear();
  fetched_candidate_.clear();
  if (outcomes_.Failed() || cancellation_token_->IsCancelled()) {
    return 0;
  }
  std::vector<size_t> fetch_of_shard(targets_.size(), SIZE_MAX);
  const int64_t remaining_ms =
      std::max<int64_t>(1, absl::ToInt64Milliseconds(deadline_ - absl::Now()));
  size_t missing = 0;
  // Every candidate up to the end of the reply window, not only those in it:
  // a candidate inside the offset that no longer matches must not take up a
  // place, as a shard's own revalidation would have kept it from doing.
  const size_t window_end = WindowEnd();
  for (size_t i = 0; i < window_end; ++i) {
    auto &candidate = candidates_[i];
    if (candidate.neighbor.attribute_contents.has_value()) {
      continue;
    }
    auto &fetch = fetch_of_shard[candidate.shard];
    if (fetch == SIZE_MAX) {
      fetch = pending_fetches_.size();
      auto request = std::make_unique<coordinator::FetchContentRequest>();
      auto *query = request->mutable_query();
      query->CopyFrom(*request_);
      query->clear_content_limit();
      query->set_timeout_ms(static_cast<uint64_t>(remaining_ms));
      query->set_slot_fingerprint(targets_[candidate.shard].slot_fingerprint);
      pending_fetches_.push_back({candidate.shard, std::move(request)});
    }
    const absl::string_view key = candidate.neighbor.external_id->Str();
    auto *entry = pending_fetches_[fetch].request->add_candidates();
    entry->set_key(key);
    entry->set_score(candidate.neighbor.score);
    entry->set_distance(candidate.neighbor.distance);
    entry->set_sequence_number(candidate.neighbor.sequence_number);
    entry->set_content_omitted(true);
    fetched_candidate_.try_emplace(key, i);
    candidate.requested = true;
    ++missing;
  }
  auto &stats = Metrics::GetStats();
  if (!refetched_) {
    if (pending_fetches_.empty()) {
      ++stats.bounded_content_single_round_queries;
    } else {
      ++stats.bounded_content_refetch_queries;
      refetched_ = true;
    }
  }
  stats.bounded_content_missing_winners += missing;
  stats.bounded_content_fetch_requests += pending_fetches_.size();
  return pending_fetches_.size();
}

void BoundedContentFanout::ContinueRefetch() {
  size_t fetch_count = 0;
  {
    absl::MutexLock lock(&mutex_);
    fetch_count = PlanFetchRound();
    outstanding_ = fetch_count;
  }
  if (fetch_count == 0) {
    Complete();
    return;
  }
  const bool async = DispatchAsync(fetch_count);
  for (size_t fetch = 0; fetch < fetch_count; ++fetch) {
    // A pool that refuses the task (stopping) must not lose the request.
    if (!async || !thread_pool_->Schedule([self = shared_from_this(),
                                           fetch] { self->SendFetch(fetch); },
                                          vmsdk::ThreadPool::Priority::kHigh)) {
      SendFetch(fetch);
    }
  }
}

void BoundedContentFanout::SendFetch(size_t fetch) {
  // Take what this send needs before sending: the reply may end the round,
  // and the next round rebuilds pending_fetches_.
  const size_t shard = pending_fetches_[fetch].shard;
  auto request = std::move(pending_fetches_[fetch].request);
  if (targets_[shard].is_local) {
    // Always deferred to the main thread's event loop, never run inline, so
    // it cannot end the round while this round is still being dispatched.
    vmsdk::RunByMain(
        [self = shared_from_this(), shard,
         request = std::move(request)]() mutable {
          self->FetchLocally(shard, std::move(request));
        },
        /*force_async=*/true);
    return;
  }
  client_pool_->GetClient(targets_[shard].address)
      ->FetchContent(std::move(request),
                     [self = shared_from_this(), shard](
                         grpc::Status status,
                         coordinator::SearchIndexPartitionResponse &response) {
                       self->OnFetchReply(shard, status, response);
                     });
}

void BoundedContentFanout::OnFetchReply(
    size_t shard, const grpc::Status &status,
    coordinator::SearchIndexPartitionResponse &response) {
  std::vector<indexes::Neighbor> fetched;
  if (status.ok()) {
    fetched.reserve(response.neighbors_size());
    for (auto &entry : *response.mutable_neighbors()) {
      fetched.push_back(DecodeNeighborEntry(entry));
    }
  }
  if (RecordFetchResult(shard, ToAbslStatus(status), std::move(fetched),
                        nullptr)) {
    ContinueRefetch();
  }
}

void BoundedContentFanout::FetchLocally(
    size_t shard, std::unique_ptr<coordinator::FetchContentRequest> request) {
  auto fetch = std::make_unique<LocalContentFetch>(shared_from_this(), shard);
  auto status = coordinator::GRPCSearchRequestToParameters(
      request->query(), nullptr, fetch.get());
  // Unlike the local search, the fetch runs a round trip later: it must pass
  // the same checks as the remote fetch.
  if (status.ok()) {
    status = coordinator::CheckRequestConsistency(request->query(),
                                                  *fetch->index_schema);
  }
  if (!status.ok()) {
    if (RecordFetchResult(shard, status, {}, nullptr)) {
      ContinueRefetch();
    }
    return;
  }
  SeedContentFetch(*fetch, DecodeNeighborEntries(request->candidates()));
  ResolveContent(std::move(fetch));
}

void BoundedContentFanout::OnLocalFetchDone(
    size_t shard, std::unique_ptr<SearchParameters> local) {
  absl::Status status = local->search_result.status;
  // A cancelled fetch returns its candidates unresolved, which would read as
  // no longer matching. Fail it instead, as the remote fetch does.
  if (status.ok() && local->cancellation_token->IsCancelled()) {
    status = absl::DeadlineExceededError(kTimeoutMsg);
  }
  auto fetched = std::move(local->search_result.neighbors);
  if (RecordFetchResult(shard, status, std::move(fetched), std::move(local))) {
    ContinueRefetch();
  }
}

bool BoundedContentFanout::RecordFetchResult(
    size_t shard, const absl::Status &status,
    std::vector<indexes::Neighbor> fetched,
    std::unique_ptr<SearchParameters> local) {
  absl::MutexLock lock(&mutex_);
  KeepAlive(std::move(local));
  if (!status.ok()) {
    ++Metrics::GetStats().bounded_content_fetch_failures;
    failed_shards_[shard] = true;
    if (outcomes_.RecordFailure(status)) {
      cancellation_token_->Cancel();
    }
  }
  for (auto &neighbor : fetched) {
    auto it = fetched_candidate_.find(neighbor.external_id->Str());
    if (it == fetched_candidate_.end()) {
      continue;
    }
    auto &candidate = candidates_[it->second].neighbor;
    // The shard recomputes a changed document's score or distance; keep it,
    // as a shard's own revalidation would, and re-rank before the next round.
    ranking_stale_ |= std::bit_cast<uint32_t>(candidate.score) !=
                          std::bit_cast<uint32_t>(neighbor.score) ||
                      std::bit_cast<uint32_t>(candidate.distance) !=
                          std::bit_cast<uint32_t>(neighbor.distance);
    candidate.score = neighbor.score;
    candidate.distance = neighbor.distance;
    candidate.has_vr_distance = neighbor.has_vr_distance;
    candidate.attribute_contents = std::move(neighbor.attribute_contents);
  }
  CHECK_GT(outstanding_, 0u);
  return --outstanding_ == 0;
}

void BoundedContentFanout::KeepAlive(std::unique_ptr<SearchParameters> local) {
  if (local == nullptr) {
    return;
  }
  local->local_responder_ = std::move(local_search_);
  local_search_ = std::move(local);
}

void BoundedContentFanout::DropKeyOnlyCandidates() {
  // Every candidate in the window has content now. Candidates past it are
  // kept as backfill for winners the reply path might still drop; one
  // without content cannot serve as that, and the reply path would count it
  // as a removed result and lower total_count. A cancelled query stops
  // fetching, so its window may still hold candidates without content: they
  // are left out of the partial reply, but they matched, so they stay
  // counted.
  const size_t first = cancellation_token_->IsCancelled() ? 0 : WindowEnd();
  candidates_.erase(
      std::remove_if(candidates_.begin() + first, candidates_.end(),
                     [](const Candidate &candidate) {
                       return !candidate.neighbor.attribute_contents;
                     }),
      candidates_.end());
}

void BoundedContentFanout::Complete() {
  std::unique_ptr<SearchParameters> parameters;
  {
    absl::MutexLock lock(&mutex_);
    const absl::Status status = outcomes_.Resolve();
    if (status.ok()) {
      candidates_.erase(candidates_.begin() + EligibleCount(),
                        candidates_.end());
      DropKeyOnlyCandidates();
      std::vector<indexes::Neighbor> neighbors;
      neighbors.reserve(candidates_.size());
      for (auto &candidate : candidates_) {
        neighbors.push_back(std::move(candidate.neighbor));
      }
      candidates_.clear();
      // Already ranked; applies the offset and the result-buffer trim.
      parameters_->search_result =
          SearchResult(total_count_, std::move(neighbors), *parameters_,
                       /*trim_offset_in_background=*/true);
    }
    parameters_->search_result.status = status;
    parameters_->local_responder_ = std::move(local_search_);
    parameters = std::move(parameters_);
  }
  CompleteQuery(std::move(parameters));
}

}  // namespace

std::optional<BoundedContentPlan> PlanBoundedContentFanout(
    const SearchParameters &parameters,
    const std::vector<vmsdk::cluster_map::NodeInfo> &targets) {
  if (!options::GetFanoutBoundedContent().GetValue()) {
    return std::nullopt;
  }
  // A shard keeps at most this many results for content resolution, so a
  // larger bound saves nothing; it would only grow the estimator's work.
  if (FanoutContentPlanner::CandidateLimit(parameters) >
      options::GetMaxNonVectorSearchResultsFetched().GetValue()) {
    return std::nullopt;
  }
  const RefetchRateEstimator estimator(options::GetFanoutContentRefetchRatio());
  return FanoutContentPlanner(estimator).Plan(parameters, targets);
}

absl::Status PerformBoundedContentFanoutAsync(
    const std::vector<vmsdk::cluster_map::NodeInfo> &targets,
    coordinator::ClientPool *client_pool,
    std::unique_ptr<SearchParameters> parameters, BoundedContentPlan plan,
    vmsdk::ThreadPool *thread_pool) {
  CHECK_EQ(plan.content_limits.size(), targets.size());
  ++Metrics::GetStats().bounded_content_queries;
  auto request = coordinator::ParametersToGRPCSearchRequest(*parameters);
  request->mutable_limit()->set_first_index(0);
  request->mutable_limit()->set_number(plan.candidate_limit);
  std::vector<ShardTarget> shard_targets;
  shard_targets.reserve(targets.size());
  for (size_t i = 0; i < targets.size(); ++i) {
    const auto &node = targets[i];
    shard_targets.push_back(ShardTarget{
        coordinator::FormatAddressWithPort(
            node.socket_address.primary_endpoint,
            coordinator::GetCoordinatorPort(node.socket_address.port)),
        TargetSlotFingerprint(node, request->slot_fingerprint()),
        plan.content_limits[i], node.is_local});
  }
  std::make_shared<BoundedContentFanout>(
      std::move(parameters), std::move(request), std::move(shard_targets),
      client_pool, thread_pool)
      ->Start();
  return absl::OkStatus();
}

}  // namespace valkey_search::query::fanout
