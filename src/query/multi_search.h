/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_MULTI_SEARCH_H_
#define VALKEYSEARCH_SRC_QUERY_MULTI_SEARCH_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/index_schema.h"
#include "src/query/search.h"
#include "src/utils/cancel.h"
#include "vmsdk/src/blocked_client.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/thread_pool.h"

// Forward declarations to avoid circular include with ft_aggregate_parser.h.
namespace valkey_search::aggregate {
struct AggregateParameters;
}  // namespace valkey_search::aggregate

namespace valkey_search::query {

// Configuration for the COMBINE fusion stage (RRF or LINEAR). Populated by the
// FT.HYBRID parser; consumed by `query::fusion::FuseRRF` / `FuseLinear` in
// Phase 4.
struct FusionConfig {
  enum class Method { kRRF, kLinear };
  Method method = Method::kRRF;
  uint32_t rrf_constant = 60;
  uint32_t window = 20;
  std::optional<double> alpha;  // required when method == kLinear
  std::optional<double> beta;   // required when method == kLinear
};

class MultiSearchTracker;

// A SearchParameters subclass used as the per-arm operation in multi-arm
// search. Carries a back-reference to the meta-tracker so that, on per-arm
// completion, results are reported into the tracker instead of unblocking a
// client directly.
class MultiArmShim : public SearchParameters {
 public:
  std::shared_ptr<MultiSearchTracker> tracker;
  size_t arm_index{0};
  void QueryCompleteBackground(std::unique_ptr<SearchParameters> self) override;
  void QueryCompleteMainThread(std::unique_ptr<SearchParameters> self) override;
};

// Canonical multi-arm command parameters. Holds the dispatch envelope, the
// per-arm SearchParameters list, the fusion config, the embedded aggregate
// pipeline parameters (populated by the FT.HYBRID parser in Phase 4), the
// per-arm raw results (populated by the tracker), and the fused SearchResult.
//
// FT.HYBRID is the only command that uses MultiSearchParameters in V1; the N=2
// shape is concrete (arms[0]=SEARCH, arms[1]=VSIM) but the underlying machinery
// generalizes to N arms.
struct MultiSearchParameters {
  // ----- envelope (shared dispatch state) -----
  uint32_t db_num{0};
  std::shared_ptr<IndexSchema> index_schema;
  std::string index_schema_name;
  uint64_t timeout_ms{0};
  bool enable_partial_results{false};
  bool enable_consistency{false};
  bool local_only{false};
  coordinator::IndexFingerprintVersion index_fingerprint_version;
  uint64_t slot_fingerprint{0};
  cancel::Token cancellation_token;
  std::optional<vmsdk::BlockedClient> blocked_client;
  vmsdk::UniqueValkeyString score_as;  // user-visible field name for the
                                       // fused score (COMBINE YIELD_SCORE_AS)

  // ----- arms (each is a MultiArmShim, which IS-A SearchParameters; the shim
  //       carries the tracker back-pointer set at dispatch time. For FT.HYBRID
  //       V1, size()==2) -----
  std::vector<std::unique_ptr<MultiArmShim>> arms;
  std::vector<std::optional<std::string>> per_arm_score_alias;

  // ----- fusion + post-pipeline -----
  FusionConfig fusion;
  // Aggregate post-pipeline state (populated by the FT.HYBRID parser in Phase
  // 4). Fed the fused neighbor list via aggregate::RunAggregatePipeline.
  std::unique_ptr<aggregate::AggregateParameters> agg;

  // ----- runtime state -----
  std::vector<SearchResult> per_arm_results;  // populated by MultiSearchTracker
  SearchResult search_result;                  // fused result

  // Invoked exactly once from MultiSearchTracker::Finalize after all arms
  // complete. Owns *this and is responsible for fusing per_arm_results,
  // running the aggregate pipeline, and unblocking the client. For Phase 3
  // testing this signals a latch; for Phase 4 production it runs FuseAndReply.
  absl::AnyInvocable<void(std::unique_ptr<MultiSearchParameters>) &&>
      on_all_arms_complete;

  // Out-of-line so the implicit destructor of `agg` (a unique_ptr to the
  // forward-declared AggregateParameters) doesn't need to be visible at every
  // include site of this header.
  virtual ~MultiSearchParameters();

 protected:
  // Constructor is protected so callers go through the Make() factory below;
  // the factory's body lives in multi_search.cc where AggregateParameters is
  // complete and the implicit destructor can be instantiated safely.
  MultiSearchParameters();
  friend std::unique_ptr<MultiSearchParameters> MakeMultiSearchParameters();
};

// Factory for MultiSearchParameters. Callers should use this rather than
// std::make_unique<MultiSearchParameters>() directly so that the implicit
// destructor of the embedded `agg` member can be instantiated in
// multi_search.cc (where AggregateParameters is complete).
std::unique_ptr<MultiSearchParameters> MakeMultiSearchParameters();

// Coordinates per-arm completion. Holds the MultiSearchParameters until all
// arms have reported, then invokes parameters->on_all_arms_complete.
class MultiSearchTracker
    : public std::enable_shared_from_this<MultiSearchTracker> {
 public:
  explicit MultiSearchTracker(std::unique_ptr<MultiSearchParameters> params);

  // Called by MultiArmShim on per-arm completion. Stashes the per-arm result
  // into parameters->per_arm_results[arm_index], retains the per-arm
  // SearchParameters in arm_owners_ (mirrors local_responder_ retention), and
  // decrements the outstanding count. When the count hits zero, calls
  // Finalize().
  void OnArmComplete(size_t arm_index, SearchResult&& result,
                     std::unique_ptr<SearchParameters> arm_self);

 private:
  void Finalize();

  absl::Mutex mu_;
  std::unique_ptr<MultiSearchParameters> parameters_ ABSL_GUARDED_BY(mu_);
  size_t outstanding_ ABSL_GUARDED_BY(mu_);
  std::vector<std::unique_ptr<SearchParameters>> arm_owners_
      ABSL_GUARDED_BY(mu_);
  std::atomic_bool any_arm_failed_{false};
  absl::Status first_error_ ABSL_GUARDED_BY(mu_);
};

// Schedules each arm onto the reader thread pool. Each arm carries a
// MultiArmShim that, on completion, reports into a freshly-constructed
// MultiSearchTracker. When the last arm reports, the tracker invokes
// parameters->on_all_arms_complete.
absl::Status PerformMultiSearchLocalAsync(
    std::unique_ptr<MultiSearchParameters> parameters,
    vmsdk::ThreadPool* reader_pool);

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_MULTI_SEARCH_H_
