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
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/index_schema.h"
#include "src/query/search.h"
#include "src/utils/cancel.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/blocked_client.h"
#include "vmsdk/src/cluster_map.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/thread_pool.h"

// Forward declarations to avoid circular include with ft_aggregate_parser.h.
namespace valkey_search::aggregate {
struct AggregateParameters;
}  // namespace valkey_search::aggregate
namespace valkey_search::expr {
class Expression;
}  // namespace valkey_search::expr

namespace valkey_search::query {

// The column FT.HYBRID puts the fused score in when the caller did not name
// one with COMBINE ... YIELD_SCORE_AS.
inline constexpr absl::string_view kDefaultOutputScoreName{"__score"};

// FT.HYBRID refuses the synchronous local execution path. Both of its
// triggers -- MULTI/EXEC or Lua, and a disabled reader thread pool -- are
// reported with this message; see MultiSearchParameters::ExecuteSyncLocal.
inline constexpr absl::string_view kSyncLocalUnsupportedMsg{
    "FT.HYBRID is not supported inside MULTI/EXEC or a Lua script, or when "
    "the reader thread pool is disabled. Both force synchronous execution, on "
    "which the per-arm results cannot be revalidated against concurrent "
    "mutations before they are fused."};

// Configuration for the COMBINE fusion stage. Populated by the FT.HYBRID
// parser; consumed by `query::rank_fusion::{RRF,Linear,Function}` in
// ft_hybrid.cc.
//   kRRF      — reciprocal rank fusion (default).
//   kLinear   — weighted linear combination of per-arm normalized scores.
//   kFunction — user-defined expression over the per-arm scores (the compiled
//               expression lives on MultiSearchParameters::combine_function).
struct FusionConfig {
  enum class Method { kRRF, kLinear, kFunction };
  Method method = Method::kRRF;
  // Fractional, to match the reference: see rank_fusion.h.
  double rrf_constant = 60.0;
  // Number of each arm's results that take part in fusion. The FT.HYBRID
  // parser never leaves this at 0: an explicit `WINDOW 0`, and COMBINE
  // FUNCTION's implicit default, both resolve to max-combine-window. 20 is the
  // RRF/LINEAR default, matching the reference.
  uint32_t window = 20;
  // Set by the FT.HYBRID parser whenever method == kLinear: to what the
  // caller wrote, or to 0.3/0.7 when the caller wrote neither. Writing only
  // one is an error, so they are never half-populated.
  std::optional<double> alpha;
  std::optional<double> beta;
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
  // Seeded from configuration, exactly as SearchParameters does. Hardcoding
  // these left both settings inert for FT.HYBRID: a deployment that asked for
  // partial results got an error from one failing shard, and one that asked
  // for consistent results had the slot check skipped, because the envelope's
  // `false` is what reaches every shard.
  bool enable_partial_results{options::GetPreferPartialResults().GetValue()};
  bool enable_consistency{options::GetPreferConsistentResults().GetValue()};
  bool local_only{false};
  coordinator::IndexFingerprintVersion index_fingerprint_version;
  uint64_t slot_fingerprint{0};
  cancel::Token cancellation_token;
  std::optional<vmsdk::BlockedClient> blocked_client;
  // Name the fused score is generated under. `__score` unless COMBINE ...
  // YIELD_SCORE_AS renamed it. The column always exists so that the
  // post-fusion pipeline can sort, filter and apply on it; whether it reaches
  // the reply depends on `output_score_name_explicit` and the LOAD clause,
  // which the FT.HYBRID parser settles once the whole command is read.
  std::string output_score_name{kDefaultOutputScoreName};
  // True when COMBINE ... YIELD_SCORE_AS named the score. An explicitly named
  // score is always in the reply; the default name only survives when the
  // caller gave no LOAD clause, because any LOAD replaces the default
  // projection.
  bool output_score_name_explicit{false};

  // ----- arms (each is a MultiArmShim, which IS-A SearchParameters; the shim
  //       carries the tracker back-pointer set at dispatch time. For FT.HYBRID
  //       V1, size()==2) -----
  std::vector<std::unique_ptr<MultiArmShim>> arms;
  std::vector<std::optional<std::string>> per_arm_score_alias;
  // True for each arm whose Neighbor::score is a raw vector distance rather
  // than a relevance score -- a pure vector arm. Such an arm's score is
  // converted to a similarity before fusion so that every arm is
  // higher-is-better; see ConvertVectorArmScoresToSimilarity in ft_hybrid.cc.
  // Recorded at parse time because `arms` is emptied at dispatch (each
  // MultiArmShim is moved into SearchAsync), so it cannot be re-derived later.
  std::vector<bool> per_arm_score_is_distance;
  // The metric each distance came from, captured for the same reason:
  // turning a distance into a similarity depends on it, and by fusion
  // time the arm is gone. UNSPECIFIED for an arm carrying a relevance
  // score rather than a distance.
  std::vector<data_model::DistanceMetric> per_arm_distance_metric;

  // ----- fusion + post-pipeline -----
  FusionConfig fusion;
  // Compiled COMBINE FUNCTION expression (null unless fusion.method ==
  // kFunction). Compiled by the FT.HYBRID parser against the per-arm score
  // aliases; evaluated per fused document in BuildFusedNeighbors.
  std::unique_ptr<expr::Expression> combine_function;
  // Aggregate post-pipeline state (populated by the FT.HYBRID parser in Phase
  // 4). Fed the fused neighbor list via aggregate::RunAggregatePipeline.
  std::unique_ptr<aggregate::AggregateParameters> agg;

  // ----- runtime state -----
  std::vector<SearchResult> per_arm_results;  // populated by MultiSearchTracker
  SearchResult search_result;                 // fused result
  // Keeps per-arm SearchParameters (and, transitively, their local-responder
  // chains) alive until the reply is sent. The per-arm Neighbor entries may
  // hold string_view keys that point into these objects, so they must outlive
  // FuseAndReply. MultiSearchTracker::Finalize moves its arm_owners_ here
  // before handing the envelope to on_all_arms_complete.
  std::vector<std::unique_ptr<SearchParameters>> retained_arm_owners;

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

  // ----- Hooks consumed by ExecuteCommand<MultiSearchParameters> -----
  static absl::Status ParseAfterIndex(MultiSearchParameters &cmd,
                                      vmsdk::ArgsIterator &itr);
  static absl::Status ExecuteSyncLocal(
      ValkeyModuleCtx *ctx, std::unique_ptr<MultiSearchParameters> cmd);
  static absl::Status DispatchLocalAsync(
      ValkeyModuleCtx *ctx, std::unique_ptr<MultiSearchParameters> cmd,
      vmsdk::ThreadPool *pool);
  static absl::Status DispatchFanoutAsync(
      ValkeyModuleCtx *ctx, std::unique_ptr<MultiSearchParameters> cmd,
      std::vector<vmsdk::cluster_map::NodeInfo> &search_targets,
      coordinator::ClientPool *client_pool, vmsdk::ThreadPool *pool);

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

// Parses everything after `FT.HYBRID <index>`: SEARCH ... VSIM ... [COMBINE
// ...] ... aggregate-suffix. Defined in src/commands/ft_hybrid_parser.cc to
// avoid pulling the FT.HYBRID grammar into the query library.
absl::Status ParseFtHybridCommand(MultiSearchParameters &params,
                                  vmsdk::ArgsIterator &itr);

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
  // `consistency_failed` reports that this arm's result came from a cluster
  // map the shard no longer trusts; it fails the whole query in Finalize
  // regardless of enable_partial_results.
  void OnArmComplete(size_t arm_index, SearchResult &&result,
                     std::unique_ptr<SearchParameters> arm_self,
                     bool consistency_failed = false);

 private:
  void Finalize();

  absl::Mutex mu_;
  std::unique_ptr<MultiSearchParameters> parameters_ ABSL_GUARDED_BY(mu_);
  size_t outstanding_ ABSL_GUARDED_BY(mu_);
  std::vector<std::unique_ptr<SearchParameters>> arm_owners_
      ABSL_GUARDED_BY(mu_);
  std::atomic_bool any_arm_failed_{false};
  std::atomic_bool consistency_failed_{false};
  absl::Status first_error_ ABSL_GUARDED_BY(mu_);
};

// Schedules each arm onto the reader thread pool. Each arm carries a
// MultiArmShim that, on completion, reports into a freshly-constructed
// MultiSearchTracker. When the last arm reports, the tracker invokes
// parameters->on_all_arms_complete.
absl::Status PerformMultiSearchLocalAsync(
    std::unique_ptr<MultiSearchParameters> parameters,
    vmsdk::ThreadPool *reader_pool);

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_MULTI_SEARCH_H_
