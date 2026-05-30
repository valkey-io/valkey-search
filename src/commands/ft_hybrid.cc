/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "src/attribute_data_type.h"
#include "src/commands/commands.h"
#include "src/commands/ft_aggregate_exec.h"
#include "src/commands/ft_aggregate_parser.h"
#include "src/commands/ft_hybrid_combine.h"
#include "src/coordinator/client_pool.h"
#include "src/expr/expr.h"
#include "src/expr/value.h"
#include "src/metrics.h"
#include "src/query/content_resolution.h"
#include "src/query/fanout.h"
#include "src/query/fusion.h"
#include "src/query/multi_search.h"
#include "src/query/response_generator.h"
#include "src/query/search.h"
#include "src/utils/cancel.h"
#include "src/valkey_search.h"
#include "vmsdk/src/blocked_client.h"
#include "vmsdk/src/cluster_map.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace query {

// Fuses the per-arm results into a single neighbor list using the configured
// COMBINE method. The fused score lives in Neighbor::distance and per-arm
// score aliases are injected into attribute_contents (see fusion.cc).
std::vector<indexes::Neighbor> BuildFusedNeighbors(
    MultiSearchParameters &params) {
  std::vector<fusion::ArmInput> arm_inputs;
  arm_inputs.reserve(params.arms.size());
  for (size_t i = 0; i < params.arms.size(); ++i) {
    fusion::ArmInput in;
    in.neighbors = &params.per_arm_results[i].neighbors;
    if (i < params.per_arm_score_alias.size()) {
      in.score_alias = params.per_arm_score_alias[i];
    }
    in.rrf_constant = params.fusion.rrf_constant;
    in.window = params.fusion.window;
    if (params.fusion.method == FusionConfig::Method::kLinear) {
      in.weight =
          (i == 0)
              ? (params.fusion.alpha.has_value() ? *params.fusion.alpha : 0.5)
              : (params.fusion.beta.has_value() ? *params.fusion.beta : 0.5);
    }
    arm_inputs.push_back(std::move(in));
  }
  std::vector<indexes::Neighbor> fused;
  if (params.fusion.method == FusionConfig::Method::kRRF) {
    fused = fusion::FuseRRF(std::move(arm_inputs));
  } else if (params.fusion.method == FusionConfig::Method::kLinear) {
    fused = fusion::FuseLinear(std::move(arm_inputs));
  } else {
    // COMBINE FUNCTION: evaluate the user expression per document, binding each
    // arm's raw score to its reference. Absent-from-arm scores bind to Nil.
    expr::Expression *fn = params.combine_function.get();
    CHECK(fn != nullptr) << "kFunction fusion without a compiled expression";
    fused = fusion::FuseFunction(
        std::move(arm_inputs),
        [fn](const std::vector<std::optional<double>> &per_arm) -> double {
          ArmScoreRecord rec;
          rec.scores.reserve(per_arm.size());
          for (const auto &s : per_arm) {
            rec.scores.push_back(s.has_value() ? expr::Value(*s)
                                               : expr::Value());
          }
          expr::Expression::EvalContext eval_ctx;
          expr::Value v = fn->Evaluate(eval_ctx, rec);
          auto d = v.AsDouble();
          return d.has_value() ? *d : 0.0;
        });
  }
  // The COMBINE WINDOW bounds the per-arm contribution AND the final result.
  // Per-arm truncation already happened inside fusion; cap the final list too.
  if (params.fusion.window > 0 && fused.size() > params.fusion.window) {
    fused.resize(params.fusion.window);
  }
  return fused;
}

// Runs the aggregate post-pipeline over the (already content-populated) fused
// neighbors in params.search_result.neighbors and writes the reply. The
// content fetch / mutation validation has already happened atomically before
// this point (see the fused-resolution path below), so the aggregate's own
// ProcessNeighborsForReply finds content present and does not re-fetch.
void RunAggregateReply(ValkeyModuleCtx *ctx, MultiSearchParameters &params) {
  if (params.agg == nullptr) {
    ValkeyModule_ReplyWithArray(ctx, 1);
    ValkeyModule_ReplyWithLongLong(ctx, 0);
    return;
  }
  params.agg->cancellation_token = params.cancellation_token;
  params.agg->index_schema = params.index_schema;
  auto status = aggregate::RunAggregatePipeline(
      ctx, params.search_result.neighbors, *params.agg);
  if (!status.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    ValkeyModule_ReplyWithError(ctx, status.message().data());
  }
}

// Moves each fused neighbor's attribute_contents (the per-arm score aliases
// injected by fusion) into a side map keyed by external id, leaving the
// neighbor's attribute_contents empty. This lets the subsequent content fetch
// (ProcessNeighborsForReply, which skips neighbors that already have content)
// actually populate the database fields. RestoreAliases re-merges them.
absl::flat_hash_map<std::string, RecordsMap> SaveAndClearAliases(
    std::vector<indexes::Neighbor> &neighbors) {
  absl::flat_hash_map<std::string, RecordsMap> saved;
  for (auto &n : neighbors) {
    if (n.attribute_contents.has_value()) {
      saved.emplace(std::string(n.external_id->Str()),
                    std::move(*n.attribute_contents));
      n.attribute_contents.reset();
    }
  }
  return saved;
}

void RestoreAliases(std::vector<indexes::Neighbor> &neighbors,
                    absl::flat_hash_map<std::string, RecordsMap> &saved) {
  for (auto &n : neighbors) {
    auto it = saved.find(n.external_id->Str());
    if (it == saved.end()) {
      continue;
    }
    if (!n.attribute_contents.has_value()) {
      n.attribute_contents.emplace();
    }
    for (auto &[k, v] : it->second) {
      n.attribute_contents->emplace(k, std::move(v));
    }
  }
}

// SearchParameters used to drive the SINGLE, atomic, post-fusion content
// resolution for a local FT.HYBRID. ResolveContent runs the mutation/contention
// check and fetches the database fields for the whole fused neighbor list at
// once (on the main thread). On completion it re-merges the per-arm score
// aliases and unblocks the client, whose reply callback runs the aggregate
// pipeline. This is what makes the multi-arm results "come together before the
// final main-thread validation".
class FusedResolver : public query::SearchParameters {
 public:
  std::unique_ptr<MultiSearchParameters> envelope;
  absl::flat_hash_map<std::string, RecordsMap> saved_aliases;
  // FT.HYBRID always runs the contention check on the fused result.
  query::ContentProcessing GetContentProcessing() const override {
    return query::kContentionCheckRequired;
  }
  void QueryCompleteMainThread(
      std::unique_ptr<query::SearchParameters> self) override {
    DoComplete(std::move(self));
  }
  void QueryCompleteBackground(
      std::unique_ptr<query::SearchParameters> self) override {
    DoComplete(std::move(self));
  }

 private:
  void DoComplete(std::unique_ptr<query::SearchParameters> /*self*/) {
    RestoreAliases(search_result.neighbors, saved_aliases);
    envelope->search_result = std::move(search_result);
    envelope->search_result.status = absl::OkStatus();
    auto *raw = envelope.release();
    raw->blocked_client->SetReplyPrivateData(raw);
    raw->blocked_client->UnblockClient();
  }
};

// Local async completion: all arms have delivered raw (content-free) results.
// Fuse, then run the single atomic content resolution over the fused list.
void FuseThenResolveLocal(std::unique_ptr<MultiSearchParameters> params) {
  auto fused = BuildFusedNeighbors(*params);
  auto resolver = std::make_unique<FusedResolver>();
  resolver->index_schema = params->index_schema;
  resolver->db_num = params->db_num;
  resolver->cancellation_token = params->cancellation_token;
  resolver->enable_partial_results = params->enable_partial_results;
  resolver->saved_aliases = SaveAndClearAliases(fused);
  resolver->search_result.total_count = fused.size();
  resolver->search_result.neighbors = std::move(fused);
  resolver->envelope = std::move(params);
  // ResolveContent performs the contention/mutation check (re-queueing if a
  // mutation is in flight) and then the content fetch, atomically on the main
  // thread, over the whole fused list.
  vmsdk::RunByMain([resolver = std::move(resolver)]() mutable {
    query::ResolveContent(std::move(resolver));
  });
}

// Inline (synchronous, main-thread) variant used by the MULTI/EXEC fast path.
// Already atomic by virtue of running on the main thread with no concurrent
// mutations, so it fetches content directly without the re-queue machinery.
void ResolveFusedContentInline(ValkeyModuleCtx *ctx,
                               MultiSearchParameters &params,
                               std::vector<indexes::Neighbor> &fused) {
  auto saved = SaveAndClearAliases(fused);
  FusedResolver resolver;
  resolver.index_schema = params.index_schema;
  resolver.db_num = params.db_num;
  query::ProcessNeighborsForReply(ctx,
                                  params.index_schema->GetAttributeDataType(),
                                  fused, resolver, std::nullopt);
  RestoreAliases(fused, saved);
}

}  // namespace query

namespace async {

// MultiSearchParameters-flavored async callbacks. Mirror QueryCommand's
// async::Reply / Timeout / Free triplet but cast the privdata to
// MultiSearchParameters*.
int ReplyMulti(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
  auto *params = static_cast<query::MultiSearchParameters *>(
      ValkeyModule_GetBlockedClientPrivateData(ctx));
  CHECK(params != nullptr);
  if (!params->search_result.status.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    return ValkeyModule_ReplyWithError(
        ctx, params->search_result.status.message().data());
  }
  if (!params->enable_partial_results && params->cancellation_token &&
      params->cancellation_token->IsCancelled()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    return ValkeyModule_ReplyWithError(
        ctx, "Search operation cancelled due to timeout");
  }
  // By the time we reach the reply callback the fused neighbors have already
  // been content-resolved (local: atomic post-fusion ResolveContent; cluster:
  // per-shard fetch). Run only the aggregate transforms + serialization here.
  query::RunAggregateReply(ctx, *params);
  return VALKEYMODULE_OK;
}

int TimeoutMulti(ValkeyModuleCtx *ctx,
                 [[maybe_unused]] ValkeyModuleString **argv,
                 [[maybe_unused]] int argc) {
  return ValkeyModule_ReplyWithError(
      ctx, "Search operation cancelled due to timeout");
}

void FreeMulti([[maybe_unused]] ValkeyModuleCtx *ctx, void *privdata) {
  auto *params = static_cast<query::MultiSearchParameters *>(privdata);
  // Drop schema/agg references on the main thread (consistent with
  // QueryCommand::async::Free).
  params->index_schema = nullptr;
  params->agg.reset();
  ValkeySearch::Instance().ScheduleSearchResultCleanup(
      [params]() { delete params; });
}

}  // namespace async

namespace query {

absl::Status MultiSearchParameters::ParseAfterIndex(MultiSearchParameters &cmd,
                                                    vmsdk::ArgsIterator &itr) {
  return ParseFtHybridCommand(cmd, itr);
}

// Synchronous-fast-path for inside MULTI/EXEC or no-parallel-queries mode.
// Runs each arm inline on the main thread (no per-arm content resolution),
// fuses, then resolves content for the fused list once — all on the main
// thread, so it is inherently atomic — then sends the reply.
absl::Status MultiSearchParameters::ExecuteSyncLocal(
    ValkeyModuleCtx *ctx, std::unique_ptr<MultiSearchParameters> cmd) {
  for (auto &arm : cmd->arms) {
    // Run the index search only; defer the database content fetch until after
    // fusion so the multi-arm result is validated as a unit.
    arm->no_content = true;
    auto s = query::Search(*arm, query::SearchMode::kLocal);
    if (!s.ok()) {
      ValkeyModule_ReplyWithError(ctx, s.message().data());
      ++Metrics::GetStats().query_failed_requests_cnt;
      return absl::OkStatus();
    }
    cmd->per_arm_results.push_back(std::move(arm->search_result));
  }
  if (!cmd->enable_partial_results && cmd->cancellation_token &&
      cmd->cancellation_token->IsCancelled()) {
    ValkeyModule_ReplyWithError(ctx,
                                "Search operation cancelled due to timeout");
    ++Metrics::GetStats().query_failed_requests_cnt;
    return absl::OkStatus();
  }
  auto fused = query::BuildFusedNeighbors(*cmd);
  query::ResolveFusedContentInline(ctx, *cmd, fused);
  cmd->search_result.neighbors = std::move(fused);
  query::RunAggregateReply(ctx, *cmd);
  return absl::OkStatus();
}

absl::Status MultiSearchParameters::DispatchLocalAsync(
    ValkeyModuleCtx *ctx, std::unique_ptr<MultiSearchParameters> cmd,
    vmsdk::ThreadPool *pool) {
  cmd->blocked_client =
      vmsdk::BlockedClient(ctx, async::ReplyMulti, async::TimeoutMulti,
                           async::FreeMulti, cmd->timeout_ms);
  cmd->blocked_client->MeasureTimeStart();
  // When the meta-tracker finishes assembling per_arm_results, fuse the arms
  // and run a SINGLE atomic content resolution (mutation check + populate)
  // over the fused list before unblocking the client. This is the requirement
  // that the multi-arms come together before the final main-thread validation.
  cmd->on_all_arms_complete =
      [](std::unique_ptr<MultiSearchParameters> p) mutable {
        FuseThenResolveLocal(std::move(p));
      };
  return PerformMultiSearchLocalAsync(std::move(cmd), pool);
}

absl::Status MultiSearchParameters::DispatchFanoutAsync(
    ValkeyModuleCtx *ctx, std::unique_ptr<MultiSearchParameters> cmd,
    std::vector<vmsdk::cluster_map::NodeInfo> &search_targets,
    coordinator::ClientPool *client_pool, vmsdk::ThreadPool *pool) {
  cmd->blocked_client =
      vmsdk::BlockedClient(ctx, async::ReplyMulti, async::TimeoutMulti,
                           async::FreeMulti, cmd->timeout_ms);
  cmd->blocked_client->MeasureTimeStart();
  // Cluster path: each shard already performed its own content fetch (the
  // coordinator cannot read keys it does not own). Per-shard atomicity across
  // arms is handled shard-side. Here we only fuse the per-arm results — which
  // already carry their database content — and unblock; the reply callback
  // runs the aggregate transforms.
  cmd->on_all_arms_complete =
      [](std::unique_ptr<MultiSearchParameters> p) mutable {
        p->search_result.neighbors = BuildFusedNeighbors(*p);
        auto *raw = p.release();
        raw->blocked_client->SetReplyPrivateData(raw);
        raw->blocked_client->UnblockClient();
      };
  return fanout::PerformMultiSearchFanoutAsync(ctx, search_targets, client_pool,
                                               std::move(cmd), pool);
}

}  // namespace query

absl::Status FTHybridCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                         int argc) {
  auto params = query::MakeMultiSearchParameters();
  return ExecuteCommand<query::MultiSearchParameters>(ctx, argv, argc,
                                                      std::move(params));
}

}  // namespace valkey_search
