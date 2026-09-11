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
#include "src/query/multi_search.h"
#include "src/query/rank_fusion.h"
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

// Converts a pure vector arm's raw distances into the similarity that fusion
// and the per-arm score alias both report: `1 / (1 + distance)`.
//
// Every other arm already carries a relevance score where higher is better (a
// text arm's BM25 value), so this is what puts all arms on one footing: fusion
// can then read Neighbor::score uniformly, LINEAR can sum the arms directly,
// and a COMBINE FUNCTION expression sees the same number the user sees through
// YIELD_SCORE_AS. It is also the value Redis reports for the VSIM arm.
//
// `distance` is non-negative for the supported metrics, so 1 + distance is
// never zero; the guard is defensive.
void ConvertVectorArmScoresToSimilarity(std::vector<indexes::Neighbor> &ns) {
  for (auto &n : ns) {
    const double d = static_cast<double>(n.distance);
    n.score = static_cast<float>(d > -1.0 ? 1.0 / (1.0 + d) : 0.0);
  }
}

// Fuses the per-arm results into a single neighbor list using the configured
// COMBINE method. The fused score lives in Neighbor::score and per-arm score
// aliases are injected into attribute_contents (see rank_fusion.cc).
std::vector<indexes::Neighbor> BuildFusedNeighbors(
    MultiSearchParameters &params) {
  std::vector<rank_fusion::ArmInput> arm_inputs;
  arm_inputs.reserve(params.arms.size());
  for (size_t i = 0; i < params.arms.size(); ++i) {
    // A pure vector arm reports a raw distance (lower = better) in
    // Neighbor::score; rewrite it to a similarity so every arm handed to
    // fusion is higher-is-better. `per_arm_score_is_distance` was captured at
    // parse time because `params.arms` is empty by now (the shims were moved
    // into SearchAsync at dispatch).
    if (i < params.per_arm_score_is_distance.size() &&
        params.per_arm_score_is_distance[i]) {
      ConvertVectorArmScoresToSimilarity(params.per_arm_results[i].neighbors);
    }
    rank_fusion::ArmInput in;
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
    fused = rank_fusion::RRF(std::move(arm_inputs));
  } else if (params.fusion.method == FusionConfig::Method::kLinear) {
    fused = rank_fusion::Linear(std::move(arm_inputs));
  } else {
    // COMBINE FUNCTION: evaluate the user expression per document, binding each
    // arm's raw score to its reference. Absent-from-arm scores bind to Nil.
    expr::Expression *fn = params.combine_function.get();
    CHECK(fn != nullptr) << "kFunction fusion without a compiled expression";
    fused = rank_fusion::Function(
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
  // COMBINE WINDOW bounds each arm's contribution, and only that: the fused
  // list itself is not capped by it. Truncating here as well would drop
  // documents that both arms ranked inside their window, which is not what
  // WINDOW means -- LIMIT is what bounds the reply.
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
      // A YIELD_SCORE_AS alias may collide with a real database field name that
      // the content fetch already populated. The user explicitly asked for the
      // score under this alias, so it must win — overwrite rather than emplace
      // (which would silently keep the database field and drop the score).
      n.attribute_contents->insert_or_assign(k, std::move(v));
    }
  }
}

// Copies the aggregate's LOAD-derived return attributes onto the parameters
// that drive the fused content fetch. They are the same list the FT.AGGREGATE
// search would have carried; a copy rather than a move because the aggregate
// keeps its own for the reply. ReturnAttribute holds UniqueValkeyStrings, so
// each has to be rebuilt rather than assigned.
// Marks every fused neighbor as content-resolved.
//
// A neighbor carries `attribute_contents` once its content has been resolved,
// and both content passes -- the fused resolver's and the aggregate
// pipeline's -- skip a neighbor that already has it. A neighbor that no arm
// attached a score alias to, and that the fetch skipped or found nothing for,
// has none at all; leaving it unset would send the aggregate pass off to the
// database for it after this command already resolved content once.
void MarkContentResolved(std::vector<indexes::Neighbor> &neighbors) {
  for (auto &n : neighbors) {
    if (!n.attribute_contents.has_value()) {
      n.attribute_contents.emplace();
    }
  }
}

// Whether the resolved LOAD clause asks for any database field at all. `LOAD *`
// asks for all of them and leaves the attribute list empty by convention, so it
// is checked separately.
bool WantsNoDatabaseContent(const aggregate::AggregateParameters &agg) {
  return !agg.loadall_ && agg.return_attributes.empty();
}

std::vector<query::ReturnAttribute> CopyReturnAttributes(
    const std::vector<query::ReturnAttribute> &src) {
  std::vector<query::ReturnAttribute> out;
  out.reserve(src.size());
  for (const auto &a : src) {
    out.push_back(query::ReturnAttribute{
        .identifier = a.identifier
                          ? vmsdk::MakeUniqueValkeyString(
                                vmsdk::ToStringView(a.identifier.get()))
                          : vmsdk::UniqueValkeyString(),
        .attribute_alias = a.attribute_alias ? vmsdk::MakeUniqueValkeyString(
                                                   vmsdk::ToStringView(
                                                       a.attribute_alias.get()))
                                             : vmsdk::UniqueValkeyString(),
        .alias = a.alias ? vmsdk::MakeUniqueValkeyString(
                               vmsdk::ToStringView(a.alias.get()))
                         : vmsdk::UniqueValkeyString(),
    });
  }
  return out;
}

// Reads the database fields a revalidation needs for one key. Mirrors what
// GetContent does before it hands a record to VerifyFilter, minus the
// reply-shaping: this content is read to make a decision, never returned.
absl::StatusOr<RecordsMap> FetchRecordForRevalidation(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    uint32_t db_num, absl::string_view key,
    const absl::flat_hash_set<absl::string_view> &identifiers,
    const std::optional<std::string> &vector_identifier) {
  vmsdk::ValkeySelectDbGuard select_db_guard(ctx, db_num);
  auto key_str = vmsdk::MakeUniqueValkeyString(key);
  // NOEXPIRE for the same reason GetContent uses it: lazy expiry deletion
  // during a read can crash the server.
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
      ctx, key_str.get(), VALKEYMODULE_OPEN_KEY_NOEXPIRE | VALKEYMODULE_READ);
  if (!key_obj) {
    return absl::NotFoundError("Key not found");
  }
  mstime_t expire = ValkeyModule_GetExpire(key_obj.get());
  if (expire != VALKEYMODULE_NO_EXPIRE && expire <= 0) {
    return absl::NotFoundError("Key expired");
  }
  return attribute_data_type.FetchAllRecords(ctx, vector_identifier,
                                             key_obj.get(), key, identifiers);
}

// Brings every arm's own result back in line with the database before the arms
// are merged.
//
// Each arm decided membership and score against the index as it stood while
// the arms ran. A document mutated after that point is described by neither:
// it may no longer match the arm at all, and if it does its score belongs to
// the version that has been overwritten. Fusion reads per-arm ranks as well as
// raw scores -- RRF scores purely by position within each arm -- so a stale
// entry does not just misreport one row, it shifts every row below it. That is
// why this runs before the merge rather than patching the merged list.
//
// Only keys whose mutation sequence number moved are touched. For every other
// neighbor this is one integer compare, and an arm that holds no mutated
// document is handed to fusion exactly as its search produced it -- nothing is
// dropped, rewritten or re-sorted. Re-sorting one on the strength of having
// looked at it would be a bug, not a no-op: an arm's order carries meaning
// fusion reads, and only the arm itself knows how it was ordered.
//
// What it deliberately does not do: recover a document the mutation made newly
// matching, or newly near enough to enter the vector arm's top K. Those were
// never in any arm's list and nothing short of re-running the arms would find
// them. The single-arm path has the same limit.
void RevalidateArmsBeforeFusion(MultiSearchParameters &params) {
  vmsdk::VerifyMainThread();
  if (params.index_schema == nullptr) {
    return;
  }
  // Arm parameters come back in completion order, so they are indexed by the
  // arm_index each one recorded. An arm that failed to dispatch left a null.
  std::vector<query::MultiArmShim *> arms(params.per_arm_results.size(),
                                          nullptr);
  for (auto &owner : params.retained_arm_owners) {
    auto *shim = dynamic_cast<query::MultiArmShim *>(owner.get());
    if (shim != nullptr && shim->arm_index < arms.size()) {
      arms[shim->arm_index] = shim;
    }
  }

  // What the arms' predicates read, plus each vector arm's own field. A JSON
  // fetch returns exactly the identifiers it is given, so an incomplete set
  // here would silently fail the predicates it cannot see.
  absl::flat_hash_set<absl::string_view> identifiers;
  std::vector<std::optional<std::string>> vector_identifier(arms.size(),
                                                            std::nullopt);
  for (size_t i = 0; i < arms.size(); ++i) {
    if (arms[i] == nullptr) {
      continue;
    }
    for (const auto &id : arms[i]->filter_parse_results.filter_identifiers) {
      identifiers.insert(id);
    }
    if (!arms[i]->attribute_alias.empty()) {
      auto id = params.index_schema->GetIdentifier(arms[i]->attribute_alias);
      if (id.ok()) {
        vector_identifier[i] = *id;
        identifiers.insert(*vector_identifier[i]);
      }
    }
  }

  auto ctx = vmsdk::MakeUniqueValkeyThreadSafeContext(nullptr);
  const auto &attribute_data_type = params.index_schema->GetAttributeDataType();
  for (size_t i = 0; i < arms.size(); ++i) {
    auto *arm = arms[i];
    if (arm == nullptr) {
      continue;
    }
    auto &neighbors = params.per_arm_results[i].neighbors;
    if (neighbors.empty()) {
      continue;
    }
    // A pure vector arm carries a raw distance in Neighbor::distance; every
    // other arm carries a relevance score in Neighbor::score. The two are
    // refreshed differently and sort in opposite directions.
    const bool arm_score_is_distance =
        i < params.per_arm_score_is_distance.size() &&
        params.per_arm_score_is_distance[i];
    indexes::VectorBase *vector_index = nullptr;
    if (arm_score_is_distance) {
      auto index = params.index_schema->GetIndex(arm->attribute_alias);
      if (index.ok()) {
        vector_index = dynamic_cast<indexes::VectorBase *>(index->get());
      }
    }
    std::unique_ptr<query::SingleDocumentScorer> document_scorer;
    std::vector<char> drop(neighbors.size(), 0);
    size_t dropped = 0;
    bool rescored = false;

    for (size_t j = 0; j < neighbors.size(); ++j) {
      auto &n = neighbors[j];
      auto db_seq =
          params.index_schema->TryGetDbMutationSequenceNumber(n.external_id);
      if (!db_seq.has_value()) {
        // The index no longer tracks the key at all, which is what a deleting
        // mutation leaves behind. No arm should still be offering it.
        drop[j] = 1;
        ++dropped;
        continue;
      }
      if (*db_seq == n.sequence_number) {
        continue;
      }
      // Read per arm rather than once per key: the two arms ask for the
      // document differently, and a vector arm's fetch fails outright when the
      // vector field is gone, which says nothing about the text arm.
      auto fetched = FetchRecordForRevalidation(
          ctx.get(), attribute_data_type, params.db_num, n.external_id->Str(),
          identifiers, vector_identifier[i]);
      if (!fetched.ok()) {
        // The key is gone, or unreadable. Either way this arm no longer
        // matches it.
        drop[j] = 1;
        ++dropped;
        continue;
      }
      const RecordsMap &records = *fetched;
      auto verification = query::VerifyFilter(
          *arm, records, n, document_scorer,
          /*recompute_score_override=*/!arm_score_is_distance);
      if (!verification.matches) {
        drop[j] = 1;
        ++dropped;
        continue;
      }
      if (arm_score_is_distance) {
        if (vector_index == nullptr || !vector_identifier[i].has_value()) {
          continue;
        }
        auto record = records.find(*vector_identifier[i]);
        if (record == records.end()) {
          drop[j] = 1;
          ++dropped;
          continue;
        }
        auto distance = vector_index->RecomputeDistance(
            vmsdk::ToStringView(record->second.value.get()), arm->query);
        if (distance.ok()) {
          n.distance = *distance;
          n.score = *distance;
          rescored = true;
        }
      } else if (verification.recomputed_score.has_value()) {
        n.score = *verification.recomputed_score;
        rescored = true;
      }
    }

    // An arm none of whose documents was mutated is left exactly as the search
    // produced it: not compacted, not renumbered, and not re-sorted. Both
    // blocks below are reachable only from the branches above that actually
    // rewrote something.
    if (dropped > 0) {
      size_t kept = 0;
      for (size_t j = 0; j < neighbors.size(); ++j) {
        if (drop[j]) {
          continue;
        }
        if (kept != j) {
          neighbors[kept] = std::move(neighbors[j]);
        }
        ++kept;
      }
      neighbors.resize(kept);
      params.per_arm_results[i].total_count = kept;
    }

    // Dropping preserves order, so only a fresh score can have put the arm out
    // of order.
    if (rescored) {
      std::stable_sort(neighbors.begin(), neighbors.end(),
                       [arm_score_is_distance](const indexes::Neighbor &a,
                                               const indexes::Neighbor &b) {
                         if (arm_score_is_distance) {
                           if (a.distance != b.distance) {
                             return a.distance < b.distance;
                           }
                         } else if (a.score != b.score) {
                           return a.score > b.score;
                         }
                         return a.external_id->Str() < b.external_id->Str();
                       });
    }
  }
}

// SearchParameters used to drive the SINGLE, atomic, post-fusion content
// resolution for a local FT.HYBRID. ResolveContent fetches the database fields
// for the whole fused neighbor list at once (on the main thread). On
// completion it re-merges the per-arm score aliases and unblocks the client,
// whose reply callback runs the aggregate pipeline. This is what makes the
// multi-arm results "come together before the final main-thread validation".
class FusedResolver : public query::SearchParameters {
 public:
  std::unique_ptr<MultiSearchParameters> envelope;
  absl::flat_hash_map<std::string, RecordsMap> saved_aliases;
  // The contention check already ran, on the arms, before they were fused (see
  // ArmGate). All that is left here is the fetch, which ResolveContent skips
  // by itself when no_content is set.
  query::ContentProcessing GetContentProcessing() const override {
    return query::kContentRequired;
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
    MarkContentResolved(search_result.neighbors);
    // Preserve whatever status ResolveContent produced (e.g. an index-dropped
    // error). Forcing OkStatus here would turn a content-resolution failure
    // into a silent empty-but-successful reply.
    envelope->search_result = std::move(search_result);
    auto *raw = envelope.release();
    raw->blocked_client->SetReplyPrivateData(raw);
    raw->blocked_client->UnblockClient();
  }
};

// Local async completion: all arms have delivered raw (content-free) results.
// Fuse, then run the content resolution over the fused list. Reached only once
// the arms have cleared the mutation queue and been revalidated against it.
void FuseAndResolveLocal(std::unique_ptr<MultiSearchParameters> params) {
  auto fused = BuildFusedNeighbors(*params);
  auto resolver = std::make_unique<FusedResolver>();
  resolver->index_schema = params->index_schema;
  resolver->db_num = params->db_num;
  resolver->cancellation_token = params->cancellation_token;
  resolver->enable_partial_results = params->enable_partial_results;
  if (params->agg != nullptr) {
    resolver->return_attributes =
        CopyReturnAttributes(params->agg->return_attributes);
    resolver->no_content = WantsNoDatabaseContent(*params->agg);
  }
  resolver->saved_aliases = SaveAndClearAliases(fused);
  resolver->search_result.total_count = fused.size();
  resolver->search_result.neighbors = std::move(fused);
  resolver->envelope = std::move(params);
  // Already on the main thread: ArmGate's completion runs there.
  query::ResolveContent(std::move(resolver));
}

// Parks the whole operation behind any in-flight mutation on a key either arm
// matched, and does it BEFORE the arms are fused.
//
// The check itself is the same one a single-arm query runs, reached through
// the same ResolveContent: hand it a neighbor list, and if any of those keys
// has a mutation queued, the parameters are moved into the mutation queue and
// ResolveContent is called again once the mutation has applied. Carrying the
// arms' own neighbors rather than the fused ones is what makes the sequence
// numbers meaningful, since fusion does not carry them across.
//
// On the way out the arms are revalidated against the state the mutation left
// behind, and only then merged.
class ArmGate : public query::SearchParameters {
 public:
  std::unique_ptr<MultiSearchParameters> envelope;
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
    auto env = std::move(envelope);
    if (!search_result.status.ok()) {
      // A cancelled query or a dropped index. Surface it rather than fusing
      // whatever the arms happened to return.
      env->search_result.status = search_result.status;
      auto *raw = env.release();
      raw->blocked_client->SetReplyPrivateData(raw);
      raw->blocked_client->UnblockClient();
      return;
    }
    RevalidateArmsBeforeFusion(*env);
    FuseAndResolveLocal(std::move(env));
  }
};

// Local async completion: all arms have delivered raw (content-free) results.
void FuseThenResolveLocal(std::unique_ptr<MultiSearchParameters> params) {
  // If an arm already failed (MultiSearchTracker::Finalize populated the error
  // in non-partial mode), skip fusion and content resolution and surface the
  // specific arm error directly, rather than processing failed arms as success.
  if (!params->search_result.status.ok()) {
    auto *raw = params.release();
    raw->blocked_client->SetReplyPrivateData(raw);
    raw->blocked_client->UnblockClient();
    return;
  }
  auto gate = std::make_unique<ArmGate>();
  gate->index_schema = params->index_schema;
  gate->db_num = params->db_num;
  gate->cancellation_token = params->cancellation_token;
  gate->enable_partial_results = params->enable_partial_results;
  // Nothing is fetched for the gate; it exists to be parked.
  gate->no_content = true;
  // One entry per (arm, key), each carrying the sequence number that arm
  // recorded. Duplicates across arms cost a map lookup and save a dedupe.
  for (const auto &arm_result : params->per_arm_results) {
    for (const auto &n : arm_result.neighbors) {
      indexes::Neighbor probe;
      probe.external_id = n.external_id;
      probe.sequence_number = n.sequence_number;
      gate->search_result.neighbors.push_back(std::move(probe));
    }
  }
  gate->envelope = std::move(params);
  vmsdk::RunByMain([gate = std::move(gate)]() mutable {
    query::ResolveContent(std::move(gate));
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
  if (params.agg != nullptr) {
    resolver.return_attributes =
        CopyReturnAttributes(params.agg->return_attributes);
    resolver.no_content = WantsNoDatabaseContent(*params.agg);
  }
  if (!resolver.no_content) {
    query::ProcessNeighborsForReply(ctx,
                                    params.index_schema->GetAttributeDataType(),
                                    fused, resolver, std::nullopt);
  }
  RestoreAliases(fused, saved);
  MarkContentResolved(fused);
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
