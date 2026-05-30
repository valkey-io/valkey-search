/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "src/commands/ft_aggregate_parser.h"
#include "src/commands/ft_hybrid_combine.h"
#include "src/expr/expr.h"
#include "src/query/multi_search.h"
#include "src/query/search.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search::query {

// Top-level keywords that terminate a SEARCH-arm or VSIM-arm scoped subclause
// scan. Anything else under SEARCH/VSIM is consumed as a sub-clause.
constexpr absl::string_view kSearchKw{"SEARCH"};
constexpr absl::string_view kVsimKw{"VSIM"};
constexpr absl::string_view kCombineKw{"COMBINE"};
constexpr absl::string_view kPolicyKw{"POLICY"};
constexpr absl::string_view kLocalOnlyKw{"LOCALONLY"};
constexpr absl::string_view kRrfKw{"RRF"};
constexpr absl::string_view kLinearKw{"LINEAR"};
constexpr absl::string_view kFunctionKw{"FUNCTION"};
constexpr absl::string_view kExprKw{"EXPR"};
constexpr absl::string_view kKnnKw{"KNN"};
constexpr absl::string_view kRangeKw{"RANGE"};
constexpr absl::string_view kKKw{"K"};
constexpr absl::string_view kEfRuntimeKw{"EF_RUNTIME"};
constexpr absl::string_view kRadiusKw{"RADIUS"};
constexpr absl::string_view kEpsilonKw{"EPSILON"};
constexpr absl::string_view kConstantKw{"CONSTANT"};
constexpr absl::string_view kWindowKw{"WINDOW"};
constexpr absl::string_view kAlphaKw{"ALPHA"};
constexpr absl::string_view kBetaKw{"BETA"};
constexpr absl::string_view kYieldScoreAsKw{"YIELD_SCORE_AS"};
constexpr absl::string_view kScorerKw{"SCORER"};
constexpr absl::string_view kFilterKw{"FILTER"};
constexpr absl::string_view kReturnKw{"RETURN"};
constexpr absl::string_view kNocontentKw{"NOCONTENT"};
constexpr absl::string_view kDialectKw{"DIALECT"};

// Returns true if `tok` matches one of the top-level FT.HYBRID keywords that
// would terminate the SEARCH/VSIM scoped subclause scan. Used by both
// ParseSearchClause and ParseVsimClause to know when to stop consuming
// sub-clause tokens and let the next top-level handler take over.
//
// Note: NOCONTENT is intentionally NOT in this list — we want SEARCH-scope
// scan to see it and emit a precise rejection ("NOCONTENT is not supported
// by FT.HYBRID") rather than letting it slip through as a top-level token
// that subsequent handlers misinterpret.
bool IsTopLevelKeyword(absl::string_view tok) {
  return absl::EqualsIgnoreCase(tok, kSearchKw) ||
         absl::EqualsIgnoreCase(tok, kVsimKw) ||
         absl::EqualsIgnoreCase(tok, kCombineKw) ||
         absl::EqualsIgnoreCase(tok, kPolicyKw) ||
         absl::EqualsIgnoreCase(tok, kLocalOnlyKw) ||
         // Aggregate-suffix keywords also terminate the SEARCH/VSIM scopes.
         // Listing the most common ones; the aggregate parser will reject any
         // unknown leading keyword anyway.
         absl::EqualsIgnoreCase(tok, "LOAD") ||
         absl::EqualsIgnoreCase(tok, "APPLY") ||
         absl::EqualsIgnoreCase(tok, "GROUPBY") ||
         absl::EqualsIgnoreCase(tok, "SORTBY") ||
         absl::EqualsIgnoreCase(tok, "LIMIT") ||
         absl::EqualsIgnoreCase(tok, "PARAMS") ||
         absl::EqualsIgnoreCase(tok, "TIMEOUT") ||
         absl::EqualsIgnoreCase(tok, "FILTER") ||
         absl::EqualsIgnoreCase(tok, "DIALECT");
}

namespace {

// Plumb the envelope-shared dispatch fields onto a freshly-allocated arm.
void InitArmFromEnvelope(MultiSearchParameters &env, MultiArmShim &arm) {
  arm.db_num = env.db_num;
  arm.index_schema = env.index_schema;
  arm.index_schema_name = env.index_schema_name;
  arm.timeout_ms = env.timeout_ms;
  arm.enable_partial_results = env.enable_partial_results;
  arm.enable_consistency = env.enable_consistency;
  arm.cancellation_token = env.cancellation_token;
  arm.index_fingerprint_version = env.index_fingerprint_version;
  arm.slot_fingerprint = env.slot_fingerprint;
  arm.dialect = 2;  // FT.HYBRID is fixed at DIALECT 2.
}

// SEARCH arm: SEARCH <query> [SCORER ...] [YIELD_SCORE_AS name] [...]
// The Valkey super-set allows any query the underlying parser accepts —
// including vector expressions. The arm becomes a vector arm if its query
// string contains `=>[KNN ...]` etc.
//
// IMPORTANT: parse_vars.query_string and parse_vars.query_vector_string are
// absl::string_view fields — they MUST point at storage that outlives parse
// and PreParseQueryString/PostParseQueryString. The argv backing array lives
// for the full command lifetime, so we capture string_views directly from
// the iterator rather than materializing into local std::string objects
// (which would dangle).
absl::Status ParseSearchClause(MultiSearchParameters &env,
                               vmsdk::ArgsIterator &itr) {
  if (!itr.PopIfNextIgnoreCase(kSearchKw)) {
    return absl::InvalidArgumentError("FT.HYBRID requires SEARCH clause");
  }
  auto arm = std::make_unique<MultiArmShim>();
  InitArmFromEnvelope(env, *arm);
  // Consume the bare query string immediately following SEARCH.
  VMSDK_ASSIGN_OR_RETURN(auto query_sv, itr.GetStringView());
  itr.Next();
  arm->parse_vars.query_string = query_sv;  // backed by argv lifetime

  // Walk SEARCH-scoped subclauses until we hit a top-level keyword or the
  // end of args.
  std::optional<std::string> per_arm_alias;
  while (itr.HasNext()) {
    auto next_or = itr.GetStringView();
    if (!next_or.ok()) {
      break;
    }
    auto next = next_or.value();
    if (IsTopLevelKeyword(next)) {
      break;
    }
    if (absl::EqualsIgnoreCase(next, kYieldScoreAsKw)) {
      itr.Next();
      VMSDK_ASSIGN_OR_RETURN(auto alias_sv, itr.GetStringView());
      itr.Next();
      per_arm_alias = std::string(alias_sv);
      arm->score_as = vmsdk::MakeUniqueValkeyString(alias_sv);
    } else if (absl::EqualsIgnoreCase(next, kScorerKw)) {
      itr.Next();
      // Parse-but-no-op in V1.
      VMSDK_ASSIGN_OR_RETURN(auto scorer_sv, itr.GetStringView());
      (void)scorer_sv;
      itr.Next();
      // TODO(text-scoring): apply the scorer when text scoring lands.
    } else if (absl::EqualsIgnoreCase(next, kNocontentKw)) {
      return absl::InvalidArgumentError(
          "NOCONTENT is not supported by FT.HYBRID");
    } else {
      return absl::InvalidArgumentError(
          absl::StrCat("Unexpected token in SEARCH clause: `", next, "`"));
    }
  }
  env.arms.push_back(std::move(arm));
  env.per_arm_score_alias.push_back(per_arm_alias);
  return absl::OkStatus();
}

// VSIM arm: VSIM @field $param (KNN <count> K <k> [EF_RUNTIME <ef>]
//                              | RANGE <count> RADIUS <r> [EPSILON <e>])
//           [YIELD_SCORE_AS name]
absl::Status ParseVsimClause(MultiSearchParameters &env,
                             vmsdk::ArgsIterator &itr, bool *vsim_uses_range) {
  *vsim_uses_range = false;
  if (!itr.PopIfNextIgnoreCase(kVsimKw)) {
    return absl::InvalidArgumentError("FT.HYBRID requires VSIM clause");
  }
  auto arm = std::make_unique<MultiArmShim>();
  InitArmFromEnvelope(env, *arm);

  // Vector field: @<name>
  VMSDK_ASSIGN_OR_RETURN(auto field_sv, itr.GetStringView());
  itr.Next();
  if (field_sv.empty() || field_sv[0] != '@') {
    return absl::InvalidArgumentError(absl::StrCat(
        "VSIM vector field must start with '@', got `", field_sv, "`"));
  }
  arm->attribute_alias = std::string(field_sv.substr(1));

  // Vector parameter: $<name>. Stored as string_view backed by argv lifetime;
  // resolved to the actual blob at PostParseQueryString time via the shared
  // parse_vars.params map.
  VMSDK_ASSIGN_OR_RETURN(auto param_sv, itr.GetStringView());
  itr.Next();
  if (param_sv.empty() || param_sv[0] != '$') {
    return absl::InvalidArgumentError(absl::StrCat(
        "VSIM vector parameter must start with '$', got `", param_sv, "`"));
  }
  arm->parse_vars.query_vector_string = param_sv;

  // Mode: KNN | RANGE
  VMSDK_ASSIGN_OR_RETURN(auto mode_sv, itr.GetStringView());
  itr.Next();
  uint32_t inner_count = 0;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, inner_count));
  if (absl::EqualsIgnoreCase(mode_sv, kKnnKw)) {
    // KNN <count> K <k> [EF_RUNTIME <ef>] [YIELD_SCORE_AS <name>]
    arm->k = 10;  // default
    auto inner_itr_or = itr.SubIterator(inner_count);
    if (!inner_itr_or.ok()) {
      return inner_itr_or.status();
    }
    auto inner_itr = inner_itr_or.value();
    itr.Next(inner_count);
    while (inner_itr.HasNext()) {
      VMSDK_ASSIGN_OR_RETURN(auto kw, inner_itr.GetStringView());
      inner_itr.Next();
      if (absl::EqualsIgnoreCase(kw, kKKw)) {
        VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(inner_itr, arm->k));
      } else if (absl::EqualsIgnoreCase(kw, kEfRuntimeKw)) {
        unsigned ef = 0;
        VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(inner_itr, ef));
        arm->ef = ef;
      } else if (absl::EqualsIgnoreCase(kw, kYieldScoreAsKw)) {
        VMSDK_ASSIGN_OR_RETURN(auto alias_sv, inner_itr.GetStringView());
        inner_itr.Next();
        arm->score_as = vmsdk::MakeUniqueValkeyString(alias_sv);
      } else {
        return absl::InvalidArgumentError(
            absl::StrCat("Unknown VSIM KNN sub-arg: `", kw, "`"));
      }
    }
  } else if (absl::EqualsIgnoreCase(mode_sv, kRangeKw)) {
    *vsim_uses_range = true;
    // Validate the RANGE shape; ParseFtHybridCommand returns
    // UnimplementedError after parse completes.
    auto inner_itr_or = itr.SubIterator(inner_count);
    if (!inner_itr_or.ok()) {
      return inner_itr_or.status();
    }
    auto inner_itr = inner_itr_or.value();
    itr.Next(inner_count);
    bool seen_radius = false;
    while (inner_itr.HasNext()) {
      VMSDK_ASSIGN_OR_RETURN(auto kw, inner_itr.GetStringView());
      inner_itr.Next();
      if (absl::EqualsIgnoreCase(kw, kRadiusKw)) {
        double radius = 0.0;
        VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(inner_itr, radius));
        if (radius < 0) {
          return absl::InvalidArgumentError("Invalid RADIUS value");
        }
        seen_radius = true;
      } else if (absl::EqualsIgnoreCase(kw, kEpsilonKw)) {
        double epsilon = 0.0;
        VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(inner_itr, epsilon));
      } else if (absl::EqualsIgnoreCase(kw, kYieldScoreAsKw)) {
        VMSDK_ASSIGN_OR_RETURN(auto alias_sv, inner_itr.GetStringView());
        inner_itr.Next();
        (void)alias_sv;
      } else {
        return absl::InvalidArgumentError(
            absl::StrCat("Unknown VSIM RANGE sub-arg: `", kw, "`"));
      }
    }
    if (!seen_radius) {
      return absl::InvalidArgumentError("VSIM RANGE requires RADIUS");
    }
  } else {
    return absl::InvalidArgumentError(
        absl::StrCat("VSIM expects KNN or RANGE, got `", mode_sv, "`"));
  }

  // VSIM-scoped tail subclauses (top-level YIELD_SCORE_AS for VSIM).
  std::optional<std::string> per_arm_alias;
  if (arm->score_as) {
    per_arm_alias = std::string(vmsdk::ToStringView(arm->score_as.get()));
  }
  while (itr.HasNext()) {
    auto next_or = itr.GetStringView();
    if (!next_or.ok()) {
      break;
    }
    auto next = next_or.value();
    if (IsTopLevelKeyword(next)) {
      break;
    }
    if (absl::EqualsIgnoreCase(next, kYieldScoreAsKw)) {
      itr.Next();
      VMSDK_ASSIGN_OR_RETURN(auto alias_sv, itr.GetStringView());
      itr.Next();
      per_arm_alias = std::string(alias_sv);
      arm->score_as = vmsdk::MakeUniqueValkeyString(alias_sv);
    } else {
      return absl::InvalidArgumentError(
          absl::StrCat("Unexpected token in VSIM clause: `", next, "`"));
    }
  }

  env.arms.push_back(std::move(arm));
  env.per_arm_score_alias.push_back(per_arm_alias);
  return absl::OkStatus();
}

// COMBINE [RRF <count> [CONSTANT n] [WINDOW n] [YIELD_SCORE_AS name]]
//        | [LINEAR <count> ALPHA <a> BETA <b> [WINDOW n] [YIELD_SCORE_AS name]]
absl::Status ParseCombineClause(MultiSearchParameters &env,
                                vmsdk::ArgsIterator &itr) {
  VMSDK_ASSIGN_OR_RETURN(auto method_sv, itr.GetStringView());
  itr.Next();
  uint32_t inner_count = 0;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, inner_count));
  auto inner_itr_or = itr.SubIterator(inner_count);
  if (!inner_itr_or.ok() && inner_count > 0) {
    return inner_itr_or.status();
  }
  vmsdk::ArgsIterator inner_itr =
      inner_count > 0 ? inner_itr_or.value() : vmsdk::ArgsIterator(nullptr, 0);
  itr.Next(inner_count);
  bool saw_alpha = false;
  bool saw_beta = false;
  bool saw_window = false;
  if (absl::EqualsIgnoreCase(method_sv, kRrfKw)) {
    env.fusion.method = FusionConfig::Method::kRRF;
  } else if (absl::EqualsIgnoreCase(method_sv, kLinearKw)) {
    env.fusion.method = FusionConfig::Method::kLinear;
  } else if (absl::EqualsIgnoreCase(method_sv, kFunctionKw)) {
    env.fusion.method = FusionConfig::Method::kFunction;
  } else {
    return absl::InvalidArgumentError(
        absl::StrCat("COMBINE method must be RRF, LINEAR, or FUNCTION, got `",
                     method_sv, "`"));
  }
  bool saw_expr = false;
  while (inner_itr.HasNext()) {
    VMSDK_ASSIGN_OR_RETURN(auto kw, inner_itr.GetStringView());
    inner_itr.Next();
    if (absl::EqualsIgnoreCase(kw, kConstantKw)) {
      uint32_t v = 0;
      VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(inner_itr, v));
      env.fusion.rrf_constant = v;
    } else if (absl::EqualsIgnoreCase(kw, kWindowKw)) {
      uint32_t v = 0;
      VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(inner_itr, v));
      env.fusion.window = v;
      saw_window = true;
    } else if (absl::EqualsIgnoreCase(kw, kAlphaKw)) {
      double v = 0;
      VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(inner_itr, v));
      env.fusion.alpha = v;
      saw_alpha = true;
    } else if (absl::EqualsIgnoreCase(kw, kBetaKw)) {
      double v = 0;
      VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(inner_itr, v));
      env.fusion.beta = v;
      saw_beta = true;
    } else if (absl::EqualsIgnoreCase(kw, kYieldScoreAsKw)) {
      VMSDK_ASSIGN_OR_RETURN(auto alias_sv, inner_itr.GetStringView());
      inner_itr.Next();
      env.score_as = vmsdk::MakeUniqueValkeyString(alias_sv);
    } else if (absl::EqualsIgnoreCase(kw, kExprKw)) {
      // COMBINE FUNCTION EXPR "<expression>". Compile the expression against
      // a context that maps every arm's score to a reference. Each arm is
      // reachable via its YIELD_SCORE_AS alias (if any), the positional
      // default @__arm<i>_score, and (for the two-arm SEARCH/VSIM shape)
      // @__search_score / @__vector_score.
      VMSDK_ASSIGN_OR_RETURN(auto expr_sv, inner_itr.GetStringView());
      inner_itr.Next();
      CombineFunctionContext cctx;
      for (size_t i = 0; i < env.arms.size(); ++i) {
        cctx.alias_to_arm[absl::StrCat("__arm", i, "_score")] = i;
        if (i < env.per_arm_score_alias.size() &&
            env.per_arm_score_alias[i].has_value()) {
          cctx.alias_to_arm[*env.per_arm_score_alias[i]] = i;
        }
      }
      if (env.arms.size() >= 1) {
        cctx.alias_to_arm["__search_score"] = 0;
      }
      if (env.arms.size() >= 2) {
        cctx.alias_to_arm["__vector_score"] = 1;
      }
      VMSDK_ASSIGN_OR_RETURN(env.combine_function,
                             expr::Expression::Compile(cctx, expr_sv));
      saw_expr = true;
    } else {
      return absl::InvalidArgumentError(
          absl::StrCat("Unknown COMBINE sub-arg: `", kw, "`"));
    }
  }
  if (env.fusion.method == FusionConfig::Method::kLinear) {
    if (!saw_alpha || !saw_beta) {
      return absl::InvalidArgumentError(
          "COMBINE LINEAR requires ALPHA and BETA");
    }
  }
  if (env.fusion.method == FusionConfig::Method::kFunction && !saw_expr) {
    return absl::InvalidArgumentError("COMBINE FUNCTION requires EXPR");
  }
  // For COMBINE FUNCTION, the user expression is computed per document and
  // typically expected to see EVERY matching candidate (not the RRF/LINEAR
  // top-window slice). If the caller did not set WINDOW explicitly, default
  // to unlimited so the fusion stage does not silently drop docs the user
  // expected the function to score.
  if (env.fusion.method == FusionConfig::Method::kFunction && !saw_window) {
    env.fusion.window = 0;
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status ParseFtHybridCommand(MultiSearchParameters &env,
                                  vmsdk::ArgsIterator &itr) {
  bool vsim_uses_range = false;
  // 1. SEARCH (mandatory, must come first)
  VMSDK_RETURN_IF_ERROR(ParseSearchClause(env, itr));
  // 2. VSIM (mandatory, must follow)
  VMSDK_RETURN_IF_ERROR(ParseVsimClause(env, itr, &vsim_uses_range));
  // 3. Optional COMBINE
  if (itr.PopIfNextIgnoreCase(kCombineKw)) {
    VMSDK_RETURN_IF_ERROR(ParseCombineClause(env, itr));
  }
  // 4. Walk remaining tokens. POLICY is accepted-and-discarded; everything
  //    else is forwarded to the aggregate-suffix parser via the embedded
  //    AggregateParameters in env.agg.
  if (env.agg == nullptr) {
    env.agg = std::make_unique<aggregate::AggregateParameters>(env.db_num);
    env.agg->index_schema = env.index_schema;
    env.agg->index_schema_name = env.index_schema_name;
    env.agg->dialect = 2;
    env.agg->cancellation_token = env.cancellation_token;
    env.agg->timeout_ms = env.timeout_ms;
    env.agg->no_content = false;
    // Make the aggregate pipeline treat the fused result as a "scored" set so
    // the score_as -> Neighbor::distance plumbing in CreateRecordsFromNeighbors
    // fires (it gates on AggregateParameters::IsVectorQuery, which checks
    // whether attribute_alias is non-empty). Reuse the VSIM arm's vector
    // field; the aggregate code calls index_schema->GetIdentifier on it, so
    // it must name a real vector index in the schema.
    if (env.arms.size() > 1 && !env.arms[1]->attribute_alias.empty()) {
      env.agg->attribute_alias = env.arms[1]->attribute_alias;
    }
    // Set the score_as on the aggregate the same way ft_aggregate.cc:91 does
    // for plain FT.AGGREGATE — so the post-fusion aggregate pipeline routes
    // the fused score into a known field name.
    if (env.score_as) {
      env.agg->score_as = vmsdk::MakeUniqueValkeyString(
          vmsdk::ToStringView(env.score_as.get()));
    } else {
      env.agg->score_as = vmsdk::MakeUniqueValkeyString("__hybrid_score");
      env.score_as = vmsdk::MakeUniqueValkeyString("__hybrid_score");
    }
    // Pre-populate the two reserved record slots that AggregateParameters
    // expects: __key at index 0 and the score alias at index 1. Mirrors the
    // setup at ft_aggregate.cc:94-97. Without this, MakeReference fails when
    // any APPLY/FILTER/SORTBY references @<score_alias>.
    CHECK_EQ(env.agg->AddRecordAttribute("__key", "__key",
                                         indexes::IndexerType::kNone),
             0u);
    auto score_sv = vmsdk::ToStringView(env.agg->score_as.get());
    CHECK_EQ(env.agg->AddRecordAttribute(score_sv, score_sv,
                                         indexes::IndexerType::kNone),
             1u);
  }
  // The aggregate parser uses parse_vars_.index_interface_ during expression
  // compilation (APPLY/FILTER/REDUCE) to resolve @<field> references. Stack
  // allocate the interface — it only needs to be alive during the agg-parser
  // walk below, then we clear the pointer before returning.
  struct HybridIndexInterface : public aggregate::IndexInterface {
    std::shared_ptr<IndexSchema> schema;
    explicit HybridIndexInterface(std::shared_ptr<IndexSchema> s)
        : schema(std::move(s)) {}
    absl::StatusOr<indexes::IndexerType> GetFieldType(
        absl::string_view s) const override {
      VMSDK_ASSIGN_OR_RETURN(auto indexer, schema->GetIndex(s));
      return indexer->GetIndexerType();
    }
    absl::StatusOr<std::string> GetIdentifier(
        absl::string_view alias) const override {
      return schema->GetIdentifier(alias);
    }
    absl::StatusOr<std::string> GetAlias(
        absl::string_view identifier) const override {
      return schema->GetAlias(identifier);
    }
  };
  HybridIndexInterface ii(env.index_schema);
  env.agg->parse_vars_.index_interface_ = &ii;
  // The aggregate parser owns the rest of the iterator (LOAD/APPLY/FILTER/
  // GROUPBY/SORTBY/LIMIT/PARAMS/TIMEOUT/SCORER). Strip POLICY tokens before
  // forwarding so the aggregate parser doesn't trip on them.
  //
  // Implementation: walk until end of args, accumulating a filtered argv-like
  // list. To avoid building a synthetic ValkeyModuleString**, just consume
  // POLICY in a loop here and let the aggregate parser handle the gaps.
  while (itr.HasNext()) {
    auto next_or = itr.GetStringView();
    if (!next_or.ok()) {
      break;
    }
    if (absl::EqualsIgnoreCase(next_or.value(), kPolicyKw)) {
      itr.Next();  // consume POLICY
      if (!itr.HasNext()) {
        return absl::InvalidArgumentError("POLICY requires a value");
      }
      itr.Next();  // consume the value (silently discarded)
      continue;
    }
    if (absl::EqualsIgnoreCase(next_or.value(), kLocalOnlyKw)) {
      itr.Next();
      env.local_only = true;
      continue;
    }
    if (absl::EqualsIgnoreCase(next_or.value(), kDialectKw)) {
      return absl::InvalidArgumentError(
          "DIALECT is not configurable for FT.HYBRID");
    }
    if (absl::EqualsIgnoreCase(next_or.value(), kNocontentKw)) {
      return absl::InvalidArgumentError(
          "NOCONTENT is not supported by FT.HYBRID");
    }
    // Forward to the aggregate parser, which will consume one block of
    // tokens. If it returns an error (e.g. unknown keyword), we surface it.
    // We use the static aggregate parser table.
    static auto agg_parser = aggregate::CreateAggregateParser();
    // The parser walks itr as long as it recognizes leading keywords; on the
    // first unknown keyword it returns OK and stops, leaving itr at that
    // token. To support our POLICY-skip loop, drive the aggregate parser one
    // keyword at a time by capturing the current position and asking it to
    // parse non-strictly. The aggregate parser's Parse signature is
    // (T&, ArgsIterator&, bool stop_on_unknown).
    int before = itr.Position();
    auto status = agg_parser.Parse(*env.agg, itr, true);
    if (!status.ok()) {
      return status;
    }
    if (itr.Position() == before) {
      // Aggregate parser didn't recognize the leading keyword; surface error.
      return absl::InvalidArgumentError(
          absl::StrCat("Unexpected token at position ", itr.Position() + 1,
                       ": `", next_or.value(), "`"));
    }
  }

  // Now that PARAMS (if any) are populated on env.agg->parse_vars.params,
  // share the params map with each arm so $name resolves identically. Also
  // run the per-arm pre/post-parse so vector arms (or vector-in-SEARCH per
  // the Valkey super-set) get their k/ef/query-blob populated.
  for (auto &arm : env.arms) {
    arm->parse_vars.params = env.agg->parse_vars.params;
    arm->cancellation_token = env.cancellation_token;
    // Two paths:
    //  - SEARCH arm (or any arm that came from a non-empty query string,
    //    including a Valkey-super-set vector-in-SEARCH query like
    //    `*=>[KNN ...]`): run the existing PreParseQueryString /
    //    PostParseQueryString pipeline.
    //  - Pure VSIM arm (no query string; @field, $param, K already set
    //    directly by the VSIM clause parser): skip PreParse (which would
    //    reject the empty query) and manually substitute the vector
    //    parameter to populate arm->query.
    if (arm->parse_vars.query_string.empty() && arm->IsVectorQuery()) {
      // Resolve the vector field is actually a vector index.
      VMSDK_ASSIGN_OR_RETURN(auto index,
                             arm->index_schema->GetIndex(arm->attribute_alias));
      if (index->GetIndexerType() != indexes::IndexerType::kHNSW &&
          index->GetIndexerType() != indexes::IndexerType::kFlat) {
        return absl::InvalidArgumentError(absl::StrCat(
            "Index field `", arm->attribute_alias, "` is not a Vector index"));
      }
      // Substitute $name from shared params map.
      auto param_name = arm->parse_vars.query_vector_string;
      if (param_name.empty() || param_name[0] != '$') {
        return absl::InvalidArgumentError(
            "VSIM vector parameter must be a $-prefixed PARAMS reference");
      }
      param_name.remove_prefix(1);
      auto it = arm->parse_vars.params.find(param_name);
      if (it == arm->parse_vars.params.end()) {
        return absl::InvalidArgumentError(
            absl::StrCat("Parameter ", param_name, " not found."));
      }
      it->second.first++;  // bump usage refcount
      arm->query = std::string(it->second.second);
      // Default score_as if the user didn't YIELD_SCORE_AS.
      if (!arm->score_as) {
        auto schema_default =
            arm->index_schema->DefaultReplyScoreAs(arm->attribute_alias);
        if (schema_default.ok()) {
          arm->score_as = std::move(*schema_default);
        }
      }
    } else {
      VMSDK_RETURN_IF_ERROR(arm->PreParseQueryString());
      VMSDK_RETURN_IF_ERROR(arm->PostParseQueryString());
    }
  }

  // Clear the now-stale stack-local index_interface_ pointer.
  env.agg->parse_vars_.index_interface_ = nullptr;

  // After per-arm parse: VSIM RANGE is parsed for shape but not yet
  // executable.
  if (vsim_uses_range) {
    return absl::UnimplementedError("VSIM RANGE is not yet supported; use KNN");
  }
  return absl::OkStatus();
}

}  // namespace valkey_search::query
