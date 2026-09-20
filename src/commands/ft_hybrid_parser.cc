/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "src/commands/ft_aggregate_parser.h"
#include "src/commands/ft_create_parser.h"
#include "src/commands/ft_hybrid_combine.h"
#include "src/commands/ft_search_parser.h"
#include "src/expr/expr.h"
#include "src/indexes/scoring/scorer.h"
#include "src/query/multi_search.h"
#include "src/query/search.h"
#include "src/valkey_search_options.h"
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
// K when the caller does not say. Matches Redis, which applies the same
// default when the KNN block is omitted entirely.
constexpr uint64_t kDefaultKnnK = 10;
// LINEAR arm weights when the caller writes neither of them. Measured against
// the reference (Redis 8.10.1, search module 81000): `COMBINE LINEAR 0`,
// `COMBINE LINEAR 2 WINDOW 5` and `COMBINE LINEAR 2 YIELD_SCORE_AS fs` all
// reply exactly as `COMBINE LINEAR 4 ALPHA 0.3 BETA 0.7` does.
constexpr double kDefaultLinearAlpha = 0.3;
constexpr double kDefaultLinearBeta = 0.7;
constexpr absl::string_view kKnnKw{"KNN"};
constexpr absl::string_view kRangeKw{"RANGE"};
constexpr absl::string_view kKKw{"K"};
constexpr absl::string_view kEfRuntimeKw{"EF_RUNTIME"};
constexpr absl::string_view kShardKRatioKw{"SHARD_K_RATIO"};
constexpr absl::string_view kRadiusKw{"RADIUS"};
constexpr absl::string_view kEpsilonKw{"EPSILON"};
constexpr absl::string_view kConstantKw{"CONSTANT"};
constexpr absl::string_view kWindowKw{"WINDOW"};
constexpr absl::string_view kAlphaKw{"ALPHA"};
constexpr absl::string_view kBetaKw{"BETA"};
constexpr absl::string_view kYieldScoreAsKw{"YIELD_SCORE_AS"};
constexpr absl::string_view kScorerKw{"SCORER"};
constexpr absl::string_view kFilterKw{"FILTER"};
constexpr absl::string_view kBatchSizeKw{"BATCH_SIZE"};
constexpr absl::string_view kReturnKw{"RETURN"};
constexpr absl::string_view kNocontentKw{"NOCONTENT"};
constexpr absl::string_view kDialectKw{"DIALECT"};

// The aggregate's column 0, seeded before any clause is parsed (see the
// AddRecordAttribute calls in ParseFtHybridCommand). A YIELD_SCORE_AS alias
// naming it would ask for the same column to be both the key and the score,
// so it is refused wherever the clause can appear. Matched exactly as the
// seeding writes it: `__KEY` is a different output name and claims a column
// of its own, as the rest of the column machinery is also case-sensitive.
constexpr absl::string_view kKeyColumnName{"__key"};

// Refuses a YIELD_SCORE_AS alias that cannot name a column: the empty string,
// or the reserved key column.
absl::Status RejectReservedScoreAlias(absl::string_view alias) {
  if (alias.empty()) {
    // Every other COMBINE sub-arg rejects an empty token; an empty alias would
    // otherwise become the score column's output name and be unreferencable by
    // any later pipeline stage.
    return absl::InvalidArgumentError("YIELD_SCORE_AS requires a name");
  }
  if (alias == kKeyColumnName) {
    return absl::InvalidArgumentError(absl::StrCat(
        "YIELD_SCORE_AS `", kKeyColumnName,
        "` attempts to redefine the reserved `", kKeyColumnName, "` field"));
  }
  return absl::OkStatus();
}

// Returns true if `tok` matches one of the top-level FT.HYBRID keywords that
// would terminate the SEARCH/VSIM scoped subclause scan. Used by both
// ParseSearchClause and ParseVsimClause to know when to stop consuming
// sub-clause tokens and let the next top-level handler take over.
//
// Note: NOCONTENT is intentionally NOT in this list — we want SEARCH-scope
// scan to see it and emit a precise rejection ("NOCONTENT is not supported
// by FT.HYBRID") rather than letting it slip through as a top-level token
// that subsequent handlers misinterpret.
// FT.HYBRID returns 10 rows when the caller omits LIMIT.
constexpr size_t kDefaultHybridLimit = 10;

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

// K and EF_RUNTIME are bounded exactly as FT.SEARCH bounds them in
// VerifyQueryString -- same limits, same message -- because FT.HYBRID never
// reaches that function.
absl::Status VerifyKnnK(int k) {
  const auto max_knn_value = options::GetMaxKnn().GetValue();
  VMSDK_RETURN_IF_ERROR(vmsdk::VerifyRange(k, 1, max_knn_value))
      << "KNN parameter must be a positive integer greater than 0 and cannot "
         "exceed "
      << max_knn_value << ".";
  return absl::OkStatus();
}

absl::Status VerifyEfRuntime(unsigned ef) {
  const auto max_ef_runtime_value = options::GetMaxEfRuntime().GetValue();
  VMSDK_RETURN_IF_ERROR(vmsdk::VerifyRange(ef, 1, max_ef_runtime_value))
      << "`EF_RUNTIME` must be a positive integer greater than 0 and cannot "
         "exceed "
      << max_ef_runtime_value << ".";
  return absl::OkStatus();
}

// Reads a whole token as a non-negative integer.
//
// The shared `ParseParamValue` reaches `std::from_chars`, which consumes what
// it can and reports success on the prefix, so it turned `WINDOW 20abc` into
// 20, `WINDOW 1.5` into 1, `WINDOW 1e3` into 1 and `WINDOW 0x10` into 0.
// Silently acting on a number the caller did not write is worse than either
// engine's answer, so these insist the token is entirely the number.
absl::StatusOr<uint64_t> ParseWholeUint(vmsdk::ArgsIterator &itr,
                                        absl::string_view label) {
  VMSDK_ASSIGN_OR_RETURN(auto tok, itr.GetStringView());
  itr.Next();
  uint64_t value = 0;
  if (tok.empty() || !absl::SimpleAtoi(tok, &value)) {
    return absl::InvalidArgumentError(absl::StrCat(
        label, " must be a non-negative integer, got `", tok, "`"));
  }
  return value;
}

// Reads a block's leading token count -- the `<n>` in `KNN <n> ...`,
// `RANGE <n> ...` and `COMBINE RRF <n> ...`, which says how many argv tokens
// the block spans. Whole-token for the same reason the sub-args are: the
// shared ParseParamValue accepted a numeric prefix, so `KNN 2abc` and
// `COMBINE RRF 4.5` silently read 2 and 4. Bounded to uint32_t because that is
// what ArgsIterator::SubIterator takes.
absl::StatusOr<uint32_t> ParseBlockTokenCount(vmsdk::ArgsIterator &itr,
                                              absl::string_view label) {
  VMSDK_ASSIGN_OR_RETURN(auto value, ParseWholeUint(itr, label));
  if (value > std::numeric_limits<uint32_t>::max()) {
    return absl::InvalidArgumentError(
        absl::StrCat(label, " is out of range, got `", value, "`"));
  }
  return static_cast<uint32_t>(value);
}

// True for everything except NaN and +/-infinity, whose IEEE-754 form is the
// only one with every exponent bit set.
//
// `std::isfinite` cannot be used: this project builds with `-ffast-math`,
// which implies `-ffinite-math-only` and lets the compiler fold the call to
// `true`. It did -- `ALPHA inf` was accepted and produced infinite scores
// with the call in place. src/expr/value.cc and src/indexes/scoring/scorer.h
// avoid the same trap the same way.
bool IsFiniteDouble(double value) {
  static constexpr uint64_t kExponentMask = 0x7FF0000000000000ULL;
  uint64_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  return (bits & kExponentMask) != kExponentMask;
}

// Reads a whole token as a finite real number. `absl::SimpleAtod` accepts
// `nan` and `inf`, and overflows `1e400` to infinity, any of which then
// propagates through every fused score; the reference rejects all three.
absl::StatusOr<double> ParseWholeFinite(vmsdk::ArgsIterator &itr,
                                        absl::string_view keyword) {
  VMSDK_ASSIGN_OR_RETURN(auto tok, itr.GetStringView());
  itr.Next();
  double value = 0;
  if (tok.empty() || !absl::SimpleAtod(tok, &value) || !IsFiniteDouble(value)) {
    return absl::InvalidArgumentError(absl::StrCat(
        "COMBINE ", keyword, " must be a finite number, got `", tok, "`"));
  }
  return value;
}

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
// The query is any non-vector query the underlying parser accepts. A vector
// expression -- `*=>[KNN ...]` and friends -- is the VSIM clause's job and is
// refused; the arm it would build is only detectable once the query string has
// been parsed, so the rejection lives in ParseFtHybridCommand.
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
    // POLICY is a top-level clause, so it would end the SEARCH arm here and
    // leave the parse reporting a missing VSIM clause. Say what is wrong
    // instead. (Inside VSIM it is a legitimate sub-clause; see
    // ParseVsimClause.)
    if (absl::EqualsIgnoreCase(next, kPolicyKw)) {
      return absl::InvalidArgumentError(
          "POLICY is not supported in the SEARCH clause");
    }
    if (IsTopLevelKeyword(next)) {
      break;
    }
    if (absl::EqualsIgnoreCase(next, kYieldScoreAsKw)) {
      itr.Next();
      VMSDK_ASSIGN_OR_RETURN(auto alias_sv, itr.GetStringView());
      itr.Next();
      VMSDK_RETURN_IF_ERROR(RejectReservedScoreAlias(alias_sv));
      per_arm_alias = std::string(alias_sv);
      arm->score_as = vmsdk::MakeUniqueValkeyString(alias_sv);
    } else if (absl::EqualsIgnoreCase(next, kScorerKw)) {
      itr.Next();
      // Same validator FT.SEARCH uses, so the two commands accept and reject
      // the same names with the same message. The arm's `scorer` is already
      // seeded from the `default-scorer` config; this overrides it.
      VMSDK_ASSIGN_OR_RETURN(auto scorer_sv, itr.GetStringView());
      itr.Next();
      VMSDK_ASSIGN_OR_RETURN(arm->scorer,
                             indexes::scoring::ParseScorerType(scorer_sv));
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

// VSIM arm: VSIM @field $param
//             [ KNN <count> [K <k>] [EF_RUNTIME <ef>] [SHARD_K_RATIO <r>]
//             | RANGE <count> RADIUS <r> [EPSILON <e>] ]
//             [YIELD_SCORE_AS name]
//
// The KNN/RANGE block is optional; omitting it means KNN with the default K.
// YIELD_SCORE_AS names the arm and sits after the block, never inside it.
absl::Status ParseVsimClause(MultiSearchParameters &env,
                             vmsdk::ArgsIterator &itr, bool *vsim_uses_range,
                             std::string *vsim_filter_storage,
                             std::string *vsim_query_storage) {
  *vsim_uses_range = false;
  // Owned by the caller, because arm->parse_vars.query_string is a view.
  std::string &vsim_filter = *vsim_filter_storage;
  std::string &vsim_query_string = *vsim_query_storage;
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

  // Mode: [KNN <count> ...] | [RANGE <count> ...]
  //
  // The block is optional. Omitting it means KNN with the default K, which is
  // what Redis does; `VSIM @vec $q YIELD_SCORE_AS vs` and `VSIM @vec $q
  // COMBINE ...` both land here with the next token already belonging to an
  // enclosing clause.
  arm->k = kDefaultKnnK;
  absl::string_view mode_sv;
  bool has_mode_block = false;
  if (auto next_or = itr.GetStringView(); next_or.ok()) {
    auto next = next_or.value();
    has_mode_block = absl::EqualsIgnoreCase(next, kKnnKw) ||
                     absl::EqualsIgnoreCase(next, kRangeKw);
    if (!has_mode_block && !IsTopLevelKeyword(next) &&
        !absl::EqualsIgnoreCase(next, kYieldScoreAsKw)) {
      return absl::InvalidArgumentError(
          absl::StrCat("VSIM expects KNN or RANGE, got `", next, "`"));
    }
    if (has_mode_block) {
      mode_sv = next;
      itr.Next();
    }
  }
  uint32_t inner_count = 0;
  if (has_mode_block) {
    VMSDK_ASSIGN_OR_RETURN(
        inner_count, ParseBlockTokenCount(itr, absl::StrCat("VSIM ", mode_sv)));
  }
  // With no block there is nothing to read, and the defaults set above stand.
  if (has_mode_block && absl::EqualsIgnoreCase(mode_sv, kKnnKw)) {
    // KNN <count> [K <k>] [EF_RUNTIME <ef>] [SHARD_K_RATIO <r>]
    //
    // YIELD_SCORE_AS is deliberately NOT accepted here: it names the arm, not
    // the KNN search, and belongs after the block (see the VSIM tail below).
    // Redis rejects it inside the block too.
    auto inner_itr_or = itr.SubIterator(inner_count);
    if (!inner_itr_or.ok() && inner_count > 0) {
      return inner_itr_or.status();
    }
    // `KNN 0` is an empty block: the defaults stand, exactly as when the block
    // is left out. SubIterator rejects a zero distance, so it is not asked.
    auto inner_itr = inner_count > 0 ? inner_itr_or.value()
                                     : vmsdk::ArgsIterator(nullptr, 0);
    itr.Next(inner_count);
    while (inner_itr.HasNext()) {
      VMSDK_ASSIGN_OR_RETURN(auto kw, inner_itr.GetStringView());
      inner_itr.Next();
      if (absl::EqualsIgnoreCase(kw, kKKw)) {
        VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(inner_itr, arm->k));
        VMSDK_RETURN_IF_ERROR(VerifyKnnK(arm->k));
      } else if (absl::EqualsIgnoreCase(kw, kEfRuntimeKw)) {
        unsigned ef = 0;
        VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(inner_itr, ef));
        VMSDK_RETURN_IF_ERROR(VerifyEfRuntime(ef));
        arm->ef = ef;
      } else if (absl::EqualsIgnoreCase(kw, kShardKRatioKw)) {
        // Parsed and discarded. It tunes how much of K each shard returns
        // during a cluster fanout; this implementation does not use it, and
        // the value does not change the result of a query, only how much work
        // the shards do to produce it. Accepted so a command written for Redis
        // is not rejected here.
        double shard_k_ratio = 0.0;
        VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(inner_itr, shard_k_ratio));
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
    if (!inner_itr_or.ok() && inner_count > 0) {
      return inner_itr_or.status();
    }
    auto inner_itr = inner_count > 0 ? inner_itr_or.value()
                                     : vmsdk::ArgsIterator(nullptr, 0);
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
      } else {
        return absl::InvalidArgumentError(
            absl::StrCat("Unknown VSIM RANGE sub-arg: `", kw, "`"));
      }
    }
    if (!seen_radius) {
      return absl::InvalidArgumentError("VSIM RANGE requires RADIUS");
    }
  }
  if (!*vsim_uses_range) {
    // Covers the default K, which applies when the KNN block is omitted or
    // empty and can exceed a configured max-vector-knn below it.
    VMSDK_RETURN_IF_ERROR(VerifyKnnK(arm->k));
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
    // FILTER, POLICY and BATCH_SIZE are checked before the top-level break,
    // because inside a VSIM clause they belong to the vector search. FILTER
    // means a pre-filter on it rather than the aggregate stage it means
    // everywhere else -- a FILTER meant as that stage comes after COMBINE,
    // which has already ended this loop -- and POLICY/BATCH_SIZE tune how that
    // search runs. Letting POLICY end the loop left the token after it to be
    // read as a top-level one: `KNN 2 K 5 POLICY BATCHES YIELD_SCORE_AS vs`
    // reached the aggregate parser as a stray `YIELD_SCORE_AS`.
    const bool vsim_scoped = absl::EqualsIgnoreCase(next, kFilterKw) ||
                             absl::EqualsIgnoreCase(next, kPolicyKw) ||
                             absl::EqualsIgnoreCase(next, kBatchSizeKw);
    if (IsTopLevelKeyword(next) && !vsim_scoped) {
      break;
    }
    if (absl::EqualsIgnoreCase(next, kYieldScoreAsKw)) {
      itr.Next();
      VMSDK_ASSIGN_OR_RETURN(auto alias_sv, itr.GetStringView());
      itr.Next();
      VMSDK_RETURN_IF_ERROR(RejectReservedScoreAlias(alias_sv));
      per_arm_alias = std::string(alias_sv);
      arm->score_as = vmsdk::MakeUniqueValkeyString(alias_sv);
    } else if (absl::EqualsIgnoreCase(next, kPolicyKw) ||
               absl::EqualsIgnoreCase(next, kBatchSizeKw)) {
      // Both pick how the vector search executes rather than what it answers,
      // so the value is read and discarded -- the same thing the top-level
      // POLICY handler does with it.
      itr.Next();
      if (!itr.HasNext()) {
        return absl::InvalidArgumentError(
            absl::StrCat(next, " requires a value"));
      }
      itr.Next();
    } else if (absl::EqualsIgnoreCase(next, kFilterKw)) {
      // FILTER [count] <search-expression> [POLICY <p>] [BATCH_SIZE <n>]
      //
      // A pre-filter on the vector search, in the same query language the
      // SEARCH arm uses -- not the aggregate FILTER's expression language,
      // which is what the token means once the VSIM clause has ended. The
      // count is optional, and when given it counts every token that follows,
      // the POLICY options included.
      itr.Next();
      VMSDK_ASSIGN_OR_RETURN(auto first, itr.GetStringView());
      uint32_t token_count = 0;
      if (absl::SimpleAtoi(first, &token_count)) {
        itr.Next();
      } else {
        token_count = 1;  // no count given: the expression alone
      }
      if (token_count == 0) {
        return absl::InvalidArgumentError("VSIM FILTER requires an expression");
      }
      VMSDK_ASSIGN_OR_RETURN(auto expr_sv, itr.GetStringView());
      itr.Next();
      vsim_filter = std::string(expr_sv);
      // Anything else inside the count tunes how the pre-filter is executed --
      // POLICY picks between an ad-hoc and a batched strategy, BATCH_SIZE sizes
      // the batches. Both change how much work the search does rather than what
      // it answers, so they are consumed and discarded. The count is a raw
      // token count, as it is on the reference engine, so the tokens inside it
      // are taken as they come rather than validated.
      for (uint32_t consumed = 1; consumed < token_count; ++consumed) {
        if (!itr.HasNext()) {
          return absl::InvalidArgumentError(
              "VSIM FILTER count exceeds the arguments given");
        }
        itr.Next();
      }
      // And the same options are accepted outside a count, which is how the
      // command reference writes them.
      while (itr.HasNext()) {
        auto opt_or = itr.GetStringView();
        if (!opt_or.ok()) {
          break;
        }
        if (!absl::EqualsIgnoreCase(opt_or.value(), kPolicyKw) &&
            !absl::EqualsIgnoreCase(opt_or.value(), kBatchSizeKw)) {
          break;
        }
        itr.Next();
        if (!itr.HasNext()) {
          return absl::InvalidArgumentError(
              absl::StrCat(opt_or.value(), " requires a value"));
        }
        itr.Next();
      }
    } else {
      return absl::InvalidArgumentError(
          absl::StrCat("Unexpected token in VSIM clause: `", next, "`"));
    }
  }

  // With a pre-filter, the arm becomes an ordinary query in the FT.SEARCH
  // language -- `<filter>=>[KNN k @field $param]` -- and the existing parser
  // populates it, so the pre-filter gets the query planner's filtering for
  // free instead of a second implementation inside this clause.
  if (!vsim_filter.empty()) {
    std::string knn =
        absl::StrCat("=>[KNN ", arm->k, " @", arm->attribute_alias, " ",
                     arm->parse_vars.query_vector_string);
    if (arm->ef.has_value()) {
      absl::StrAppend(&knn, " EF_RUNTIME ", *arm->ef);
    }
    absl::StrAppend(&knn, "]");
    // Parenthesized: it is the spelling the reference engine requires for a
    // filter of more than one predicate -- it refuses `@a:x @b:y=>[KNN ...]`
    // as a syntax error and accepts `(@a:x @b:y)=>[KNN ...]` -- and it leaves
    // no question about what the `=>` binds to.
    vsim_query_string = absl::StrCat("(", vsim_filter, ")", knn);
    arm->parse_vars.query_string = vsim_query_string;
    // The filter decides membership, never the score.
    arm->vector_score_only = true;
  }

  env.arms.push_back(std::move(arm));
  env.per_arm_score_alias.push_back(per_arm_alias);
  return absl::OkStatus();
}

// COMBINE RRF      <count> [CONSTANT n] [WINDOW n] [YIELD_SCORE_AS name]
//       | LINEAR   <count> [ALPHA <a> BETA <b>] [WINDOW n]
//                         [YIELD_SCORE_AS name]
//       | FUNCTION <count> EXPR <expr> [WINDOW n] [YIELD_SCORE_AS name]
//
// <count> is the number of argv tokens in the block, not a count of
// sub-arguments. Sub-arguments may appear in any order within it. EXPR is
// required by FUNCTION; LINEAR's two weights are optional but go together,
// defaulting to ALPHA 0.3 / BETA 0.7 only when neither is written. Every other
// sub-argument is optional, and each is rejected by the methods that do not
// take it.
absl::Status ParseCombineClause(MultiSearchParameters &env,
                                vmsdk::ArgsIterator &itr) {
  VMSDK_ASSIGN_OR_RETURN(auto method_sv, itr.GetStringView());
  itr.Next();
  uint32_t inner_count = 0;
  VMSDK_ASSIGN_OR_RETURN(inner_count, ParseBlockTokenCount(itr, "COMBINE"));
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
      if (env.fusion.method != FusionConfig::Method::kRRF) {
        return absl::InvalidArgumentError(
            "COMBINE CONSTANT is only valid with RRF");
      }
      // Fractional constants are meaningful and the reference honours them,
      // so this is a real number. Negative is refused: it puts a pole at
      // `rank == -constant`, and the reference answers infinity there.
      VMSDK_ASSIGN_OR_RETURN(auto v, ParseWholeFinite(inner_itr, kConstantKw));
      if (v < 0.0) {
        return absl::InvalidArgumentError(absl::StrCat(
            "COMBINE CONSTANT must not be negative, got `", v, "`"));
      }
      env.fusion.rrf_constant = v;
    } else if (absl::EqualsIgnoreCase(kw, kWindowKw)) {
      VMSDK_ASSIGN_OR_RETURN(
          auto v,
          ParseWholeUint(inner_itr, absl::StrCat("COMBINE ", kWindowKw)));
      // The ceiling is max-combine-window (a Dev config, default 1,000,000),
      // never wider than uint32_t because that config's own maximum is
      // UINT_MAX. WINDOW decides how many of each arm's results take part in
      // fusion and, in cluster mode, how many each shard is asked to fetch, so
      // an unbounded value is paid for on every shard as well as here.
      const uint64_t max_window =
          static_cast<uint64_t>(options::GetMaxCombineWindow().GetValue());
      if (v > max_window) {
        return absl::InvalidArgumentError(
            absl::StrCat("COMBINE WINDOW is out of range, got `", v,
                         "` (maximum is ", max_window, ")"));
      }
      // `WINDOW 0` is accepted and means "as wide as allowed", so it resolves
      // to the ceiling rather than staying a sentinel. Leaving a zero here
      // would mean asking for no cap and getting one anyway: the fanout turns
      // a zero window into a per-shard fetch limit of 10 (see the
      // std::max(window, 10) in PerformMultiSearchFanoutAsync), which is the
      // opposite of unlimited. Nothing downstream has to know about 0 now.
      env.fusion.window =
          v == 0 ? static_cast<uint32_t>(max_window) : static_cast<uint32_t>(v);
      saw_window = true;
    } else if (absl::EqualsIgnoreCase(kw, kAlphaKw)) {
      if (env.fusion.method != FusionConfig::Method::kLinear) {
        return absl::InvalidArgumentError(
            "COMBINE ALPHA is only valid with LINEAR");
      }
      VMSDK_ASSIGN_OR_RETURN(auto v, ParseWholeFinite(inner_itr, kAlphaKw));
      env.fusion.alpha = v;
      saw_alpha = true;
    } else if (absl::EqualsIgnoreCase(kw, kBetaKw)) {
      if (env.fusion.method != FusionConfig::Method::kLinear) {
        return absl::InvalidArgumentError(
            "COMBINE BETA is only valid with LINEAR");
      }
      VMSDK_ASSIGN_OR_RETURN(auto v, ParseWholeFinite(inner_itr, kBetaKw));
      env.fusion.beta = v;
      saw_beta = true;
    } else if (absl::EqualsIgnoreCase(kw, kYieldScoreAsKw)) {
      VMSDK_ASSIGN_OR_RETURN(auto alias_sv, inner_itr.GetStringView());
      inner_itr.Next();
      VMSDK_RETURN_IF_ERROR(RejectReservedScoreAlias(alias_sv));
      env.output_score_name = std::string(alias_sv);
      env.output_score_name_explicit = true;
    } else if (absl::EqualsIgnoreCase(kw, kExprKw)) {
      if (env.fusion.method != FusionConfig::Method::kFunction) {
        return absl::InvalidArgumentError(
            "COMBINE EXPR is only valid with FUNCTION");
      }
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
    // The reference is deliberately asymmetric here and this matches it:
    // writing neither weight takes the 0.3/0.7 defaults, while writing exactly
    // one is an error rather than defaulting the other. A half-written pair is
    // far more likely to be a typo than a request for a default, so refusing
    // it loses nothing that the bare form does not already offer.
    if (!saw_alpha && !saw_beta) {
      env.fusion.alpha = kDefaultLinearAlpha;
      env.fusion.beta = kDefaultLinearBeta;
    } else if (!saw_beta) {
      return absl::InvalidArgumentError(
          "COMBINE LINEAR: missing value for BETA");
    } else if (!saw_alpha) {
      return absl::InvalidArgumentError(
          "COMBINE LINEAR: missing value for ALPHA");
    }
  }
  if (env.fusion.method == FusionConfig::Method::kFunction && !saw_expr) {
    return absl::InvalidArgumentError("COMBINE FUNCTION requires EXPR");
  }
  // For COMBINE FUNCTION, the user expression is computed per document and
  // typically expected to see EVERY matching candidate (not the RRF/LINEAR
  // top-window slice). If the caller did not set WINDOW explicitly, default to
  // the widest window allowed so the fusion stage does not silently drop docs
  // the user expected the function to score. "As wide as allowed" rather than
  // literally unlimited: a zero would reach the fanout, which turns it into a
  // per-shard fetch limit of 10 and truncates far harder than any window.
  if (env.fusion.method == FusionConfig::Method::kFunction && !saw_window) {
    env.fusion.window =
        static_cast<uint32_t>(options::GetMaxCombineWindow().GetValue());
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status ParseFtHybridCommand(MultiSearchParameters &env,
                                  vmsdk::ArgsIterator &itr) {
  bool vsim_uses_range = false;
  // The VSIM arm's pre-filter and the query string synthesized from it. Held
  // here because the arm keeps a string_view into the latter and both have to
  // outlive the parse.
  std::string vsim_filter;
  std::string vsim_query_string;
  // 1. SEARCH (mandatory, must come first)
  VMSDK_RETURN_IF_ERROR(ParseSearchClause(env, itr));
  // 2. VSIM (mandatory, must follow)
  VMSDK_RETURN_IF_ERROR(ParseVsimClause(env, itr, &vsim_uses_range,
                                        &vsim_filter, &vsim_query_string));
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
    // the score_as -> Neighbor::score plumbing in CreateRecordsFromNeighbors
    // fires (it gates on AggregateParameters::IsVectorQuery, which checks
    // whether attribute_alias is non-empty). Reuse the VSIM arm's vector
    // field; the aggregate code calls index_schema->GetIdentifier on it, so
    // it must name a real vector index in the schema.
    if (env.arms.size() > 1 && !env.arms[1]->attribute_alias.empty()) {
      env.agg->attribute_alias = env.arms[1]->attribute_alias;
    }
    // Set the score_as on the aggregate the same way ft_aggregate.cc:91 does
    // for plain FT.AGGREGATE — so the post-fusion aggregate pipeline routes
    // the fused score into a known field name. That name is `__score` unless
    // COMBINE ... YIELD_SCORE_AS renamed it. Whether the column reaches the
    // reply is decided after the suffix parse, once the LOAD clause is known.
    env.agg->score_as = vmsdk::MakeUniqueValkeyString(env.output_score_name);
    // Pre-populate the two reserved record slots that AggregateParameters
    // expects: __key at kKeyColumn and the score alias at kScoreColumn.
    // Mirrors AggregateParameters::ParseCommand. Without this, MakeReference
    // fails when any APPLY/FILTER/SORTBY references @<score_alias>.
    // AddRecordAttribute dedups on the output name and hands back the column
    // that already carries it, so an alias equal to one of these two names
    // would land the score in the wrong slot. RejectReservedScoreAlias and
    // the `__score` guard below are what keep that from happening; the checks
    // here are the backstop, and they report rather than abort so no future
    // alias path can take the server down.
    if (env.agg->AddRecordAttribute("__key", "__key", "__key",
                                    indexes::IndexerType::kNone) !=
        aggregate::AggregateParameters::kKeyColumn) {
      return absl::InternalError("FT.HYBRID could not seed the `__key` column");
    }
    auto score_sv = vmsdk::ToStringView(env.agg->score_as.get());
    if (env.agg->AddRecordAttribute(score_sv, score_sv, score_sv,
                                    indexes::IndexerType::kNone) !=
        aggregate::AggregateParameters::kScoreColumn) {
      return absl::InternalError(absl::StrCat("FT.HYBRID could not seed the `",
                                              score_sv, "` score column"));
    }
  }
  // The aggregate parser uses parse_vars_.index_interface_ during expression
  // compilation (APPLY/FILTER/REDUCE) to resolve @<field> references. Stack
  // allocate the interface — it only needs to be alive during the agg-parser
  // walk below, then we clear the pointer before returning.
  //
  // It also answers for the per-arm YIELD_SCORE_AS aliases, which is what
  // makes `SORTBY 2 @vs DESC` resolve. Fusion writes each arm's score into the
  // fused neighbor's attribute_contents under the alias, but nothing declares
  // it as a record column, so without this the stage parser asks the schema,
  // finds no such field, and rejects the whole command. Answering here rather
  // than pre-registering the columns means a column is created only for an
  // alias a stage actually names; a query that merely returns the alias is
  // untouched.
  struct HybridIndexInterface : public aggregate::IndexInterface {
    std::shared_ptr<IndexSchema> schema;
    // The named arms' aliases, and the aggregate they would become columns
    // of. Held by pointer: both outlive this stack-allocated interface.
    const std::vector<std::optional<std::string>> *arm_aliases;
    const aggregate::AggregateParameters *agg;
    HybridIndexInterface(std::shared_ptr<IndexSchema> s,
                         const std::vector<std::optional<std::string>> *aliases,
                         const aggregate::AggregateParameters *a)
        : schema(std::move(s)), arm_aliases(aliases), agg(a) {}

    // True when `name` is an arm's score alias and a stage may reach it.
    //
    // `LOAD *` is excluded to match the reference, which resolves a per-arm
    // alias under every LOAD clause except that one -- there it answers
    // "Property `vs` not loaded nor in schema", on the reasoning that `LOAD *`
    // projects the document's own fields and a synthesized score is not one.
    // Read at resolve time, so it is set whenever the LOAD clause precedes the
    // stage, which is how the clause is written. A `LOAD *` placed after the
    // stage would not be seen; the reference is order-sensitive here too (see
    // unsupported_tests.md 5.4c), so this is left as it is.
    bool IsReachableArmAlias(absl::string_view name) const {
      if (agg != nullptr && agg->loadall_) {
        return false;
      }
      for (const auto &alias : *arm_aliases) {
        if (alias.has_value() && *alias == name) {
          return true;
        }
      }
      return false;
    }

    absl::StatusOr<indexes::IndexerType> GetFieldType(
        absl::string_view s) const override {
      if (IsReachableArmAlias(s)) {
        // Numeric, not kNone: the column is filled by parsing the text fusion
        // wrote, and ProcessFieldValue only produces a number for kNumeric.
        // A string column would make SORTBY compare lexically, ordering 0.9
        // above 0.53. kNone is also refused outright by MakeReference's type
        // check on this path.
        return indexes::IndexerType::kNumeric;
      }
      VMSDK_ASSIGN_OR_RETURN(auto indexer, schema->GetIndex(s));
      return indexer->GetIndexerType();
    }
    absl::StatusOr<std::string> GetIdentifier(
        absl::string_view alias) const override {
      if (IsReachableArmAlias(alias)) {
        // Its own identifier. That is the key fusion used in
        // attribute_contents, which is what the record-population step matches
        // a column against, and it keeps MakeReference on the branch that
        // honours the numeric type above.
        return std::string(alias);
      }
      return schema->GetIdentifier(alias);
    }
    absl::StatusOr<std::string> GetAlias(
        absl::string_view identifier) const override {
      if (IsReachableArmAlias(identifier)) {
        return std::string(identifier);
      }
      return schema->GetAlias(identifier);
    }
  };
  HybridIndexInterface ii(env.index_schema, &env.per_arm_score_alias,
                          env.agg.get());
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
      // Range-checked the way FT.SEARCH and FT.AGGREGATE check it in
      // VerifyQueryString, except that FT.HYBRID implements the one dialect,
      // so the range is a single value rather than 2..4.
      itr.Next();
      uint32_t dialect = 0;
      VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, dialect));
      if (dialect != 2) {
        return absl::InvalidArgumentError(
            "DIALECT requires a non negative integer >=2 and <= 2");
      }
      env.agg->dialect = dialect;
      continue;
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

  // SORTBY's retention bound is a property of the whole pipeline, so it can
  // only be settled once every stage is parsed. FT.AGGREGATE does this at the
  // end of its own ParseCommand; FT.HYBRID drives the aggregate parser itself
  // and so has to ask too. Without it a sorted reply is capped at the SORTBY
  // stage's parse-time default of 10 whatever LIMIT asked for -- measured,
  // `SORTBY 2 @price ASC LIMIT 0 100` returned 10 rows against the
  // reference's 24.
  aggregate::ResolveSortByBounds(*env.agg);

  const bool no_load_clause = env.agg->loads_.empty() && !env.agg->loadall_;

  // A LOAD clause may name the score column back into the projection it
  // otherwise replaces -- `LOAD 1 @__score`, or `LOAD 2 @price @__score`.
  // That is as explicit a request as COMBINE ... YIELD_SCORE_AS, and the
  // reference honours it, so it must not be suppressed.
  //
  // Only `identifier`, the name as written, counts. Matching `alias` too
  // would catch `LOAD 3 @price AS __score`, which renames a different field
  // onto the score's name and asks for no score at all: un-suppressing there
  // put two columns called `__score` in one reply, one holding the fused
  // score and one holding the price.
  bool a_load_names_the_score = false;
  for (const auto &load : env.agg->loads_) {
    if (load.identifier == env.output_score_name) {
      a_load_names_the_score = true;
      break;
    }
  }

  // Measured against the reference: with no LOAD the default projection is
  // the document key and the fused score, so `__score` is in the reply and an
  // APPLY/SORTBY can reference it. Any LOAD clause -- `LOAD *` or a named
  // list -- replaces that default, and the fused score drops out unless it
  // was asked for by name, either by COMBINE ... YIELD_SCORE_AS or by the
  // LOAD clause itself. The column stays registered either way so the
  // pipeline can still sort on it.
  if (!no_load_clause && !env.output_score_name_explicit &&
      !a_load_names_the_score) {
    env.agg->suppressed_reply_column_ =
        aggregate::AggregateParameters::kScoreColumn;
  }

  // `COMBINE ... YIELD_SCORE_AS __score` with no LOAD names the column the
  // default projection already generates. The reference rejects that rather
  // than emitting one column twice.
  if (no_load_clause && env.output_score_name_explicit &&
      env.output_score_name == kDefaultOutputScoreName) {
    return absl::InvalidArgumentError(
        absl::StrCat("YIELD_SCORE_AS `", kDefaultOutputScoreName,
                     "` collides with the default score column; either rename "
                     "it or give a LOAD clause"));
  }

  // With no LOAD clause at all, FT.HYBRID replies with the document key and
  // the score aliases. FT.AGGREGATE has no such default -- it loads `__key`
  // only when the LOAD clause names it -- so ask for it here, before the
  // clause is resolved.
  if (no_load_clause) {
    env.agg->loads_.push_back(aggregate::LoadField{
        .identifier = "__key", .alias = "__key", .renamed = false});
  }

  // Turn the LOAD clause into record columns and into the return_attributes
  // the fused content fetch reads, the same way FT.AGGREGATE does at the end
  // of its own parse. Without this the fetch has no attribute list and pulls
  // every field of every key, so LOAD would name columns but never narrow the
  // reply.
  VMSDK_RETURN_IF_ERROR(aggregate::ManipulateReturnsClause(*env.agg));

  // ManipulateReturnsClause sets no_content when the LOAD clause asks for no
  // database field. For FT.AGGREGATE that means there is nothing to read off
  // a record; for FT.HYBRID there always is, because fusion injects the
  // per-arm score aliases into each neighbor's attribute_contents. Leaving
  // no_content set would drop them from the reply. What is *fetched* is
  // narrowed by the resolver's return_attributes, not by this flag.
  env.agg->no_content = false;

  // FT.HYBRID bounds its reply at 10 rows when the caller writes no LIMIT --
  // unlike FT.AGGREGATE, which returns everything. An explicit LIMIT stays
  // where the caller put it in the pipeline; only the default is appended, so
  // it runs after every other stage.
  {
    auto &stages = env.agg->stages_;
    const bool has_limit =
        std::any_of(stages.begin(), stages.end(), [](const auto &stage) {
          return dynamic_cast<const aggregate::Limit *>(stage.get()) != nullptr;
        });
    if (!has_limit) {
      auto limit_stage = std::make_unique<aggregate::Limit>();
      limit_stage->offset_ = 0;
      limit_stage->limit_ = kDefaultHybridLimit;
      stages.push_back(std::move(limit_stage));
    }
  }

  // TIMEOUT is consumed by the aggregate-suffix parser into
  // env.agg->timeout_ms. Propagate it back onto the envelope so that
  // ExecuteCommand — which builds the cancellation token from env.timeout_ms
  // AFTER ParseAfterIndex returns — honors the caller's requested timeout
  // instead of the pre-parse default.
  env.timeout_ms = env.agg->timeout_ms;
  // FT.SEARCH bounds TIMEOUT in VerifyQueryString, which FT.HYBRID never
  // reaches; same limit, same message.
  const auto max_timeout_ms = options::GetMaxTimeoutMs().GetValue();
  if (env.timeout_ms > static_cast<uint64_t>(max_timeout_ms)) {
    return absl::InvalidArgumentError(
        absl::StrCat(kTimeoutParam,
                     " must be a positive integer greater than 0 and "
                     "cannot exceed ",
                     max_timeout_ms, "."));
  }

  // Now that PARAMS (if any) are populated on env.agg->parse_vars.params,
  // share the params map with each arm so $name resolves identically. Also
  // run the per-arm pre/post-parse so an arm carrying a query string gets its
  // k/ef/query-blob populated.
  for (size_t arm_index = 0; arm_index < env.arms.size(); ++arm_index) {
    auto &arm = env.arms[arm_index];
    arm->parse_vars.params = env.agg->parse_vars.params;
    arm->timeout_ms = env.timeout_ms;
    arm->cancellation_token = env.cancellation_token;
    // Two paths:
    //  - Any arm that came from a non-empty query string -- the SEARCH arm,
    //    and a VSIM arm whose FILTER was rewritten into one: run the existing
    //    PreParseQueryString / PostParseQueryString pipeline.
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
      // A query string arm reaches this check inside PostParseQueryString ->
      // PostParseVectorParameters; a pure VSIM arm skips both, so it has to
      // ask here. Routing this branch through PostParseVectorParameters is
      // not an option: that function reads parse_vars.k_string/ef_string/
      // score_as_string, none of which the VSIM clause parser populates (it
      // sets arm->k, arm->ef and arm->score_as directly), so it would die on
      // `vmsdk::To<unsigned>("")` before reaching the size check. The message
      // is spelled to match FT.SEARCH's, prefix included, so both spellings
      // of the same mistake read the same. Without it the wrong size is only
      // caught by VectorHNSW/VectorFlat::Search at execution, where
      // enable-partial-results turns an error into a silently empty arm.
      auto *vector_index = dynamic_cast<indexes::VectorBase *>(index.get());
      CHECK(vector_index != nullptr);
      if (arm->query.size() !=
          static_cast<size_t>(vector_index->GetVectorDataSize())) {
        return absl::InvalidArgumentError(absl::StrCat(
            "Error parsing vector similarity parameters: query vector blob "
            "size (",
            arm->query.size(), ") does not match index's expected size (",
            vector_index->GetVectorDataSize(), ")."));
      }
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
      // The vector arm is VSIM's. A SEARCH arm written as a vector query --
      // `*=>[KNN 10 @vec $q]` -- would reach the index by a path that neither
      // the VSIM clause's K/EF_RUNTIME bounds nor its scoring rules see, so it
      // is refused rather than quietly honoured. PreParseQueryString is what
      // sets attribute_alias from the query string, so this is the first point
      // at which it can be asked. Only the SEARCH arm is checked: the VSIM arm
      // reaches here too when a FILTER rewrote it into a query string, and
      // that one is a vector query by construction.
      if (arm_index == 0 && arm->IsVectorQuery()) {
        return absl::InvalidArgumentError(
            "A vector query is not supported in the SEARCH clause; use VSIM");
      }
    }
    // Record now whether this arm's score is a raw distance: `arms` is emptied
    // at dispatch (each shim is moved into SearchAsync), so fusion cannot ask
    // the arm later. Neighbor::score is a KNN distance only for a vector arm;
    // a text SEARCH arm carries a BM25-style relevance score instead. A VSIM
    // arm's score stays its distance whatever its pre-filter contains, which
    // is what vector_score_only says; the QueryHasTextPredicate arm of the
    // test guards the `text=>[KNN ...]` shape that ApplyHybridTextScore
    // overwrites with text relevance, which no FT.HYBRID arm can be written as
    // any more but which costs nothing to keep answering correctly.
    env.per_arm_score_is_distance.push_back(
        arm->IsVectorQuery() &&
        (arm->vector_score_only || !QueryHasTextPredicate(*arm)));
    // And which metric produced it: the similarity a distance maps to differs
    // by metric, and `arms` is emptied at dispatch.
    auto metric = data_model::DISTANCE_METRIC_UNSPECIFIED;
    if (arm->IsVectorQuery() && env.index_schema != nullptr) {
      auto index = env.index_schema->GetIndex(arm->attribute_alias);
      if (index.ok()) {
        auto *vector_index =
            dynamic_cast<const indexes::VectorBase *>(index->get());
        if (vector_index != nullptr) {
          metric = vector_index->GetDistanceMetric();
        }
      }
    }
    env.per_arm_distance_metric.push_back(metric);
  }

  // Clear the now-stale stack-local index_interface_ pointer.
  env.agg->parse_vars_.index_interface_ = nullptr;

  // A score alias naming a column the LOAD clause explicitly emits is
  // rejected rather than silently resolved. Both would land in the same reply
  // column, and the caller can rename either side. `LoadField::alias` is the
  // emitted name, so this covers `LOAD 3 @price AS cost` as well as a plain
  // `LOAD 1 @price`.
  //
  // `LOAD *` names no fields, so it is not a collision: it emits whatever the
  // document happens to carry, which the query cannot be expected to know.
  // Those are resolved at runtime instead, under one rule -- an explicitly
  // named score always beats a database field of the same name. The per-arm
  // aliases are attached by AttachArmScore, the fused score by the score
  // column in CreateRecordsFromNeighbors, and both now win over the fetched
  // field.
  if (env.agg != nullptr) {
    absl::flat_hash_set<absl::string_view> loaded;
    for (const auto &load : env.agg->loads_) {
      loaded.insert(load.alias);
    }
    auto reject_collision = [&](absl::string_view alias) -> absl::Status {
      if (loaded.contains(alias)) {
        return absl::InvalidArgumentError(
            absl::StrCat("YIELD_SCORE_AS `", alias,
                         "` collides with a column loaded by LOAD"));
      }
      return absl::OkStatus();
    };
    for (const auto &alias : env.per_arm_score_alias) {
      if (alias.has_value()) {
        VMSDK_RETURN_IF_ERROR(reject_collision(*alias));
      }
    }
    // Only an explicitly named fused score can collide: the default `__score`
    // is suppressed whenever there is a LOAD clause to collide with.
    if (env.output_score_name_explicit) {
      VMSDK_RETURN_IF_ERROR(reject_collision(env.output_score_name));
    }
    // An arm naming the fused score's column writes its own score into it:
    // the record-population step matches a column by identifier, and the
    // alias fusion attached wins over the fused value already in the slot.
    // The reference keeps the fused score and ignores the arm's alias; rather
    // than pick a winner silently, refuse the clash. `__key` never reaches
    // here at all: it is refused at each YIELD_SCORE_AS site, which is the
    // only guard that runs before the reserved columns are seeded.
    for (const auto &alias : env.per_arm_score_alias) {
      if (alias.has_value() && *alias == env.output_score_name) {
        return absl::InvalidArgumentError(absl::StrCat(
            "YIELD_SCORE_AS `", *alias,
            "` collides with the fused score's column; rename one of them"));
      }
    }
  }

  // After per-arm parse: VSIM RANGE is parsed for shape but not yet
  // executable.
  if (vsim_uses_range) {
    return absl::UnimplementedError("VSIM RANGE is not yet supported; use KNN");
  }
  return absl::OkStatus();
}

}  // namespace valkey_search::query
