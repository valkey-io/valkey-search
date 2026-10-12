/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/ft_search.h"

#include <strings.h>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "src/commands/commands.h"
#include "src/commands/ft_search_parser.h"
#include "src/commands/results_reply.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"
#include "src/metrics.h"
#include "src/query/response_generator.h"
#include "src/query/search.h"
#include "src/valkey_search_options.h"
#include "value.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {
// A standalone/compound VR neighbor carries its distance in Neighbor::distance.
// A non-VR OR-branch match that lies outside the radius carries no VR distance;
// SearchVectorRangeQuery marks it with has_vr_distance=false (the distance
// float then holds a sort-last sentinel). Gate the yielded-distance field,
// sorting, and sort keys on the flag rather than the float, which is unreliable
// under -ffast-math (-ffinite-math-only folds infinity comparisons away).
inline bool HasVrDistance(const indexes::Neighbor &neighbor) {
  return neighbor.has_vr_distance;
}

// FT.SEARCH idx "*=>[KNN 10 @vec $BLOB AS score]" PARAMS 2 BLOB
// "\x12\xa9\xf5\x6c" DIALECT 2

// The number of matches a reply reports. A fanout sums each shard's count, so
// a vector query's is capped at K.
size_t TotalResults(const query::SearchResult &search_result,
                    const query::SearchParameters &parameters) {
  if (parameters.IsNonVectorQuery()) {
    return search_result.total_count;
  }
  return std::min(search_result.total_count, static_cast<size_t>(parameters.k));
}

bool HasTextRelevance(const SearchCommand &parameters) {
  return parameters.IsNonVectorQuery() ||
         query::QueryHasTextPredicate(parameters);
}

void ReplyScore(ValkeyModuleCtx *ctx, ValkeyModuleString &score_as,
                const indexes::Neighbor &neighbor) {
  ValkeyModule_ReplyWithString(ctx, &score_as);
  // The score_as field carries the vector distance (Redis' __<field>_score).
  // For pure vector queries Neighbor.score == distance; for hybrid text=>[KNN]
  // queries Neighbor.score is the text relevance while distance stays here.
  auto score_value = expr::FormatDouble(neighbor.distance);
  ValkeyModule_ReplyWithString(
      ctx, vmsdk::MakeUniqueValkeyString(score_value).get());
}

std::optional<std::string> GetSortKeyValue(const indexes::Neighbor &neighbor,
                                           const SearchCommand &command);

// If the SORTBY field matches the VR distance alias, returns the formatted
// distance for this neighbor (to be emitted with the numeric '#' prefix for
// WITHSORTKEYS). Returns std::nullopt when the SORTBY field is not the VR
// alias or the neighbor has no VR distance (has_vr_distance is false), so
// callers fall back to GetSortKeyValue(). vr_field is the single VR score
// field name (empty when the query has no VR predicate).
std::optional<std::string> GetVrSortKeyValue(const indexes::Neighbor &neighbor,
                                             const SearchCommand &command,
                                             const std::string &vr_field) {
  if (!command.sortby_parameter.has_value()) {
    return std::nullopt;
  }
  if (vr_field.empty() || vr_field != command.sortby_parameter->field) {
    return std::nullopt;
  }
  if (!HasVrDistance(neighbor)) {
    return std::nullopt;
  }
  return expr::FormatDouble(neighbor.distance);
}

// WITHSORTKEYS prefixes each sort key by the SORTBY field's declared type:
// '#' for NUMERIC fields, '$' for everything else (RediSearch-compatible).
bool IsSortByFieldNumeric(const SearchCommand &command,
                          const bool sort_by_vec_score) {
  // sort by vector is considered special numeric but cannot be determined from
  // IndexerType
  if (sort_by_vec_score) {
    return true;
  }
  if (!command.sortby_parameter.has_value()) {
    return false;
  }
  // The VR distance alias (from GetVrScoreFieldName) is a synthesized numeric
  // distance, not a stored attribute, so treat it as numeric for the
  // WITHSORTKEYS type prefix.
  const std::string vr_field = query::GetVrScoreFieldName(command);
  if (!vr_field.empty() && vr_field == command.sortby_parameter->field) {
    return true;
  }
  auto idx = command.index_schema->GetIndex(command.sortby_parameter->field);
  return idx.ok() &&
         idx.value()->GetIndexerType() == indexes::IndexerType::kNumeric;
}

// Returns std::nullopt when the query has no SORTBY or the document lacks the
// sort field.
std::optional<std::string> GetSortKeyValue(const indexes::Neighbor &neighbor,
                                           const SearchCommand &command) {
  if (!command.sortby_parameter.has_value() ||
      !neighbor.attribute_contents.has_value()) {
    return std::nullopt;
  }

  auto it = neighbor.attribute_contents->find(command.sortby_parameter->field);
  if (it == neighbor.attribute_contents->end()) {
    return std::nullopt;
  }

  return std::string(vmsdk::ToStringView(it->second.value.get()));
}

template <typename Comparator>
void PerformSortingOnRelevantPortion(std::vector<indexes::Neighbor> &neighbors,
                                     const SearchCommand &parameters,
                                     Comparator &&comparator) {
  auto amountToKeep = parameters.limit.first_index + parameters.limit.number;
  if (amountToKeep >= neighbors.size()) {
    std::stable_sort(neighbors.begin(), neighbors.end(), comparator);
  } else {
    std::partial_sort(neighbors.begin(), neighbors.begin() + amountToKeep,
                      neighbors.end(), comparator);
  }
}

}  // namespace

// Apply sorting to neighbors based on attribute values in attribute_contents
void ApplySorting(std::vector<indexes::Neighbor> &neighbors,
                  const SearchCommand &parameters) {
  if (!parameters.sortby_parameter.has_value() || neighbors.empty()) {
    return;
  }

  auto sortby = parameters.sortby_parameter.value();

  // If sorting by the vector range distance alias, sort directly by the
  // precomputed Neighbor::distance rather than looking it up in
  // attribute_contents. Single-VR model: at most one VR field per query.
  const std::string vr_field = query::GetVrScoreFieldName(parameters);
  if (!vr_field.empty() && vr_field == sortby.field) {
    auto distance_compare = [&](const indexes::Neighbor &a,
                                const indexes::Neighbor &b) -> bool {
      // A neighbor with no VR distance (a non-VR OR-branch match outside the
      // radius) must sort after all matched neighbors, in both ascending and
      // descending order.
      const bool a_unmatched = !HasVrDistance(a);
      const bool b_unmatched = !HasVrDistance(b);
      if (a_unmatched || b_unmatched) {
        return !a_unmatched && b_unmatched;
      }
      if (a.distance < b.distance) {
        return sortby.order == query::SortOrder::kAscending;
      }
      if (a.distance > b.distance) {
        return sortby.order == query::SortOrder::kDescending;
      }
      // Tie-break on key ascending for a deterministic order.
      return a.external_id->Str() < b.external_id->Str();
    };
    PerformSortingOnRelevantPortion(neighbors, parameters, distance_compare);
    return;
  }
  // A SORTBY on the vector score field (the KNN distance, reported via
  // score_as) orders by Neighbor.distance directly: the distance is a
  // synthesized reply field, not a stored attribute, so it is not present in
  // attribute_contents. Default order is ascending (nearest first).
  const bool is_vector_score =
      parameters.score_as &&
      sortby.field == vmsdk::ToStringView(parameters.score_as.get());

  // Check if field is a declared numeric attribute
  auto index_result = parameters.index_schema->GetIndex(sortby.field);
  bool is_numeric =
      index_result.ok() &&
      index_result.value()->GetIndexerType() == indexes::IndexerType::kNumeric;
  auto compare = [&](const indexes::Neighbor &a,
                     const indexes::Neighbor &b) -> bool {
    if (is_vector_score) {
      if (a.distance != b.distance) {
        return sortby.order == query::SortOrder::kAscending
                   ? a.distance < b.distance
                   : a.distance > b.distance;
      }
      // Tie-break on key ascending for a deterministic order.
      return a.external_id->Str() < b.external_id->Str();
    }
    if (!a.attribute_contents.has_value() ||
        !b.attribute_contents.has_value()) {
      return false;
    }

    auto it_a = a.attribute_contents->find(sortby.field);
    auto it_b = b.attribute_contents->find(sortby.field);

    if (it_a == a.attribute_contents->end()) {
      return false;
    }
    if (it_b == b.attribute_contents->end()) {
      return true;
    }

    auto str_a = vmsdk::ToStringView(it_a->second.value.get());
    auto str_b = vmsdk::ToStringView(it_b->second.value.get());

    expr::Value val_a, val_b;
    if (is_numeric) {
      auto num_a = vmsdk::To<double>(str_a).value_or(0.0);
      auto num_b = vmsdk::To<double>(str_b).value_or(0.0);
      val_a = expr::Value(num_a);
      val_b = expr::Value(num_b);
    } else {
      val_a = expr::Value(str_a);
      val_b = expr::Value(str_b);
    }

    auto cmp = expr::Compare(val_a, val_b);
    if (cmp == expr::Ordering::kLESS) {
      return sortby.order == query::SortOrder::kAscending;
    }
    if (cmp == expr::Ordering::kGREATER) {
      return sortby.order == query::SortOrder::kDescending;
    }
    return false;
  };

  PerformSortingOnRelevantPortion(neighbors, parameters, compare);
}

// Process neighbors for both vector and non-vector queries
absl::Status ProcessNeighborsForQuery(ValkeyModuleCtx *ctx,
                                      query::SearchResult &search_result,
                                      SearchCommand &command) {
  size_t original_size = search_result.neighbors.size();

  std::optional<std::string> vector_identifier = std::nullopt;

  if (command.IsVectorQuery()) {
    VMSDK_ASSIGN_OR_RETURN(
        vector_identifier,
        command.index_schema->GetIdentifier(command.attribute_alias));
  }
  // Handle vector queries

  query::ProcessNeighborsForReply(
      ctx, command.index_schema->GetAttributeDataType(),
      search_result.neighbors, command, vector_identifier);
  // Adjust total count based on neighbors removed during processing
  // due to filtering or missing attributes.
  search_result.total_count -= (original_size - search_result.neighbors.size());

  return absl::OkStatus();
}

namespace {

// The rows of an FT.SEARCH ... WITHCURSOR not yet read by the client.
class CursorSearchResult : public Cursor {
 public:
  CursorSearchResult(std::unique_ptr<SearchCommand> command, size_t next,
                     size_t end)
      : Cursor(command->index_schema_name, command->index_schema,
               *command->cursor_options),
        command_(std::move(command)),
        next_(next),
        end_(end) {
    command_->adopted_by_cursor = true;
    // Don't keep a dropped index alive; READ supplies the live schema.
    command_->index_schema = nullptr;
    // The query itself is over; only its saved output is still held.
    command_->DeclareOperationTerminated();
    // The rows already replied, the ones LIMIT skipped, and anything past the
    // LIMIT window are never read again.
    auto &neighbors = command_->search_result.neighbors;
    ReleaseRows(0, next_);
    ReleaseRows(end_, neighbors.size());
  }
  size_t RemainingRows() const override { return end_ - next_; }
  void ReplyRows(ValkeyModuleCtx *ctx,
                 const std::shared_ptr<IndexSchema> &index_schema,
                 size_t count) override {
    size_t n = std::min(count, RemainingRows());
    // Like Redis Search, a read reports no total under RESP3.
    auto reply = ResultsReply::Create(ctx, ResultsReply::Layout::kRows);
    reply->Begin(0, n);
    command_->index_schema = index_schema;
    command_->ReplyRows(ctx, *reply, command_->search_result, next_, next_ + n);
    command_->index_schema = nullptr;
    reply->End();
    // Hand back what the rows just replied were holding, rather than keeping
    // it until the whole cursor is destroyed.
    ReleaseRows(next_, next_ + n);
    next_ += n;
  }
  void ReleaseMainThreadState() override { command_->ReleaseMainThreadState(); }

 private:
  // Frees the key and the fetched content of the neighbors in [begin, end).
  void ReleaseRows(size_t begin, size_t end) {
    auto &neighbors = command_->search_result.neighbors;
    for (size_t i = begin; i < end; ++i) {
      neighbors[i] = indexes::Neighbor();
    }
  }

  std::unique_ptr<SearchCommand> command_;
  size_t next_;
  size_t end_;
};

}  // namespace

SearchCommand::RowFormat SearchCommand::GetRowFormat(bool resp3) const {
  RowFormat format;
  format.has_relevance = HasTextRelevance(*this);
  // The vector-distance sort key is numeric ('#').
  format.sort_by_vec_score =
      !IsNonVectorQuery() && sortby_parameter.has_value() && score_as &&
      sortby_parameter->field == vmsdk::ToStringView(score_as.get());
  // RESP2 rows drop the sort key under NOCONTENT; RESP3 rows keep it, as
  // Redis Search does. Resp2ResultsReply already leaves it out, but skipping
  // here keeps the compatibility-fix counters below unchanged for RESP2.
  if (with_sort_keys && (resp3 || !no_content)) {
    auto type_prefix = [&]() -> std::string {
      return IsSortByFieldNumeric(*this, format.sort_by_vec_score) ? "#" : "$";
    };
    // RESP3 replies are newer than these fixes, so no client relies on the old
    // sort keys there and they always follow Redis Search.
    format.sort_key_prefix =
        resp3 ? type_prefix()
              : VALKEY_SEARCH_COMPATIBILITY_FIX(
                    1, 3, 0, "ft_search_sortkey_type_prefix", type_prefix,
                    [&]() -> std::string { return "#"; });
    // Issue #1353 item 5.
    format.nil_absent_sort_key =
        resp3 || VALKEY_SEARCH_COMPATIBILITY_FIX(
                     1, 3, 0, "ft_search_sortkey_nil", [&]() { return true; },
                     [&]() { return false; });
  }
  if (IsVectorRangeQuery()) {
    format.vr_field = query::GetVrScoreFieldName(*this);
  }
  return format;
}

std::optional<std::string> SearchCommand::SortKey(
    const indexes::Neighbor &neighbor, const RowFormat &format) const {
  // Prefix the sort key: '#' for NUMERIC fields, '$' for string fields
  // (RediSearch-compatible). A SORTBY on the VR distance alias must emit the
  // formatted distance (Neighbor::distance) with the numeric '#' prefix; it
  // is not in attribute_contents, so GetSortKeyValue() would return "".
  std::optional<std::string> vr_value =
      GetVrSortKeyValue(neighbor, *this, format.vr_field);
  std::optional<std::string> value;
  std::string prefix = format.sort_key_prefix;
  if (vr_value.has_value()) {
    value = std::move(vr_value);
    prefix = "#";
  } else if (format.sort_by_vec_score) {
    value = expr::FormatDouble(neighbor.distance);
  } else {
    value = GetSortKeyValue(neighbor, *this);
  }
  if (!value.has_value() && format.nil_absent_sort_key) {
    return std::nullopt;
  }
  return prefix + value.value_or("");
}

size_t SearchCommand::ReplyAttributes(ValkeyModuleCtx *ctx,
                                      const indexes::Neighbor &neighbor,
                                      const RowFormat &format,
                                      bool unique_names) const {
  // Vector queries also reply the distance, as the score_as field. A
  // standalone/compound VR query (no KNN) instead reports its distance under
  // the $yield_distance_as (or AS) name (format.vr_field), and only for a
  // neighbor that carries a VR distance (not a non-VR OR-branch match outside
  // the radius).
  const bool is_vector = !IsNonVectorQuery();
  const bool emit_vr_field =
      !format.vr_field.empty() && HasVrDistance(neighbor);
  const auto &contents = neighbor.attribute_contents.value();
  size_t cnt = 0;
  if (return_attributes.empty()) {
    if (is_vector) {
      ReplyScore(ctx, *score_as, neighbor);
      ++cnt;
    }
    if (emit_vr_field) {
      ReplyScore(ctx, *vmsdk::MakeUniqueValkeyString(format.vr_field),
                 neighbor);
      ++cnt;
    }
    for (const auto &attribute_content : contents) {
      // Unique names leave out a field the distance is named like, as Redis
      // Search does.
      if (unique_names &&
          ((is_vector &&
            attribute_content.first == vmsdk::ToStringView(score_as.get())) ||
           (emit_vr_field && attribute_content.first == format.vr_field))) {
        continue;
      }
      ValkeyModule_ReplyWithString(ctx,
                                   attribute_content.second.GetIdentifier());
      ValkeyModule_ReplyWithString(ctx, attribute_content.second.value.get());
      ++cnt;
    }
    return cnt;
  }
  for (const auto &return_attribute : return_attributes) {
    absl::string_view ret_id =
        vmsdk::ToStringView(return_attribute.identifier.get());
    if (is_vector && vmsdk::ToStringView(score_as.get()) == ret_id) {
      ReplyScore(ctx, *score_as, neighbor);
      ++cnt;
      continue;
    }
    if (!format.vr_field.empty() && ret_id == format.vr_field) {
      if (HasVrDistance(neighbor)) {
        ReplyScore(ctx, *return_attribute.alias, neighbor);
        ++cnt;
      }
      continue;
    }
    auto it = contents.find(ret_id);
    if (it != contents.end()) {
      ValkeyModule_ReplyWithString(ctx, return_attribute.alias.get());
      ValkeyModule_ReplyWithString(ctx, it->second.value.get());
      ++cnt;
    }
  }
  return cnt;
}

void SearchCommand::ReplyRows(ValkeyModuleCtx *ctx, ResultsReply &reply,
                              const query::SearchResult &search_result,
                              size_t start, size_t end) const {
  auto format = GetRowFormat(reply.IsResp3());
  for (auto i = start; i < end; ++i) {
    const auto &neighbor = search_result.neighbors[i];
    ResultsReply::Row row;
    row.id = neighbor.external_id->Str();
    if (with_scores) {
      row.score = format.has_relevance ? neighbor.score : 0.0;
    }
    if (with_sort_keys) {
      row.sort_key = SortKey(neighbor, format);
    }
    if (!no_content) {
      row.fields = [&](bool unique_names) {
        return ReplyAttributes(ctx, neighbor, format, unique_names);
      };
    }
    reply.AddRow(row);
  }
}

// Replies through ResultsReply, see there for the RESP2 and RESP3 layouts.
// SendReply respects the Limit, see https://valkey.io/commands/ft.search/
void SearchCommand::SendReply(ValkeyModuleCtx *ctx,
                              query::SearchResult &search_result) {
  // Increment success counter.
  ++Metrics::GetStats().query_successful_requests_cnt;

  auto reply = ResultsReply::Create(
      ctx, cursor_options ? ResultsReply::Layout::kSearchCursor
                          : ResultsReply::Layout::kSearch);
  if (query::ShouldReturnNoResults(*this)) {
    reply->Begin(TotalResults(search_result, *this), 0);
    reply->End();
    return;
  }

  // NOCONTENT without SORTBY: skip content resolution entirely. A SORTBY
  // still needs the sort field resolved to order the reply (regression #1215),
  // so fall through to content resolution when sortby is present.
  const bool skip_content = no_content && !sortby_parameter.has_value();
  if (!skip_content) {
    auto status = ProcessNeighborsForQuery(ctx, search_result, *this);
    if (!status.ok()) {
      ++Metrics::GetStats().query_failed_requests_cnt;
      ValkeyModule_ReplyWithError(ctx, status.message().data());
      return;
    }
  }

  // INKEYS post-filter + sort
  if (inkeys.has_value()) {
    ApplyInkeysFilter(search_result, *inkeys);
  }
  if (!skip_content) {
    ApplySorting(search_result.neighbors, *this);
  }

  auto range = search_result.GetSerializationRange(*this);
  // WITHCURSOR replies at most COUNT rows now and keeps the rest in a cursor.
  const size_t count =
      cursor_options
          ? std::min(static_cast<size_t>(cursor_options->count), range.count())
          : range.count();
  reply->Begin(TotalResults(search_result, *this), count);
  ReplyRows(ctx, *reply, search_result, range.start_index,
            range.start_index + count);
  if (count == range.count()) {
    reply->End();
    return;
  }
  // The cursor keeps this command, including its search_result.
  CHECK(&search_result == &this->search_result);
  auto cursor = std::make_unique<CursorSearchResult>(
      std::unique_ptr<SearchCommand>(this), range.start_index + count,
      range.end_index);
  auto id =
      CursorTable::Instance().Insert(std::move(cursor), db_num, absl::Now());
  reply->End(id);
}

absl::Status FTSearchCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                         int argc) {
  return QueryCommand::Execute(ctx, argv, argc,
                               std::unique_ptr<QueryCommand>(new SearchCommand(
                                   ValkeyModule_GetSelectedDb(ctx))));
}

}  // namespace valkey_search
