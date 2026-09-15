/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

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
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "src/commands/commands.h"
#include "src/commands/ft_search_parser.h"
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
// FT.SEARCH idx "*=>[KNN 10 @vec $BLOB AS score]" PARAMS 2 BLOB
// "\x12\xa9\xf5\x6c" DIALECT 2

void ReplyAvailNeighbors(ValkeyModuleCtx *ctx,
                         const query::SearchResult &search_result,
                         const query::SearchParameters &parameters) {
  if (parameters.IsNonVectorQuery()) {
    ValkeyModule_ReplyWithLongLong(ctx, search_result.total_count);
  } else {
    ValkeyModule_ReplyWithLongLong(
        ctx,
        std::min(search_result.total_count, static_cast<size_t>(parameters.k)));
  }
}

void ReplyScoreTopLevel(ValkeyModuleCtx *ctx, float score);

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
  auto score_value = absl::StrFormat("%.12g", neighbor.distance);
  ValkeyModule_ReplyWithString(
      ctx, vmsdk::MakeUniqueValkeyString(score_value).get());
}

// Reply with just the score value as a top-level element (Redis WITHSCORES
// format: score appears between document ID and attributes array).
void ReplyScoreTopLevel(ValkeyModuleCtx *ctx, float score) {
  auto score_value = absl::StrFormat("%.12g", score);
  ValkeyModule_ReplyWithString(
      ctx, vmsdk::MakeUniqueValkeyString(score_value).get());
}

std::string GetSortKeyValue(const indexes::Neighbor &neighbor,
                            const SearchCommand &command);

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
  auto idx = command.index_schema->GetIndex(command.sortby_parameter->field);
  return idx.ok() &&
         idx.value()->GetIndexerType() == indexes::IndexerType::kNumeric;
}

// Helper function to get the sort key value for a neighbor
std::string GetSortKeyValue(const indexes::Neighbor &neighbor,
                            const SearchCommand &command) {
  if (!command.sortby_parameter.has_value() ||
      !neighbor.attribute_contents.has_value()) {
    return "";
  }

  auto it = neighbor.attribute_contents->find(command.sortby_parameter->field);
  if (it == neighbor.attribute_contents->end()) {
    return "";
  }

  return std::string(vmsdk::ToStringView(it->second.value.get()));
}

}  // namespace
// Apply sorting to neighbors based on attribute values in attribute_contents
void ApplySorting(std::vector<indexes::Neighbor> &neighbors,
                  const SearchCommand &parameters) {
  if (!parameters.sortby_parameter.has_value() || neighbors.empty()) {
    return;
  }

  auto sortby = parameters.sortby_parameter.value();

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

  auto amountToKeep = parameters.limit.first_index + parameters.limit.number;
  if (amountToKeep >= neighbors.size()) {
    std::stable_sort(neighbors.begin(), neighbors.end(), compare);
  } else {
    std::partial_sort(neighbors.begin(), neighbors.begin() + amountToKeep,
                      neighbors.end(), compare);
  }
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
                     size_t end, absl::Duration max_idle)
      : Cursor(command->db_num, command->index_schema_name,
               command->index_schema, max_idle),
        command_(std::move(command)),
        next_(next),
        end_(end) {
    command_->adopted_by_cursor = true;
    // Don't keep a dropped index alive; READ supplies the live schema.
    command_->index_schema = nullptr;
  }
  size_t RemainingRows() const override { return end_ - next_; }
  void ReplyRows(ValkeyModuleCtx *ctx,
                 const std::shared_ptr<IndexSchema> &index_schema,
                 size_t count) override {
    size_t n = std::min(count, RemainingRows());
    ValkeyModule_ReplyWithArray(ctx, n + 1);
    ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(n));
    command_->index_schema = index_schema;
    command_->ReplyRows(ctx, command_->search_result, next_, next_ + n);
    command_->index_schema = nullptr;
    next_ += n;
  }
  void ReleaseMainThreadState() override { command_->ReleaseMainThreadState(); }

 private:
  std::unique_ptr<SearchCommand> command_;
  size_t next_;
  size_t end_;
};

}  // namespace

SearchCommand::RowFormat SearchCommand::GetRowFormat() const {
  RowFormat format;
  format.has_relevance = HasTextRelevance(*this);
  // The vector-distance sort key is numeric ('#').
  format.sort_by_vec_score =
      !IsNonVectorQuery() && sortby_parameter.has_value() && score_as &&
      sortby_parameter->field == vmsdk::ToStringView(score_as.get());
  if (with_sort_keys && !no_content) {
    format.sort_key_prefix = VALKEY_SEARCH_COMPATIBILITY_FIX(
        1, 3, 0, "ft_search_sortkey_type_prefix",
        [&]() -> std::string {
          return IsSortByFieldNumeric(*this, format.sort_by_vec_score) ? "#"
                                                                       : "$";
        },
        [&]() -> std::string { return "#"; });
  }
  return format;
}

size_t SearchCommand::ReplyRowElements(ValkeyModuleCtx *ctx,
                                       const indexes::Neighbor &neighbor,
                                       const RowFormat &format) const {
  // Document ID
  ValkeyModule_ReplyWithString(
      ctx, vmsdk::MakeUniqueValkeyString(*neighbor.external_id).get());
  size_t elements = 1;

  // Score as top-level element when WITHSCORES is specified
  if (with_scores) {
    ReplyScoreTopLevel(ctx, format.has_relevance ? neighbor.score : 0.0f);
    ++elements;
  }
  if (no_content) {
    return elements;
  }

  // Prefix the sort key: '#' for NUMERIC fields, '$' for string fields
  // (RediSearch-compatible).
  if (with_sort_keys) {
    std::string value = format.sort_by_vec_score
                            ? absl::StrFormat("%.12g", neighbor.distance)
                            : GetSortKeyValue(neighbor, *this);
    ValkeyModule_ReplyWithString(
        ctx,
        vmsdk::MakeUniqueValkeyString(format.sort_key_prefix + value).get());
    ++elements;
  }

  // Vector queries also reply the distance, as the score_as field.
  const bool is_vector = !IsNonVectorQuery();
  const auto &contents = neighbor.attribute_contents.value();
  if (return_attributes.empty()) {
    ValkeyModule_ReplyWithArray(ctx, 2 * contents.size() + (is_vector ? 2 : 0));
    if (is_vector) {
      ReplyScore(ctx, *score_as, neighbor);
    }
    for (const auto &attribute_content : contents) {
      ValkeyModule_ReplyWithString(ctx,
                                   attribute_content.second.GetIdentifier());
      ValkeyModule_ReplyWithString(ctx, attribute_content.second.value.get());
    }
  } else {
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_LEN);
    size_t cnt = 0;
    for (const auto &return_attribute : return_attributes) {
      if (is_vector &&
          vmsdk::ToStringView(score_as.get()) ==
              vmsdk::ToStringView(return_attribute.identifier.get())) {
        ReplyScore(ctx, *score_as, neighbor);
        ++cnt;
        continue;
      }
      auto it =
          contents.find(vmsdk::ToStringView(return_attribute.identifier.get()));
      if (it != contents.end()) {
        ValkeyModule_ReplyWithString(ctx, return_attribute.alias.get());
        ValkeyModule_ReplyWithString(ctx, it->second.value.get());
        ++cnt;
      }
    }
    ValkeyModule_ReplySetArrayLength(ctx, 2 * cnt);
  }
  return elements + 1;
}

void SearchCommand::ReplyRows(ValkeyModuleCtx *ctx,
                              const query::SearchResult &search_result,
                              size_t start, size_t end) const {
  auto format = GetRowFormat();
  for (auto i = start; i < end; ++i) {
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_LEN);
    ValkeyModule_ReplySetArrayLength(
        ctx, ReplyRowElements(ctx, search_result.neighbors[i], format));
  }
}

// Without a cursor the reply is [total, row elements...], with each row's
// elements inline. With WITHCURSOR it is [total, [row...], cursor_id].
// SendReply respects the Limit, see https://valkey.io/commands/ft.search/
void SearchCommand::SendReply(ValkeyModuleCtx *ctx,
                              query::SearchResult &search_result) {
  // Increment success counter.
  ++Metrics::GetStats().query_successful_requests_cnt;

  if (query::ShouldReturnNoResults(*this)) {
    ValkeyModule_ReplyWithArray(ctx, cursor_options ? 3 : 1);
    ValkeyModule_ReplyWithLongLong(ctx, search_result.total_count);
    if (cursor_options) {
      ValkeyModule_ReplyWithArray(ctx, 0);
      ValkeyModule_ReplyWithLongLong(ctx, 0);
    }
    return;
  }

  if (!NoProcessingRequired()) {
    auto status = ProcessNeighborsForQuery(ctx, search_result, *this);
    if (!status.ok()) {
      ++Metrics::GetStats().query_failed_requests_cnt;
      ValkeyModule_ReplyWithError(ctx, status.message().data());
      return;
    }
    ApplySorting(search_result.neighbors, *this);
  }

  auto range = search_result.GetSerializationRange(*this);
  if (!cursor_options) {
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_LEN);
    ReplyAvailNeighbors(ctx, search_result, *this);
    auto format = GetRowFormat();
    size_t elements = 1;
    for (auto i = range.start_index; i < range.end_index; ++i) {
      elements += ReplyRowElements(ctx, search_result.neighbors[i], format);
    }
    ValkeyModule_ReplySetArrayLength(ctx, elements);
    return;
  }

  const size_t count =
      std::min(static_cast<size_t>(cursor_options->count), range.count());
  ValkeyModule_ReplyWithArray(ctx, 3);
  ReplyAvailNeighbors(ctx, search_result, *this);
  ValkeyModule_ReplyWithArray(ctx, count);
  ReplyRows(ctx, search_result, range.start_index, range.start_index + count);
  if (count == range.count()) {
    ValkeyModule_ReplyWithLongLong(ctx, 0);
    return;
  }
  // The cursor keeps this command, including its search_result.
  CHECK(&search_result == &this->search_result);
  auto cursor = std::make_unique<CursorSearchResult>(
      std::unique_ptr<SearchCommand>(this), range.start_index + count,
      range.end_index, cursor_options->max_idle);
  auto id = CursorTable::Instance().Insert(std::move(cursor), absl::Now());
  ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(id));
}

absl::Status FTSearchCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                         int argc) {
  return QueryCommand::Execute(ctx, argv, argc,
                               std::unique_ptr<QueryCommand>(new SearchCommand(
                                   ValkeyModule_GetSelectedDb(ctx))));
}

}  // namespace valkey_search
