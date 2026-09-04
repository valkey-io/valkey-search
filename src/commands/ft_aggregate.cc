/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include <algorithm>
#include <string>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "ft_search_parser.h"
#include "src/commands/commands.h"
#include "src/commands/ft_aggregate_exec.h"
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/metrics.h"

namespace valkey_search {
namespace aggregate {

struct RealIndexInterface : public IndexInterface {
  std::shared_ptr<IndexSchema> schema_;
  absl::StatusOr<indexes::IndexerType> GetFieldType(
      absl::string_view s) const override {
    VMSDK_ASSIGN_OR_RETURN(auto indexer, schema_->GetIndex(s));
    return indexer->GetIndexerType();
  }
  absl::StatusOr<std::string> GetIdentifier(
      absl::string_view alias) const override {
    return schema_->GetIdentifier(alias);
  }
  absl::StatusOr<std::string> GetAlias(
      absl::string_view identifier) const override {
    return schema_->GetAlias(identifier);
  }
  RealIndexInterface(std::shared_ptr<IndexSchema> schema) : schema_(schema) {}
};

absl::Status ManipulateReturnsClause(AggregateParameters &params) {
  // Figure out what fields actually need to be returned by the aggregation
  // operation. And modify the common search returns list accordingly
  CHECK(!params.no_content);
  bool content = false;
  if (params.loadall_) {
    CHECK(params.return_attributes.empty());
    return absl::OkStatus();
  } else {
    std::vector<std::string> loads_to_process = params.loads_;

    // A field named by a pipeline stage but absent from the LOAD clause still
    // has to be fetched. Redisearch loads such fields implicitly; without that
    // the field reads as Nil, so a GROUPBY key vanishes from the reply and
    // every record lands in one group, a REDUCE aggregates nothing, and a
    // SORTBY does not sort. See issue #919.
    //
    // Every `@name` in a GROUPBY key, REDUCE argument, SORTBY key, APPLY or
    // FILTER expression has already been turned into a record column by
    // AggregateParameters::MakeReference() -- GROUPBY keys directly, the rest
    // via expr::Expression::Compile -- so the column table is a complete list
    // of the names the pipeline references. No stage walking is needed.
    //
    // Only names that resolve to a declared attribute are loaded. Anything
    // else is produced by the pipeline itself (an APPLY or REDUCE output, a
    // chained GROUPBY over a reducer alias) and has no stored value to fetch.
    const auto score_name = vmsdk::ToStringView(params.score_as.get());
    for (const auto &info : params.record_info_by_index_) {
      const std::string &name = info.alias_;
      if (name == "__key" || name == score_name) {
        continue;
      }
      auto indexer = params.index_schema->GetIndex(name);
      if (!indexer.ok()) {
        continue;
      }
      // Vector fields cannot be loaded at all (rejected below). Auto-loading
      // one would turn a query that merely produced a Nil into an error, which
      // is a bigger behavior change than this fix intends.
      if (indexes::IsVectorIndex(*indexer)) {
        continue;
      }
      if (std::find(loads_to_process.begin(), loads_to_process.end(), name) ==
          loads_to_process.end()) {
        loads_to_process.push_back(name);
      }
    }

    for (const auto &load : loads_to_process) {
      //
      // Skip loading of the score and the key, we always get those...
      //
      if (load == "__key") {
        params.load_key = true;
        continue;
      }
      if (load == vmsdk::ToStringView(params.score_as.get())) {
        continue;
      }
      content = true;
      VMSDK_ASSIGN_OR_RETURN(auto indexer, params.index_schema->GetIndex(load));
      if (indexes::IsVectorIndex(indexer)) {
        return absl::InvalidArgumentError(absl::StrCat(
            "Loading of vector fields is not supported (field `", load, "`)"));
      }
      auto schema_identifier = params.index_schema->GetIdentifier(load);
      if (schema_identifier.ok()) {
        params.return_attributes.emplace_back(query::ReturnAttribute{
            .identifier = vmsdk::MakeUniqueValkeyString(*schema_identifier),
            .attribute_alias = vmsdk::MakeUniqueValkeyString(load),
            .alias = vmsdk::MakeUniqueValkeyString(load)});
        params.AddRecordAttribute(*schema_identifier, load,
                                  indexer->GetIndexerType());
      } else {
        params.return_attributes.emplace_back(query::ReturnAttribute{
            .identifier = vmsdk::MakeUniqueValkeyString(load),
            .attribute_alias = vmsdk::UniqueValkeyString(),
            .alias = vmsdk::MakeUniqueValkeyString(load)});
        params.AddRecordAttribute(load, load, indexes::IndexerType::kNone);
      }
    }
  }
  params.no_content = !content;
  return absl::OkStatus();
}

absl::Status AggregateParameters::ParseCommand(vmsdk::ArgsIterator &itr) {
  static vmsdk::KeyValueParser<AggregateParameters> parser =
      CreateAggregateParser();
  RealIndexInterface real_index_interface(index_schema);
  parse_vars_.index_interface_ = &real_index_interface;

  VMSDK_RETURN_IF_ERROR(PreParseQueryString());
  // Ensure that key is first value if it gets included...
  CHECK(AddRecordAttribute("__key", "__key", indexes::IndexerType::kNone) == 0);
  auto score_sv = vmsdk::ToStringView(score_as.get());
  CHECK(AddRecordAttribute(score_sv, score_sv, indexes::IndexerType::kNone) ==
        1);

  VMSDK_RETURN_IF_ERROR(parser.Parse(*this, itr, true));
  if (itr.DistanceEnd() > 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected parameter at position ", (itr.Position() + 1),
                     ":", vmsdk::ToStringView(itr.Get().value())));
  }

  if (dialect < 2 || dialect > 4) {
    return absl::InvalidArgumentError("Only Dialects 2, 3 and 4 are supported");
  }

  // Set limit parameters based on GetSerializationRange logic
  auto range = GetSerializationRange();
  limit.first_index = range.start_index;
  limit.number = range.end_index - range.start_index;

  VMSDK_RETURN_IF_ERROR(PostParseQueryString());
  VMSDK_RETURN_IF_ERROR(VerifyQueryString(*this));
  VMSDK_RETURN_IF_ERROR(ManipulateReturnsClause(*this));

  return absl::OkStatus();
}

// Returns whether the entire search results are needed to be able to form the
// aggregated response.
bool AggregateParameters::RequiresCompleteResults() const {
  return GetSerializationRange() == query::SerializationRange::All();
}

// Determine the serialization range required based on the stages in the
// aggregation. This is only used in construction of the aggregate command to
// set limit params. These params will be used later on in the SearchResult.
query::SerializationRange AggregateParameters::GetSerializationRange() const {
  for (const auto &stage : stages_) {
    auto stage_range = stage->GetSerializationRange();
    // Use the first limit.
    if (stage_range) {
      return *stage_range;
    }
  }
  // Fallback to no limit
  return query::SerializationRange::All();
}

void AggregateParameters::SendReply(ValkeyModuleCtx *ctx,
                                    query::SearchResult &result) {
  auto status = RunAggregatePipeline(ctx, result.neighbors, *this);
  if (!status.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    ValkeyModule_ReplyWithError(ctx, status.message().data());
  }
}

}  // namespace aggregate

absl::Status FTAggregateCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                            int argc) {
  return QueryCommand::Execute(
      ctx, argv, argc,
      std::unique_ptr<QueryCommand>(
          new aggregate::AggregateParameters(ValkeyModule_GetSelectedDb(ctx))));
}

}  // namespace valkey_search
