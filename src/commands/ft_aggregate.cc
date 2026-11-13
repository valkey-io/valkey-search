/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "ft_search_parser.h"
#include "src/commands/commands.h"
#include "src/commands/ft_aggregate_exec.h"
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/metrics.h"
#include "src/valkey_search_options.h"

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

namespace {

// Whether fetching the named and stage-read fields under `LOAD *` changes the
// reply of 1.2.1, which fetched only the whole record. Decided before any key
// is read, so a value that happens to look the same either way still counts.
bool LoadAllFetchChangesReply(const AggregateParameters &params) {
  const bool is_json = params.index_schema->GetAttributeDataType().ToProto() ==
                       data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON;
  const auto score_name = vmsdk::ToStringView(params.score_as.get());
  auto is_score = [&](absl::string_view name) {
    return name == score_name || (!params.vr_score_field_name_.empty() &&
                                  name == params.vr_score_field_name_);
  };
  auto loads_key = std::any_of(
      params.loads_.begin(), params.loads_.end(),
      [](const LoadField &load) { return load.identifier == "__key"; });
  if (loads_key && params.read_names_.contains("__key")) {
    return true;
  }
  // The JSON record is the single `$` root, so a stage finds no field in it.
  if (is_json) {
    for (const auto &name : params.read_names_) {
      if (name == "__key" || is_score(name)) {
        continue;
      }
      auto indexer = params.index_schema->GetIndex(name);
      if (indexer.ok() && !indexes::IsVectorIndex(*indexer)) {
        return true;
      }
    }
  }
  const bool grouped = std::any_of(
      params.stages_.begin(), params.stages_.end(),
      [](const std::unique_ptr<Stage> &stage) {
        return dynamic_cast<const GroupBy *>(stage.get()) != nullptr;
      });
  for (const auto &load : params.loads_) {
    if (is_score(load.identifier)) {
      continue;
    }
    // A GROUPBY leaves only groups, and an APPLY onto the name overwrites it.
    const bool hidden = grouped || params.written_names_.contains(load.alias);
    if (load.identifier == "__key") {
      if (!hidden) {
        return true;
      }
      continue;
    }
    // Fetching a field the schema lacks, or a vector, is an error.
    auto indexer = params.index_schema->GetIndex(load.identifier);
    if (!indexer.ok() || indexes::IsVectorIndex(*indexer)) {
      return true;
    }
    if (hidden) {
      continue;
    }
    // A HASH record already holds the field under its own name, as text; a
    // fetched NUMERIC field is printed as a number instead, as it already is
    // when a stage reads it.
    if (is_json ||
        ((*indexer)->GetIndexerType() == indexes::IndexerType::kNumeric &&
         !params.read_names_.contains(load.alias))) {
      return true;
    }
  }
  return false;
}

}  // namespace

absl::Status ManipulateReturnsClause(AggregateParameters &params) {
  // Figure out what fields actually need to be returned by the aggregation
  // operation. And modify the common search returns list accordingly
  CHECK(!params.no_content);
  bool content = false;
  // `LOAD *` asks for the whole record. From 1.3.0 it is not a short circuit: a
  // field named by a pipeline stage still has to be fetched by name alongside
  // it, because on a JSON index the whole record arrives as the single `$` root
  // and no `@name` resolves against that blob. Fall through into the same
  // load processing every other clause gets, with an empty `loads_`, so the
  // implicit loads below are collected exactly as usual.
  if (params.loadall_) {
    CHECK(params.return_attributes.empty());
    params.all_content = true;
    // 1.2.1 fetched nothing but the whole record under `LOAD *`: named loads
    // were ignored, and stages read only from that record.
    if (!options::EnabledInVersion(1, 3, 0)) {
      if (LoadAllFetchChangesReply(params)) {
        VALKEY_SEARCH_COMPATIBILITY_FIX(
            1, 3, 0, "ft_aggregate_loadall_fetch", [] {}, [] {});
      }
      return absl::OkStatus();
    }
  }
  std::vector<LoadField> loads_to_process = params.loads_;

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
  //
  // An implicit load is never a rename: it is emitted under the attribute
  // name, exactly as if the query had written `@name` in the LOAD clause.
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
    if (std::find_if(loads_to_process.begin(), loads_to_process.end(),
                     [&name](const LoadField &f) {
                       return f.identifier == name;
                     }) == loads_to_process.end()) {
      loads_to_process.push_back(
          LoadField{.identifier = name, .alias = name, .renamed = false});
    }
  }

  for (const auto &load : loads_to_process) {
    const std::string &identifier = load.identifier;
    const std::string &alias = load.alias;  // output name (== identifier
                                            // when there is no AS clause)
    const bool renamed = load.renamed;
    // Apply a LOAD ... AS rename to an attribute already present in the
    // record table: emit it under `alias` and let `@alias` resolve in later
    // pipeline stages (APPLY/SORTBY/FILTER).
    auto apply_rename = [&](size_t record_index) {
      params.record_info_by_index_[record_index].output_name_ = alias;
      params.record_indexes_by_alias_[alias] = record_index;
    };
    //
    // Skip loading of the score and the key, we always get those...
    //
    if (identifier == "__key") {
      params.load_key = true;
      if (renamed) {
        apply_rename(params.record_indexes_by_alias_.at("__key"));
      }
      continue;
    }
    if (identifier == vmsdk::ToStringView(params.score_as.get())) {
      if (renamed) {
        apply_rename(params.record_indexes_by_alias_.at(identifier));
      }
      continue;
    }
    // Also skip the VR score field name — it is a synthetic computed field
    // that is not in the index schema.
    bool is_vr_field = !params.vr_score_field_name_.empty() &&
                       identifier == params.vr_score_field_name_;
    if (is_vr_field) {
      if (renamed) {
        apply_rename(params.record_indexes_by_alias_.at(identifier));
      }
      continue;
    }
    content = true;
    VMSDK_ASSIGN_OR_RETURN(auto indexer,
                           params.index_schema->GetIndex(identifier));
    auto indexer_type = indexer->GetIndexerType();
    if (indexes::IsVectorIndex(indexer)) {
      return absl::InvalidArgumentError(
          absl::StrCat("Loading of vector fields is not supported (field `",
                       identifier, "`)"));
    }
    auto schema_identifier = params.index_schema->GetIdentifier(identifier);
    size_t record_index;
    if (schema_identifier.ok()) {
      params.return_attributes.emplace_back(query::ReturnAttribute{
          .identifier = vmsdk::MakeUniqueValkeyString(*schema_identifier),
          .attribute_alias = vmsdk::MakeUniqueValkeyString(identifier),
          .alias = vmsdk::MakeUniqueValkeyString(alias)});
      record_index = params.AddRecordAttribute(
          *schema_identifier, identifier,
          renamed ? alias : OutputNameFor(alias, *schema_identifier),
          indexer_type, AggregateParameters::ResultTypeFor(indexer_type));
    } else {
      params.return_attributes.emplace_back(query::ReturnAttribute{
          .identifier = vmsdk::MakeUniqueValkeyString(identifier),
          .attribute_alias = vmsdk::UniqueValkeyString(),
          .alias = vmsdk::MakeUniqueValkeyString(alias)});
      record_index = params.AddRecordAttribute(
          identifier, identifier, renamed ? alias : identifier,
          indexes::IndexerType::kNone, expr::Expression::Type::kString);
    }
    if (renamed) {
      apply_rename(record_index);
    }
  }
  // `LOAD *` on its own loads no named field, so `content` stays false even
  // though the whole record is wanted. Nothing is fetched only when neither
  // was asked for.
  params.no_content = !content && !params.all_content;
  return absl::OkStatus();
}

absl::Status AggregateParameters::ParseCommand(vmsdk::ArgsIterator &itr) {
  static vmsdk::KeyValueParser<AggregateParameters> parser =
      CreateAggregateParser();
  RealIndexInterface real_index_interface(index_schema);
  parse_vars_.index_interface_ = &real_index_interface;

  VMSDK_RETURN_IF_ERROR(PreParseQueryString());
  // Resolve the single VR predicate's distance field name (if any).
  if (has_vector_range) {
    vr_score_field_name_ = query::GetVrScoreFieldName(*this);

    // For non-vector queries the mandatory slot-1 record attribute must carry
    // the VR distance. Set score_as so the unconditional AddRecordAttribute
    // call below uses the correct name.
    if (IsNonVectorQuery() && !vr_score_field_name_.empty()) {
      score_as = vmsdk::MakeUniqueValkeyString(vr_score_field_name_);
    }
  }
  // Non-vector queries have no KNN AS clause to name the score field, so
  // default to Redis' ADDSCORES name. This also makes @__score resolvable by
  // LOAD and by stages (SORTBY/APPLY/GROUPBY) via record_indexes_by_alias_.
  if (score_as == nullptr) {
    score_as = vmsdk::MakeUniqueValkeyString("__score");
  }
  // Ensure that key is first value if it gets included...
  CHECK(AddRecordAttribute("__key", "__key", "__key",
                           indexes::IndexerType::kNone,
                           expr::Expression::Type::kString) == kKeyColumn);
  auto score_sv = vmsdk::ToStringView(score_as.get());
  CHECK(AddRecordAttribute(score_sv, score_sv, score_sv,
                           indexes::IndexerType::kNone,
                           expr::Expression::Type::kNumber) == kScoreColumn);

  VMSDK_RETURN_IF_ERROR(parser.Parse(*this, itr, true));
  if (itr.DistanceEnd() > 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected parameter at position ", (itr.Position() + 1),
                     ":", vmsdk::ToStringView(itr.Get().value())));
  }
  if (dialect < 2 || dialect > 4) {
    return absl::InvalidArgumentError("Only Dialects 2, 3 and 4 are supported");
  }

  // Redisearch ignores these on an index without offsets.
  if (!index_schema->HasTextOffsets()) {
    inorder = false;
    slop.reset();
  }
  // The filter tree was built before VERBATIM / INORDER / SLOP were read.
  if (verbatim || inorder || slop.has_value()) {
    // The first parse already counted this query text.
    const bool was_suppressed =
        std::exchange(options::compat_counters_suppressed, true);
    absl::Cleanup restore = [was_suppressed] {
      options::compat_counters_suppressed = was_suppressed;
    };
    VMSDK_RETURN_IF_ERROR(ParseFilter());
  }

  // SORTBY's retention bound depends on the LIMIT stages around it, so it can
  // only be settled once the whole pipeline is known. Must precede
  // GetSerializationRange(), which reads the resolved stages.
  ResolveSortByBounds(*this);

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
