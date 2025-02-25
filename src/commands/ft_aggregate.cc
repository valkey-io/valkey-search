#include "src/commands/ft_aggregate.h"

#include <algorithm>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "src/commands/ft_aggregate_exec.h"
#include "src/commands/ft_search.h"
#include "src/commands/ft_search_parser.h"
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/metrics.h"
#include "src/query/fanout.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"

namespace valkey_search {
namespace aggregate {

struct RealIndexInterface : public IndexInterface {
  std::shared_ptr<IndexSchema> schema_;
  absl::StatusOr<indexes::IndexerType> GetFieldType(
      absl::string_view s) const override {
    VMSDK_ASSIGN_OR_RETURN(auto indexer, schema_->GetIndex(s));
    return indexer->GetIndexerType();
  }
  RealIndexInterface(std::shared_ptr<IndexSchema> schema) : schema_(schema) {}
};

absl::Status ManipulateReturnsClause(AggregateParameters &params) {
  // Figure out what fields actually need to be returned by the aggregation
  // operation. And modify the common search returns list accordingly
  RedisModule_Assert(!params.no_content);
  if (params.loadall_) {
    RedisModule_Assert(params.return_attributes.empty());
  } else {
    while (!params.loads_.empty()) {
      vmsdk::UniqueRedisString id;
      std::swap(params.loads_.back(), id);
      params.loads_.pop_back();
      params.return_attributes.emplace_back(query::ReturnAttribute{
          .identifier = std::move(id), .alias = nullptr});
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<AggregateParameters>> ParseCommand(
    RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
    const SchemaManager &schema_manager) {
  static vmsdk::KeyValueParser<AggregateParameters> parser =
      CreateAggregateParser();
  vmsdk::ArgsIterator itr{argv, argc};
  std::string index_schema_name;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, index_schema_name));
  VMSDK_ASSIGN_OR_RETURN(
      auto index_schema,
      SchemaManager::Instance().GetIndexSchema(RedisModule_GetSelectedDb(ctx),
                                               index_schema_name));
  RealIndexInterface index_interface(index_schema);
  auto params = std::make_unique<AggregateParameters>(&index_interface);
  params->index_schema_name = std::move(index_schema_name);
  params->index_schema = std::move(index_schema);

  VMSDK_RETURN_IF_ERROR(
      vmsdk::ParseParamValue(itr, params->parse_vars.query_string));
  VMSDK_RETURN_IF_ERROR(PreParseQueryString(*params));

  RedisModule_Assert(params->AddRecordAttribute("__key") ==
                     AggregateParameters::kKeyIndex);
  RedisModule_Assert(!params->parse_vars.score_as_string.empty());
  RedisModule_Assert(
      params->AddRecordAttribute(params->parse_vars.score_as_string) ==
      AggregateParameters::kScoreIndex);

  VMSDK_RETURN_IF_ERROR(parser.Parse(*params, itr, true));
  if (itr.DistanceEnd() > 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected parameter at position ", (itr.Position() + 1),
                     ":", vmsdk::ToStringView(itr.Get().value())));
  }

  VMSDK_RETURN_IF_ERROR(PostParseQueryString(*params));
  VMSDK_RETURN_IF_ERROR(ManipulateReturnsClause(*params));

  std::cerr << "At end of parse: " << *params << "\n";
  params->parse_vars.ClearAtEndOfParse();
  return std::move(params);
}

absl::Status SendReplyInner(RedisModuleCtx *ctx,
                            std::deque<indexes::Neighbor> &neighbors,
                            const AggregateParameters &parameters) {
  //
  //  1. Process the collected Neighbors into Aggregate Records.
  //
  RecordSet records;
  for (auto &n : neighbors) {
    auto rec = std::make_unique<Record>(parameters.attr_record_indexes_.size());
    // Set the key
    rec->fields_[AggregateParameters::kKeyIndex] =
        expr::Value(n.external_id.get());
    // Set the distance/score parameter
    rec->fields_[AggregateParameters::kScoreIndex] = expr::Value(n.distance);

    // For the fields that were fetched, stash them into the RecordSet
    if (n.attribute_contents) {
      for (auto &[name, records_map_value] : *n.attribute_contents) {
        auto value =
            expr::Value(vmsdk::ToStringView(records_map_value.GetIdentifier()));
        auto ref = parameters.attr_record_indexes_.find(name);
        if (ref != parameters.attr_record_indexes_.end()) {
          rec->fields_[ref->second] = value;
        } else {
          rec->extra_fields_.push_back(
              std::make_pair(std::string(name), value));
        }
      }
    }
    records.push_back(std::move(rec));
  }
  //
  //  2. Perform the aggregation stages
  //
  for (auto &stage : parameters.stages_) {
    // Todo Check for timeout
    VMSDK_RETURN_IF_ERROR(stage->Execute(records));
  }
  std::cerr << "Finished stages, size " << records.size() << "\n";

  //
  //  3. Generate the result
  //
  RedisModule_ReplyWithArray(ctx, 1 + records.size());
  RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(records.size()));
  while (!records.empty()) {
    auto rec = records.pop_front();
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    //
    // First the referenced fields
    //
    std::cerr << "Starting row:";
    size_t array_count = 0;
    for (size_t i = 0; i < rec->fields_.size(); ++i) {
      const auto &value = rec->fields_[i];
      if (!value.IsNil()) {
        RedisModule_ReplyWithSimpleString(
            ctx, parameters.attr_record_names_[i].data());
        auto value_sv = value.AsStringView();
        RedisModule_ReplyWithStringBuffer(ctx, value_sv.data(),
                                          value_sv.size());
        std::cerr << " " << parameters.attr_record_names_[i] << ":" << value_sv;
        array_count += 2;
      }
    }
    //
    // Now the unreferenced ones
    //
    for (const auto &[name, value] : rec->extra_fields_) {
      RedisModule_ReplyWithSimpleString(ctx, name.data());
      auto value_sv = value.AsStringView();
      RedisModule_ReplyWithStringBuffer(ctx, value_sv.data(), value_sv.size());
      std::cerr << " " << name << ":" << value_sv;
      array_count += 2;
    }
    std::cerr << " (Total length) " << array_count << "\n";
    RedisModule_ReplySetArrayLength(ctx, array_count);
  }

  return absl::OkStatus();
}

void SendReply(RedisModuleCtx *ctx, std::deque<indexes::Neighbor> &neighbors,
               const AggregateParameters &parameters) {
  auto result = SendReplyInner(ctx, neighbors, parameters);
  if (!result.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    RedisModule_ReplyWithError(ctx, result.message().data());
  }
}

}  // namespace aggregate

absl::Status FTAggregateCmd(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc) {
  auto status = [&]() {
    auto &schema_manager = SchemaManager::Instance();
    VMSDK_ASSIGN_OR_RETURN(
        auto parameters,
        aggregate::ParseCommand(ctx, argv + 1, argc - 1, schema_manager));
    parameters->index_schema->ProcessMultiQueue();
    bool inside_multi =
        (RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_MULTI) != 0;
    if (ABSL_PREDICT_FALSE(!ValkeySearch::Instance().SupportParallelQueries() ||
                           inside_multi)) {
      VMSDK_ASSIGN_OR_RETURN(auto neighbors, query::Search(*parameters, true));
      SendReply(ctx, neighbors, *parameters);
      return absl::OkStatus();
    }
    vmsdk::BlockedClient blocked_client(ctx, async::Reply, async::Timeout,
                                        async::Free, 0);
    blocked_client.MeasureTimeStart();
    auto on_done_callback = [blocked_client = std::move(blocked_client)](
                                auto &neighbors, auto parameters) mutable {
      auto result = std::make_unique<async::Result>(async::Result{
          .neighbors = std::move(neighbors),
          .parameters = std::move(parameters),
      });
      blocked_client.SetReplyPrivateData(result.release());
    };

    if (ValkeySearch::Instance().UsingCoordinator() &&
        ValkeySearch::Instance().IsCluster() && !parameters->local_only) {
      auto search_targets = query::fanout::GetSearchTargetsForFanout(ctx);
      return query::fanout::PerformSearchFanoutAsync(
          ctx, search_targets,
          ValkeySearch::Instance().GetCoordinatorClientPool(),
          std::move(parameters), ValkeySearch::Instance().GetReaderThreadPool(),
          std::move(on_done_callback));
    }
    return query::SearchAsync(std::move(parameters),
                              ValkeySearch::Instance().GetReaderThreadPool(),
                              std::move(on_done_callback), true);
    RedisModule_Assert(false);
  }();
  if (!status.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
  }
  return status;
}

}  // namespace valkey_search
