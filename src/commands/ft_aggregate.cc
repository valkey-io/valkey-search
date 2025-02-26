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
#include "src/query/response_generator.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"

// #define DBG std::cerr
#define DBG 0 && std::cerr

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
    DBG << "**LOADALL**\n";
    RedisModule_Assert(params.return_attributes.empty());
  } else {
    DBG << "LOADING: ";
    for (const auto &load : params.loads_) {
      DBG << " " << load;
      //
      // Skip loading of the score and the key, we always get those...
      //
      if (load == "__key") {
        DBG << "*skipped*";
        params.load_key = true;
        continue;
      }
      if (load == vmsdk::ToStringView(params.score_as.get())) {
        DBG << "*skipped*";
        continue;
      }
      params.return_attributes.emplace_back(query::ReturnAttribute{
          .identifier = vmsdk::MakeUniqueRedisString(load),
          .alias = vmsdk::MakeUniqueRedisString(load)});
    }
    DBG << "\n";
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

  // Ensure that key is first value if it gets included...
  RedisModule_Assert(params->AddRecordAttribute("__key") == 0);

  VMSDK_RETURN_IF_ERROR(parser.Parse(*params, itr, true));
  if (itr.DistanceEnd() > 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected parameter at position ", (itr.Position() + 1),
                     ":", vmsdk::ToStringView(itr.Get().value())));
  }

  VMSDK_RETURN_IF_ERROR(PostParseQueryString(*params));
  VMSDK_RETURN_IF_ERROR(ManipulateReturnsClause(*params));

  DBG << "At end of parse: " << *params << "\n";
  params->parse_vars.ClearAtEndOfParse();
  return std::move(params);
}

bool ReplyWithValue(RedisModuleCtx *ctx, absl::string_view name,
                    const expr::Value &value) {
  if (value.IsNil()) {
    return false;
  } else {
    RedisModule_ReplyWithSimpleString(ctx, name.data());
    DBG << " " << name << ":";
    if (value.IsBool()) {
      DBG << *value.AsBool();
      RedisModule_ReplyWithBool(ctx, *value.AsBool());
    } else if (value.IsDouble()) {
      DBG << *value.AsDouble();
      RedisModule_ReplyWithDouble(ctx, *value.AsDouble());
    } else {
      auto value_sv = value.AsStringView();
      RedisModule_ReplyWithStringBuffer(ctx, value_sv.data(), value_sv.size());
      DBG << value_sv;
    }
    return true;
  }
}

absl::Status SendReplyInner(RedisModuleCtx *ctx,
                            std::deque<indexes::Neighbor> &neighbors,
                            AggregateParameters &parameters) {
  auto identifier =
      parameters.index_schema->GetIdentifier(parameters.attribute_alias);
  if (!identifier.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    return identifier.status();
  }
  query::ProcessNeighborsForReply(
      ctx, parameters.index_schema->GetAttributeDataType(), neighbors,
      parameters, identifier.value());

  size_t key_index = 0, scores_index = 0;
  if (parameters.load_key) {
    key_index = parameters.AddRecordAttribute("__key");
  }
  if (parameters.addscores_) {
    scores_index = parameters.AddRecordAttribute(
        vmsdk::ToStringView(parameters.score_as.get()));
  }
  //
  //  1. Process the collected Neighbors into Aggregate Records.
  //
  RecordSet records(&parameters);
  for (auto &n : neighbors) {
    DBG << "Neighbor: " << n << "\n";
    auto rec = std::make_unique<Record>(parameters.attr_record_indexes_.size());
    if (parameters.load_key) {
      rec->fields_[key_index] = expr::Value(n.external_id.get()->Str());
    }
    if (parameters.addscores_) {
      rec->fields_[scores_index] = expr::Value(n.distance);
    }
    // For the fields that were fetched, stash them into the RecordSet
    if (n.attribute_contents.has_value()) {
      for (auto &[name, records_map_value] : *n.attribute_contents) {
        auto value =
            expr::Value(vmsdk::ToStringView(records_map_value.value.get()));
        DBG << "Attribute_contents: " << name << " : " << value << "\n";
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
  DBG << "After Record Fetch\n" << records << "\n";
  //
  //  2. Perform the aggregation stages
  //
  for (auto &stage : parameters.stages_) {
    // Todo Check for timeout
    VMSDK_RETURN_IF_ERROR(stage->Execute(records));
  }
  DBG << ">> Finished stages\n" << records;

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
    DBG << "Starting row:";
    size_t array_count = 0;
    for (size_t i = 0; i < rec->fields_.size(); ++i) {
      if (ReplyWithValue(ctx, parameters.attr_record_names_[i],
                         rec->fields_[i])) {
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
      DBG << " " << name << ":" << value_sv;
      array_count += 2;
    }
    DBG << " (Total length) " << array_count << "\n";
    RedisModule_ReplySetArrayLength(ctx, array_count);
  }

  return absl::OkStatus();
}

void SendReply(RedisModuleCtx *ctx, std::deque<indexes::Neighbor> &neighbors,
               AggregateParameters &parameters) {
  auto identifier =
      parameters.index_schema->GetIdentifier(parameters.attribute_alias);
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
