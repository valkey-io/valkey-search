#include "src/commands/ft_aggregate.h"

#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "src/commands/ft_search.h"
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
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, params->query));

  VMSDK_RETURN_IF_ERROR(parser.Parse(*params, itr, false));
  params->parse_vars.ClearAtEndOfParse();
  return std::move(params);
}

absl::Status FTAggregateCmd(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc) {
  auto status = [&]() {
    auto &schema_manager = SchemaManager::Instance();
    VMSDK_ASSIGN_OR_RETURN(
        auto parameters, ParseCommand(ctx, argv + 1, argc - 1, schema_manager));
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
    assert(false);
  }();
  if (!status.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
  }
  return status;
}

void SendReply(RedisModuleCtx *ctx, std::deque<indexes::Neighbor> &neighbors,
               const AggregateParameters &parameters) {
  assert(false);
}

}  // namespace aggregate
}  // namespace valkey_search
