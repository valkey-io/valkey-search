#include "src/commands/ft_aggregate.h"

#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/metrics.h"
#include "src/schema_manager.h"

namespace valkey_search {
namespace aggregate {

struct RealIndexInterface : public IndexInterface {
  IndexSchema *schema_;
  absl::StatusOr<indexes::IndexerType> GetFieldType(absl::string_view s) const {
    assert(false);
  }
  RealIndexInterface(IndexSchema *schema) : schema_(schema) {}
};

struct Command {
  Command(std::shared_ptr<IndexSchema> schema) : schema_(schema), index_interface_(schema.get()), params_(&index_interface_) {}
  std::shared_ptr<IndexSchema> schema_;
  RealIndexInterface index_interface_;
  AggregateParameters params_;
};

absl::Status Verify(AggregateParameters& p) {
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<Command>>
parseCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc, const SchemaManager &schema_manager) {
static vmsdk::KeyValueParser<AggregateParameters> parser =
      CreateAggregateParser();
  vmsdk::ArgsIterator itr{argv, argc};
  std::string index_schema_name;
  VMSDK_RETURN_IF_ERROR(
      vmsdk::ParseParamValue(itr, index_schema_name));
  VMSDK_ASSIGN_OR_RETURN(
      auto index_schema_,
      SchemaManager::Instance().GetIndexSchema(RedisModule_GetSelectedDb(ctx),
                                               index_schema_name));
  auto command = std::make_unique<Command>(index_schema_);                                                
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, command->params_.query_));

  VMSDK_RETURN_IF_ERROR(parser.Parse(command->params_, itr, false));
  VMSDK_RETURN_IF_ERROR(Verify(command->params_));
  return command;
}

absl::Status FTAggregateCmd(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc) {
  auto status = [&]() {
    auto &schema_manager = SchemaManager::Instance();
    VMSDK_ASSIGN_OR_RETURN(
        auto command,
        parseCommand(ctx, argv + 1, argc - 1, schema_manager));
/*
    parameters->index_schema_->ProcessMultiQueue();
    bool inside_multi =
        (RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_MULTI) != 0;
    if (ABSL_PREDICT_FALSE(!ValkeySearch::Instance().SupportParralelQueries() ||
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
*/
    assert(false);
  }();
  if (!status.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
  }
  return status;
}

} // namespace aggregate
} // namespace valkey_search

