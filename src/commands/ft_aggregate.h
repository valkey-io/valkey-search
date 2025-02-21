#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_AGGREGATE_H
#define VALKEYSEARCH_SRC_COMMANDS_FT_AGGREGATE_H

#include "absl/status/status.h"
#include "src/commands/ft_aggregate_parser.h"

namespace valkey_search {
namespace aggregate {

absl::Status FTAggregateCmd(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc);

struct AggregateParameters;
void SendReply(RedisModuleCtx *ctx, std::deque<indexes::Neighbor> &neighbors,
               const AggregateParameters &parameters);

}  // namespace aggregate
};  // namespace valkey_search
#endif