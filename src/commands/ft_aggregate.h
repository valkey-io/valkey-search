#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_AGGREGATE_H
#define VALKEYSEARCH_SRC_COMMANDS_FT_AGGREGATE_H

#include "src/commands/ft_aggregate_parser.h"

#include "absl/status/status.h"

namespace valkey_search {
namespace aggregate {

absl::Status FTAggregateCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

}
};
#endif