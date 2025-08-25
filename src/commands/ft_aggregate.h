/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_AGGREGATE_H
#define VALKEYSEARCH_SRC_COMMANDS_FT_AGGREGATE_H

#include "absl/status/status.h"
#include "src/commands/ft_aggregate_parser.h"

namespace valkey_search {
namespace aggregate {

absl::Status FTAggregateCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                            int argc);

struct AggregateParameters;
void SendAggReply(ValkeyModuleCtx *ctx,
                  std::deque<indexes::Neighbor> &neighbors,
                  AggregateParameters &parameters);

}  // namespace aggregate
};  // namespace valkey_search
#endif
