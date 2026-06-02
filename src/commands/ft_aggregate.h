/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_AGGREGATE_H
#define VALKEYSEARCH_SRC_COMMANDS_FT_AGGREGATE_H

#include "absl/status/status.h"
#include "valkey_module.h"

namespace valkey_search {
namespace aggregate {

absl::Status FTAggregateCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                            int argc);

}  // namespace aggregate
};  // namespace valkey_search
#endif
