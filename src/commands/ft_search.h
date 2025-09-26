/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_H_
#define VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_H_

#include <deque>
#include <memory>

#include "absl/status/statusor.h"
#include "src/indexes/vector_base.h"
#include "src/query/search.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
class ValkeySearch;
// Declared here to support testing
void SendReply(ValkeyModuleCtx *ctx, std::deque<indexes::Neighbor> &neighbors,
               query::VectorSearchParameters &parameters);
namespace async {

struct Result {
  cancel::Token cancellation_token;
  absl::StatusOr<std::deque<indexes::Neighbor>> neighbors;
  std::unique_ptr<query::VectorSearchParameters> parameters;
};

int Reply(ValkeyModuleCtx *ctx, [[maybe_unused]] ValkeyModuleString **argv,
          [[maybe_unused]] int argc);

int Timeout(ValkeyModuleCtx *ctx, [[maybe_unused]] ValkeyModuleString **argv,
            [[maybe_unused]] int argc);

void Free(ValkeyModuleCtx * /*ctx*/, void *privdata);

}  // namespace async

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_H_
