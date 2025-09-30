/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_
#define VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_

#include <cstddef>
#include <cstdint>
#include <memory>

#include "absl/status/statusor.h"
#include "src/commands/ft_create_parser.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

constexpr int64_t kTimeoutMS{50000};
const size_t kMaxTimeoutMs{60000};

namespace options {
vmsdk::config::Number &GetMaxKnn();
}  // namespace options

struct LimitParameter {
  uint64_t first_index{0};
  uint64_t number{10};
};

absl::StatusOr<std::unique_ptr<query::SearchParameters>>
ParseVectorSearchParameters(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                            int argc, const SchemaManager &schema_manager);

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_
