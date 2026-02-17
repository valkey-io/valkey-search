/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_H_
#define VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_H_

#include <memory>
#include <vector>
#include "src/index_schema.h"
#include "src/indexes/vector_base.h"
#include "src/query/search.h"

namespace valkey_search {
class ValkeySearch;

void ApplySortingWithParams(std::vector<indexes::Neighbor> &neighbors,
                            const std::shared_ptr<IndexSchema> &index_schema,
                            const query::SortByParameter &sortby,
                            const query::LimitParameter &limit);

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_H_
