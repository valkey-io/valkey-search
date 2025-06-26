/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_
#define VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_

#include <cstddef>
#include <cstdint>
#include <memory>

#include "absl/status/statusor.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

constexpr int64_t kTimeoutMS{50000};
const size_t kMaxTimeoutMs{60000};

struct LimitParameter {
  uint64_t first_index{0};
  uint64_t number{10};
};

absl::StatusOr<std::unique_ptr<query::VectorSearchParameters>>
ParseVectorSearchParameters(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                            int argc, const SchemaManager &schema_manager);

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_PARSER_H_
