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

#ifndef VALKEYSEARCH_SRC_QUERY_RESPONSE_GENERATOR_H_
#define VALKEYSEARCH_SRC_QUERY_RESPONSE_GENERATOR_H_

#include <deque>
#include <string>

#include "src/attribute_data_type.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/indexes/vector_base.h"
#include "src/query/search.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query {

// Adds all local content for neighbors to the list of neighbors.
// Skipping neighbors if one of the following:
// Neighbor already contained in the attribute content map.
// Neighbor without any attribute content.
// Neighbor not comply to the pre-filter expression.
void ProcessNeighborsForReply(ValkeyModuleCtx *ctx,
                              const AttributeDataType &attribute_data_type,
                              std::deque<indexes::Neighbor> &neighbors,
                              const query::VectorSearchParameters &parameters,
                              const std::string &identifier);

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_RESPONSE_GENERATOR_H_
