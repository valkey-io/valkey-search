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

#ifndef VALKEYSEARCH_SRC_QUERY_FANOUT_H_
#define VALKEYSEARCH_SRC_QUERY_FANOUT_H_

#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/index_schema.h"
#include "src/query/search.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query::fanout {

struct FanoutSearchTarget {
  enum Type {
    kLocal,
    kRemote,
  };
  Type type;
  // Empty string if type is kLocal.
  std::string address;

  bool operator==(const FanoutSearchTarget& other) const {
    return type == other.type && address == other.address;
  }

  friend std::ostream& operator<<(std::ostream& os,
                                  const FanoutSearchTarget& target) {
    os << "FanoutSearchTarget{type: " << target.type
       << ", address: " << target.address << "}";
    return os;
  }
};

absl::Status PerformSearchFanoutAsync(
    RedisModuleCtx* ctx, std::vector<FanoutSearchTarget>& search_targets,
    coordinator::ClientPool* coordinator_client_pool,
    std::unique_ptr<query::VectorSearchParameters> parameters,
    vmsdk::ThreadPool* thread_pool, query::SearchResponseCallback callback);

std::vector<FanoutSearchTarget> GetSearchTargetsForFanout(RedisModuleCtx* ctx);

}  // namespace valkey_search::query::fanout

#endif  // VALKEYSEARCH_SRC_QUERY_FANOUT_H_
