/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
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
    ValkeyModuleCtx* ctx, std::vector<FanoutSearchTarget>& search_targets,
    coordinator::ClientPool* coordinator_client_pool,
    std::unique_ptr<query::VectorSearchParameters> parameters,
    vmsdk::ThreadPool* thread_pool, query::SearchResponseCallback callback);

std::vector<FanoutSearchTarget> GetSearchTargetsForFanout(ValkeyModuleCtx* ctx);

}  // namespace valkey_search::query::fanout

#endif  // VALKEYSEARCH_SRC_QUERY_FANOUT_H_
