/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_CLUSTER_INFO_FANOUT_H_
#define VALKEYSEARCH_SRC_QUERY_CLUSTER_INFO_FANOUT_H_

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "src/coordinator/client_pool.h"
#include "src/query/fanout_template.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query::cluster_info_fanout {

struct ClusterInfoParameters {
  std::string index_name;
  int timeout_ms;
};

struct ClusterInfoResult {
  bool exists = false;
  std::string index_name;
  float backfill_complete_percent = 0.0f;
  float backfill_complete_percent_max = 0.0f;
  float backfill_complete_percent_min = 0.0f;
  bool backfill_in_progress = false;
  std::string state;
  std::string error;
  std::optional<uint64_t> schema_fingerprint;
  bool has_schema_mismatch = false;
  std::optional<uint32_t> encoding_version;
  bool has_version_mismatch = false;
};

using ClusterInfoResponseCallback = absl::AnyInvocable<void(
    absl::StatusOr<ClusterInfoResult>, std::unique_ptr<ClusterInfoParameters>)>;

absl::Status PerformClusterInfoFanoutAsync(
    ValkeyModuleCtx* ctx, std::vector<fanout::FanoutSearchTarget>& info_targets,
    coordinator::ClientPool* coordinator_client_pool,
    std::unique_ptr<ClusterInfoParameters> parameters,
    vmsdk::ThreadPool* thread_pool, ClusterInfoResponseCallback callback);

}  // namespace valkey_search::query::cluster_info_fanout

#endif  // VALKEYSEARCH_SRC_QUERY_CLUSTER_INFO_FANOUT_H_
