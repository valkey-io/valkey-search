/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_PRIMARY_INFO_FANOUT_H_
#define VALKEYSEARCH_SRC_QUERY_PRIMARY_INFO_FANOUT_H_

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

namespace valkey_search::query::primary_info_fanout {

struct PrimaryInfoParameters {
  std::string index_name;
  int timeout_ms;
};

struct PrimaryInfoResult {
  bool exists = false;
  std::string index_name;
  uint64_t num_docs = 0;
  uint64_t num_records = 0;
  uint64_t hash_indexing_failures = 0;
  std::string error;
  std::optional<uint64_t> schema_fingerprint;
  bool has_schema_mismatch = false;
  std::optional<uint32_t> encoding_version;
  bool has_version_mismatch = false;
};

using PrimaryInfoResponseCallback = absl::AnyInvocable<void(
    absl::StatusOr<PrimaryInfoResult>, std::unique_ptr<PrimaryInfoParameters>)>;

absl::Status PerformPrimaryInfoFanoutAsync(
    ValkeyModuleCtx* ctx, std::vector<fanout::FanoutSearchTarget>& info_targets,
    coordinator::ClientPool* coordinator_client_pool,
    std::unique_ptr<PrimaryInfoParameters> parameters,
    vmsdk::ThreadPool* thread_pool, PrimaryInfoResponseCallback callback);

}  // namespace valkey_search::query::primary_info_fanout

#endif  // VALKEYSEARCH_SRC_QUERY_PRIMARY_INFO_FANOUT_H_
