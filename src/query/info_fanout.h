/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_INFO_FANOUT_H_
#define VALKEYSEARCH_SRC_QUERY_INFO_FANOUT_H_

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "src/coordinator/client_pool.h"
#include "src/query/fanout_template.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query::info_fanout {

struct InfoParameters {
  std::string index_name;
};

struct InfoResult {
  bool exists = false;
  std::string index_name;
  uint64_t num_docs = 0;
  uint64_t num_records = 0;
  uint64_t hash_indexing_failures = 0;
  uint64_t backfill_scanned_count = 0;
  uint64_t backfill_db_size = 0;
  uint64_t backfill_inqueue_tasks = 0;
  float backfill_complete_percent = 0.0f;
  float backfill_complete_percent_max = 0.0f;
  float backfill_complete_percent_min = 0.0f;
  bool backfill_in_progress = false;
  uint64_t mutation_queue_size = 0;
  uint64_t recent_mutations_queue_delay = 0;
  std::string state;
  std::string error;
  uint64_t schema_fingerprint = 0;
  bool has_schema_mismatch = false;
};

using InfoResponseCallback = absl::AnyInvocable<void(absl::StatusOr<InfoResult>, std::unique_ptr<InfoParameters>)>;

absl::Status PerformInfoFanoutAsync(
    ValkeyModuleCtx* ctx, 
    std::vector<fanout::FanoutSearchTarget>& info_targets,
    coordinator::ClientPool* coordinator_client_pool,
    std::unique_ptr<InfoParameters> parameters,
    vmsdk::ThreadPool* thread_pool, 
    InfoResponseCallback callback);

std::vector<fanout::FanoutSearchTarget> GetInfoTargetsForFanout(ValkeyModuleCtx* ctx);

}  // namespace valkey_search::query::info_fanout

#endif  // VALKEYSEARCH_SRC_QUERY_INFO_FANOUT_H_
