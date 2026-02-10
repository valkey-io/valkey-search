/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_METRICS_H_
#define VALKEYSEARCH_SRC_METRICS_H_

#include <atomic>
#include <cstdint>

#include "absl/time/time.h"
#include "vmsdk/src/latency_sampler.h"

// 2 is the value used by Valkey and correlates to ~40KiB and ~1% precision.
#define LATENCY_PRECISION 2

namespace valkey_search {
class Metrics {
 public:
  static Metrics& GetInstance() {
    static Metrics instance;
    return instance;
  }
  ~Metrics() = default;

  struct Stats {
    uint64_t reclaimable_memory{0};
    uint64_t query_successful_requests_cnt{0};
    uint64_t query_failed_requests_cnt{0};
    uint64_t query_result_record_dropped_cnt{0};
    uint64_t query_hybrid_requests_cnt{0};
    std::atomic<uint64_t> query_nonvector_requests_cnt{0};
    std::atomic<uint64_t> query_vector_requests_cnt{0};
    std::atomic<uint64_t> query_text_requests_cnt{0};
    std::atomic<uint64_t> query_inline_filtering_requests_cnt{0};
    std::atomic<uint64_t> query_prefiltering_requests_cnt{0};
    std::atomic<uint64_t> hnsw_add_exceptions_cnt{0};
    std::atomic<uint64_t> hnsw_remove_exceptions_cnt{0};
    std::atomic<uint64_t> hnsw_modify_exceptions_cnt{0};
    std::atomic<uint64_t> hnsw_search_exceptions_cnt{0};
    std::atomic<uint64_t> hnsw_create_exceptions_cnt{0};
    std::atomic<uint64_t> flat_add_exceptions_cnt{0};
    std::atomic<uint64_t> flat_remove_exceptions_cnt{0};
    std::atomic<uint64_t> flat_modify_exceptions_cnt{0};
    std::atomic<uint64_t> flat_search_exceptions_cnt{0};
    std::atomic<uint64_t> flat_create_exceptions_cnt{0};
    std::atomic<uint64_t> worker_thread_pool_suspend_cnt{0};
    std::atomic<uint64_t> writer_worker_thread_pool_resumed_cnt{0};
    std::atomic<uint64_t> reader_worker_thread_pool_resumed_cnt{0};
    std::atomic<uint64_t> writer_worker_thread_pool_suspension_expired_cnt{0};
    uint64_t rdb_load_success_cnt{0};
    uint64_t rdb_load_failure_cnt{0};
    uint64_t rdb_save_success_cnt{0};
    uint64_t rdb_save_failure_cnt{0};

    // FT.INTERNAL_UPDATE error handling metrics
    std::atomic<uint64_t> ft_internal_update_parse_failures_cnt{0};
    std::atomic<uint64_t> ft_internal_update_process_failures_cnt{0};
    std::atomic<uint64_t> ft_internal_update_call_failures_cnt{0};
    std::atomic<uint64_t> process_internal_update_callback_failures_cnt{0};
    std::atomic<uint64_t> ft_internal_update_skipped_entries_cnt{0};
    vmsdk::LatencySampler hnsw_vector_index_search_latency{
        absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
        absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    vmsdk::LatencySampler flat_vector_index_search_latency{
        absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
        absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    std::atomic<uint64_t> coordinator_server_get_global_metadata_success_cnt{0};
    std::atomic<uint64_t> coordinator_server_get_global_metadata_failure_cnt{0};
    std::atomic<uint64_t> coordinator_server_search_index_partition_success_cnt{
        0};
    std::atomic<uint64_t> coordinator_server_search_index_partition_failure_cnt{
        0};
    std::atomic<uint64_t> coordinator_client_get_global_metadata_success_cnt{0};
    std::atomic<uint64_t> coordinator_client_get_global_metadata_failure_cnt{0};
    std::atomic<uint64_t> coordinator_client_search_index_partition_success_cnt{
        0};
    std::atomic<uint64_t> coordinator_client_search_index_partition_failure_cnt{
        0};
    std::atomic<uint64_t> coordinator_bytes_out{0};
    std::atomic<uint64_t> coordinator_bytes_in{0};

    // Global ingestion stats (counts across all indexes)
    std::atomic<uint64_t> ingest_hash_keys{0};
    std::atomic<uint64_t> backfill_hash_keys{0};
    std::atomic<uint64_t> ingest_json_keys{0};
    std::atomic<uint64_t> backfill_json_keys{0};
    std::atomic<uint64_t> ingest_field_vector{0};
    std::atomic<uint64_t> ingest_field_numeric{0};
    std::atomic<uint64_t> ingest_field_tag{0};
    std::atomic<uint64_t> ingest_field_text{0};
    std::atomic<uint64_t> ingest_last_batch_size{0};
    std::atomic<uint64_t> ingest_total_batches{0};
    std::atomic<uint64_t> ingest_total_failures{0};
    vmsdk::LatencySampler
        coordinator_client_get_global_metadata_failure_latency{
            absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
            absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    vmsdk::LatencySampler
        coordinator_client_search_index_partition_failure_latency{
            absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
            absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    vmsdk::LatencySampler
        coordinator_client_get_global_metadata_success_latency{
            absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
            absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    vmsdk::LatencySampler
        coordinator_client_search_index_partition_success_latency{
            absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
            absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    vmsdk::LatencySampler
        coordinator_server_get_global_metadata_failure_latency{
            absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
            absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    vmsdk::LatencySampler
        coordinator_server_search_index_partition_failure_latency{
            absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
            absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    vmsdk::LatencySampler
        coordinator_server_get_global_metadata_success_latency{
            absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
            absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    vmsdk::LatencySampler
        coordinator_server_search_index_partition_success_latency{
            absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
            absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    // Time Slice Mutex metrics
    std::atomic<uint64_t> time_slice_read_periods{0};
    std::atomic<uint64_t> time_slice_read_time{0};  // microseconds, cumulative
    std::atomic<uint64_t> time_slice_queries{0};
    std::atomic<uint64_t> time_slice_write_periods{0};
    std::atomic<uint64_t> time_slice_write_time{0};  // microseconds, cumulative
    std::atomic<uint64_t> time_slice_upserts{0};
    std::atomic<uint64_t> time_slice_deletes{0};

    std::atomic<uint64_t> info_fanout_retry_cnt{0};
    std::atomic<uint64_t> info_fanout_fail_cnt{0};
    std::atomic<uint64_t> pause_handle_cluster_message_round_cnt{0};

    // Full-text query in-flight blocking metrics
    std::atomic<uint64_t> fulltext_query_blocked_cnt{0};
    std::atomic<uint64_t> fulltext_query_retry_cnt{0};
  };
  static Stats& GetStats() { return GetInstance().stats_; }

 private:
  mutable Stats stats_;
  Metrics() = default;
  Metrics(const Metrics&) = delete;
  Metrics& operator=(const Metrics&) = delete;
};
}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_METRICS_H_
