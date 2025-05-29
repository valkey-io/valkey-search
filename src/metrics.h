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

#ifndef VALKEYSEARCH_SRC_METRICS_H_
#define VALKEYSEARCH_SRC_METRICS_H_

#include <atomic>
#include <cstdint>

#include "absl/time/time.h"
#include "vmsdk/src/latency_sampler.h"

// 2 is the value used by Redis and correlates to ~40KiB and ~1% precision.
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
    uint64_t query_successful_requests_cnt{0};
    uint64_t query_failed_requests_cnt{0};
    uint64_t query_hybrid_requests_cnt{0};
    uint64_t query_inline_filtering_requests_cnt{0};
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
