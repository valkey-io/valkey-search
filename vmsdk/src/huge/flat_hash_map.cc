/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/huge/flat_hash_map.h"

#include <atomic>
#include <cmath>
#include <cstdint>

#include "vmsdk/src/info.h"
#include "vmsdk/src/module_config.h"

namespace vmsdk {

namespace {

constexpr long long kDefaultMigrateBatch = 8;
constexpr long long kDefaultStartJitterPercent = 10;
constexpr long long kDefaultAdvisoryMigrateBatch = 32;
constexpr long long kDefaultBulkMigrateBatch = 4096;
constexpr long long kDefaultKeyInfoMigrateBatch = 512;

/// Register the "huge-hash-map-migrate-batch" config: the minimum number of
/// entries a vmsdk::HugeFlatHashMap migrates per insertion or erase.
auto migrate_batch =
    config::NumberBuilder("huge-hash-map-migrate-batch",  // name
                          kDefaultMigrateBatch,           // default
                          1,                              // min
                          1 << 20)                        // max
        .Dev()
        .WithModifyCallback(
            [](long long value) { internal::g_huge_map_migrate_batch = value; })
        .Build();

/// Register the "huge-hash-map-start-jitter-percent" config: how much of its
/// growth budget a vmsdk::HugeFlatHashMap may leave unused when it starts a
/// growth cycle. Capped at 25 so the new table still doubles.
auto start_jitter_percent =
    config::NumberBuilder("huge-hash-map-start-jitter-percent",  // name
                          kDefaultStartJitterPercent,            // default
                          0,                                     // min
                          25)                                    // max
        .Dev()
        .WithModifyCallback([](long long value) {
          internal::g_huge_map_start_jitter_percent = value;
        })
        .Build();

/// Register the "huge-hash-map-advisory-migrate-batch" config: the number of
/// entries each advisory MigrateSome() call on the ingest and query paths
/// moves: on the main thread, and at the start of each writer mutation task.
auto advisory_migrate_batch =
    config::NumberBuilder("huge-hash-map-advisory-migrate-batch",  // name
                          kDefaultAdvisoryMigrateBatch,            // default
                          1,                                       // min
                          1 << 20)                                 // max
        .Dev()
        .WithModifyCallback([](long long value) {
          internal::g_huge_map_advisory_migrate_batch = value;
        })
        .Build();

/// Register the "huge-hash-map-bulk-migrate-batch" config: the number of
/// entries each advisory MigrateSome() call moves where a bigger batch is
/// cheap: the writer task a query schedules, and server cron.
auto bulk_migrate_batch =
    config::NumberBuilder("huge-hash-map-bulk-migrate-batch",  // name
                          kDefaultBulkMigrateBatch,            // default
                          1,                                   // min
                          1 << 20)                             // max
        .Dev()
        .WithModifyCallback([](long long value) {
          internal::g_huge_map_bulk_migrate_batch = value;
        })
        .Build();

/// Register the "huge-hash-map-key-info-migrate-batch" config: the number of
/// entries each advisory MigrateSome() call on a schema's db_key_info_ moves.
/// Only the ingest and query paths drain that map, so it gets a bigger batch
/// than the other main-thread maps.
auto key_info_migrate_batch =
    config::NumberBuilder("huge-hash-map-key-info-migrate-batch",  // name
                          kDefaultKeyInfoMigrateBatch,             // default
                          1,                                       // min
                          1 << 20)                                 // max
        .Dev()
        .WithModifyCallback([](long long value) {
          internal::g_huge_map_key_info_migrate_batch = value;
        })
        .Build();

DEV_INTEGER_COUNTER(huge, huge_hash_map_rehashes);

std::atomic<uint64_t> start_sequence{0};

}  // namespace

namespace internal {

size_t g_huge_map_migrate_batch = kDefaultMigrateBatch;
size_t g_huge_map_start_jitter_percent = kDefaultStartJitterPercent;
size_t g_huge_map_advisory_migrate_batch = kDefaultAdvisoryMigrateBatch;
size_t g_huge_map_bulk_migrate_batch = kDefaultBulkMigrateBatch;
size_t g_huge_map_key_info_migrate_batch = kDefaultKeyInfoMigrateBatch;

size_t HugeMapEarlyStart(size_t growth) {
  constexpr double kGoldenRatioFraction = 0.6180339887498949;
  uint64_t n = start_sequence.fetch_add(1, std::memory_order_relaxed);
  double fraction = std::fmod(n * kGoldenRatioFraction, 1.0);
  return static_cast<size_t>(fraction * growth *
                             g_huge_map_start_jitter_percent / 100);
}

void CountHugeMapRehash() { huge_hash_map_rehashes.Increment(); }

long long GetHugeMapRehashCount() { return huge_hash_map_rehashes.Get(); }

}  // namespace internal

config::Number &GetHugeFlatHashMapMigrateBatch() {
  return dynamic_cast<config::Number &>(*migrate_batch);
}

config::Number &GetHugeFlatHashMapStartJitterPercent() {
  return dynamic_cast<config::Number &>(*start_jitter_percent);
}

config::Number &GetHugeFlatHashMapAdvisoryMigrateBatch() {
  return dynamic_cast<config::Number &>(*advisory_migrate_batch);
}

config::Number &GetHugeFlatHashMapBulkMigrateBatch() {
  return dynamic_cast<config::Number &>(*bulk_migrate_batch);
}

config::Number &GetHugeFlatHashMapKeyInfoMigrateBatch() {
  return dynamic_cast<config::Number &>(*key_info_migrate_batch);
}

}  // namespace vmsdk
