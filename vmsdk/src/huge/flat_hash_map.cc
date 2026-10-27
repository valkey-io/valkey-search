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

/// Register the "huge-hash-map-migrate-batch" config: the minimum number of
/// entries a vmsdk::HugeFlatHashMap migrates per insertion or erase.
auto migrate_batch =
    config::NumberBuilder("huge-hash-map-migrate-batch",  // name
                          kDefaultMigrateBatch,           // default
                          1,                              // min
                          1 << 20)                        // max
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
        .WithModifyCallback([](long long value) {
          internal::g_huge_map_start_jitter_percent = value;
        })
        .Build();

DEV_INTEGER_COUNTER(huge, huge_hash_map_rehashes);

std::atomic<uint64_t> start_sequence{0};

}  // namespace

namespace internal {

size_t g_huge_map_migrate_batch = kDefaultMigrateBatch;
size_t g_huge_map_start_jitter_percent = kDefaultStartJitterPercent;

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

}  // namespace vmsdk
