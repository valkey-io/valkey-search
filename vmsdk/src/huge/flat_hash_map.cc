/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/huge/flat_hash_map.h"

#include "vmsdk/src/info.h"
#include "vmsdk/src/module_config.h"

namespace vmsdk {

namespace {

constexpr long long kDefaultMigrateBatch = 8;

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

DEV_INTEGER_COUNTER(huge, huge_hash_map_rehashes);

}  // namespace

namespace internal {

size_t g_huge_map_migrate_batch = kDefaultMigrateBatch;

void CountHugeMapRehash() { huge_hash_map_rehashes.Increment(); }

long long GetHugeMapRehashCount() { return huge_hash_map_rehashes.Get(); }

}  // namespace internal

config::Number &GetHugeFlatHashMapMigrateBatch() {
  return dynamic_cast<config::Number &>(*migrate_batch);
}

}  // namespace vmsdk
