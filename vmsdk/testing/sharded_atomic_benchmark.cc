/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <atomic>
#include <thread>

#include "benchmark/benchmark.h"
#include "vmsdk/src/sharded_atomic.h"

namespace vmsdk {

namespace {

struct StandardAtomicWrapper {
  std::atomic<int64_t> val{0};

  inline void Add(int64_t n) { val.fetch_add(n, std::memory_order_relaxed); }

  int64_t GetTotal() const { return val.load(std::memory_order_relaxed); }
};

static StandardAtomicWrapper global_std_atomic;

static void BM_StandardAtomic_Add(benchmark::State& state) {
  // Each thread runs this loop
  for (auto _ : state) {
    global_std_atomic.Add(1);
  }
}

static void BM_StandardAtomic_GetTotal(benchmark::State& state) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(global_std_atomic.GetTotal());
  }
}

static vmsdk::ShardedAtomic<int64_t> global_sharded_atomic;

static void BM_ShardedAtomic_Add(benchmark::State& state) {
  // Each thread runs this loop
  for (auto _ : state) {
    // This hits the "Hot Path" (TLS lookup + relaxed store)
    global_sharded_atomic.Add(1);
  }
}

static void BM_ShardedAtomic_GetTotal(benchmark::State& state) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(global_sharded_atomic.GetTotal());
  }
}

// ----------------------------------------------------------------------------
// REGISTRATION
// ----------------------------------------------------------------------------

// 1. Standard Atomic: Run with 1, 2, 4, 8... threads
BENCHMARK(BM_StandardAtomic_Add)
    ->ThreadRange(1, std::thread::hardware_concurrency())
    ->UseRealTime();  // Use wall clock time to measure throughput accurately

// 2. Sharded Atomic: Run with 1, 2, 4, 8... threads
BENCHMARK(BM_ShardedAtomic_Add)
    ->ThreadRange(1, std::thread::hardware_concurrency())
    ->UseRealTime();

// 3. Standard Atomic: Read Cost of a single thread
BENCHMARK(BM_StandardAtomic_GetTotal);
// 4. Sharded Atomic: Read Cost of a single thread
BENCHMARK(BM_ShardedAtomic_GetTotal);

}  // namespace

}  // namespace vmsdk
BENCHMARK_MAIN();