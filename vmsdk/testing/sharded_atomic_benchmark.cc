/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <atomic>
#include <thread>

#include "benchmark/benchmark.h"

// Include your header file
#include "vmsdk/src/sharded_atomic.h"

namespace vmsdk {

namespace {
// ----------------------------------------------------------------------------
// BASELINE: Standard std::atomic
// This represents the "old" way (Global contention)
// ----------------------------------------------------------------------------
struct StandardAtomicWrapper {
  std::atomic<int64_t> val{0};

  inline void Add(int64_t n) {
    // Typical fetch_add used in global counters
    val.fetch_add(n, std::memory_order_relaxed);
  }

  int64_t GetTotal() const { return val.load(std::memory_order_relaxed); }
};

// Global instance for the benchmark
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

// ----------------------------------------------------------------------------
// TARGET: vmsdk::ShardedAtomic
// This represents the "new" way (Thread-local, no contention)
// ----------------------------------------------------------------------------
// Global instance for the benchmark
static vmsdk::ShardedAtomic<int64_t> global_sharded_atomic;

static void BM_ShardedAtomic_Add(benchmark::State& state) {
  // Each thread runs this loop
  for (auto _ : state) {
    // This hits the "Hot Path" (TLS lookup + relaxed store)
    global_sharded_atomic.Add(1);
  }
}

// ----------------------------------------------------------------------------
// READ BENCHMARK (Optional)
// Tests the cost of "GetTotal" while other threads are writing
// ----------------------------------------------------------------------------
static void BM_ShardedAtomic_GetTotal(benchmark::State& state) {
  // Only the main thread does the reading in this specific test setup,
  // or all threads read (depending on what you want to test).
  // Here we test the cost of summation.
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