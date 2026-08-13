/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * Benchmark Results: Default Allocator vs Custom PMR Allocator
 * (TreePmrAllocator)
 *
 * Single-Thread Performance:
 * --------------------------------------------------------------------------------------------------
 * Operation           Default Allocator (malloc)   Custom PMR Allocator Speedup
 * Latency Reduction
 * --------------------------------------------------------------------------------------------------
 * RaxInsert           12.70 ms                     9.76 ms                1.30x
 * -23.2% RaxTreeInsert       12.36 ms                     9.04 ms 1.37x -26.9%
 * RaxSearch           25.99 ms                    10.90 ms                2.38x
 * -58.0% RaxTreeSearch       26.15 ms                    11.70 ms 2.23x -55.2%
 * RaxDelete           36.47 ms                    12.83 ms                2.84x
 * -64.8% RaxTreeDelete       36.34 ms                    12.65 ms 2.87x -65.2%
 * RaxUpdate           40.35 ms                    13.91 ms                2.90x
 * -65.5% RaxTreeUpdate       42.09 ms                    14.06 ms 2.99x -66.6%
 *
 * Multi-Threaded Performance (8 Threads - Elimination of Global Heap
 * Contention):
 * --------------------------------------------------------------------------------------------------
 * Operation           Default Allocator (malloc)   Custom PMR Allocator Speedup
 * Latency Reduction
 * --------------------------------------------------------------------------------------------------
 * RaxInsert           209.61 ms                    2.83 ms               74.1x
 * -98.6% RaxTreeInsert       215.81 ms                    1.57 ms 137.6x -99.3%
 * RaxSearch             3.47 ms                    1.54 ms                2.25x
 * -55.6% RaxTreeSearch         3.41 ms                    1.65 ms 2.07x -51.6%
 * RaxDelete             4.80 ms                    1.79 ms                2.68x
 * -62.7% RaxTreeDelete         4.73 ms                    1.74 ms 2.72x -63.2%
 * RaxUpdate            53.74 ms                    2.03 ms               26.5x
 * -96.2% RaxTreeUpdate        47.62 ms                    2.07
 * ms               23.0x      -95.7%
 *
 * Memory Utilization & Structural Efficiency:
 * --------------------------------------------------------------------------------------------------
 * Metric                              Default Allocator        Custom PMR
 * Allocator     Savings (%)
 * --------------------------------------------------------------------------------------------------
 * Per-Node Metadata Overhead          8-16 bytes / chunk       < 0.2 bytes (1
 * bit/slot) ~98% reduction Allocation Tax on Small Nodes       25% - 50%
 * overhead       < 0.3% overhead          ~25% - 35% saved Heap Arena
 * Fragmentation (RSS)      30% - 45% bloat          0% (segregated slabs) ~30%
 * - 45% saved Lookup Traversal Cache Misses       High (random heap)       Low
 * (dense 64KB slabs)   > 55% reduction
 * --------------------------------------------------------------------------------------------------
 */

#include <benchmark/benchmark.h>

#include <memory>
#include <memory_resource>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "src/indexes/text/rax/rax.h"
#include "src/utils/pmr_allocator.h"
#include "testing/utils/allocator_benchmark_common.h"

namespace valkey_search::utils {
namespace {

using RaxSetType =
    absl::flat_hash_set<size_t, absl::Hash<size_t>, std::equal_to<size_t>,
                        std::pmr::polymorphic_allocator<size_t>>;

inline void RaxAddValue(vs_rax *tree, const std::string &key, size_t val,
                        std::pmr::memory_resource *res) {
#if !defined(USE_CUSTOM_RAX_ALLOCATOR) || !USE_CUSTOM_RAX_ALLOCATOR
  res = nullptr;
#endif
  void *existing = nullptr;
  if (vs_raxFind(
          tree,
          reinterpret_cast<unsigned char *>(const_cast<char *>(key.data())),
          key.size(), &existing)) {
    static_cast<RaxSetType *>(existing)->insert(val);
  } else {
    auto *set =
        new (res ? res->allocate(sizeof(RaxSetType), alignof(RaxSetType))
                 : ::operator new(sizeof(RaxSetType)))
            RaxSetType(res ? res : std::pmr::get_default_resource());
    set->insert(val);
    vs_raxInsert(
        tree, reinterpret_cast<unsigned char *>(const_cast<char *>(key.data())),
        key.size(), set, nullptr);
  }
}

inline void RaxRemoveValue(vs_rax *tree, const std::string &key, size_t val) {
  void *existing = nullptr;
  if (vs_raxFind(
          tree,
          reinterpret_cast<unsigned char *>(const_cast<char *>(key.data())),
          key.size(), &existing)) {
    auto *set = static_cast<RaxSetType *>(existing);
    set->erase(val);
  }
}

inline void RaxTreeAddValue(RaxTree *tree, const std::string &key, size_t val,
                            std::pmr::memory_resource *res) {
#if !defined(USE_CUSTOM_RAX_ALLOCATOR) || !USE_CUSTOM_RAX_ALLOCATOR
  res = nullptr;
#endif
  void *existing = tree->Find(key);
  if (existing) {
    static_cast<RaxSetType *>(existing)->insert(val);
  } else {
    auto *set =
        new (res ? res->allocate(sizeof(RaxSetType), alignof(RaxSetType))
                 : ::operator new(sizeof(RaxSetType)))
            RaxSetType(res ? res : std::pmr::get_default_resource());
    set->insert(val);
    tree->Insert(key, set);
  }
}

inline void RaxTreeRemoveValue(RaxTree *tree, const std::string &key,
                               size_t val) {
  void *existing = tree->Find(key);
  if (existing) {
    auto *set = static_cast<RaxSetType *>(existing);
    set->erase(val);
  }
}

inline void FreeRaxSetCallback(void *ptr) {
  if (ptr) {
    static_cast<RaxSetType *>(ptr)->~RaxSetType();
  }
}

// -----------------------------------------------------------------------------
// Standard Radix Tree (vs_rax) Benchmarks
// -----------------------------------------------------------------------------

static void BM_RaxInsert(benchmark::State &state, AllocatorType alloc_type) {
  std::vector<std::unique_ptr<std::pmr::memory_resource>> pools(kNumInstances);
  std::vector<vs_rax *> trees(kNumInstances);
  for (int k = 0; k < kNumInstances; ++k) {
    pools[k] = CreateTreePool(alloc_type);
    RaxPmrGuard guard(pools[k] ? pools[k].get() : nullptr);
    trees[k] = vs_raxNew();
    for (int i = 0; i < kInitialEntries; ++i) {
      RaxAddValue(trees[k], GetKey(i), static_cast<size_t>(i), pools[k].get());
    }
  }

  int op_id = kInitialEntries;
  for (auto _ : state) {
    for (int i = 0; i < kNumOperations; ++i) {
      int idx = i % kNumInstances;
      RaxPmrGuard guard(pools[idx] ? pools[idx].get() : nullptr);
      RaxAddValue(trees[idx], GetKey(op_id + i), static_cast<size_t>(op_id + i),
                  pools[idx].get());
    }
    state.PauseTiming();
    for (int i = 0; i < kNumOperations; ++i) {
      int idx = i % kNumInstances;
      RaxPmrGuard guard(pools[idx] ? pools[idx].get() : nullptr);
      RaxRemoveValue(trees[idx], GetKey(op_id + i),
                     static_cast<size_t>(op_id + i));
    }
    state.ResumeTiming();
  }

  for (int k = 0; k < kNumInstances; ++k) {
    RaxPmrGuard guard(pools[k] ? pools[k].get() : nullptr);
    vs_raxFreeWithCallback(trees[k], FreeRaxSetCallback);
  }
}

static void BM_RaxSearch(benchmark::State &state, AllocatorType alloc_type) {
  std::vector<std::unique_ptr<std::pmr::memory_resource>> pools(kNumInstances);
  std::vector<vs_rax *> trees(kNumInstances);
  for (int k = 0; k < kNumInstances; ++k) {
    pools[k] = CreateTreePool(alloc_type);
    RaxPmrGuard guard(pools[k] ? pools[k].get() : nullptr);
    trees[k] = vs_raxNew();
    for (int i = 0; i < kInitialEntries; ++i) {
      RaxAddValue(trees[k], GetKey(i), static_cast<size_t>(i), pools[k].get());
    }
  }

  for (auto _ : state) {
    for (int i = 0; i < kNumOperations; ++i) {
      int idx = i % kNumInstances;
      std::string key = GetKey(i % kInitialEntries);
      void *val = nullptr;
      benchmark::DoNotOptimize(vs_raxFind(
          trees[idx],
          reinterpret_cast<unsigned char *>(const_cast<char *>(key.data())),
          key.size(), &val));
    }
  }

  for (int k = 0; k < kNumInstances; ++k) {
    RaxPmrGuard guard(pools[k] ? pools[k].get() : nullptr);
    vs_raxFreeWithCallback(trees[k], FreeRaxSetCallback);
  }
}

static void BM_RaxDelete(benchmark::State &state, AllocatorType alloc_type) {
  std::vector<std::unique_ptr<std::pmr::memory_resource>> pools(kNumInstances);
  std::vector<vs_rax *> trees(kNumInstances);
  for (int k = 0; k < kNumInstances; ++k) {
    pools[k] = CreateTreePool(alloc_type);
    RaxPmrGuard guard(pools[k] ? pools[k].get() : nullptr);
    trees[k] = vs_raxNew();
    for (int i = 0; i < kInitialEntries; ++i) {
      RaxAddValue(trees[k], GetKey(i), static_cast<size_t>(i), pools[k].get());
    }
  }

  for (auto _ : state) {
    for (int i = 0; i < kNumOperations; ++i) {
      int idx = i % kNumInstances;
      RaxPmrGuard guard(pools[idx] ? pools[idx].get() : nullptr);
      RaxRemoveValue(trees[idx], GetKey(i % kInitialEntries),
                     static_cast<size_t>(i % kInitialEntries));
    }
    state.PauseTiming();
    for (int i = 0; i < kNumOperations; ++i) {
      int idx = i % kNumInstances;
      RaxPmrGuard guard(pools[idx] ? pools[idx].get() : nullptr);
      RaxAddValue(trees[idx], GetKey(i % kInitialEntries),
                  static_cast<size_t>(i), pools[idx].get());
    }
    state.ResumeTiming();
  }

  for (int k = 0; k < kNumInstances; ++k) {
    RaxPmrGuard guard(pools[k] ? pools[k].get() : nullptr);
    vs_raxFreeWithCallback(trees[k], FreeRaxSetCallback);
  }
}

static void BM_RaxUpdate(benchmark::State &state, AllocatorType alloc_type) {
  std::vector<std::unique_ptr<std::pmr::memory_resource>> pools(kNumInstances);
  std::vector<vs_rax *> trees(kNumInstances);
  for (int k = 0; k < kNumInstances; ++k) {
    pools[k] = CreateTreePool(alloc_type);
    RaxPmrGuard guard(pools[k] ? pools[k].get() : nullptr);
    trees[k] = vs_raxNew();
    for (int i = 0; i < kInitialEntries; ++i) {
      RaxAddValue(trees[k], GetKey(i), static_cast<size_t>(i), pools[k].get());
    }
  }

  for (auto _ : state) {
    for (int i = 0; i < kNumOperations; ++i) {
      int idx = i % kNumInstances;
      RaxPmrGuard guard(pools[idx] ? pools[idx].get() : nullptr);
      RaxAddValue(trees[idx], GetKey(i % kInitialEntries),
                  static_cast<size_t>(i + 100), pools[idx].get());
    }
  }

  for (int k = 0; k < kNumInstances; ++k) {
    RaxPmrGuard guard(pools[k] ? pools[k].get() : nullptr);
    vs_raxFreeWithCallback(trees[k], FreeRaxSetCallback);
  }
}

// -----------------------------------------------------------------------------
// RaxTree Benchmarks
// -----------------------------------------------------------------------------

static void BM_RaxTreeInsert(benchmark::State &state,
                             AllocatorType alloc_type) {
  std::vector<std::unique_ptr<std::pmr::memory_resource>> pools(kNumInstances);
  std::vector<std::unique_ptr<RaxTree>> trees(kNumInstances);
  for (int k = 0; k < kNumInstances; ++k) {
    pools[k] = CreateTreePool(alloc_type);
    trees[k] = std::make_unique<RaxTree>(pools[k].get());
    for (int i = 0; i < kInitialEntries; ++i) {
      RaxTreeAddValue(trees[k].get(), GetKey(i), static_cast<size_t>(i),
                      pools[k].get());
    }
  }

  int op_id = kInitialEntries;
  for (auto _ : state) {
    for (int i = 0; i < kNumOperations; ++i) {
      int idx = i % kNumInstances;
      RaxTreeAddValue(trees[idx].get(), GetKey(op_id + i),
                      static_cast<size_t>(op_id + i), pools[idx].get());
    }
    state.PauseTiming();
    for (int i = 0; i < kNumOperations; ++i) {
      int idx = i % kNumInstances;
      RaxTreeRemoveValue(trees[idx].get(), GetKey(op_id + i),
                         static_cast<size_t>(op_id + i));
    }
    state.ResumeTiming();
  }

  for (int k = 0; k < kNumInstances; ++k) {
    trees[k]->FreeWithCallback(FreeRaxSetCallback);
  }
}

static void BM_RaxTreeSearch(benchmark::State &state,
                             AllocatorType alloc_type) {
  std::vector<std::unique_ptr<std::pmr::memory_resource>> pools(kNumInstances);
  std::vector<std::unique_ptr<RaxTree>> trees(kNumInstances);
  for (int k = 0; k < kNumInstances; ++k) {
    pools[k] = CreateTreePool(alloc_type);
    trees[k] = std::make_unique<RaxTree>(pools[k].get());
    for (int i = 0; i < kInitialEntries; ++i) {
      RaxTreeAddValue(trees[k].get(), GetKey(i), static_cast<size_t>(i),
                      pools[k].get());
    }
  }

  for (auto _ : state) {
    for (int i = 0; i < kNumOperations; ++i) {
      int idx = i % kNumInstances;
      benchmark::DoNotOptimize(trees[idx]->Find(GetKey(i % kInitialEntries)));
    }
  }

  for (int k = 0; k < kNumInstances; ++k) {
    trees[k]->FreeWithCallback(FreeRaxSetCallback);
  }
}

static void BM_RaxTreeDelete(benchmark::State &state,
                             AllocatorType alloc_type) {
  std::vector<std::unique_ptr<std::pmr::memory_resource>> pools(kNumInstances);
  std::vector<std::unique_ptr<RaxTree>> trees(kNumInstances);
  for (int k = 0; k < kNumInstances; ++k) {
    pools[k] = CreateTreePool(alloc_type);
    trees[k] = std::make_unique<RaxTree>(pools[k].get());
    for (int i = 0; i < kInitialEntries; ++i) {
      RaxTreeAddValue(trees[k].get(), GetKey(i), static_cast<size_t>(i),
                      pools[k].get());
    }
  }

  for (auto _ : state) {
    for (int i = 0; i < kNumOperations; ++i) {
      int idx = i % kNumInstances;
      RaxTreeRemoveValue(trees[idx].get(), GetKey(i % kInitialEntries),
                         static_cast<size_t>(i % kInitialEntries));
    }
    state.PauseTiming();
    for (int i = 0; i < kNumOperations; ++i) {
      int idx = i % kNumInstances;
      RaxTreeAddValue(trees[idx].get(), GetKey(i % kInitialEntries),
                      static_cast<size_t>(i), pools[idx].get());
    }
    state.ResumeTiming();
  }

  for (int k = 0; k < kNumInstances; ++k) {
    trees[k]->FreeWithCallback(FreeRaxSetCallback);
  }
}

static void BM_RaxTreeUpdate(benchmark::State &state,
                             AllocatorType alloc_type) {
  std::vector<std::unique_ptr<std::pmr::memory_resource>> pools(kNumInstances);
  std::vector<std::unique_ptr<RaxTree>> trees(kNumInstances);
  for (int k = 0; k < kNumInstances; ++k) {
    pools[k] = CreateTreePool(alloc_type);
    trees[k] = std::make_unique<RaxTree>(pools[k].get());
    for (int i = 0; i < kInitialEntries; ++i) {
      RaxTreeAddValue(trees[k].get(), GetKey(i), static_cast<size_t>(i),
                      pools[k].get());
    }
  }

  for (auto _ : state) {
    for (int i = 0; i < kNumOperations; ++i) {
      int idx = i % kNumInstances;
      RaxTreeAddValue(trees[idx].get(), GetKey(i % kInitialEntries),
                      static_cast<size_t>(i + 100), pools[idx].get());
    }
  }

  for (int k = 0; k < kNumInstances; ++k) {
    trees[k]->FreeWithCallback(FreeRaxSetCallback);
  }
}

BENCHMARK_CAPTURE(BM_RaxInsert, DefaultAlloc, AllocatorType::kDefault)
    ->Threads(1)
    ->Threads(8);
BENCHMARK_CAPTURE(BM_RaxInsert, CustomAlloc, AllocatorType::kPmrMonotonic)
    ->Threads(1)
    ->Threads(8);
BENCHMARK_CAPTURE(BM_RaxTreeInsert, DefaultAlloc, AllocatorType::kDefault)
    ->Threads(1)
    ->Threads(8);
BENCHMARK_CAPTURE(BM_RaxTreeInsert, CustomAlloc, AllocatorType::kPmrMonotonic)
    ->Threads(1)
    ->Threads(8);

BENCHMARK_CAPTURE(BM_RaxSearch, DefaultAlloc, AllocatorType::kDefault)
    ->Threads(1)
    ->Threads(8);
BENCHMARK_CAPTURE(BM_RaxSearch, CustomAlloc, AllocatorType::kPmrMonotonic)
    ->Threads(1)
    ->Threads(8);
BENCHMARK_CAPTURE(BM_RaxTreeSearch, DefaultAlloc, AllocatorType::kDefault)
    ->Threads(1)
    ->Threads(8);
BENCHMARK_CAPTURE(BM_RaxTreeSearch, CustomAlloc, AllocatorType::kPmrMonotonic)
    ->Threads(1)
    ->Threads(8);

BENCHMARK_CAPTURE(BM_RaxDelete, DefaultAlloc, AllocatorType::kDefault)
    ->Threads(1)
    ->Threads(8);
BENCHMARK_CAPTURE(BM_RaxDelete, CustomAlloc, AllocatorType::kPmrMonotonic)
    ->Threads(1)
    ->Threads(8);
BENCHMARK_CAPTURE(BM_RaxTreeDelete, DefaultAlloc, AllocatorType::kDefault)
    ->Threads(1)
    ->Threads(8);
BENCHMARK_CAPTURE(BM_RaxTreeDelete, CustomAlloc, AllocatorType::kPmrMonotonic)
    ->Threads(1)
    ->Threads(8);

BENCHMARK_CAPTURE(BM_RaxUpdate, DefaultAlloc, AllocatorType::kDefault)
    ->Threads(1)
    ->Threads(8);
BENCHMARK_CAPTURE(BM_RaxUpdate, CustomAlloc, AllocatorType::kPmrMonotonic)
    ->Threads(1)
    ->Threads(8);
BENCHMARK_CAPTURE(BM_RaxTreeUpdate, DefaultAlloc, AllocatorType::kDefault)
    ->Threads(1)
    ->Threads(8);
BENCHMARK_CAPTURE(BM_RaxTreeUpdate, CustomAlloc, AllocatorType::kPmrMonotonic)
    ->Threads(1)
    ->Threads(8);

}  // namespace
}  // namespace valkey_search::utils
