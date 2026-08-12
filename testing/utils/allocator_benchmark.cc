/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * Performance Benchmark Results for Optimized FixedSizeAllocator (Atomic
 * Bitvector Per Chunk) Run Environment: Linux (48 CPU cores @ 2600 MHz), L3
 * Cache 55296 KiB
 *
 * ------------------------------------------------------------------------------------------------------------------
 * Benchmark Time             CPU   Iterations
 * ------------------------------------------------------------------------------------------------------------------
 * FixedSizeAllocatorFixture/AllocFreePair/64/1/real_time/threads:1              70.2
 * ns         70.2 ns     10198361
 * FixedSizeAllocatorFixture/AllocFreePair/64/1/real_time/threads:2 240 ns 481
 * ns      2957492
 * FixedSizeAllocatorFixture/AllocFreePair/64/1/real_time/threads:4 370 ns 1467
 * ns      2236036
 * FixedSizeAllocatorFixture/AllocFreePair/64/1/real_time/threads:8 366 ns 2820
 * ns      1871776
 * FixedSizeAllocatorFixture/AllocFreePair/64/1/real_time/threads:16 351 ns 2078
 * ns      2132160
 * FixedSizeAllocatorFixture/AllocFreePair/256/1/real_time/threads:1             71.2
 * ns         71.2 ns      9969159
 * FixedSizeAllocatorFixture/AllocFreePair/256/1/real_time/threads:2 299 ns 597
 * ns      2374700
 * FixedSizeAllocatorFixture/AllocFreePair/256/1/real_time/threads:4 380 ns 1511
 * ns      1772828
 * FixedSizeAllocatorFixture/AllocFreePair/256/1/real_time/threads:8 395 ns 2708
 * ns      1983056
 * FixedSizeAllocatorFixture/AllocFreePair/256/1/real_time/threads:16 313 ns
 * 1673 ns      2315392
 * FixedSizeAllocatorFixture/AllocFreePair/1024/1/real_time/threads:1            71.7
 * ns         71.7 ns     10135703
 * FixedSizeAllocatorFixture/AllocFreePair/1024/1/real_time/threads:2 278 ns 555
 * ns      2539346
 * FixedSizeAllocatorFixture/AllocFreePair/1024/1/real_time/threads:4 368 ns
 * 1455 ns      2176548
 * FixedSizeAllocatorFixture/AllocFreePair/1024/1/real_time/threads:8 347 ns
 * 1846 ns      1778360
 * FixedSizeAllocatorFixture/AllocFreePair/1024/1/real_time/threads:16 347 ns
 * 2425 ns      1981488
 * FixedSizeAllocatorFixture/BatchAllocFree/256/1/16/real_time/threads:1 1123 ns
 * 1123 ns       633240
 * FixedSizeAllocatorFixture/BatchAllocFree/256/1/16/real_time/threads:2 4924 ns
 * 9841 ns       134486
 * FixedSizeAllocatorFixture/BatchAllocFree/256/1/16/real_time/threads:4 5755 ns
 * 22867 ns       121388
 * FixedSizeAllocatorFixture/BatchAllocFree/256/1/16/real_time/threads:8 5927 ns
 * 39158 ns       118888
 * FixedSizeAllocatorFixture/BatchAllocFree/256/1/16/real_time/threads:16 5451
 * ns        30932 ns       119936
 * FixedSizeAllocatorFixture/BatchAllocFree/256/1/64/real_time/threads:1 4470 ns
 * 4469 ns       155761
 * FixedSizeAllocatorFixture/BatchAllocFree/256/1/64/real_time/threads:2 21215
 * ns        42390 ns        33032
 * FixedSizeAllocatorFixture/BatchAllocFree/256/1/64/real_time/threads:4 22512
 * ns        88767 ns        29420
 * FixedSizeAllocatorFixture/BatchAllocFree/256/1/64/real_time/threads:8 24193
 * ns       160195 ns        29264
 * FixedSizeAllocatorFixture/BatchAllocFree/256/1/64/real_time/threads:16 22462
 * ns       143594 ns        28576
 * FixedSizeAllocatorFixture/BatchAllocFree/256/1/256/real_time/threads:1 18109
 * ns        18105 ns        38490
 * FixedSizeAllocatorFixture/BatchAllocFree/256/1/256/real_time/threads:2 64067
 * ns       128047 ns        10614
 * FixedSizeAllocatorFixture/BatchAllocFree/256/1/256/real_time/threads:4 81094
 * ns       320095 ns         8688
 * FixedSizeAllocatorFixture/BatchAllocFree/256/1/256/real_time/threads:8 89554
 * ns       555686 ns         8224
 * FixedSizeAllocatorFixture/BatchAllocFree/256/1/256/real_time/threads:16 83684
 * ns       556487 ns         8720
 */

#include <condition_variable>
#include <mutex>
#include <vector>

#include "benchmark/benchmark.h"
#include "src/utils/allocator.h"
#include "src/utils/intrusive_ref_count.h"
#include "vmsdk/src/utils.h"

namespace valkey_search {

namespace {

// ----------------------------------------------------------------------------
// Optimized FixedSizeAllocator Fixture
// ----------------------------------------------------------------------------

class FixedSizeAllocatorFixture : public benchmark::Fixture {
 public:
  void SetUp(const ::benchmark::State &state) override {
    std::unique_lock<std::mutex> lock(mu_);
    if (state.thread_index() == 0) {
      size_t alloc_size = state.range(0);
      bool align = (state.range(1) == 1);
      allocator_ = CREATE_UNIQUE_PTR(FixedSizeAllocator, alloc_size, align);
    }
    uint64_t current_phase = phase_;
    if (++arrived_in_setup_ == state.threads()) {
      arrived_in_setup_ = 0;
      ++phase_;
      cv_.notify_all();
    } else {
      cv_.wait(lock, [current_phase] { return phase_ > current_phase; });
    }
  }

  void TearDown(const ::benchmark::State &state) override {
    std::unique_lock<std::mutex> lock(mu_);
    uint64_t current_phase = phase_;
    if (++arrived_in_teardown_ == state.threads()) {
      allocator_.reset();
      arrived_in_teardown_ = 0;
      ++phase_;
      cv_.notify_all();
    } else {
      cv_.wait(lock, [current_phase] { return phase_ > current_phase; });
    }

    current_phase = phase_;
    if (++arrived_out_teardown_ == state.threads()) {
      arrived_out_teardown_ = 0;
      ++phase_;
      cv_.notify_all();
    } else {
      cv_.wait(lock, [current_phase] { return phase_ > current_phase; });
    }
  }

  static UniqueFixedSizeAllocatorPtr allocator_;
  static std::mutex mu_;
  static std::condition_variable cv_;
  static uint64_t phase_;
  static int arrived_in_setup_;
  static int arrived_in_teardown_;
  static int arrived_out_teardown_;
};

UniqueFixedSizeAllocatorPtr FixedSizeAllocatorFixture::allocator_(
    nullptr, [](FixedSizeAllocator *allocator_instance) {
      if (allocator_instance) {
        allocator_instance->DecrementRef();
      }
    });
std::mutex FixedSizeAllocatorFixture::mu_;
std::condition_variable FixedSizeAllocatorFixture::cv_;
uint64_t FixedSizeAllocatorFixture::phase_{0};
int FixedSizeAllocatorFixture::arrived_in_setup_{0};
int FixedSizeAllocatorFixture::arrived_in_teardown_{0};
int FixedSizeAllocatorFixture::arrived_out_teardown_{0};

BENCHMARK_DEFINE_F(FixedSizeAllocatorFixture, AllocFreePair)
(benchmark::State &state) {
  const size_t alloc_size = state.range(0);
  for (auto _ : state) {
    char *ptr = allocator_->Allocate(alloc_size);
    benchmark::DoNotOptimize(ptr);
    Allocator::Free(ptr);
  }
}

BENCHMARK_REGISTER_F(FixedSizeAllocatorFixture, AllocFreePair)
    ->Args({64, 1})
    ->Args({256, 1})
    ->Args({1024, 1})
    ->ThreadRange(1, 16)
    ->UseRealTime();

BENCHMARK_DEFINE_F(FixedSizeAllocatorFixture, BatchAllocFree)
(benchmark::State &state) {
  const size_t alloc_size = state.range(0);
  const size_t batch_size = state.range(2);
  std::vector<char *> batch(batch_size);

  for (auto _ : state) {
    for (size_t batch_index = 0; batch_index < batch_size; ++batch_index) {
      batch[batch_index] = allocator_->Allocate(alloc_size);
      benchmark::DoNotOptimize(batch[batch_index]);
    }
    for (size_t batch_index = 0; batch_index < batch_size; ++batch_index) {
      Allocator::Free(batch[batch_index]);
    }
  }
}

BENCHMARK_REGISTER_F(FixedSizeAllocatorFixture, BatchAllocFree)
    ->Args({256, 1, 16})
    ->Args({256, 1, 64})
    ->Args({256, 1, 256})
    ->ThreadRange(1, 16)
    ->UseRealTime();

}  // namespace

}  // namespace valkey_search

int main(int argc, char **argv) {
  vmsdk::TrackCurrentAsMainThread();
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
