/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/scoped_thread_cpu_monitor.h"

#include <pthread.h>

#include <cstdint>
#include <thread>  // NOLINT(build/c++11)
#include <vector>

#include "gtest/gtest.h"
#include "vmsdk/src/sharded_atomic.h"
// Installs the allocator ShardedAtomic needs before main.
#include "vmsdk/src/testing_infra/module.h"  // IWYU pragma: keep
#include "vmsdk/src/thread_monitoring.h"

namespace vmsdk {

namespace {

constexpr uint64_t kBurnCpuTimeUsec = 10000;

// Spins until the calling thread has consumed at least `cpu_time_usec` of CPU
// time, as reported by the same source ScopedThreadCPUMonitor reads from.
void BurnCpu(uint64_t cpu_time_usec) {
  ThreadMonitor monitor(pthread_self());
  auto start = monitor.GetCPUTime();
  ASSERT_TRUE(start.ok());
  volatile uint64_t sink = 0;
  while (true) {
    for (int i = 0; i < 10000; ++i) {
      sink = sink + i;
    }
    auto now = monitor.GetCPUTime();
    ASSERT_TRUE(now.ok());
    if (now.value() - start.value() >= cpu_time_usec) {
      return;
    }
  }
}

}  // namespace

TEST(ScopedThreadCPUMonitorTest, NullAccumulatorIsNoop) {
  // Must neither crash nor account anything when no accumulator is given.
  ScopedThreadCPUMonitor monitor(nullptr);
  BurnCpu(kBurnCpuTimeUsec);
}

TEST(ScopedThreadCPUMonitorTest, AccumulatesCpuTimeOfScope) {
  ShardedAtomic<uint64_t> accumulator;
  {
    ScopedThreadCPUMonitor monitor(&accumulator);
    BurnCpu(kBurnCpuTimeUsec);
  }
  uint64_t total = accumulator.GetTotal();
  EXPECT_GE(total, kBurnCpuTimeUsec);

  // CPU time spent outside the scope is not accounted.
  BurnCpu(kBurnCpuTimeUsec);
  EXPECT_EQ(accumulator.GetTotal(), total);
}

TEST(ScopedThreadCPUMonitorTest, AccumulatesAcrossScopes) {
  ShardedAtomic<uint64_t> accumulator;
  for (int i = 0; i < 3; ++i) {
    ScopedThreadCPUMonitor monitor(&accumulator);
    BurnCpu(kBurnCpuTimeUsec);
  }
  EXPECT_GE(accumulator.GetTotal(), 3 * kBurnCpuTimeUsec);
}

TEST(ScopedThreadCPUMonitorTest, AccumulatesAcrossThreads) {
  constexpr int kNumThreads = 4;
  ShardedAtomic<uint64_t> accumulator;
  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&accumulator]() {
      ScopedThreadCPUMonitor monitor(&accumulator);
      BurnCpu(kBurnCpuTimeUsec);
    });
  }
  for (auto &thread : threads) {
    thread.join();
  }
  EXPECT_GE(accumulator.GetTotal(), kNumThreads * kBurnCpuTimeUsec);
}

}  // namespace vmsdk
