/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/sharded_atomic.h"

#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace vmsdk {
namespace {

TEST(ShardedAtomicTest, BasicOperations) {
  ShardedAtomic<int64_t> atomic;

  EXPECT_EQ(atomic.GetTotal(), 0);

  atomic.Add(5);
  EXPECT_EQ(atomic.GetTotal(), 5);

  atomic.Subtract(2);
  EXPECT_EQ(atomic.GetTotal(), 3);

  ++atomic;
  EXPECT_EQ(atomic.GetTotal(), 4);

  atomic++;
  EXPECT_EQ(atomic.GetTotal(), 5);

  --atomic;
  EXPECT_EQ(atomic.GetTotal(), 4);

  atomic--;
  EXPECT_EQ(atomic.GetTotal(), 3);

  atomic.Reset();
  EXPECT_EQ(atomic.GetTotal(), 0);
}

TEST(ShardedAtomicTest, IndependentCounters) {
  ShardedAtomic<int64_t> atomic1;
  ShardedAtomic<int64_t> atomic2;
  ShardedAtomic<uint64_t> atomic3;

  atomic1.Add(10);
  atomic2.Add(20);
  atomic3.Add(30);

  EXPECT_EQ(atomic1.GetTotal(), 10);
  EXPECT_EQ(atomic2.GetTotal(), 20);
  EXPECT_EQ(atomic3.GetTotal(), 30);

  atomic1.Reset();
  EXPECT_EQ(atomic1.GetTotal(), 0);
  EXPECT_EQ(atomic2.GetTotal(), 20);
  EXPECT_EQ(atomic3.GetTotal(), 30);
}

TEST(ShardedAtomicTest, MultithreadedOperations) {
  ShardedAtomic<int64_t> atomic;

  const int kNumThreads = 10;
  const int kNumIterations = 10000;

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&atomic]() {
      for (int j = 0; j < kNumIterations; ++j) {
        atomic.Add(1);
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(atomic.GetTotal(), kNumThreads * kNumIterations);
}

TEST(ShardedAtomicTest, CopyConstructorAndAssignment) {
  ShardedAtomic<int64_t> atomic1;
  atomic1.Add(10);

  // Copy constructor
  ShardedAtomic<int64_t> atomic2 = atomic1;
  EXPECT_EQ(atomic2.GetTotal(), 10);

  atomic2.Add(5);
  EXPECT_EQ(atomic1.GetTotal(), 15);
  EXPECT_EQ(atomic2.GetTotal(), 15);

  // Assignment operator
  ShardedAtomic<int64_t> atomic3;
  atomic3.Add(100);

  atomic3 = atomic1;
  EXPECT_EQ(atomic3.GetTotal(), 15);

  atomic3.Add(5);
  EXPECT_EQ(atomic1.GetTotal(), 20);
  EXPECT_EQ(atomic2.GetTotal(), 20);
  EXPECT_EQ(atomic3.GetTotal(), 20);
}

TEST(ShardedAtomicTest, ThreadLocalNodesResize) {
  // Test what happens when many ShardedAtomic instances are created
  // This will force the ThreadLocalNode to resize its internal array.
  std::vector<ShardedAtomic<int64_t>> atomics;
  atomics.reserve(100);
  for (int i = 0; i < 100; ++i) {
    atomics.emplace_back();
    atomics.back().Add(i);
  }

  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(atomics[i].GetTotal(), i);
  }
}

TEST(ShardedAtomicTest, RetiredTotalsOnThreadExit) {
  ShardedAtomic<int64_t> atomic;

  std::thread t([&atomic]() {
    atomic.Add(10);
  });
  t.join();

  // The thread has exited, so its ThreadLocalNode was destroyed.
  // The value should now be in the retired_totals_ of the CounterRegistry.
  EXPECT_EQ(atomic.GetTotal(), 10);
}

TEST(ShardedAtomicTest, AggressiveAddRemove) {
  ShardedAtomic<int64_t> baseline;
  baseline.Add(42);

  // Aggressively create and destroy ShardedAtomics to ensure index reuse works
  // and no state bleeds over from previous usages of the same index.
  for (int i = 0; i < 1000; ++i) {
    ShardedAtomic<int64_t> temp;
    temp.Add(i);
    EXPECT_EQ(temp.GetTotal(), i);
  }

  EXPECT_EQ(baseline.GetTotal(), 42);

  // Concurrently race allocation and deallocation across threads.
  std::vector<std::thread> threads;
  threads.reserve(10);
  for (int t = 0; t < 10; ++t) {
    threads.emplace_back([]() {
      for (int i = 0; i < 100; ++i) {
        ShardedAtomic<int64_t> temp;
        temp.Add(i);
        EXPECT_EQ(temp.GetTotal(), i);
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  EXPECT_EQ(baseline.GetTotal(), 42);
}

}  // namespace
}  // namespace vmsdk
