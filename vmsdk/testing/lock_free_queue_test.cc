/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "vmsdk/src/lock_free_queue.h"

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace vmsdk {
namespace {

TEST(LockFreeQueueTest, BasicOperations) {
  LockFreeQueue<int, 16> queue;
  EXPECT_TRUE(queue.IsEmpty());
  EXPECT_EQ(queue.Size(), 0);

  int val = 0;
  EXPECT_FALSE(queue.TryDequeue(val));

  EXPECT_TRUE(queue.TryEnqueue(10));
  EXPECT_TRUE(queue.TryEnqueue(20));
  EXPECT_FALSE(queue.IsEmpty());
  EXPECT_EQ(queue.Size(), 2);

  EXPECT_TRUE(queue.TryDequeue(val));
  EXPECT_EQ(val, 10);
  EXPECT_EQ(queue.Size(), 1);

  EXPECT_TRUE(queue.TryDequeue(val));
  EXPECT_EQ(val, 20);
  EXPECT_TRUE(queue.IsEmpty());
  EXPECT_EQ(queue.Size(), 0);

  EXPECT_FALSE(queue.TryDequeue(val));
}

TEST(LockFreeQueueTest, QueueFullCondition) {
  constexpr size_t kCapacity = 4;
  LockFreeQueue<int, kCapacity> queue;

  for (size_t i = 0; i < kCapacity; ++i) {
    EXPECT_TRUE(queue.TryEnqueue(static_cast<int>(i + 1)));
  }
  EXPECT_EQ(queue.Size(), kCapacity);

  // Queue is full; next enqueue must fail
  EXPECT_FALSE(queue.TryEnqueue(999));

  // Dequeue one item and verify we can enqueue again
  int val = 0;
  EXPECT_TRUE(queue.TryDequeue(val));
  EXPECT_EQ(val, 1);
  EXPECT_EQ(queue.Size(), kCapacity - 1);

  EXPECT_TRUE(queue.TryEnqueue(999));
  EXPECT_EQ(queue.Size(), kCapacity);
}

TEST(LockFreeQueueTest, WrapAround) {
  constexpr size_t kCapacity = 8;
  constexpr size_t kNumIterations = 1000;
  LockFreeQueue<size_t, kCapacity> queue;

  for (size_t i = 0; i < kNumIterations; ++i) {
    EXPECT_TRUE(queue.TryEnqueue(size_t(i)));
    size_t val = 0;
    EXPECT_TRUE(queue.TryDequeue(val));
    EXPECT_EQ(val, i);
  }
  EXPECT_TRUE(queue.IsEmpty());
}

TEST(LockFreeQueueTest, MoveOnlyTypeSupport) {
  LockFreeQueue<std::unique_ptr<std::string>, 16> queue;

  auto ptr1 = std::make_unique<std::string>("hello");
  auto ptr2 = std::make_unique<std::string>("world");

  EXPECT_TRUE(queue.TryEnqueue(std::move(ptr1)));
  EXPECT_TRUE(queue.TryEnqueue(std::move(ptr2)));
  EXPECT_EQ(queue.Size(), 2);

  std::unique_ptr<std::string> out;
  EXPECT_TRUE(queue.TryDequeue(out));
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(*out, "hello");

  EXPECT_TRUE(queue.TryDequeue(out));
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(*out, "world");

  EXPECT_TRUE(queue.IsEmpty());
}

TEST(LockFreeQueueTest, MultiProducerMultiConsumer) {
  constexpr size_t kCapacity = 1024;
  constexpr size_t kProducers = 4;
  constexpr size_t kConsumers = 4;
  constexpr size_t kItemsPerProducer = 10000;
  constexpr size_t kTotalItems = kProducers * kItemsPerProducer;

  LockFreeQueue<size_t, kCapacity> queue;
  std::atomic<bool> start_flag{false};
  std::atomic<size_t> items_dequeued{0};
  std::vector<std::atomic<size_t>> received_counts(kTotalItems);
  for (size_t i = 0; i < kTotalItems; ++i) {
    received_counts[i].store(0);
  }

  std::vector<std::thread> producers;
  producers.reserve(kProducers);
  for (size_t p = 0; p < kProducers; ++p) {
    producers.emplace_back([&, p]() {
      while (!start_flag.load(std::memory_order_relaxed)) {
        std::this_thread::yield();
      }
      size_t start_val = p * kItemsPerProducer;
      for (size_t i = 0; i < kItemsPerProducer; ++i) {
        size_t val = start_val + i;
        while (!queue.TryEnqueue(size_t(val))) {
          std::this_thread::yield();
        }
      }
    });
  }

  std::vector<std::thread> consumers;
  consumers.reserve(kConsumers);
  for (size_t c = 0; c < kConsumers; ++c) {
    consumers.emplace_back([&]() {
      while (!start_flag.load(std::memory_order_relaxed)) {
        std::this_thread::yield();
      }
      while (items_dequeued.load(std::memory_order_relaxed) < kTotalItems) {
        size_t val = 0;
        if (queue.TryDequeue(val)) {
          received_counts[val].fetch_add(1, std::memory_order_relaxed);
          items_dequeued.fetch_add(1, std::memory_order_relaxed);
        } else {
          std::this_thread::yield();
        }
      }
    });
  }

  start_flag.store(true);

  for (auto &t : producers) {
    t.join();
  }
  for (auto &t : consumers) {
    t.join();
  }

  EXPECT_EQ(items_dequeued.load(), kTotalItems);
  for (size_t i = 0; i < kTotalItems; ++i) {
    EXPECT_EQ(received_counts[i].load(), 1)
        << "Item " << i << " was received " << received_counts[i].load() << " times";
  }
  EXPECT_TRUE(queue.IsEmpty());
}

}  // namespace
}  // namespace vmsdk
