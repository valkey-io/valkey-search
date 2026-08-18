/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include <chrono>
#include <iostream>
#include <vector>
#include <atomic>
#include <thread>

#include "vmsdk/src/thread_pool.h"

int main() {
  constexpr size_t kNumThreads = 16;
  constexpr size_t kProducerThreads = 8;

  std::cout << "Running 16-Worker ThreadPool Benchmark..." << std::endl;

  vmsdk::ThreadPool pool("benchmark-pool", kNumThreads);
  pool.StartWorkers();

  std::atomic<size_t> completed_count{0};
  std::atomic<bool> stop_flag{false};

  auto start_time = std::chrono::high_resolution_clock::now();

  std::vector<std::thread> producers;
  producers.reserve(kProducerThreads);

  for (size_t i = 0; i < kProducerThreads; ++i) {
    producers.emplace_back([&pool, &completed_count, &stop_flag]() {
      while (!stop_flag.load(std::memory_order_relaxed)) {
        pool.Schedule([&completed_count]() {
          completed_count.fetch_add(1, std::memory_order_relaxed);
        }, vmsdk::ThreadPool::Priority::kHigh);
      }
    });
  }

  // Run producers for 2 seconds
  std::this_thread::sleep_for(std::chrono::seconds(2));
  stop_flag.store(true, std::memory_order_relaxed);

  for (auto& producer : producers) {
    producer.join();
  }

  pool.JoinWorkers();
  auto end_time = std::chrono::high_resolution_clock::now();

  double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
  double throughput = completed_count.load() / elapsed_sec;

  std::cout << "Completed Tasks: " << completed_count.load() << std::endl;
  std::cout << "Elapsed Time:    " << elapsed_sec << " seconds" << std::endl;
  std::cout << "Throughput:      " << throughput / 1e6 << " M ops/sec" << std::endl;

  return 0;
}
