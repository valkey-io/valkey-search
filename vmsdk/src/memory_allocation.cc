/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/memory_allocation.h"

#include <unistd.h>

#include <atomic>
#include <cstdint>

namespace vmsdk {

// Use the standard system allocator by default. Note that this is required
// since any allocation done before Valkey module initialization (namely global
// static constructors that do heap allocation, which are run on dl_open) cannot
// invoke Valkey modules api since the associated C function pointers are only
// initialized as part of the module initialization process. Refer
// https://redis.com/blog/using-the-redis-allocator-in-rust for more details.

thread_local static int64_t memory_delta = 0;

std::atomic<uint64_t> used_memory_bytes{0};

void ResetValkeyAllocStats() {
  used_memory_bytes.store(0, std::memory_order_relaxed);
  memory_delta = 0;
}

uint64_t GetUsedMemoryCnt() { return used_memory_bytes; }

void ReportAllocMemorySize(uint64_t size) {
  vmsdk::used_memory_bytes.fetch_add(size, std::memory_order_relaxed);

  memory_delta += static_cast<int64_t>(size);
}

void ReportFreeMemorySize(uint64_t size) {
  if (size > used_memory_bytes) {
    vmsdk::used_memory_bytes.store(0, std::memory_order_relaxed);
  } else {
    vmsdk::used_memory_bytes.fetch_sub(size, std::memory_order_relaxed);
  }

  memory_delta -= static_cast<int64_t>(size);
}

int64_t GetMemoryDelta() { return memory_delta; }

void SetMemoryDelta(int64_t delta) { memory_delta = delta; }

}  // namespace vmsdk
