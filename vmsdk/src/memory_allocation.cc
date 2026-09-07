/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/memory_allocation.h"

#include <unistd.h>

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "absl/base/optimization.h"
#include "vmsdk/src/sharded_atomic.h"

namespace vmsdk {

// Use the standard system allocator by default. Note that this is required
// since any allocation done before Valkey module initialization (namely global
// static constructors that do heap allocation, which are run on dl_open) cannot
// invoke Valkey modules api since the associated C function pointers are only
// initialized as part of the module initialization process. Refer
// https://redis.com/blog/using-the-redis-allocator-in-rust for more details.

thread_local static int64_t memory_delta = 0;

ShardedAtomic<uint64_t> used_memory_bytes;

void ResetValkeyAllocStats() {
  used_memory_bytes.Reset();
  memory_delta = 0;
}

uint64_t GetUsedMemoryCnt() { return used_memory_bytes.GetTotal(); }

void ReportAllocMemorySize(uint64_t size) {
  used_memory_bytes.Add(size);

  memory_delta += static_cast<int64_t>(size);
}

void ReportFreeMemorySize(uint64_t size) {
  used_memory_bytes.Subtract(size);
  memory_delta -= static_cast<int64_t>(size);
}

int64_t GetMemoryDelta() { return memory_delta; }

void SetMemoryDelta(int64_t delta) { memory_delta = delta; }

// We use a combination of a thread local static variable and a global atomic
// variable to perform the switch to the new allocator. The global is only
// accessed during the initial loading phase, and once we switch allocators the
// thread local variable is exclusively used. This should guarantee that the
// switch is done atomically while not having performance impact during steady
// state.
//
// In the module the switch happens at the very top of ValkeyModule_OnLoad,
// before the deferred static initializers run, so nothing in the module ever
// takes the system-allocator path. Unit test executables never switch, and do
// not link the allocator definitions at all.
thread_local static bool thread_using_valkey_module_alloc = false;
static std::atomic<bool> use_valkey_module_alloc_switch = false;

bool IsUsingValkeyAlloc() {
  if (ABSL_PREDICT_FALSE(
          !thread_using_valkey_module_alloc &&
          use_valkey_module_alloc_switch.load(std::memory_order_relaxed))) {
    thread_using_valkey_module_alloc = true;
    return true;
  }
  return thread_using_valkey_module_alloc;
}

void UseValkeyAlloc() {
  use_valkey_module_alloc_switch.store(true, std::memory_order_relaxed);
}

void ResetValkeyAlloc() {
  use_valkey_module_alloc_switch.store(false, std::memory_order_relaxed);
  thread_using_valkey_module_alloc = false;
  ResetValkeyAllocStats();
}

// Records use of the system-allocator fallback path. std::atomic so that this
// stays race-free wherever the fallback is the normal path; both have constexpr
// constructors and so are constant-initialized, which makes them safe to touch
// before any static initializer has run.
static std::atomic<size_t> preinit_alloc_count{0};
static std::atomic<void*> preinit_first_caller{nullptr};

void RecordSystemAllocation(void* caller) {
  if (preinit_alloc_count.fetch_add(1, std::memory_order_relaxed) == 0) {
    preinit_first_caller.store(caller, std::memory_order_relaxed);
  }
}

size_t GetPreInitAllocationCount() {
  return preinit_alloc_count.load(std::memory_order_relaxed);
}

void* GetPreInitFirstCaller() {
  return preinit_first_caller.load(std::memory_order_relaxed);
}

void* PerformAndTrackMalloc(size_t size, void* (*malloc_fn)(size_t),
                            size_t (*malloc_size_fn)(void*)) {
  void* ptr = malloc_fn(size);
  if (ABSL_PREDICT_TRUE(ptr != nullptr)) {
    ReportAllocMemorySize(malloc_size_fn(ptr));
  }
  return ptr;
}

void* PerformAndTrackCalloc(size_t n, size_t size,
                            void* (*calloc_fn)(size_t, size_t),
                            size_t (*malloc_size_fn)(void*)) {
  void* ptr = calloc_fn(n, size);
  if (ABSL_PREDICT_TRUE(ptr != nullptr)) {
    ReportAllocMemorySize(malloc_size_fn(ptr));
  }
  return ptr;
}

void PerformAndTrackFree(void* ptr, void (*free_fn)(void*),
                         size_t (*malloc_size_fn)(void*)) {
  ReportFreeMemorySize(malloc_size_fn(ptr));
  free_fn(ptr);
}

void* PerformAndTrackRealloc(void* ptr, size_t size,
                             void* (*realloc_fn)(void*, size_t),
                             size_t (*malloc_size_fn)(void*)) {
  size_t old_size = 0;
  if (ABSL_PREDICT_TRUE(ptr != nullptr)) {
    old_size = malloc_size_fn(ptr);
  }
  void* new_ptr = realloc_fn(ptr, size);
  if (ABSL_PREDICT_TRUE(new_ptr != nullptr)) {
    if (ABSL_PREDICT_TRUE(ptr != nullptr)) {
      ReportFreeMemorySize(old_size);
    }
    ReportAllocMemorySize(malloc_size_fn(new_ptr));
  }
  return new_ptr;
}

}  // namespace vmsdk
