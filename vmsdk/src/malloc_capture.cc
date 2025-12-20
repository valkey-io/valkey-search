/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/malloc_capture.h"

#include <absl/container/flat_hash_map.h>
#include <absl/log/check.h>
#include <absl/synchronization/mutex.h>
#include <execinfo.h>

#include <functional>

// clang-format off
// We put this at the end since it will otherwise mangle the malloc symbols in
// the dependencies.
#include "vmsdk/src/memory_allocation_overrides.h"

// RawSystemAllocator implements an allocator that will not go through
// the SystemAllocTracker, for use by the SystemAllocTracker to prevent
// infinite recursion when tracking pointers.
template <typename T>
struct RawSystemAllocator {
  // NOLINTNEXTLINE
  typedef T value_type;

  RawSystemAllocator() = default;
  template <typename U>
  constexpr RawSystemAllocator(const RawSystemAllocator<U>&) noexcept {}
  // NOLINTNEXTLINE
  T* allocate(std::size_t n) {
    return static_cast<T*>(__real_malloc(n * sizeof(T)));
  }
  // NOLINTNEXTLINE
  void deallocate(T* p, std::size_t) {
    __real_free(p);
  }
};

namespace vmsdk {

extern void (*malloc_hook)(size_t);

namespace malloc_capture {

static bool thread_local PerformCapture = false;
bool CaptureRequested = false;

MarkStack::MarkStack() {
  PerformCapture = CaptureRequested;
}

MarkStack::~MarkStack() {
  PerformCapture = false;
}

static absl::Mutex pool_debug_mutex;
static absl::flat_hash_map<vmsdk::Backtrace, size_t, std::hash<vmsdk::Backtrace>, std::equal_to<vmsdk::Backtrace>, RawSystemAllocator<std::pair<const vmsdk::Backtrace, size_t>>> backtraces;

void DoCapture(size_t size) {
  vmsdk::Backtrace backtrace;
  backtrace.Capture();
  absl::MutexLock lock(&pool_debug_mutex);
  auto itr = backtraces.find(backtrace);
  if (itr == backtraces.end()) {
    backtraces.emplace(std::move(backtrace), 1);
    return;
  } else {
    itr->second++;
  }
}

void MallocCaptureControl(bool enable) {
  if (enable) {
    malloc_hook = DoCapture;
    CaptureRequested = true;
  } else {
    malloc_hook = [](size_t){};
    CaptureRequested = false;
  }
}

}

} // namespace vmsdk

