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

bool thread_local PerformCapture = false;
bool CaptureRequested = false;

Enable::Enable() : previous_(PerformCapture) {
  PerformCapture = CaptureRequested;
}

Enable::~Enable() {
  PerformCapture = previous_;
}

Disable::Disable() : previous_(PerformCapture) {
  PerformCapture = false;
}

Disable::~Disable() {
  PerformCapture = previous_;
}

static absl::Mutex pool_capture_mutex{};
static absl::flat_hash_map<vmsdk::Backtrace, size_t, std::hash<vmsdk::Backtrace>, std::equal_to<vmsdk::Backtrace>, RawSystemAllocator<std::pair<const vmsdk::Backtrace, size_t>>> backtraces;

//
// Danger Will Robinson:
// This is called from malloc, so NO normal allocations are allowed here
//
void DoCapture(size_t size) {
  if (!PerformCapture) {
    return;
  }
  vmsdk::Backtrace backtrace;
  backtrace.Capture();
  absl::MutexLock lock(&pool_capture_mutex);
  auto itr = backtraces.find(backtrace);
  if (itr == backtraces.end()) {
    backtraces.emplace(std::move(backtrace), 1);
    return;
  } else {
    itr->second++;
  }
}

void Control(bool enable) {
  if (enable) {
    malloc_hook = DoCapture;
    CaptureRequested = true;
  } else {
    malloc_hook = [](size_t){};
    CaptureRequested = false;
  }
}

std::multimap<size_t, Backtrace> GetCaptures() {
  Disable disable;
  absl::MutexLock lock(&pool_capture_mutex);
  std::multimap<size_t, Backtrace> result;
  for (auto& [backtrace, count] : backtraces) {
    result.emplace(count, backtrace);
    if (result.size() > 20) {
      result.erase(result.begin());
    }
  }
  return result;
}

}

} // namespace vmsdk

