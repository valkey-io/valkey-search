/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <unistd.h>

#include <atomic>
#include <cstddef>
#include <new>

#include "absl/base/optimization.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

// clang-format off
// We put this at the end since it will otherwise mangle the malloc symbols in
// the dependencies.
#include "vmsdk/src/memory_allocation_overrides.h"

namespace vmsdk {
// We use a combination of a thread local static variable and a global atomic
// variable to perform the switch to the new allocator. The global is only
// accessed during the initial loading phase, and once we switch allocators the
// thread local variable is exclusively used. This should guarantee that the
// switch is done atomically while not having performance impact during steady
// state.
//
// In the module the switch happens at the very top of ValkeyModule_OnLoad,
// before the deferred static initializers run, so nothing in the module ever
// takes the system-allocator path. Unit test executables never switch: their
// static initializers run at process start as usual and everything stays on the
// system allocator.
thread_local static bool thread_using_valkey_module_alloc = false;
static std::atomic<bool> use_valkey_module_alloc_switch = false;

bool IsUsingValkeyAlloc() {
  if (ABSL_PREDICT_FALSE(!thread_using_valkey_module_alloc &&
      use_valkey_module_alloc_switch.load(std::memory_order_relaxed))) {
    thread_using_valkey_module_alloc = true;
    return true;
  }
  return thread_using_valkey_module_alloc;
}

// Records use of the system-allocator fallback path. std::atomic so that unit
// test executables, where this path is the normal one, do not race; both have
// constexpr constructors and so are constant-initialized, which makes them safe
// to touch before any static initializer has run.
static std::atomic<size_t> preinit_alloc_count{0};
static std::atomic<void*> preinit_first_caller{nullptr};

static void RecordSystemAllocation(void* caller) {
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
void* PerformAndTrackAlignedAlloc(size_t align, size_t size,
                                  void*(aligned_alloc_fn)(size_t, size_t),
                                  size_t (*malloc_size_fn)(void*)) {
  void* ptr = aligned_alloc_fn(align, size);
  if (ABSL_PREDICT_TRUE(ptr != nullptr)) {
    ReportAllocMemorySize(malloc_size_fn(ptr));
  }
  return ptr;
}

void UseValkeyAlloc() {
  use_valkey_module_alloc_switch.store(true, std::memory_order_relaxed);
}

void ResetValkeyAlloc() {
  use_valkey_module_alloc_switch.store(false, std::memory_order_relaxed);
  thread_using_valkey_module_alloc = false;
  ResetValkeyAllocStats();
}

}  // namespace vmsdk

extern "C" {
// Our allocator doesn't support tracking system memory size, so we just
// return 0.
// NOLINTNEXTLINE
__attribute__((weak)) size_t empty_usable_size(void* ptr) noexcept { return 0; }

// For Valkey allocation - we need to ensure alignment by taking advantage of
// jemalloc alignment properties, as there is no aligned malloc module
// function.
//
// "... Chunks are always aligned to multiples of the chunk size..."
//
// See https://linux.die.net/man/3/jemalloc
size_t AlignSize(size_t size, int alignment = 16) {
  return (size + alignment - 1) & ~(alignment - 1);
}

void* __wrap_malloc(size_t size) noexcept {
  if (ABSL_PREDICT_FALSE(!vmsdk::IsUsingValkeyAlloc())) {
    vmsdk::RecordSystemAllocation(__builtin_return_address(0));
    return vmsdk::PerformAndTrackMalloc(size, __real_malloc,
                                        empty_usable_size);
  }
  // Forcing 16-byte alignment in Valkey, which may otherwise return 8-byte
  // aligned memory.
  return vmsdk::PerformAndTrackMalloc(AlignSize(size), ValkeyModule_Alloc,
                                      ValkeyModule_MallocUsableSize);
}
void __wrap_free(void* ptr) noexcept {
  if (ptr == nullptr) {
    return;
  }
  if (ABSL_PREDICT_FALSE(!vmsdk::IsUsingValkeyAlloc())) {
    vmsdk::RecordSystemAllocation(__builtin_return_address(0));
    vmsdk::PerformAndTrackFree(ptr, __real_free, empty_usable_size);
    return;
  }
  vmsdk::PerformAndTrackFree(ptr, ValkeyModule_Free,
                             ValkeyModule_MallocUsableSize);
}
// NOLINTNEXTLINE
void* __wrap_calloc(size_t __nmemb, size_t size) noexcept {
  if (ABSL_PREDICT_FALSE(!vmsdk::IsUsingValkeyAlloc())) {
    vmsdk::RecordSystemAllocation(__builtin_return_address(0));
    return vmsdk::PerformAndTrackCalloc(__nmemb, size, __real_calloc,
                                        empty_usable_size);
  }
  return vmsdk::PerformAndTrackCalloc(__nmemb, AlignSize(size),
                                      ValkeyModule_Calloc,
                                      ValkeyModule_MallocUsableSize);
}

void* __wrap_realloc(void* ptr, size_t size) noexcept {
  if (ABSL_PREDICT_FALSE(ptr == nullptr)) {
    return __wrap_malloc(size);
  }
  if (ABSL_PREDICT_FALSE(!vmsdk::IsUsingValkeyAlloc())) {
    vmsdk::RecordSystemAllocation(__builtin_return_address(0));
    return vmsdk::PerformAndTrackRealloc(ptr, size, __real_realloc,
                                         empty_usable_size);
  }
  return vmsdk::PerformAndTrackRealloc(ptr, AlignSize(size),
                                       ValkeyModule_Realloc,
                                       ValkeyModule_MallocUsableSize);
}
// NOLINTNEXTLINE
void* __wrap_aligned_alloc(size_t __alignment, size_t __size) noexcept {
  if (ABSL_PREDICT_FALSE(!vmsdk::IsUsingValkeyAlloc())) {
    vmsdk::RecordSystemAllocation(__builtin_return_address(0));
    return vmsdk::PerformAndTrackAlignedAlloc(
        __alignment, __size, __real_aligned_alloc, empty_usable_size);
  }

  return vmsdk::PerformAndTrackMalloc(AlignSize(__size, __alignment),
                                      ValkeyModule_Alloc,
                                      ValkeyModule_MallocUsableSize);
}

int __wrap_malloc_usable_size(void* ptr) noexcept {
  if (ABSL_PREDICT_FALSE(!vmsdk::IsUsingValkeyAlloc())) {
    return empty_usable_size(ptr);
  }
  return ValkeyModule_MallocUsableSize(ptr);
}

// NOLINTNEXTLINE
int __wrap_posix_memalign(void** r, size_t __alignment, size_t __size) PMES {
  *r = __wrap_aligned_alloc(__alignment, __size);
  return 0;
}

void* __wrap_valloc(size_t size) noexcept {
  return __wrap_aligned_alloc(sysconf(_SC_PAGESIZE), size);
}

}  // extern "C"

size_t GetNewAllocSize(size_t size) {
  if (size == 0) {
    return 1;
  }
  return size;
}

#ifdef VMSDK_USE_VALKEY_ALLOC_OVERRIDES
void* operator new(size_t size) noexcept(false) {
  return __wrap_malloc(GetNewAllocSize(size));
}
void operator delete(void* p) noexcept { __wrap_free(p); }
void operator delete(void* p, size_t size) noexcept { __wrap_free(p); }
void* operator new[](size_t size) noexcept(false) {
  // A non-null pointer is expected to be returned even if size = 0.
  if (size == 0) {
    size++;
  }
  return __wrap_malloc(size);
}
void operator delete[](void* p) noexcept { __wrap_free(p); }
// NOLINTNEXTLINE
void operator delete[](void* p, size_t size) noexcept { __wrap_free(p); }
// NOLINTNEXTLINE
void* operator new(size_t size, const std::nothrow_t& nt) noexcept {
  return __wrap_malloc(GetNewAllocSize(size));
}
// NOLINTNEXTLINE
void* operator new[](size_t size, const std::nothrow_t& nt) noexcept {
  return __wrap_malloc(GetNewAllocSize(size));
}
void operator delete(void* p, const std::nothrow_t& nt) noexcept {
  __wrap_free(p);
}
void operator delete[](void* p, const std::nothrow_t& nt) noexcept {
  __wrap_free(p);
}
void* operator new(size_t size, std::align_val_t alignment) noexcept(false) {
  return __wrap_aligned_alloc(static_cast<size_t>(alignment),
                              GetNewAllocSize(size));
}
void* operator new(size_t size, std::align_val_t alignment,
                   const std::nothrow_t&) noexcept {
  return __wrap_aligned_alloc(static_cast<size_t>(alignment),
                              GetNewAllocSize(size));
}
void operator delete(void* p, std::align_val_t alignment) noexcept {
  __wrap_free(p);
}
void operator delete(void* p, std::align_val_t alignment,
                     const std::nothrow_t&) noexcept {
  __wrap_free(p);
}
void operator delete(void* p, size_t size,
                     std::align_val_t alignment) noexcept {
  __wrap_free(p);
}
void* operator new[](size_t size, std::align_val_t alignment) noexcept(false) {
  return __wrap_aligned_alloc(static_cast<size_t>(alignment),
                              GetNewAllocSize(size));
}
void* operator new[](size_t size, std::align_val_t alignment,
                     const std::nothrow_t&) noexcept {
  return __wrap_aligned_alloc(static_cast<size_t>(alignment),
                              GetNewAllocSize(size));
}
void operator delete[](void* p, std::align_val_t alignment) noexcept {
  __wrap_free(p);
}
void operator delete[](void* p, std::align_val_t alignment,
                       const std::nothrow_t&) noexcept {
  __wrap_free(p);
}
void operator delete[](void* p, size_t size,
                       std::align_val_t alignment) noexcept {
  __wrap_free(p);
}
#endif  // VMSDK_USE_VALKEY_ALLOC_OVERRIDES
