/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <cstddef>
#include <cstring>
#include <functional>
#include <new>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "absl/synchronization/mutex.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

// clang-format off
// We put this at the end since it will otherwise mangle the malloc symbols in
// the dependencies.
#include "vmsdk/src/memory_allocation_overrides.h"

namespace vmsdk {

// SystemAllocTracker tracks memory allocations to the system allocator, so that
// subsequent free calls can be redirected to the appropriate allocator.
class SystemAllocTracker {
 public:
  static SystemAllocTracker& GetInstance() {
    static absl::NoDestructor<SystemAllocTracker> instance;
    return *instance;
  }
  SystemAllocTracker() = default;
  SystemAllocTracker(const SystemAllocTracker&) = delete;
  SystemAllocTracker& operator=(const SystemAllocTracker&) = delete;
  ~SystemAllocTracker() = default;
  void TrackPointer(void* ptr) {
    if (ptr == nullptr) {
      return;
    }
    absl::MutexLock lock(&mutex_);
    tracked_ptrs_.insert(ptr);
  }
  bool IsTracked(void* ptr) {
    absl::MutexLock lock(&mutex_);
    return tracked_ptrs_.contains(ptr);
  }
  bool UntrackPointer(void* ptr) {
    absl::MutexLock lock(&mutex_);
    return tracked_ptrs_.erase(ptr);
  }

 private:
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
      ReportAllocMemorySize(n * sizeof(T));
      return static_cast<T*>(__real_malloc(n * sizeof(T)));
    }
    // NOLINTNEXTLINE
    void deallocate(T* p, std::size_t) {
      ReportFreeMemorySize(sizeof(T));
      __real_free(p);
    }
  };

  absl::Mutex mutex_;
  absl::flat_hash_set<void*, absl::Hash<void*>, std::equal_to<void*>,
                      RawSystemAllocator<void*>>
      tracked_ptrs_ ABSL_GUARDED_BY(mutex_);
};

void* PerformAndTrackMalloc(size_t size, void* (*malloc_fn)(size_t),
                            size_t (*malloc_size_fn)(void*)) {
  void* ptr = malloc_fn(size);
  if (ptr != nullptr) {
    ReportAllocMemorySize(malloc_size_fn(ptr));
  }
  return ptr;
}
void* PerformAndTrackCalloc(size_t n, size_t size,
                            void* (*calloc_fn)(size_t, size_t),
                            size_t (*malloc_size_fn)(void*)) {
  void* ptr = calloc_fn(n, size);
  if (ptr != nullptr) {
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
  if (ptr != nullptr) {
    old_size = malloc_size_fn(ptr);
  }
  void* new_ptr = realloc_fn(ptr, size);
  if (new_ptr != nullptr) {
    if (ptr != nullptr) {
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
  if (ptr != nullptr) {
    ReportAllocMemorySize(malloc_size_fn(ptr));
  }
  return ptr;
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
  if (!vmsdk::IsUsingValkeyAlloc()) {
    auto ptr =
        vmsdk::PerformAndTrackMalloc(size, __real_malloc, empty_usable_size);
    vmsdk::SystemAllocTracker::GetInstance().TrackPointer(ptr);
    return ptr;
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
  bool was_tracked =
      vmsdk::SystemAllocTracker::GetInstance().UntrackPointer(ptr);
  // During bootstrap - there are some cases where memory is still allocated
  // outside of our wrapper functions - for example if a library calls into
  // another DSO which doesn't have our wrapped symbols (namely libc.so). For
  // this reason, we bypass the tracking during the bootstrap phase.
  if (was_tracked || !vmsdk::IsUsingValkeyAlloc()) {
    vmsdk::PerformAndTrackFree(ptr, __real_free, empty_usable_size);
  } else {
    vmsdk::PerformAndTrackFree(ptr, ValkeyModule_Free,
                               ValkeyModule_MallocUsableSize);
  }
}
// NOLINTNEXTLINE
void* __wrap_calloc(size_t __nmemb, size_t size) noexcept {
  if (!vmsdk::IsUsingValkeyAlloc()) {
    auto ptr = vmsdk::PerformAndTrackCalloc(__nmemb, size, __real_calloc,
                                            empty_usable_size);
    vmsdk::SystemAllocTracker::GetInstance().TrackPointer(ptr);
    return ptr;
  }
  return vmsdk::PerformAndTrackCalloc(__nmemb, AlignSize(size), ValkeyModule_Calloc,
                                      ValkeyModule_MallocUsableSize);
}

void* __wrap_realloc(void* ptr, size_t size) noexcept {
  bool was_tracked = false;
  if (ptr != nullptr) {
    was_tracked = vmsdk::SystemAllocTracker::GetInstance().UntrackPointer(ptr);
  }
  if (vmsdk::IsUsingValkeyAlloc() && !was_tracked) {
    // Forcing 16-byte alignment in Valkey, which may otherwise return 8-byte
    // aligned memory.
    return vmsdk::PerformAndTrackRealloc(ptr, AlignSize(size),
                                         ValkeyModule_Realloc,
                                         ValkeyModule_MallocUsableSize);
  } else {
    auto new_ptr = vmsdk::PerformAndTrackRealloc(ptr, size, __real_realloc,
                                                 empty_usable_size);
    vmsdk::SystemAllocTracker::GetInstance().TrackPointer(new_ptr);
    return new_ptr;
  }
}
// NOLINTNEXTLINE
void* __wrap_aligned_alloc(size_t __alignment, size_t __size) noexcept {
  if (!vmsdk::IsUsingValkeyAlloc()) {
    auto ptr = vmsdk::PerformAndTrackAlignedAlloc(
        __alignment, __size, __real_aligned_alloc, empty_usable_size);
    vmsdk::SystemAllocTracker::GetInstance().TrackPointer(ptr);
    return ptr;
  }

  return vmsdk::PerformAndTrackMalloc(AlignSize(__size, __alignment),
                                      ValkeyModule_Alloc,
                                      ValkeyModule_MallocUsableSize);
}

int __wrap_malloc_usable_size(void* ptr) noexcept {
  if (vmsdk::SystemAllocTracker::GetInstance().IsTracked(ptr)) {
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

#ifndef ASAN_BUILD
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
#endif // !ASAN_BUILD
