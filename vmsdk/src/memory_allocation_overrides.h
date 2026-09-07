/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_MEMORY_ALLOCATION_OVERRIDES_H_
#define VMSDK_SRC_MEMORY_ALLOCATION_OVERRIDES_H_

#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <type_traits>

#include "vmsdk/src/memory_allocation.h"

// VMSDK_USE_VALKEY_ALLOC_OVERRIDES is defined when this build routes the
// module's heap through ValkeyModule_Alloc/Free. When it is not defined, the
// malloc macros and the operator new/delete replacements below are compiled
// out and everything runs on the system allocator.
//
// Sanitizer builds opt out so that the sanitizer's own allocator sees every
// allocation.
//
// macOS opts out as well. It is a build-only target:
// .github/workflows/macos.yml runs build.sh with no tests, and the module is
// never executed there. That matters because the deferral of static
// initializers that makes the Valkey allocator usable from the very start of
// module load (see vmsdk/deferred_init.lds and vmsdk/src/deferred_init.cc) is
// implemented with a GNU linker script, and Mach-O has no equivalent.
//
// IF macOS EVER BECOMES A PRODUCTION TARGET, this problem must be solved for
// that platform before the overrides can be enabled there. The Mach-O analogue
// of the .init_array rename is the __DATA,__mod_init_func section, which would
// need to be renamed at link time (ld64 -rename_section) and walked explicitly
// from ValkeyModule_OnLoad the same way deferred_init.cc does. Simply defining
// VMSDK_USE_VALKEY_ALLOC_OVERRIDES on macOS without that would reintroduce the
// bug this design removes: static initializers allocating from the system
// allocator and later being freed with ValkeyModule_Free.
#if !defined(SAN_BUILD) && !defined(__APPLE__)
#define VMSDK_USE_VALKEY_ALLOC_OVERRIDES 1
#endif

#if defined(__clang__)
#define WEAK_SYMBOL __attribute__((weak))
#else
#define WEAK_SYMBOL
#endif

extern "C" {
// NOLINTNEXTLINE
WEAK_SYMBOL void* (*__real_malloc)(size_t) = malloc;
// NOLINTNEXTLINE
WEAK_SYMBOL void (*__real_free)(void*) = free;
// NOLINTNEXTLINE
WEAK_SYMBOL void* (*__real_calloc)(size_t, size_t) = calloc;
// NOLINTNEXTLINE
WEAK_SYMBOL void* (*__real_realloc)(void*, size_t) = realloc;
// NOLINTNEXTLINE
WEAK_SYMBOL void* (*__real_aligned_alloc)(size_t, size_t) = aligned_alloc;
// NOLINTNEXTLINE
WEAK_SYMBOL int (*__real_posix_memalign)(void**, size_t,
                                         size_t) = posix_memalign;
// NOLINTNEXTLINE
WEAK_SYMBOL void* (*__real_valloc)(size_t) = valloc;
// NOLINTNEXTLINE
__attribute__((weak)) size_t empty_usable_size(void* ptr) noexcept;
}  // extern "C"

// Different exception specifier between CLANG & GCC
#ifdef __clang__
#define PMES
#else
#define PMES noexcept
#endif

extern "C" {
// See https://www.gnu.org/software/libc/manual/html_node/Replacing-malloc.html
// NOLINTNEXTLINE
void* __wrap_malloc(size_t size) noexcept;
// NOLINTNEXTLINE
void __wrap_free(void* ptr) noexcept;
// NOLINTNEXTLINE
void* __wrap_calloc(size_t __nmemb, size_t size) noexcept;
// NOLINTNEXTLINE
void* __wrap_realloc(void* ptr, size_t size) noexcept;
// NOLINTNEXTLINE
void* __wrap_aligned_alloc(size_t __alignment, size_t __size) noexcept;
// NOLINTNEXTLINE
int __wrap_malloc_usable_size(void* ptr) noexcept;
// NOLINTNEXTLINE
int __wrap_posix_memalign(void** r, size_t __alignment, size_t __size) PMES;
// NOLINTNEXTLINE
void* __wrap_valloc(size_t size) noexcept;
}  // extern "C"

#ifdef VMSDK_USE_VALKEY_ALLOC_OVERRIDES
// NOLINTNEXTLINE
#define malloc(...) __wrap_malloc(__VA_ARGS__)
// NOLINTNEXTLINE
#define calloc(...) __wrap_calloc(__VA_ARGS__)
// NOLINTNEXTLINE
#define realloc(...) __wrap_realloc(__VA_ARGS__)
// NOLINTNEXTLINE
#define free(...) __wrap_free(__VA_ARGS__)
// NOLINTNEXTLINE
#define aligned_alloc(...) __wrap_aligned_alloc(__VA_ARGS__)
// NOLINTNEXTLINE
#define posix_memalign(...) __wrap_posix_memalign(__VA_ARGS__)
// NOLINTNEXTLINE
#define valloc(...) __wrap_valloc(__VA_ARGS__)

void* operator new(size_t size) noexcept(false);
void operator delete(void* p) noexcept;
void operator delete(void* p, size_t size) noexcept;
void* operator new[](size_t size) noexcept(false);
void operator delete[](void* p) noexcept;
void operator delete[](void* p, size_t size) noexcept;
void* operator new(size_t size, const std::nothrow_t& nt) noexcept;
void* operator new[](size_t size, const std::nothrow_t& nt) noexcept;
void operator delete(void* p, const std::nothrow_t& nt) noexcept;
void operator delete[](void* p, const std::nothrow_t& nt) noexcept;
void* operator new(size_t size, std::align_val_t alignment) noexcept(false);
void* operator new(size_t size, std::align_val_t alignment,
                   const std::nothrow_t&) noexcept;
void operator delete(void* p, std::align_val_t alignment) noexcept;
void operator delete(void* p, std::align_val_t alignment,
                     const std::nothrow_t&) noexcept;
void operator delete(void* p, size_t size, std::align_val_t alignment) noexcept;
void* operator new[](size_t size, std::align_val_t alignment) noexcept(false);
void* operator new[](size_t size, std::align_val_t alignment,
                     const std::nothrow_t&) noexcept;
void operator delete[](void* p, std::align_val_t alignment) noexcept;
void operator delete[](void* p, std::align_val_t alignment,
                       const std::nothrow_t&) noexcept;
void operator delete[](void* p, size_t size,
                       std::align_val_t alignment) noexcept;
#endif  // VMSDK_USE_VALKEY_ALLOC_OVERRIDES

namespace vmsdk {
// UseValkeyAlloc, ResetValkeyAlloc and the pre-init accounting are declared in
// memory_allocation.h, which callers can include without picking up the malloc
// macros above.

struct DisableRawSystemAllocatorReporting {
};  // Pass this (or void) to DISABLE reporting
// RawSystemAllocator implements an allocator that will not go through
// the SystemAllocTracker, for use by the SystemAllocTracker to prevent
// infinite recursion when tracking pointers.
template <typename T, typename Tag = void>
struct RawSystemAllocator {
  // NOLINTNEXTLINE
  typedef T value_type;

  RawSystemAllocator() = default;
  template <typename U>
  constexpr RawSystemAllocator(const RawSystemAllocator<U>&) noexcept {}
  // NOLINTNEXTLINE
  T* allocate(std::size_t n) {
    if constexpr (!std::is_same_v<Tag, DisableRawSystemAllocatorReporting>) {
      ReportAllocMemorySize(n * sizeof(T));
    }
    return static_cast<T*>(__real_malloc(n * sizeof(T)));
  }
  // NOLINTNEXTLINE
  void deallocate(T* p, std::size_t) {
    if constexpr (!std::is_same_v<Tag, DisableRawSystemAllocatorReporting>) {
      ReportFreeMemorySize(sizeof(T));
    }
    __real_free(p);
  }
};

}  // namespace vmsdk

#endif  // VMSDK_SRC_MEMORY_ALLOCATION_OVERRIDES_H_
