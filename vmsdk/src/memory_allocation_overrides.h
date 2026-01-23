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

#ifndef SAN_BUILD
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
#endif  // !SAN_BUILD

namespace vmsdk {
// Updates the custom allocator to perform any future allocations using the
// Valkey allocator.
void UseValkeyAlloc();

// Switch back to the default allocator. No guarantees around atomicity. Only
// safe in single-threaded or testing environments.
void ResetValkeyAlloc();
}  // namespace vmsdk

#endif  // VMSDK_SRC_MEMORY_ALLOCATION_OVERRIDES_H_
