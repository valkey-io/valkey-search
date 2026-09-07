/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_MEMORY_ALLOCATION_OVERRIDES_H_
#define VMSDK_SRC_MEMORY_ALLOCATION_OVERRIDES_H_

#include <cstddef>
#include <type_traits>

#include "vmsdk/src/memory_allocation.h"

// VMSDK_USE_VALKEY_ALLOC_OVERRIDES is defined when this build routes the
// module's heap through ValkeyModule_Alloc/Free. When it is not defined, the
// allocator definitions in memory_allocation_c_api.cc are compiled out and
// everything runs on the system allocator.
//
// Sanitizer builds opt out so that the sanitizer's own allocator sees every
// allocation -- defining malloc here would fight its interceptors.
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

extern "C" {
// glibc's allocator, reached by name so that it is not captured by the module's
// own malloc/free (see memory_allocation_c_api.cc). Used for the pre-switch
// fallback path and by RawSystemAllocator below.
void* __libc_malloc(size_t size);
void __libc_free(void* ptr);
void* __libc_calloc(size_t nmemb, size_t size);
void* __libc_realloc(void* ptr, size_t size);
}  // extern "C"

namespace vmsdk {

struct DisableRawSystemAllocatorReporting {
};  // Pass this (or void) to DISABLE reporting

// RawSystemAllocator allocates straight from glibc, bypassing both the Valkey
// allocator and the memory accounting.
//
// This is not an optimization and it cannot be replaced with std::allocator.
// The accounting counters are themselves ShardedAtomics, so
// ReportAllocMemorySize -> ShardedAtomic::Add allocates: it constructs a
// thread_local ThreadLocalNode, whose constructor registers it in a vector, and
// it grows that node's value array under resize_mutex. Route those allocations
// through the module allocator and each one calls ReportAllocMemorySize again,
// re-entering either a thread_local's own initialization or a non-reentrant
// absl::Mutex. Tried it: the module hangs on a futex during load, accumulating
// no CPU time, before the server ever accepts connections.
//
// Allocating from Valkey but skipping the accounting would break the cycle too,
// but ShardedAtomic is also linked into the unit test executables, where
// ValkeyModule_Alloc is a mock that is unset until a fixture installs it. Going
// straight to glibc is what keeps this allocator independent of everything it
// underpins.
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
    return static_cast<T*>(__libc_malloc(n * sizeof(T)));
  }
  // NOLINTNEXTLINE
  void deallocate(T* p, std::size_t) {
    if constexpr (!std::is_same_v<Tag, DisableRawSystemAllocatorReporting>) {
      ReportFreeMemorySize(sizeof(T));
    }
    __libc_free(p);
  }
};

}  // namespace vmsdk

#endif  // VMSDK_SRC_MEMORY_ALLOCATION_OVERRIDES_H_
