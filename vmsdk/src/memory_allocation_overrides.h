/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_MEMORY_ALLOCATION_OVERRIDES_H_
#define VMSDK_SRC_MEMORY_ALLOCATION_OVERRIDES_H_

#include <cstddef>
#include <cstdlib>

#include "vmsdk/src/valkey_module_api/valkey_module.h"

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

namespace vmsdk {

// An allocation that is never reported to the memory accounting, which is what
// makes it safe to use from inside the accounting itself.
//
// The module defines malloc, and every call to it reports, so the counters
// cannot use malloc without re-entering themselves. ValkeyModule_Alloc is the
// same heap without the reporting, so it breaks that cycle.
//
// It is always established by the time this runs. In the module the only path
// here is from inside the module's own malloc, which has just called it, and
// static initializers are deferred until after the allocator switch (see
// vmsdk/deferred_init.lds). In the unit tests, which link this without the
// module's malloc, vmsdk/src/testing_infra/module.h installs a plain
// malloc-backed implementation before main -- late enough to need no
// allocation during static initialization, which is why ShardedAtomic's
// containers carry inline capacity.
inline void* RawSystemMalloc(std::size_t size) {
  return ValkeyModule_Alloc(size);
}

inline void RawSystemFree(void* ptr) { ValkeyModule_Free(ptr); }

}  // namespace vmsdk

namespace vmsdk {

// RawSystemAllocator allocates without reporting to the memory accounting.
//
// This is not an optimization and it cannot be replaced with std::allocator.
// The accounting counters are themselves ShardedAtomics, so
// ReportAllocMemorySize -> ShardedAtomic::Add allocates: it constructs a
// thread_local ThreadLocalNode, whose constructor registers it in a vector, and
// it grows that node's value array under resize_mutex. Route those allocations
// through a reporting allocator and each one calls ReportAllocMemorySize again,
// re-entering either a thread_local's own initialization or a non-reentrant
// absl::Mutex. Tried it: the module hangs on a futex during load, accumulating
// no CPU time, before the server ever accepts connections.
//
// Reporting is not optional here, which is why there is no knob for it: an
// instance that reported would be exactly the cycle above.
template <typename T>
struct RawSystemAllocator {
  // NOLINTNEXTLINE
  typedef T value_type;

  RawSystemAllocator() = default;
  template <typename U>
  constexpr RawSystemAllocator(const RawSystemAllocator<U>&) noexcept {}
  // NOLINTNEXTLINE
  T* allocate(std::size_t n) {
    return static_cast<T*>(RawSystemMalloc(n * sizeof(T)));
  }
  // NOLINTNEXTLINE
  void deallocate(T* p, std::size_t) { RawSystemFree(p); }
};

}  // namespace vmsdk

#endif  // VMSDK_SRC_MEMORY_ALLOCATION_OVERRIDES_H_
