/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

// This translation unit runs before the module's static initializers have run.
// It must therefore not define, or depend on, anything that is dynamically
// initialized -- no absl logging, no std::string, no function-local statics
// with non-trivial types. Everything here is zero-initialized or constant.

#include "vmsdk/src/deferred_init.h"

#include <cstddef>
#include <cstdio>
#include <cstdlib>

#include "vmsdk/src/memory_allocation.h"

extern "C" {
// Bounds of the relocated .init_array, provided by vmsdk/deferred_init.lds.
// Weak: builds that do not apply the linker script (macOS, sanitizer builds)
// leave these undefined, and static initialization happens at dlopen() as
// usual.
extern void (*__vmsdk_init_array_start[])(int, char **, char **)
    __attribute__((weak));
extern void (*__vmsdk_init_array_end[])(int, char **, char **)
    __attribute__((weak));
}  // extern "C"

namespace vmsdk {
namespace {
// Zero-initialized, so safe to read before any initializer has run.
size_t initializers_run = 0;
}  // namespace

size_t GetDeferredInitializerCount() { return initializers_run; }

size_t RunDeferredStaticInitializers() {
  if (__vmsdk_init_array_start == nullptr ||
      __vmsdk_init_array_end == nullptr) {
    // Static initialization was not deferred on this build (macOS, sanitizer
    // builds); it already ran at dlopen() time, legitimately using the system
    // allocator. The invariant checked below does not apply.
    return 0;
  }

  // Where initialization *is* deferred, nothing in the module may allocate
  // before the Valkey allocator is established. If anything did, the pointer
  // came from the system allocator and nothing is left that can route its
  // free() back there.
  //
  // Reported with fprintf/abort rather than CHECK: absl's logging globals are
  // themselves among the initializers that have not run yet. Run addr2line on
  // the reported address to identify the caller.
  size_t preinit = GetPreInitAllocationCount();
  if (preinit != 0) {
    fprintf(stderr,
            "FATAL: %zu allocation(s) reached the vmsdk allocators before "
            "ValkeyModule_Alloc was established; first caller at %p. The "
            "module cannot route these to ValkeyModule_Free.\n",
            preinit, GetPreInitFirstCaller());
    abort();
  }

  // Guard against a second module load re-running initializers.
  if (initializers_run != 0) {
    return initializers_run;
  }

  const size_t count = __vmsdk_init_array_end - __vmsdk_init_array_start;
  for (size_t i = 0; i < count; ++i) {
    __vmsdk_init_array_start[i](0, nullptr, nullptr);
  }
  initializers_run = count;
  return count;
}

}  // namespace vmsdk
