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

#ifndef __APPLE__
extern "C" {
// Bounds of the relocated .init_array, provided by vmsdk/deferred_init.lds.
// Weak: builds that do not apply the linker script (sanitizer builds) leave
// these undefined, and static initialization happens at dlopen() as usual.
extern void (*__vmsdk_init_array_start[])(int, char **, char **)
    __attribute__((weak));
extern void (*__vmsdk_init_array_end[])(int, char **, char **)
    __attribute__((weak));
}  // extern "C"
#endif  // !__APPLE__

namespace vmsdk {
namespace {
// Zero-initialized, so safe to read before any initializer has run.
size_t initializers_run = 0;
}  // namespace

size_t GetDeferredInitializerCount() { return initializers_run; }

size_t RunDeferredStaticInitializers() {
#ifdef __APPLE__
  // Static initialization is never deferred here: the relocation is done by a
  // GNU linker script and Mach-O has no equivalent, so the initializers already
  // ran at dlopen() time using the system allocator.
  //
  // The bounds symbols cannot even be declared on this platform. ELF resolves a
  // weak undefined symbol to a null address, which is what the check below
  // relies on; Mach-O has no such thing, and a plain weak declaration of a
  // missing symbol is a link error.
  return 0;
#else
  if (__vmsdk_init_array_start == nullptr ||
      __vmsdk_init_array_end == nullptr) {
    // Static initialization was not deferred on this build (sanitizer builds);
    // it already ran at dlopen() time, legitimately using the system allocator.
    return 0;
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
#endif  // __APPLE__
}

}  // namespace vmsdk
