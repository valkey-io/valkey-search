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
    // allocator.
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
}

}  // namespace vmsdk
