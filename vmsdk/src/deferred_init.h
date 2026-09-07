/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_DEFERRED_INIT_H_
#define VMSDK_SRC_DEFERRED_INIT_H_

#include <cstddef>

namespace vmsdk {

// Runs the module's C++ static initializers, which vmsdk/deferred_init.lds has
// relocated out of .init_array so that they do NOT run at dlopen() time.
//
// Must be called from ValkeyModule_OnLoad after ValkeyModule_Init has
// established ValkeyModule_Alloc/Free and after UseValkeyAlloc(), and before
// anything touches a dynamically-initialized global. Until it returns, every
// such global is still zero-initialized.
//
// Aborts if any allocation reached the __wrap_* allocators before this point:
// that means something escaped to the system allocator during a window in which
// nothing is supposed to allocate, and the pointer could later be handed to
// ValkeyModule_Free. See kPreInit* in memory_allocation_overrides.cc.
//
// Returns the number of initializers run. Returns 0 without doing anything on
// builds that do not apply the linker script (see GetDeferredInitializerCount).
size_t RunDeferredStaticInitializers();

// Number of initializers RunDeferredStaticInitializers() ran, for logging once
// logging is available. Zero on builds that do not defer static initialization.
size_t GetDeferredInitializerCount();

}  // namespace vmsdk

#endif  // VMSDK_SRC_DEFERRED_INIT_H_
