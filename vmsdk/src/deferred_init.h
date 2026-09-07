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
// established ValkeyModule_Alloc/Free, and before anything touches a
// dynamically-initialized global. Until it returns, every such global is still
// zero-initialized.
//
// The initializers allocate, so the ordering against ValkeyModule_Init is what
// keeps the module off the system allocator entirely. Get it wrong and
// ValkeyModule_Alloc is still null, so the first allocation faults at its call
// site rather than quietly succeeding.
//
// Returns the number of initializers run. Returns 0 without doing anything on
// builds that do not apply the linker script (see GetDeferredInitializerCount).
size_t RunDeferredStaticInitializers();

// Number of initializers RunDeferredStaticInitializers() ran, for logging once
// logging is available. Zero on builds that do not defer static initialization.
size_t GetDeferredInitializerCount();

}  // namespace vmsdk

#endif  // VMSDK_SRC_DEFERRED_INIT_H_
