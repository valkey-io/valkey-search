/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_MEMORY_ALLOCATION_H_
#define VMSDK_SRC_MEMORY_ALLOCATION_H_

#include <cstddef>
#include <cstdint>

namespace vmsdk {

// Updates the custom allocator to perform any future allocations using the
// Valkey allocator.
void UseValkeyAlloc();

// Switch back to the default allocator. No guarantees around atomicity. Only
// safe in single-threaded or testing environments.
void ResetValkeyAlloc();

// Number of allocations that took the system-allocator fallback path, and the
// return address of the first one. In the module these must both be zero when
// static initialization starts; RunDeferredStaticInitializers() enforces that.
// In unit test executables the fallback path is the normal path and these are
// never checked.
size_t GetPreInitAllocationCount();
void* GetPreInitFirstCaller();

void ResetValkeyAllocStats();
// Report used memory counter.
uint64_t GetUsedMemoryCnt();

void ReportAllocMemorySize(uint64_t size);
void ReportFreeMemorySize(uint64_t size);

int64_t GetMemoryDelta();

void SetMemoryDelta(int64_t delta);

}  // namespace vmsdk

#endif  // VMSDK_SRC_MEMORY_ALLOCATION_H_
