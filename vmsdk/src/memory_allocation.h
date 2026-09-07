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

// True once the module has switched to the Valkey allocator. In the module this
// happens at the top of ValkeyModule_OnLoad, before any static initializer
// runs. Unit test executables never switch.
bool IsUsingValkeyAlloc();

// Records that an allocation took the system-allocator fallback path, and where
// from. See GetPreInitAllocationCount above.
void RecordSystemAllocation(void* caller);

// Perform an allocator operation and update the memory accounting counters.
// Parameterised over the allocator so that the same accounting applies to both
// the Valkey and the fallback paths.
void* PerformAndTrackMalloc(size_t size, void* (*malloc_fn)(size_t),
                            size_t (*malloc_size_fn)(void*));
void* PerformAndTrackCalloc(size_t n, size_t size,
                            void* (*calloc_fn)(size_t, size_t),
                            size_t (*malloc_size_fn)(void*));
void PerformAndTrackFree(void* ptr, void (*free_fn)(void*),
                         size_t (*malloc_size_fn)(void*));
void* PerformAndTrackRealloc(void* ptr, size_t size,
                             void* (*realloc_fn)(void*, size_t),
                             size_t (*malloc_size_fn)(void*));

void ResetValkeyAllocStats();
// Report used memory counter.
uint64_t GetUsedMemoryCnt();

void ReportAllocMemorySize(uint64_t size);
void ReportFreeMemorySize(uint64_t size);

int64_t GetMemoryDelta();

void SetMemoryDelta(int64_t delta);

}  // namespace vmsdk

#endif  // VMSDK_SRC_MEMORY_ALLOCATION_H_
