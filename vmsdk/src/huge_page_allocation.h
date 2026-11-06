/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_HUGE_PAGE_ALLOCATION_H_
#define VMSDK_SRC_HUGE_PAGE_ALLOCATION_H_

#include <cstddef>

namespace vmsdk {

// Allocation of large, long-lived buffers directly from the kernel in units of
// transparent huge pages. The memory lives outside the Valkey allocator, so it
// is reported to the server with ValkeyModule_IncrExternalMemory /
// ValkeyModule_DecrExternalMemory (Valkey 9.2+), and to the module's own
// used-memory counter.

constexpr size_t kHugePageSize = 2 * 1024 * 1024;

// Set from the module config.
void SetHugePageAllocationEnabled(bool enabled);

// True when enabled by config and the server provides the external memory
// accounting APIs.
bool UseHugePageAllocation();

// `bytes` must be a non-zero multiple of kHugePageSize. The result is aligned
// to kHugePageSize and uninitialized. Throws std::bad_alloc on failure.
char *AllocateHugePages(size_t bytes);

// Releases memory from AllocateHugePages; `bytes` must match.
void FreeHugePages(char *ptr, size_t bytes);

}  // namespace vmsdk

#endif  // VMSDK_SRC_HUGE_PAGE_ALLOCATION_H_
