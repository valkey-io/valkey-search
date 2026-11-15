/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_HUGE_PAGE_ALLOCATION_H_
#define VMSDK_SRC_HUGE_PAGE_ALLOCATION_H_

#include <cstddef>

#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

// Allocation of large, long-lived buffers directly from the kernel in units of
// transparent huge pages. The memory lives outside the Valkey allocator, so it
// is reported to the server with ValkeyModule_IncrExternalMemory /
// ValkeyModule_DecrExternalMemory (Valkey 9.2+), and to the module's own
// used-memory counter.

constexpr size_t kHugePageSize = 2 * 1024 * 1024;

constexpr size_t RoundUpToHugePage(size_t bytes) {
  return (bytes + kHugePageSize - 1) / kHugePageSize * kHugePageSize;
}

// Called at module load. Disables huge page allocation when transparent huge
// pages can't back it: THP is "never" on the host or disabled for this process
// (Valkey's disable-thp). Logs the decision.
void InitHugePageAllocation(ValkeyModuleCtx *ctx);

// Set from the module config.
void SetHugePageAllocationEnabled(bool enabled);

// True when enabled by config, THP is usable, and the server provides the
// external memory accounting APIs.
bool UseHugePageAllocation();

// `bytes` must be a non-zero multiple of kHugePageSize. The result is aligned
// to kHugePageSize and uninitialized. Throws std::bad_alloc on failure.
char *AllocateHugePages(size_t bytes);

// Releases memory from AllocateHugePages; `bytes` must match.
void FreeHugePages(char *ptr, size_t bytes);

}  // namespace vmsdk

#endif  // VMSDK_SRC_HUGE_PAGE_ALLOCATION_H_
