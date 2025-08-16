/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_MEMORY_ALLOCATION_H_
#define VMSDK_SRC_MEMORY_ALLOCATION_H_

#include <cstdint>

namespace vmsdk {

// Updates the custom allocator to perform any future allocations using the
// Valkey allocator.
void UseValkeyAlloc();
bool IsUsingValkeyAlloc();

// Switch back to the default allocator. No guarantees around atomicity. Only
// safe in single-threaded or testing environments.
void ResetValkeyAlloc();

// Report used memory counter.
uint64_t GetUsedMemoryCnt();

void ReportAllocMemorySize(uint64_t size);
void ReportFreeMemorySize(uint64_t size);

int64_t GetMemoryDelta();

void SetMemoryDelta(int64_t delta);

}  // namespace vmsdk

#endif  // VMSDK_SRC_MEMORY_ALLOCATION_H_
