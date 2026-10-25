/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_NUMA_SLAB_H_
#define VMSDK_SRC_NUMA_SLAB_H_

#include <cstddef>

// Reserved address space that is materialized in huge-page units. On Linux a
// reserved region is advised to use transparent huge pages.
// Materialized bytes are reported via ValkeyModule_IncrExternalMemory /
// DecrExternalMemory and the module's own memory counter.
namespace vmsdk::numa_slab {

constexpr size_t kHugePageBytes = 2 * 1024 * 1024;

inline size_t RoundUpToHugePage(size_t bytes) {
  return (bytes + kHugePageBytes - 1) & ~(kHugePageBytes - 1);
}

// Reserves `bytes` (a multiple of kHugePageBytes) of huge-page-aligned address
// space with no memory behind it. Throws std::bad_alloc.
char *ReserveRegion(size_t bytes);

// Releases address space returned by ReserveRegion. Nothing in it may be
// materialized.
void UnreserveRegion(char *base, size_t bytes);

// Makes [p, p + len) readable and writable. `p` and `len` must be multiples of
// kHugePageBytes. Throws std::bad_alloc.
void Materialize(char *p, size_t len);

// Drops the pages of a materialized [p, p + len) and makes it inaccessible.
// The range keeps its placement policy for later re-materialization.
void Release(char *p, size_t len);

}  // namespace vmsdk::numa_slab

#endif  // VMSDK_SRC_NUMA_SLAB_H_
