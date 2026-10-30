/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/huge_page_allocation.h"

#include <sys/mman.h>

#include <atomic>
#include <cstdint>
#include <new>

#include "absl/log/check.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

namespace {
// Matches the default of the "vector-chunk-huge-pages" config.
std::atomic<bool> huge_page_allocation_enabled{true};
}  // namespace

void SetHugePageAllocationEnabled(bool enabled) {
  huge_page_allocation_enabled.store(enabled, std::memory_order_relaxed);
}

bool UseHugePageAllocation() {
  return huge_page_allocation_enabled.load(std::memory_order_relaxed) &&
         ValkeyModule_IncrExternalMemory != nullptr &&
         ValkeyModule_DecrExternalMemory != nullptr;
}

char *AllocateHugePages(size_t bytes) {
  CHECK(bytes > 0 && bytes % kHugePageSize == 0);
  // Over-map by one huge page, then trim, so the region is huge-page aligned:
  // THP only backs fully aligned 2MB ranges.
  size_t map_len = bytes + kHugePageSize;
  void *map = mmap(nullptr, map_len, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (map == MAP_FAILED) {
    throw std::bad_alloc();
  }
  uintptr_t base = reinterpret_cast<uintptr_t>(map);
  uintptr_t aligned = (base + kHugePageSize - 1) & ~(kHugePageSize - 1);
  size_t head = aligned - base;
  size_t tail = map_len - head - bytes;
  if (head > 0) {
    munmap(map, head);
  }
  if (tail > 0) {
    munmap(reinterpret_cast<void *>(aligned + bytes), tail);
  }
  char *ptr = reinterpret_cast<char *>(aligned);
  // Best effort: without THP (or when the server disabled it for the process)
  // the region is simply backed by base pages.
#ifdef MADV_HUGEPAGE
  madvise(ptr, bytes, MADV_HUGEPAGE);
#endif
  if (ValkeyModule_IncrExternalMemory(bytes) != VALKEYMODULE_OK) {
    munmap(ptr, bytes);
    throw std::bad_alloc();
  }
  ReportAllocMemorySize(bytes);
  return ptr;
}

void FreeHugePages(char *ptr, size_t bytes) {
  munmap(ptr, bytes);
  CHECK(ValkeyModule_DecrExternalMemory(bytes) == VALKEYMODULE_OK);
  ReportFreeMemorySize(bytes);
}

}  // namespace vmsdk
