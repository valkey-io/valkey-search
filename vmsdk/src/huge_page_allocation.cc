/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/huge_page_allocation.h"

#include <sys/mman.h>
#ifdef __linux__
#include <sys/prctl.h>
#endif

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <fstream>
#include <new>
#include <string>

#include "absl/log/check.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

namespace {
// Matches the default of the "vector-chunk-huge-pages" config.
std::atomic<bool> huge_page_allocation_enabled{true};
// Cleared by InitHugePageAllocation when THP can't back the memory.
std::atomic<bool> thp_usable{true};
// Lowest address of the most recent mapping. New mappings are placed directly
// below it when possible so the kernel merges them into one VMA; otherwise
// every chunk is its own VMA and vm.max_map_count limits the index size.
std::atomic<uintptr_t> last_mapping{0};

// Returns an empty string when THP is usable, otherwise the reason it isn't.
std::string ThpUnusableReason() {
#ifdef PR_GET_THP_DISABLE
  if (prctl(PR_GET_THP_DISABLE, 0, 0, 0, 0) > 0) {
    return "THP is disabled for this process (disable-thp)";
  }
#endif
  std::ifstream f("/sys/kernel/mm/transparent_hugepage/enabled");
  std::string mode;
  if (!std::getline(f, mode)) {
    return "THP is not supported";
  }
  if (mode.find("[never]") != std::string::npos) {
    return "THP is set to never";
  }
  return "";
}

// Maps `bytes` at `want` only if that range is free; nullptr otherwise.
char *MapAt(uintptr_t want, size_t bytes) {
#ifdef MAP_FIXED_NOREPLACE
  void *hint = reinterpret_cast<void *>(want);
  void *map = mmap(hint, bytes, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED_NOREPLACE, -1, 0);
  if (map == hint) {
    return static_cast<char *>(map);
  }
  // Kernels before 4.17 treat the flag as a hint and may map elsewhere.
  if (map != MAP_FAILED) {
    munmap(map, bytes);
  }
#endif
  return nullptr;
}

// Maps an aligned region anywhere, or returns nullptr.
char *MapAligned(size_t bytes) {
  // Over-map by one huge page, then trim, so the region is huge-page aligned:
  // THP only backs fully aligned 2MB ranges.
  size_t map_len = bytes + kHugePageSize;
  void *map = mmap(nullptr, map_len, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (map == MAP_FAILED) {
    return nullptr;
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
  return reinterpret_cast<char *>(aligned);
}
}  // namespace

void InitHugePageAllocation(ValkeyModuleCtx *ctx) {
  std::string reason = ThpUnusableReason();
  thp_usable.store(reason.empty(), std::memory_order_relaxed);
  if (!reason.empty()) {
    VMSDK_LOG(NOTICE, ctx) << "Vector index huge page allocation disabled: "
                           << reason;
  } else if (ValkeyModule_IncrExternalMemory == nullptr ||
             ValkeyModule_DecrExternalMemory == nullptr) {
    VMSDK_LOG(NOTICE, ctx) << "Vector index huge page allocation disabled: "
                              "server lacks the external memory APIs";
  } else {
    VMSDK_LOG(NOTICE, ctx) << "Vector index huge page allocation available";
  }
}

void SetHugePageAllocationEnabled(bool enabled) {
  huge_page_allocation_enabled.store(enabled, std::memory_order_relaxed);
}

bool UseHugePageAllocation() {
  return huge_page_allocation_enabled.load(std::memory_order_relaxed) &&
         thp_usable.load(std::memory_order_relaxed) &&
         ValkeyModule_IncrExternalMemory != nullptr &&
         ValkeyModule_DecrExternalMemory != nullptr;
}

char *AllocateHugePages(size_t bytes) {
  CHECK(bytes > 0 && bytes % kHugePageSize == 0);
  // Racing threads may both try the same spot; MAP_FIXED_NOREPLACE lets only
  // one win and the other falls back.
  uintptr_t below = last_mapping.load(std::memory_order_relaxed);
  char *ptr = below > bytes ? MapAt(below - bytes, bytes) : nullptr;
  if (ptr == nullptr) {
    ptr = MapAligned(bytes);
    if (ptr == nullptr) {
      throw std::bad_alloc();
    }
  }
  last_mapping.store(reinterpret_cast<uintptr_t>(ptr),
                     std::memory_order_relaxed);
  // Best effort: without THP the region is simply backed by base pages.
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
  if (munmap(ptr, bytes) != 0) {
    int err = errno;
    // Unmapping from the middle of a merged VMA splits it, which fails at
    // vm.max_map_count. Release the memory and leave the range mapped.
    static std::atomic<bool> logged{false};
    if (!logged.exchange(true, std::memory_order_relaxed)) {
      VMSDK_LOG(WARNING, nullptr)
          << "munmap of a vector index chunk failed (errno " << err
          << "); releasing its memory without unmapping. Consider raising "
             "vm.max_map_count.";
    }
    madvise(ptr, bytes, MADV_DONTNEED);
  }
  CHECK(ValkeyModule_DecrExternalMemory(bytes) == VALKEYMODULE_OK);
  ReportFreeMemorySize(bytes);
}

}  // namespace vmsdk
