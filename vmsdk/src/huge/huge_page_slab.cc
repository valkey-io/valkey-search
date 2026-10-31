/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/huge/huge_page_slab.h"

#include <sys/mman.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>

#include "absl/strings/str_cat.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk::huge_page_slab {

namespace {

constexpr size_t kHugePage = kHugePageBytes;

#ifdef __linux__
// Advises huge pages. Pages are placed by the kernel's default first-touch
// policy. Best effort: a failure is logged once and ignored.
void ApplyPlacementPolicy(char *base, size_t bytes) {
  static std::atomic<bool> logged_madvise{false};
  if (madvise(base, bytes, MADV_HUGEPAGE) != 0 &&
      !logged_madvise.exchange(true)) {
    VMSDK_LOG(WARNING, nullptr)
        << "madvise(MADV_HUGEPAGE) failed: " << std::strerror(errno);
  }
}
#endif

}  // namespace

absl::StatusOr<char *> ReserveRegion(size_t bytes) {
  // Over-reserve by one huge page so the base can be aligned to one.
  size_t map_bytes = bytes + kHugePage;
  void *map = mmap(nullptr, map_bytes, PROT_NONE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (map == MAP_FAILED) {
    return absl::ResourceExhaustedError(absl::StrCat(
        "mmap of ", map_bytes, " bytes failed: ", std::strerror(errno)));
  }
  auto start = reinterpret_cast<uintptr_t>(map);
  auto aligned = (start + kHugePage - 1) & ~(kHugePage - 1);
  size_t head = aligned - start;
  size_t tail = map_bytes - head - bytes;
  if (head > 0) {
    munmap(map, head);
  }
  if (tail > 0) {
    munmap(reinterpret_cast<char *>(aligned) + bytes, tail);
  }
  auto base = reinterpret_cast<char *>(aligned);
#ifdef __linux__
  ApplyPlacementPolicy(base, bytes);
#endif
  return base;
}

void UnreserveRegion(char *base, size_t bytes) { munmap(base, bytes); }

absl::Status Materialize(char *p, size_t len) {
  if (mprotect(p, len, PROT_READ | PROT_WRITE) != 0) {
    return absl::ResourceExhaustedError(absl::StrCat(
        "mprotect of ", len, " bytes failed: ", std::strerror(errno)));
  }
  if (ValkeyModule_IncrExternalMemory) {
    ValkeyModule_IncrExternalMemory(len);
  }
  ReportAllocMemorySize(len);
  return absl::OkStatus();
}

void Release(char *p, size_t len) {
#ifdef __linux__
  // Drop the pages but keep the mapping, so the placement policy still holds
  // if the range is materialized again.
  madvise(p, len, MADV_DONTNEED);
  mprotect(p, len, PROT_NONE);
#else
  mmap(p, len, PROT_NONE,
       MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE | MAP_FIXED, -1, 0);
#endif
  if (ValkeyModule_DecrExternalMemory) {
    ValkeyModule_DecrExternalMemory(len);
  }
  ReportFreeMemorySize(len);
}

}  // namespace vmsdk::huge_page_slab
