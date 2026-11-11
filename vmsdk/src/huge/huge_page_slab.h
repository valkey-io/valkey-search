/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_HUGE_HUGE_PAGE_SLAB_H_
#define VMSDK_SRC_HUGE_HUGE_PAGE_SLAB_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

// Reserved, huge-page-aligned address space. On Linux a reserved region is
// advised to use transparent huge pages; its pages are placed on first touch.
// The first reservation logs a warning if huge pages are disabled for this
// process.
// Materialized bytes are reported via ValkeyModule_IncrExternalMemory /
// DecrExternalMemory and the module's own memory counter.
namespace vmsdk::huge_page_slab {

constexpr size_t kHugePageBytes = 2 * 1024 * 1024;

// The system page size (sysconf(_SC_PAGESIZE)): the unit region lengths are
// rounded to. Full, aligned kHugePageBytes extents of a region are backed by
// huge pages; a shorter tail uses ordinary pages.
size_t PageBytes();

inline size_t RoundUpToPage(size_t bytes) {
  size_t page = PageBytes();
  return (bytes + page - 1) & ~(page - 1);
}

// Reserves `bytes` (a multiple of PageBytes()) of huge-page-aligned address
// space with no memory behind it. Returns ResourceExhausted if the address
// space cannot be reserved.
absl::StatusOr<char *> ReserveRegion(size_t bytes);

// Releases address space returned by ReserveRegion. Nothing in it may be
// materialized.
void UnreserveRegion(char *base, size_t bytes);

// Makes [p, p + len) readable and writable. `p` and `len` must be multiples of
// PageBytes(). Returns ResourceExhausted on failure, in which case nothing
// is materialized and no memory is reported.
absl::Status Materialize(char *p, size_t len);

// Unmaps a region returned by ReserveRegion whose whole length is
// materialized, and stops reporting its memory.
void ReleaseRegion(char *base, size_t bytes);

// Bytes currently materialized by all regions (the "huge_page_region_bytes"
// dev info field).
int64_t MaterializedBytes();

// The AnonHugePages value, in bytes, of /proc/<pid>/smaps_rollup text, or 0
// if absent. Exposed for testing; it backs the "anon_huge_pages_bytes" dev
// info field.
int64_t ParseAnonHugePages(absl::string_view smaps_rollup);

// The bracketed (active) word of a sysfs THP setting such as
// "always [madvise] never", or "" if there is none. Exposed for testing.
std::string ActiveThpMode(absl::string_view setting);

}  // namespace vmsdk::huge_page_slab

#endif  // VMSDK_SRC_HUGE_HUGE_PAGE_SLAB_H_
