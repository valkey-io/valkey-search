/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/huge/huge_page_slab.h"

#include <sys/mman.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iterator>
#include <mutex>
#include <string>
#ifdef __linux__
#include <sys/prctl.h>
#endif

#include "absl/strings/ascii.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/strip.h"
#include "vmsdk/src/info.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk::huge_page_slab {

namespace {

constexpr size_t kHugePage = kHugePageBytes;

std::atomic<int64_t> materialized_bytes{0};

// The process's transparent-huge-page usage. The kernel computes it by walking
// every mapping, holding the mmap lock for reading, so this is a dev field
// meant for occasional diagnostics. Covers the whole process, not only these
// regions.
int64_t ReadAnonHugePages() {
#ifdef __linux__
  std::ifstream in("/proc/self/smaps_rollup");
  std::string text((std::istreambuf_iterator<char>(in)),
                   std::istreambuf_iterator<char>());
  return ParseAnonHugePages(text);
#else
  return 0;
#endif
}

vmsdk::info_field::Integer huge_page_region_bytes(
    "huge", "huge_page_region_bytes",
    vmsdk::info_field::IntegerBuilder().Dev().Computed(MaterializedBytes));
vmsdk::info_field::Integer anon_huge_pages_bytes(
    "huge", "anon_huge_pages_bytes",
    vmsdk::info_field::IntegerBuilder().Dev().Computed(ReadAnonHugePages));

#ifdef __linux__
// Kernels from 6.18 report this bit when madvised regions are exempt from a
// process-wide THP disable.
constexpr int kThpDisableExceptAdvised = 1 << 1;

std::string ReadFirstLine(const char *path) {
  std::ifstream in(path);
  std::string line;
  std::getline(in, line);
  return line;
}

// Logs, once, a warning if this process's huge-page regions cannot get
// transparent huge pages.
void WarnIfHugePagesDisabled() {
  std::string reason;
  int disabled = prctl(PR_GET_THP_DISABLE, 0, 0, 0, 0);
  if (disabled > 0 && (disabled & kThpDisableExceptAdvised) == 0) {
    reason =
        "THP is disabled for this process, as Valkey's disable-thp does when "
        "transparent_hugepage/enabled=always";
  } else if (ActiveThpMode(ReadFirstLine(
                 "/sys/kernel/mm/transparent_hugepage/enabled")) == "never") {
    reason = "transparent_hugepage/enabled=never";
  }
  if (!reason.empty()) {
    VMSDK_LOG(WARNING, nullptr)
        << "Huge pages are disabled for search's large hash tables (" << reason
        << "); performance may improve with "
           "/sys/kernel/mm/transparent_hugepage/enabled=madvise";
  }
}

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

int64_t MaterializedBytes() {
  return materialized_bytes.load(std::memory_order_relaxed);
}

int64_t ParseAnonHugePages(absl::string_view smaps_rollup) {
  constexpr absl::string_view kField = "AnonHugePages:";
  size_t pos = 0;
  while (pos < smaps_rollup.size()) {
    size_t end = smaps_rollup.find('\n', pos);
    if (end == absl::string_view::npos) {
      end = smaps_rollup.size();
    }
    absl::string_view line = smaps_rollup.substr(pos, end - pos);
    if (absl::ConsumePrefix(&line, kField)) {
      // "AnonHugePages:     342016 kB"
      line = absl::StripAsciiWhitespace(line);
      absl::ConsumeSuffix(&line, "kB");
      int64_t kb = 0;
      if (absl::SimpleAtoi(absl::StripAsciiWhitespace(line), &kb)) {
        return kb * 1024;
      }
      return 0;
    }
    pos = end + 1;
  }
  return 0;
}

size_t PageBytes() {
  static const size_t page = static_cast<size_t>(sysconf(_SC_PAGESIZE));
  return page;
}

std::string ActiveThpMode(absl::string_view setting) {
  size_t open = setting.find('[');
  size_t close = setting.find(']', open);
  if (open == absl::string_view::npos || close == absl::string_view::npos) {
    return "";
  }
  return std::string(setting.substr(open + 1, close - open - 1));
}

absl::StatusOr<char *> ReserveRegion(size_t bytes) {
#ifdef __linux__
  static std::once_flag warned;
  std::call_once(warned, WarnIfHugePagesDisabled);
#endif
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
  materialized_bytes.fetch_add(len, std::memory_order_relaxed);
  return absl::OkStatus();
}

void ReleaseRegion(char *base, size_t bytes) {
  // munmap drops the pages and the mapping in one call.
  munmap(base, bytes);
  if (ValkeyModule_DecrExternalMemory) {
    ValkeyModule_DecrExternalMemory(bytes);
  }
  ReportFreeMemorySize(bytes);
  materialized_bytes.fetch_sub(bytes, std::memory_order_relaxed);
}

}  // namespace vmsdk::huge_page_slab
