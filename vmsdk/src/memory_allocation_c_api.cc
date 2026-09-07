/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

// The module's C allocator entry points.
//
// Everything the module allocates must come from ValkeyModule_Alloc, so that
// Valkey accounts for it and it lives in the server's jemalloc arena. Rather
// than redirecting call sites -- which only ever covered the translation units
// that included a particular header, plus C++ via replaced operator new/delete
// -- this file simply *defines* malloc and friends inside the module.
//
// That works because of two link options on the module (see src/CMakeLists.txt
// and vmsdk/versionscript.lds):
//
//   -static-libstdc++    puts libstdc++, including operator new/delete, inside
//                        the module, so C++ allocation reaches these functions.
//   version script       lists these symbols as local, so they are not exported
//   + --exclude-libs     and, being non-preemptible, every reference from
//                        within the module binds here at link time.
//
// The second point is what makes this work at all: a dlopened library's symbol
// lookups search the global scope first, and libc.so.6 defines malloc, so a
// module-defined malloc with default visibility is simply ignored -- even by
// the module's own operator new. Made local, it captures everything linked into
// the module: libstdc++, abseil, protobuf, gRPC, ICU, hdrhistogram and rax.
//
// Because these are local to the module, libc.so.6 never sees them and keeps
// using its own allocator. Memory allocated inside libc and freed inside libc
// therefore stays self-consistent; the only hazard is a pointer that crosses
// that boundary, which is what the strdup/realpath/getcwd definitions at the
// bottom of this file address.

// Defines VMSDK_USE_VALKEY_ALLOC_OVERRIDES.
#include "vmsdk/src/memory_allocation_overrides.h"

#ifdef VMSDK_USE_VALKEY_ALLOC_OVERRIDES

// Deliberately inside the guard: <malloc.h> is glibc-only, and this file
// compiles to nothing on the platforms that do not define the guard.
#include <errno.h>
#include <malloc.h>
#include <unistd.h>

#include <cstddef>
#include <cstdlib>
#include <cstring>

#include "absl/base/optimization.h"
#include "absl/log/check.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace {

// Valkey exposes no aligned allocation entry point. jemalloc returns memory
// aligned to the size class, so rounding the request up to a multiple of the
// alignment gets us the alignment we need.
//
// See https://linux.die.net/man/3/jemalloc: "... Chunks are always aligned to
// multiples of the chunk size..."
size_t AlignSize(size_t size, size_t alignment = 16) {
  return (size + alignment - 1) & ~(alignment - 1);
}

// Valkey cannot tell us the usable size of a pointer it did not allocate, so
// allocations that took the fallback path are accounted as zero bytes.
size_t SystemUsableSize(void* ptr) { return 0; }

}  // namespace

extern "C" {

void* malloc(size_t size) noexcept {
  if (ABSL_PREDICT_FALSE(!vmsdk::IsUsingValkeyAlloc())) {
    vmsdk::RecordSystemAllocation(__builtin_return_address(0));
    return vmsdk::PerformAndTrackMalloc(size, __libc_malloc, SystemUsableSize);
  }
  // Force 16-byte alignment; Valkey may otherwise return 8-byte aligned memory.
  return vmsdk::PerformAndTrackMalloc(AlignSize(size), ValkeyModule_Alloc,
                                      ValkeyModule_MallocUsableSize);
}

void free(void* ptr) noexcept {
  if (ptr == nullptr) {
    return;
  }
  if (ABSL_PREDICT_FALSE(!vmsdk::IsUsingValkeyAlloc())) {
    vmsdk::RecordSystemAllocation(__builtin_return_address(0));
    vmsdk::PerformAndTrackFree(ptr, __libc_free, SystemUsableSize);
    return;
  }
  vmsdk::PerformAndTrackFree(ptr, ValkeyModule_Free,
                             ValkeyModule_MallocUsableSize);
}

void* calloc(size_t nmemb, size_t size) noexcept {
  if (ABSL_PREDICT_FALSE(!vmsdk::IsUsingValkeyAlloc())) {
    vmsdk::RecordSystemAllocation(__builtin_return_address(0));
    return vmsdk::PerformAndTrackCalloc(nmemb, size, __libc_calloc,
                                        SystemUsableSize);
  }
  return vmsdk::PerformAndTrackCalloc(nmemb, AlignSize(size),
                                      ValkeyModule_Calloc,
                                      ValkeyModule_MallocUsableSize);
}

void* realloc(void* ptr, size_t size) noexcept {
  if (ABSL_PREDICT_FALSE(ptr == nullptr)) {
    return malloc(size);
  }
  if (ABSL_PREDICT_FALSE(!vmsdk::IsUsingValkeyAlloc())) {
    vmsdk::RecordSystemAllocation(__builtin_return_address(0));
    return vmsdk::PerformAndTrackRealloc(ptr, size, __libc_realloc,
                                         SystemUsableSize);
  }
  return vmsdk::PerformAndTrackRealloc(ptr, AlignSize(size),
                                       ValkeyModule_Realloc,
                                       ValkeyModule_MallocUsableSize);
}

void* aligned_alloc(size_t alignment, size_t size) noexcept {
  if (ABSL_PREDICT_FALSE(!vmsdk::IsUsingValkeyAlloc())) {
    vmsdk::RecordSystemAllocation(__builtin_return_address(0));
    return vmsdk::PerformAndTrackMalloc(AlignSize(size, alignment),
                                        __libc_malloc, SystemUsableSize);
  }
  return vmsdk::PerformAndTrackMalloc(AlignSize(size, alignment),
                                      ValkeyModule_Alloc,
                                      ValkeyModule_MallocUsableSize);
}

int posix_memalign(void** memptr, size_t alignment, size_t size) noexcept {
  *memptr = aligned_alloc(alignment, size);
  return *memptr == nullptr ? ENOMEM : 0;
}

void* valloc(size_t size) noexcept {
  return aligned_alloc(sysconf(_SC_PAGESIZE), size);
}

size_t malloc_usable_size(void* ptr) noexcept {
  if (ABSL_PREDICT_FALSE(ptr == nullptr || !vmsdk::IsUsingValkeyAlloc())) {
    return 0;
  }
  return ValkeyModule_MallocUsableSize(ptr);
}

//
// libc functions that allocate and hand the result to the caller.
//
// These are the only way a pointer can cross between libc's allocator and ours:
// glibc would allocate the result with its own malloc, and the caller -- inside
// this module -- would release it through the free() above, handing a libc
// pointer to ValkeyModule_Free. Defining them here keeps both halves on the
// same allocator.
//
// ci/check_module_allocators.sh fails the build if the module ever references
// an allocate-and-return libc function that is not handled here.
//

// Reached from absl::InitializeSymbolizer and libstdc++'s message catalogs.
char* strdup(const char* s) noexcept {
  size_t size = strlen(s) + 1;
  char* copy = static_cast<char*>(malloc(size));
  if (ABSL_PREDICT_FALSE(copy == nullptr)) {
    return nullptr;
  }
  memcpy(copy, s, size);
  return copy;
}

// realpath(path, nullptr) and getcwd(nullptr, 0) allocate their result with
// glibc's malloc, and there is no way for free() above to recognise such a
// pointer. Neither is reachable today: the only references come from
// std::filesystem::canonical and std::filesystem::current_path, which the
// module never calls, and they are pulled in merely because libstdc++'s
// filesystem objects are linked. Rather than reimplement them, fail loudly if
// that ever changes.
//
// Note that ICU's uprv_tzname legitimately calls the fortified __realpath_chk
// with a caller-provided buffer, which does not allocate. That is a different
// symbol and is deliberately left alone.
char* realpath(const char* path, char* resolved_path) noexcept {
  CHECK(false) << "realpath() is not available inside the module: glibc "
                  "allocates the result with its own malloc, which cannot be "
                  "released through ValkeyModule_Free. Resolve the path with a "
                  "caller-provided buffer instead.";
  return nullptr;
}

char* getcwd(char* buf, size_t size) noexcept {
  CHECK(false) << "getcwd() is not available inside the module: glibc "
                  "allocates the result with its own malloc when buf is null, "
                  "which cannot be released through ValkeyModule_Free.";
  return nullptr;
}

}  // extern "C"

#endif  // VMSDK_USE_VALKEY_ALLOC_OVERRIDES
