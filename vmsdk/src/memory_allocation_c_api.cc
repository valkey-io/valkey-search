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
//
// There is deliberately no fallback for the window before ValkeyModule_Alloc
// is established. Nothing in the module allocates then -- static initializers
// are deferred until after it is set, see vmsdk/src/deferred_init.cc -- and if
// something ever did, ValkeyModule_Alloc is still a null function pointer, so
// the call faults immediately at the offending call site. Valkey's crash
// handler prints the backtrace, which localises the problem better than any
// bookkeeping we could carry on every allocation to detect it after the fact.

// Defines VMSDK_USE_VALKEY_ALLOC_OVERRIDES.
#include "vmsdk/src/memory_allocation_overrides.h"

#ifdef VMSDK_USE_VALKEY_ALLOC_OVERRIDES

// Deliberately inside the guard: <malloc.h> is glibc-only, and this file
// compiles to nothing on the platforms that do not define the guard.
#include <errno.h>
#include <limits.h>
#include <malloc.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cstddef>
#include <cstdlib>
#include <cstring>

#include "absl/base/optimization.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

// Each definition below must repeat the exception specification of the libc
// declaration it redefines. glibc declares these noexcept (__THROW); musl
// declares them with none, and clang rejects a noexcept definition of a
// function first declared without one.
#ifdef __GLIBC__
#define VMSDK_LIBC_NOEXCEPT noexcept
#else
#define VMSDK_LIBC_NOEXCEPT
#endif

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

}  // namespace

extern "C" {

void* malloc(size_t size) VMSDK_LIBC_NOEXCEPT {
  // Force 16-byte alignment; Valkey may otherwise return 8-byte aligned memory.
  void* ptr = ValkeyModule_Alloc(AlignSize(size));
  if (ABSL_PREDICT_TRUE(ptr != nullptr)) {
    vmsdk::ReportAllocMemorySize(ValkeyModule_MallocUsableSize(ptr));
  }
  return ptr;
}

void free(void* ptr) VMSDK_LIBC_NOEXCEPT {
  if (ptr == nullptr) {
    return;
  }
  vmsdk::ReportFreeMemorySize(ValkeyModule_MallocUsableSize(ptr));
  ValkeyModule_Free(ptr);
}

void* calloc(size_t nmemb, size_t size) VMSDK_LIBC_NOEXCEPT {
  void* ptr = ValkeyModule_Calloc(nmemb, AlignSize(size));
  if (ABSL_PREDICT_TRUE(ptr != nullptr)) {
    vmsdk::ReportAllocMemorySize(ValkeyModule_MallocUsableSize(ptr));
  }
  return ptr;
}

void* realloc(void* ptr, size_t size) VMSDK_LIBC_NOEXCEPT {
  if (ABSL_PREDICT_FALSE(ptr == nullptr)) {
    return malloc(size);
  }
  size_t old_size = ValkeyModule_MallocUsableSize(ptr);
  void* new_ptr = ValkeyModule_Realloc(ptr, AlignSize(size));
  if (ABSL_PREDICT_TRUE(new_ptr != nullptr)) {
    vmsdk::ReportFreeMemorySize(old_size);
    vmsdk::ReportAllocMemorySize(ValkeyModule_MallocUsableSize(new_ptr));
  }
  return new_ptr;
}

void* aligned_alloc(size_t alignment, size_t size) VMSDK_LIBC_NOEXCEPT {
  void* ptr = ValkeyModule_Alloc(AlignSize(size, alignment));
  if (ABSL_PREDICT_TRUE(ptr != nullptr)) {
    vmsdk::ReportAllocMemorySize(ValkeyModule_MallocUsableSize(ptr));
  }
  return ptr;
}

int posix_memalign(void** memptr, size_t alignment,
                   size_t size) VMSDK_LIBC_NOEXCEPT {
  *memptr = aligned_alloc(alignment, size);
  return *memptr == nullptr ? ENOMEM : 0;
}

void* valloc(size_t size) VMSDK_LIBC_NOEXCEPT {
  return aligned_alloc(sysconf(_SC_PAGESIZE), size);
}

size_t malloc_usable_size(void* ptr) VMSDK_LIBC_NOEXCEPT {
  if (ABSL_PREDICT_FALSE(ptr == nullptr)) {
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
char* strdup(const char* s) VMSDK_LIBC_NOEXCEPT {
  size_t size = strlen(s) + 1;
  char* copy = static_cast<char*>(malloc(size));
  if (ABSL_PREDICT_FALSE(copy == nullptr)) {
    return nullptr;
  }
  memcpy(copy, s, size);
  return copy;
}

// realpath(path, nullptr) and getcwd(nullptr, 0) return a buffer libc
// allocated with its own malloc, which free() above would hand to
// ValkeyModule_Free. getcwd is reimplemented so the result comes from our
// allocator instead; it goes straight to the kernel, since calling the libc
// function of the same name would bind to this definition and recurse.
//
// realpath(path, nullptr) allocates its result, like getcwd(nullptr, 0), so
// it is reimplemented for the same reason. The implementation lives in
// vmsdk::RealPath, where the unit tests can reach it; linked in here, its
// strdup is the one above, so both forms stay on the module's allocator.
char* realpath(const char* path, char* resolved_path) VMSDK_LIBC_NOEXCEPT {
  return vmsdk::RealPath(path, resolved_path);
}

char* getcwd(char* buf, size_t size) VMSDK_LIBC_NOEXCEPT {
  if (buf != nullptr) {
    // Nothing is allocated on this path.
    if (size == 0) {
      errno = EINVAL;
      return nullptr;
    }
    if (syscall(SYS_getcwd, buf, size) < 0) {
      return nullptr;
    }
    return buf;
  }
  // GNU extension: allocate the result. A non-zero size is a hard limit; a zero
  // size means "however much it takes", so grow until it fits.
  size_t capacity = (size != 0) ? size : PATH_MAX;
  for (;;) {
    char* cwd = static_cast<char*>(malloc(capacity));
    if (cwd == nullptr) {
      errno = ENOMEM;
      return nullptr;
    }
    if (syscall(SYS_getcwd, cwd, capacity) >= 0) {
      return cwd;
    }
    const int saved_errno = errno;
    free(cwd);
    if (saved_errno != ERANGE || size != 0) {
      errno = saved_errno;
      return nullptr;
    }
    if (capacity > (1u << 20)) {
      errno = ENAMETOOLONG;
      return nullptr;
    }
    capacity *= 2;
  }
}

}  // extern "C"

#endif  // VMSDK_USE_VALKEY_ALLOC_OVERRIDES
