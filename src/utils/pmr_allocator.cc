/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/pmr_allocator.h"

#include <algorithm>
#include <cstdlib>

#include "src/indexes/text/rax/rax_malloc.h"

extern "C" {
// NOLINTNEXTLINE(readability-identifier-naming)
void *__wrap_malloc(size_t size);
// NOLINTNEXTLINE(readability-identifier-naming)
void __wrap_free(void *ptr);
// NOLINTNEXTLINE(readability-identifier-naming)
void *__wrap_realloc(void *ptr, size_t size);
// NOLINTNEXTLINE(readability-identifier-naming)
int __wrap_malloc_usable_size(void *ptr);
}

namespace valkey_search::utils {

thread_local std::pmr::memory_resource *t_rax_res = nullptr;

namespace {
constexpr uint64_t kPmrMagic = 0x504D525F52415821ULL;

struct alignas(16) PmrHeader {
  uint64_t magic;
  std::pmr::memory_resource *res;
  size_t size;
};
}  // namespace

RaxPmrGuard::RaxPmrGuard(std::pmr::memory_resource *res)
    : prev_res_(t_rax_res) {
  t_rax_res = res;
}

RaxPmrGuard::~RaxPmrGuard() { t_rax_res = prev_res_; }

extern "C" {
bool IsRaxActive();
void *RaxMalloc(size_t size);
void RaxFree(void *ptr);
void *RaxRealloc(void *ptr, size_t size);
int RaxUsableSize(void *ptr);

void *RaxPmrMalloc(size_t size) {
  if (IsRaxActive()) {
    return RaxMalloc(size);
  }
#if !defined(USE_CUSTOM_RAX_ALLOCATOR) || !USE_CUSTOM_RAX_ALLOCATOR
  return __wrap_malloc(size);
#else
  size_t total_size = size + sizeof(PmrHeader);
  void *mem = t_rax_res ? t_rax_res->allocate(total_size, alignof(PmrHeader))
                        : __wrap_malloc(total_size);
  if (!mem) {
    return nullptr;
  }
  PmrHeader *hdr = reinterpret_cast<PmrHeader *>(mem);
  hdr->magic = kPmrMagic;
  hdr->res = t_rax_res;
  hdr->size = size;
  return reinterpret_cast<char *>(mem) + sizeof(PmrHeader);
#endif
}

void RaxPmrFree(void *ptr) {
  if (!ptr) {
    return;
  }
  if (IsRaxActive()) {
    RaxFree(ptr);
    return;
  }
#if defined(USE_CUSTOM_RAX_ALLOCATOR) && USE_CUSTOM_RAX_ALLOCATOR
  PmrHeader *hdr = reinterpret_cast<PmrHeader *>(reinterpret_cast<char *>(ptr) -
                                                 sizeof(PmrHeader));
  if (hdr->magic == kPmrMagic) {
    if (hdr->res) {
      hdr->res->deallocate(hdr, hdr->size + sizeof(PmrHeader),
                           alignof(PmrHeader));
    } else {
      __wrap_free(hdr);
    }
    return;
  }
#endif
  __wrap_free(ptr);
}

void *RaxPmrRealloc(void *ptr, size_t size) {
  if (!ptr) {
    return RaxPmrMalloc(size);
  }
  if (IsRaxActive()) {
    return RaxRealloc(ptr, size);
  }
#if defined(USE_CUSTOM_RAX_ALLOCATOR) && USE_CUSTOM_RAX_ALLOCATOR
  PmrHeader *hdr = reinterpret_cast<PmrHeader *>(reinterpret_cast<char *>(ptr) -
                                                 sizeof(PmrHeader));
  if (hdr->magic == kPmrMagic) {
    if (size == 0) {
      RaxPmrFree(ptr);
      return nullptr;
    }
    void *new_ptr = RaxPmrMalloc(size);
    if (!new_ptr) {
      return nullptr;
    }
    size_t copy_size = std::min(hdr->size, size);
    std::memcpy(new_ptr, ptr, copy_size);
    RaxPmrFree(ptr);
    return new_ptr;
  }
#endif
  return __wrap_realloc(ptr, size);
}

int RaxPmrUsableSize(void *ptr) {
  if (!ptr) {
    return 0;
  }
  if (IsRaxActive()) {
    return RaxUsableSize(ptr);
  }
#if defined(USE_CUSTOM_RAX_ALLOCATOR) && USE_CUSTOM_RAX_ALLOCATOR
  PmrHeader *hdr = reinterpret_cast<PmrHeader *>(reinterpret_cast<char *>(ptr) -
                                                 sizeof(PmrHeader));
  if (hdr->magic == kPmrMagic) {
    return static_cast<int>(hdr->size);
  }
#endif
  return __wrap_malloc_usable_size(ptr);
}
}  // extern "C"

}  // namespace valkey_search::utils
