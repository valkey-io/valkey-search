/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/pmr_allocator.h"

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
void *RaxMalloc(size_t size);
void RaxFree(void *ptr);
void *RaxRealloc(void *ptr, size_t size);
int RaxUsableSize(void *ptr);

void *RaxPmrMalloc(size_t size) { return RaxMalloc(size); }

void RaxPmrFree(void *ptr) { RaxFree(ptr); }

void *RaxPmrRealloc(void *ptr, size_t size) { return RaxRealloc(ptr, size); }

int RaxPmrUsableSize(void *ptr) { return RaxUsableSize(ptr); }
}  // extern "C"

}  // namespace valkey_search::utils
