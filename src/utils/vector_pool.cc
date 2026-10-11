/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/vector_pool.h"

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstring>
#include <new>

#include "absl/log/check.h"
#include "vmsdk/src/chunked_array.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/numa_slab.h"

namespace valkey_search {

namespace {

constexpr size_t kHugePage = vmsdk::numa_slab::kHugePageBytes;

std::atomic<VectorPoolRegistry *> global_registry{nullptr};
std::atomic<int64_t> global_active_allocations{0};
std::atomic<int64_t> global_materialized_bytes{0};

size_t RoundUpToMultipleOf8(size_t n) { return (n + 7) & ~size_t(7); }

}  // namespace

VectorPool::VectorPool(VectorPoolRegistry *registry, uint32_t type_tag,
                       size_t entry_size)
    : registry_(registry),
      type_tag_(type_tag),
      entry_size_(entry_size),
      entries_per_page_(kHugePage / entry_size) {}

size_t VectorPool::ActiveAllocations() const {
  absl::MutexLock lock(&mutex_);
  return active_allocations_;
}

int VectorPool::FreeGroup(const Page &page) const {
  size_t free_cnt = entries_per_page_ - page.used;
  if (free_cnt == 0) {
    return -1;
  }
  auto log2 = static_cast<size_t>(std::ceil(std::log2(free_cnt)));
  return static_cast<int>(std::min(log2, kFreeGroups - 1));
}

void VectorPool::Regroup(Page *page, int old_group) {
  int new_group = FreeGroup(*page);
  if (new_group == old_group) {
    return;
  }
  if (old_group >= 0) {
    groups_[old_group].Remove(page);
  }
  if (new_group >= 0) {
    groups_[new_group].PushBack(page);
  }
}

void VectorPool::AddSlab(Page *pages, char *base) {
  size_t pages_per_slab = registry_->slab_bytes_ / kHugePage;
  for (size_t i = 0; i < pages_per_slab; ++i) {
    pages[i].base = base + i * kHugePage;
    unmaterialized_.PushBack(&pages[i]);
  }
}

char *VectorPool::Allocate() {
  absl::MutexLock lock(&mutex_);
  Page *page = nullptr;
  for (auto &group : groups_) {
    if (!group.Empty()) {
      page = group.Front();
      break;
    }
  }
  int old_group = -1;
  if (page) {
    old_group = FreeGroup(*page);
  } else {
    if (unmaterialized_.Empty()) {
      auto slab = registry_->ClaimSlab(this);
      if (slab < 0) {
        return nullptr;
      }
      AddSlab(registry_->slabs_[slab].pages.get(),
              registry_->base_ + slab * registry_->slab_bytes_);
    }
    page = unmaterialized_.Front();
    vmsdk::numa_slab::Materialize(page->base, kHugePage);
    unmaterialized_.Remove(page);
    page->materialized = true;
    global_materialized_bytes.fetch_add(kHugePage, std::memory_order_relaxed);
  }

  uint32_t entry;
  if (page->free_head != kNoEntry) {
    entry = page->free_head;
    std::memcpy(&page->free_head, page->base + entry * entry_size_,
                sizeof(uint32_t));
  } else {
    entry = page->initialized++;
  }
  ++page->used;
  Regroup(page, old_group);
  ++active_allocations_;
  global_active_allocations.fetch_add(1, std::memory_order_relaxed);
  return page->base + entry * entry_size_;
}

void VectorPool::Free(char *ptr) {
  Page *page = registry_->PageOf(ptr);
  absl::MutexLock lock(&mutex_);
  size_t offset = ptr - page->base;
  DCHECK_EQ(offset % entry_size_, 0u);
  auto entry = static_cast<uint32_t>(offset / entry_size_);
  int old_group = FreeGroup(*page);
  std::memcpy(ptr, &page->free_head, sizeof(uint32_t));
  page->free_head = entry;
  --page->used;
  --active_allocations_;
  global_active_allocations.fetch_sub(1, std::memory_order_relaxed);
  if (page->used > 0) {
    Regroup(page, old_group);
    return;
  }
  if (old_group >= 0) {
    groups_[old_group].Remove(page);
  }
  vmsdk::numa_slab::Release(page->base, kHugePage);
  page->materialized = false;
  page->initialized = 0;
  page->free_head = kNoEntry;
  unmaterialized_.PushBack(page);
  global_materialized_bytes.fetch_sub(kHugePage, std::memory_order_relaxed);
}

VectorPoolRegistry::VectorPoolRegistry(size_t region_bytes, size_t slab_bytes)
    : slab_bytes_(vmsdk::numa_slab::RoundUpToHugePage(slab_bytes)),
      region_bytes_((region_bytes + slab_bytes_ - 1) / slab_bytes_ *
                    slab_bytes_),
      base_(nullptr) {
  if (region_bytes_ > 0) {
    try {
      base_ = vmsdk::numa_slab::ReserveRegion(region_bytes_);
    } catch (const std::bad_alloc &) {
      VMSDK_LOG(WARNING, nullptr)
          << "Could not reserve " << region_bytes_
          << " bytes of address space for vector pools; vectors will use the "
             "default allocator";
    }
  }
  slabs_ = std::make_unique<Slab[]>(base_ ? region_bytes_ / slab_bytes_ : 0);
}

VectorPoolRegistry::~VectorPoolRegistry() {
  if (!base_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  size_t pages_per_slab = slab_bytes_ / kHugePage;
  for (size_t s = 0; s < next_slab_; ++s) {
    for (size_t p = 0; p < pages_per_slab; ++p) {
      auto &page = slabs_[s].pages[p];
      if (page.materialized) {
        vmsdk::numa_slab::Release(page.base, kHugePage);
        global_materialized_bytes.fetch_sub(kHugePage,
                                            std::memory_order_relaxed);
      }
    }
  }
  vmsdk::numa_slab::UnreserveRegion(base_, region_bytes_);
}

VectorPoolRegistry &VectorPoolRegistry::Global() {
  static VectorPoolRegistry *registry = [] {
    size_t phys_bytes = static_cast<size_t>(sysconf(_SC_PHYS_PAGES)) *
                        static_cast<size_t>(sysconf(_SC_PAGESIZE));
    auto *r = new VectorPoolRegistry(
        phys_bytes, vmsdk::GetChunkedArraySlabSize().GetValue());
    global_registry.store(r, std::memory_order_release);
    return r;
  }();
  return *registry;
}

VectorPool *VectorPoolRegistry::Get(uint32_t type_tag, size_t entry_size) {
  entry_size = RoundUpToMultipleOf8(entry_size);
  CHECK_GE(entry_size, sizeof(uint32_t));
  CHECK_LE(entry_size, kHugePage);
  absl::MutexLock lock(&mutex_);
  auto &pool = pools_[{type_tag, entry_size}];
  if (!pool) {
    pool.reset(new VectorPool(this, type_tag, entry_size));
  }
  return pool.get();
}

VectorPool *VectorPoolRegistry::PoolOf(const void *ptr) const {
  auto p = static_cast<const char *>(ptr);
  if (!base_ || p < base_ || p >= base_ + region_bytes_) {
    return nullptr;
  }
  return slabs_[(p - base_) / slab_bytes_].owner.load(
      std::memory_order_acquire);
}

VectorPool *VectorPoolRegistry::GlobalPoolOf(const void *ptr) {
  auto *registry = global_registry.load(std::memory_order_acquire);
  return registry ? registry->PoolOf(ptr) : nullptr;
}

uint64_t VectorPoolRegistry::GlobalActiveAllocations() {
  return std::max<int64_t>(
      0, global_active_allocations.load(std::memory_order_relaxed));
}

uint64_t VectorPoolRegistry::GlobalMaterializedBytes() {
  return std::max<int64_t>(
      0, global_materialized_bytes.load(std::memory_order_relaxed));
}

int64_t VectorPoolRegistry::ClaimSlab(VectorPool *pool) {
  absl::MutexLock lock(&mutex_);
  if (next_slab_ * slab_bytes_ >= (base_ ? region_bytes_ : 0)) {
    static std::atomic<bool> logged{false};
    if (!logged.exchange(true)) {
      VMSDK_LOG(WARNING, nullptr)
          << "Vector pool address space exhausted; further vectors will use "
             "the default allocator";
    }
    return -1;
  }
  auto slab = static_cast<int64_t>(next_slab_++);
  slabs_[slab].pages =
      std::make_unique<VectorPool::Page[]>(slab_bytes_ / kHugePage);
  slabs_[slab].owner.store(pool, std::memory_order_release);
  return slab;
}

VectorPool::Page *VectorPoolRegistry::PageOf(const char *ptr) const {
  size_t offset = ptr - base_;
  return &slabs_[offset / slab_bytes_]
              .pages[(offset % slab_bytes_) / kHugePage];
}

}  // namespace valkey_search
