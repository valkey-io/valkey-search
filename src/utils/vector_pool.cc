/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/vector_pool.h"

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

std::atomic<int64_t> global_active_allocations{0};
std::atomic<int64_t> global_materialized_bytes{0};

size_t RoundUpToCacheLine(size_t n) {
  return (n + VectorPool::kCacheLineBytes - 1) &
         ~(VectorPool::kCacheLineBytes - 1);
}

}  // namespace

VectorPool::PageHeader *VectorPool::HeaderOf(const char *data) {
  return reinterpret_cast<PageHeader *>(reinterpret_cast<uintptr_t>(data) &
                                        ~(kHugePage - 1));
}

namespace {

// Bytes before the first entry of a page holding `entries` entries.
size_t FirstEntryOffset(size_t header_bytes, size_t entries) {
  return RoundUpToCacheLine(header_bytes +
                            entries * sizeof(VectorPool::EntryMeta));
}

// The most entries of `stride` bytes that fit in a page with their metadata.
uint32_t MaxEntriesPerPage(size_t header_bytes, size_t stride) {
  size_t n =
      (kHugePage - header_bytes) / (stride + sizeof(VectorPool::EntryMeta));
  while (FirstEntryOffset(header_bytes, n) + n * stride > kHugePage) {
    --n;
  }
  return static_cast<uint32_t>(n);
}

}  // namespace

VectorPool::VectorPool(VectorPoolRegistry *registry, uint32_t type_tag,
                       size_t entry_size)
    : registry_(registry),
      type_tag_(type_tag),
      entry_size_(entry_size),
      entries_per_page_(MaxEntriesPerPage(sizeof(PageHeader), entry_size)),
      first_entry_offset_(
          FirstEntryOffset(sizeof(PageHeader), entries_per_page_)) {
  CHECK_GT(entries_per_page_, 0u);
}

VectorPool::~VectorPool() {
  absl::MutexLock lock(&mutex_);
  size_t pages_per_slab = registry_->slab_bytes_ / kHugePage;
  for (auto &[_, slab] : slabs_) {
    for (size_t p = 0; p < pages_per_slab; ++p) {
      if (slab->pages[p].materialized) {
        vmsdk::numa_slab::Release(slab->pages[p].base, kHugePage);
        global_materialized_bytes.fetch_sub(kHugePage,
                                            std::memory_order_relaxed);
      }
    }
    vmsdk::numa_slab::UnreserveRegion(slab->base, registry_->slab_bytes_);
  }
}

size_t VectorPool::ActiveAllocations() const {
  absl::MutexLock lock(&mutex_);
  return active_allocations_;
}

size_t VectorPool::SlabCount() const {
  absl::MutexLock lock(&mutex_);
  return slabs_.size();
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

bool VectorPool::AddSlab() {
  char *base;
  try {
    base = vmsdk::numa_slab::ReserveRegion(registry_->slab_bytes_);
  } catch (const std::bad_alloc &) {
    static std::atomic<bool> logged{false};
    if (!logged.exchange(true)) {
      VMSDK_LOG(WARNING, nullptr)
          << "Could not map vector pool memory; falling back to the default "
             "allocator";
    }
    return false;
  }
  size_t pages_per_slab = registry_->slab_bytes_ / kHugePage;
  auto slab = std::make_unique<Slab>();
  slab->base = base;
  slab->pages = std::make_unique<Page[]>(pages_per_slab);
  for (size_t i = 0; i < pages_per_slab; ++i) {
    slab->pages[i].base = base + i * kHugePage;
    slab->pages[i].slab = slab.get();
    unmaterialized_.PushBack(&slab->pages[i]);
  }
  slabs_[slab.get()] = std::move(slab);
  return true;
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
    if (unmaterialized_.Empty() && !AddSlab()) {
      return nullptr;
    }
    page = unmaterialized_.Front();
    try {
      vmsdk::numa_slab::Materialize(page->base, kHugePage);
    } catch (const std::bad_alloc &) {
      // E.g. mprotect hitting vm.max_map_count. Let the caller fall back to
      // operator new rather than throwing out of an insert on the main thread.
      static std::atomic<bool> logged{false};
      if (!logged.exchange(true)) {
        VMSDK_LOG(WARNING, nullptr)
            << "Could not materialize vector pool memory; falling back to "
               "the default allocator";
      }
      return nullptr;
    }
    unmaterialized_.Remove(page);
    page->materialized = true;
    ++page->slab->materialized_pages;
    new (page->base) PageHeader{.pool = this,
                                .page = page,
                                .stride = static_cast<uint32_t>(entry_size_),
                                .first_entry_offset = first_entry_offset_};
    global_materialized_bytes.fetch_add(kHugePage, std::memory_order_relaxed);
  }

  uint32_t entry;
  EntryMeta *meta = MetaTable(reinterpret_cast<PageHeader *>(page->base));
  if (page->free_head != kNoEntry) {
    entry = page->free_head;
    page->free_head = meta[entry].ref_count.load(std::memory_order_relaxed);
  } else {
    entry = page->initialized++;
  }
  ++page->used;
  Regroup(page, old_group);
  ++active_allocations_;
  global_active_allocations.fetch_add(1, std::memory_order_relaxed);
  return EntryAt(*page, entry);
}

void VectorPool::Free(char *data) {
  PageHeader *header = HeaderOf(data);
  Page *page = header->page;
  absl::MutexLock lock(&mutex_);
  size_t offset = data - EntryAt(*page, 0);
  DCHECK_EQ(offset % entry_size_, 0u);
  auto entry = static_cast<uint32_t>(offset / entry_size_);
  int old_group = FreeGroup(*page);
  MetaTable(header)[entry].ref_count.store(page->free_head,
                                           std::memory_order_relaxed);
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
  ReleasePage(page);
}

void VectorPool::ReleasePage(Page *page) {
  vmsdk::numa_slab::Release(page->base, kHugePage);
  page->materialized = false;
  page->initialized = 0;
  page->free_head = kNoEntry;
  unmaterialized_.PushBack(page);
  global_materialized_bytes.fetch_sub(kHugePage, std::memory_order_relaxed);
  Slab *slab = page->slab;
  if (--slab->materialized_pages > 0) {
    return;
  }
  size_t pages_per_slab = registry_->slab_bytes_ / kHugePage;
  for (size_t i = 0; i < pages_per_slab; ++i) {
    unmaterialized_.Remove(&slab->pages[i]);
  }
  vmsdk::numa_slab::UnreserveRegion(slab->base, registry_->slab_bytes_);
  slabs_.erase(slab);
}

VectorPoolRegistry::VectorPoolRegistry(size_t slab_bytes)
    : slab_bytes_(vmsdk::numa_slab::RoundUpToHugePage(slab_bytes)) {}

VectorPoolRegistry &VectorPoolRegistry::Global() {
  static VectorPoolRegistry *registry =
      new VectorPoolRegistry(vmsdk::GetChunkedArraySlabSize().GetValue());
  return *registry;
}

VectorPool *VectorPoolRegistry::Get(uint32_t type_tag, size_t data_bytes) {
  CHECK_GT(data_bytes, 0u);
  size_t entry_size = RoundUpToCacheLine(data_bytes);
  absl::MutexLock lock(&mutex_);
  auto &pool = pools_[{type_tag, entry_size}];
  if (!pool) {
    pool.reset(new VectorPool(this, type_tag, entry_size));
  }
  return pool.get();
}

uint64_t VectorPoolRegistry::GlobalActiveAllocations() {
  return std::max<int64_t>(
      0, global_active_allocations.load(std::memory_order_relaxed));
}

uint64_t VectorPoolRegistry::GlobalMaterializedBytes() {
  return std::max<int64_t>(
      0, global_materialized_bytes.load(std::memory_order_relaxed));
}

}  // namespace valkey_search
