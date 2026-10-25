/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_UTILS_VECTOR_POOL_H_
#define VALKEYSEARCH_SRC_UTILS_VECTOR_POOL_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "src/utils/intrusive_list.h"

namespace valkey_search {

class VectorPoolRegistry;

/*
VectorPool hands out fixed-size vector entries for one (vector type, data size)
pair. Its memory comes from slabs mapped on demand, which are materialized one
2 MiB huge page at a time and unmapped once all their pages are released. A huge page is released back to the OS once its last entry is
freed.

Each page holds its own metadata, so an entry's metadata is found from the
entry's address alone, without a lock:

  [PageHeader][EntryMeta x n][pad to 64 B][entry 0][entry 1]...[entry n-1]

Entries are spaced by the data size rounded up to a cache line and start on a
cache line, with no per-entry prefix. EntryMeta holds the reference count and
reciprocal magnitude that VectorRecord would otherwise keep in a header.

Allocation prefers the materialized page with the fewest free entries, so
lightly used pages drain and can be released.
*/
class VectorPool {
 public:
  static constexpr size_t kCacheLineBytes = 64;

  struct EntryMeta {
    // While the entry is free, holds the index of the next free entry.
    std::atomic<uint32_t> ref_count;
    float reciprocal_magnitude;
  };

  VectorPool(const VectorPool &) = delete;
  VectorPool &operator=(const VectorPool &) = delete;
  ~VectorPool();

  // Returns the entry's data, aligned to a cache line, or nullptr when no
  // memory can be mapped or materialized. The entry's EntryMeta is
  // uninitialized.
  char *Allocate() ABSL_LOCKS_EXCLUDED(mutex_);
  void Free(char *data) ABSL_LOCKS_EXCLUDED(mutex_);

  // For a `data` pointer returned by Allocate, while it is allocated.
  static EntryMeta &MetaOf(const char *data) {
    auto *header = HeaderOf(data);
    size_t entry = (data - reinterpret_cast<const char *>(header) -
                    header->first_entry_offset) /
                   header->stride;
    return MetaTable(header)[entry];
  }
  static VectorPool *PoolOf(const char *data) { return HeaderOf(data)->pool; }

  uint32_t TypeTag() const { return type_tag_; }
  // Distance between consecutive entries: a multiple of kCacheLineBytes.
  size_t EntrySize() const { return entry_size_; }
  uint32_t EntriesPerPage() const { return entries_per_page_; }
  size_t ActiveAllocations() const ABSL_LOCKS_EXCLUDED(mutex_);
  size_t SlabCount() const ABSL_LOCKS_EXCLUDED(mutex_);

 private:
  friend class VectorPoolRegistry;
  struct Slab;

  struct Page {
    uint32_t used{0};
    // Entries [0, initialized) have been handed out at least once; freed ones
    // are on the free list starting at free_head, linked through EntryMeta.
    uint32_t initialized{0};
    uint32_t free_head{kNoEntry};
    bool materialized{false};
    char *base{nullptr};
    Slab *slab{nullptr};
    // Intrusive list links.
    Page *next{nullptr};
    Page *prev{nullptr};
  };

  struct Slab {
    char *base;
    std::unique_ptr<Page[]> pages;
    size_t materialized_pages{0};
  };

  // At the start of every materialized page.
  struct PageHeader {
    VectorPool *pool;
    Page *page;
    uint32_t stride;
    uint32_t first_entry_offset;
  };
  static constexpr uint32_t kNoEntry = UINT32_MAX;
  static constexpr size_t kFreeGroups = 7;

  VectorPool(VectorPoolRegistry *registry, uint32_t type_tag,
             size_t entry_size);

  static PageHeader *HeaderOf(const char *data);
  static EntryMeta *MetaTable(PageHeader *header) {
    return reinterpret_cast<EntryMeta *>(header + 1);
  }
  char *EntryAt(const Page &page, uint32_t entry) const {
    return page.base + first_entry_offset_ + entry * entry_size_;
  }
  bool AddSlab() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void ReleasePage(Page *page) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // Group of a materialized page by free entries; -1 for a full page.
  int FreeGroup(const Page &page) const;
  void Regroup(Page *page, int old_group) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  VectorPoolRegistry *registry_;
  const uint32_t type_tag_;
  const size_t entry_size_;
  const uint32_t entries_per_page_;
  const uint32_t first_entry_offset_;
  mutable absl::Mutex mutex_;
  // Materialized, non-full pages grouped by ceil(log2(free entries)).
  IntrusiveList<Page> groups_[kFreeGroups] ABSL_GUARDED_BY(mutex_);
  // Pages of mapped slabs with no memory behind them.
  IntrusiveList<Page> unmaterialized_ ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_map<Slab *, std::unique_ptr<Slab>> slabs_
      ABSL_GUARDED_BY(mutex_);
  size_t active_allocations_ ABSL_GUARDED_BY(mutex_){0};
};

/*
VectorPoolRegistry owns the pools, one per type tag and entry size, which are
never destroyed while the registry lives.
*/
class VectorPoolRegistry {
 public:
  // `slab_bytes` is rounded up to whole huge pages.
  explicit VectorPoolRegistry(size_t slab_bytes);

  VectorPoolRegistry(const VectorPoolRegistry &) = delete;
  VectorPoolRegistry &operator=(const VectorPoolRegistry &) = delete;

  // The process-wide registry, with slabs of "chunked-array-slab-size".
  static VectorPoolRegistry &Global();

  // The pool for `type_tag` entries of `data_bytes` bytes. Sizes that round up
  // to the same multiple of a cache line share a pool. Created on first
  // request.
  VectorPool *Get(uint32_t type_tag, size_t data_bytes)
      ABSL_LOCKS_EXCLUDED(mutex_);

  static uint64_t GlobalActiveAllocations();
  static uint64_t GlobalMaterializedBytes();

 private:
  friend class VectorPool;

  const size_t slab_bytes_;
  absl::Mutex mutex_;
  absl::flat_hash_map<std::pair<uint32_t, size_t>, std::unique_ptr<VectorPool>>
      pools_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_UTILS_VECTOR_POOL_H_
