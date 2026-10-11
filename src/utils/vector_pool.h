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
VectorPool hands out fixed-size entries for one (vector type, entry size)
pair. Its memory comes from slabs of its registry's reserved region, which are
NUMA-interleaved and materialized one 2 MiB huge page at a time. Entries never
straddle a huge page. A huge page is released back to the OS once its last
entry is freed.

Allocation prefers the materialized page with the fewest free entries, so
lightly used pages drain and can be released.
*/
class VectorPool {
 public:
  VectorPool(const VectorPool &) = delete;
  VectorPool &operator=(const VectorPool &) = delete;

  // Returns nullptr when the registry's region is exhausted.
  char *Allocate() ABSL_LOCKS_EXCLUDED(mutex_);
  void Free(char *ptr) ABSL_LOCKS_EXCLUDED(mutex_);

  uint32_t TypeTag() const { return type_tag_; }
  size_t EntrySize() const { return entry_size_; }
  size_t ActiveAllocations() const ABSL_LOCKS_EXCLUDED(mutex_);

 private:
  friend class VectorPoolRegistry;

  struct Page {
    uint32_t used{0};
    // Entries [0, initialized) have been handed out at least once; freed ones
    // are on the intrusive free list starting at free_head.
    uint32_t initialized{0};
    uint32_t free_head{kNoEntry};
    bool materialized{false};
    char *base{nullptr};
    // Intrusive list links.
    Page *next{nullptr};
    Page *prev{nullptr};
  };
  static constexpr uint32_t kNoEntry = UINT32_MAX;
  static constexpr size_t kFreeGroups = 7;

  VectorPool(VectorPoolRegistry *registry, uint32_t type_tag,
             size_t entry_size);

  void AddSlab(Page *pages, char *base) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // Group of a materialized page by free entries; -1 for a full page.
  int FreeGroup(const Page &page) const;
  void Regroup(Page *page, int old_group) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  VectorPoolRegistry *registry_;
  const uint32_t type_tag_;
  const size_t entry_size_;
  const uint32_t entries_per_page_;
  mutable absl::Mutex mutex_;
  // Materialized, non-full pages grouped by ceil(log2(free entries)).
  IntrusiveList<Page> groups_[kFreeGroups] ABSL_GUARDED_BY(mutex_);
  // Pages of owned slabs with no memory behind them.
  IntrusiveList<Page> unmaterialized_ ABSL_GUARDED_BY(mutex_);
  size_t active_allocations_ ABSL_GUARDED_BY(mutex_){0};
};

/*
VectorPoolRegistry owns one reserved address region, carved into slabs that
pools claim on demand, and the pools themselves (one per type tag and entry
size, never destroyed). The owner of any address in the region is found
without locking, so freeing needs no global lookup structure.
*/
class VectorPoolRegistry {
 public:
  // `region_bytes` and `slab_bytes` are rounded up to whole huge pages.
  VectorPoolRegistry(size_t region_bytes, size_t slab_bytes);
  ~VectorPoolRegistry();

  VectorPoolRegistry(const VectorPoolRegistry &) = delete;
  VectorPoolRegistry &operator=(const VectorPoolRegistry &) = delete;

  // The process-wide registry, reserved on first use with a region the size of
  // physical memory and slabs of "chunked-array-slab-size".
  static VectorPoolRegistry &Global();

  // The pool for `type_tag` entries of `entry_size` bytes (rounded up to a
  // multiple of 8). Created on first request.
  VectorPool *Get(uint32_t type_tag, size_t entry_size)
      ABSL_LOCKS_EXCLUDED(mutex_);

  // The pool that owns `ptr`, or nullptr if `ptr` is not in this region.
  VectorPool *PoolOf(const void *ptr) const;

  // PoolOf on the global registry; nullptr if it was never created.
  static VectorPool *GlobalPoolOf(const void *ptr);

  static uint64_t GlobalActiveAllocations();
  static uint64_t GlobalMaterializedBytes();

 private:
  friend class VectorPool;

  struct Slab {
    std::atomic<VectorPool *> owner{nullptr};
    std::unique_ptr<VectorPool::Page[]> pages;
  };

  // Gives `pool` an unclaimed slab and returns its index; -1 if none is left.
  int64_t ClaimSlab(VectorPool *pool) ABSL_LOCKS_EXCLUDED(mutex_);
  VectorPool::Page *PageOf(const char *ptr) const;

  const size_t slab_bytes_;
  const size_t region_bytes_;
  char *base_;
  std::unique_ptr<Slab[]> slabs_;
  absl::Mutex mutex_;
  size_t next_slab_ ABSL_GUARDED_BY(mutex_){0};
  absl::flat_hash_map<std::pair<uint32_t, size_t>, std::unique_ptr<VectorPool>>
      pools_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_UTILS_VECTOR_POOL_H_
