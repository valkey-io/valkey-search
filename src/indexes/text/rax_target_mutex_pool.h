/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_RAX_TARGET_MUTEX_POOL_H_
#define VALKEY_SEARCH_INDEXES_TEXT_RAX_TARGET_MUTEX_POOL_H_

#include <cstddef>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/indexes/text/invasive_ptr.h"

namespace valkey_search::indexes::text {

// Forward declarations
struct Postings;

// A sharded pool of mutexes and caches used to protect Rax tree targets
// and cache tree mutations during concurrent write operations.
//
// Each bucket contains:
// 1. A mutex protecting access to postings objects for words in that bucket
// 2. A cache storing pending tree mutations (word → Postings pointer)
//
// The bucket for a given word is chosen by:
//   index = absl::HashOf(word) % pool_size
//
// Mutations are cached during the write phase and applied to the tree
// serially at the end of the write phase via a callback.
//
// Pool size is configured at construction time and does not change.
class RaxTargetMutexPool {
 public:
  struct Bucket {
    struct CachedMutation {
      InvasivePtr<Postings> target;
      bool is_new_word;  // True if word wasn't in tree before this write phase
    };
    absl::Mutex mutex;
    absl::flat_hash_map<std::string, CachedMutation> cache;
  };

  explicit RaxTargetMutexPool(size_t pool_size) : buckets_(pool_size) {}

  // Non-copyable, non-movable (contains absl::Mutex).
  RaxTargetMutexPool(const RaxTargetMutexPool&) = delete;
  RaxTargetMutexPool& operator=(const RaxTargetMutexPool&) = delete;
  RaxTargetMutexPool(RaxTargetMutexPool&&) = delete;
  RaxTargetMutexPool& operator=(RaxTargetMutexPool&&) = delete;

  // Returns the bucket for the given word. Always returns the same bucket
  // for the same word string. Thread-safe: no mutation of pool state occurs.
  Bucket& GetBucket(absl::string_view word) {
    return buckets_[absl::HashOf(word) % buckets_.size()];
  }

  // Returns all buckets for iteration (e.g., during cache application).
  const std::vector<Bucket>& GetBuckets() const { return buckets_; }
  std::vector<Bucket>& GetBuckets() { return buckets_; }

  // Returns the number of buckets in the pool.
  size_t Size() const { return buckets_.size(); }

 private:
  std::vector<Bucket> buckets_;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_RAX_TARGET_MUTEX_POOL_H_
