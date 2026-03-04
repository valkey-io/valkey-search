/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_RAX_TARGET_MUTEX_POOL_H_
#define VALKEY_SEARCH_INDEXES_TEXT_RAX_TARGET_MUTEX_POOL_H_

#include <cstddef>
#include <vector>

#include "absl/hash/hash.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"

namespace valkey_search::indexes::text {

// A sharded pool of absl::Mutex objects used to protect Rax tree targets
// during concurrent write operations.
//
// This pool provides N mutexes shared across all words via
// hashing. The mutex for a given word is chosen by:
//
//   index = absl::HashOf(word) % pool_size
//
//
// Pool size is configured at construction time and does not change. absl::Mutex
// is neither copyable nor movable, so the pool cannot be resized after
// construction.
class RaxTargetMutexPool {
 public:
  explicit RaxTargetMutexPool(size_t pool_size) : mutexes_(pool_size) {}

  // Non-copyable, non-movable (contains absl::Mutex).
  RaxTargetMutexPool(const RaxTargetMutexPool&) = delete;
  RaxTargetMutexPool& operator=(const RaxTargetMutexPool&) = delete;
  RaxTargetMutexPool(RaxTargetMutexPool&&) = delete;
  RaxTargetMutexPool& operator=(RaxTargetMutexPool&&) = delete;

  // Returns the mutex for the given word. Always returns the same mutex for
  // the same word string. Thread-safe: no mutation of pool state occurs.
  absl::Mutex& Get(absl::string_view word) {
    return mutexes_[absl::HashOf(word) % mutexes_.size()];
  }

  // Returns the number of mutexes in the pool.
  size_t Size() const { return mutexes_.size(); }

 private:
  std::vector<absl::Mutex> mutexes_;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_RAX_TARGET_MUTEX_POOL_H_
