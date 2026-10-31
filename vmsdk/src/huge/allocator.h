/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_HUGE_ALLOCATOR_H_
#define VMSDK_SRC_HUGE_ALLOCATOR_H_

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <type_traits>

#include "absl/log/check.h"
#include "vmsdk/src/huge/huge_page_slab.h"
#include "vmsdk/src/log.h"

namespace vmsdk {

// STL-compliant allocator for very large containers. Requests of at least
// kHugeThreshold bytes get their own huge_page_slab region (one VMA each);
// smaller requests go to the normal heap. deallocate() repeats the same size
// test, which the allocator contract makes reliable because it is always
// passed the `n` given to allocate().
template <typename T>
class HugeAllocator {
 public:
  using value_type = T;
  using is_always_equal = std::true_type;

  static constexpr size_t kHugeThreshold = huge_page_slab::kHugePageBytes;

  HugeAllocator() noexcept = default;
  template <typename U>
  HugeAllocator(const HugeAllocator<U> &) noexcept {
  }  // NOLINT: implicit rebind.

  static constexpr size_t max_size() noexcept {
    return std::numeric_limits<size_t>::max() / sizeof(T);
  }

  T *allocate(size_t n) {
    if (n > max_size()) {
      throw std::bad_array_new_length();
    }
    size_t bytes = n * sizeof(T);
    if (bytes < kHugeThreshold) {
      return std::allocator<T>().allocate(n);
    }
    size_t region = huge_page_slab::RoundUpToHugePage(bytes);
    auto base = huge_page_slab::ReserveRegion(region);
    if (!base.ok()) {
      VMSDK_LOG(WARNING, nullptr) << base.status();
      throw std::bad_alloc();
    }
    if (auto status = huge_page_slab::Materialize(*base, region);
        !status.ok()) {
      huge_page_slab::UnreserveRegion(*base, region);
      VMSDK_LOG(WARNING, nullptr) << status;
      throw std::bad_alloc();
    }
    return reinterpret_cast<T *>(*base);
  }

  void deallocate(T *p, size_t n) noexcept {
    size_t bytes = n * sizeof(T);
    if (bytes < kHugeThreshold) {
      std::allocator<T>().deallocate(p, n);
      return;
    }
    // A misaligned pointer here means `n` differs from the allocate() call.
    CHECK_EQ(reinterpret_cast<uintptr_t>(p) % huge_page_slab::kHugePageBytes,
             0u);
    size_t region = huge_page_slab::RoundUpToHugePage(bytes);
    auto base = reinterpret_cast<char *>(p);
    huge_page_slab::Release(base, region);
    huge_page_slab::UnreserveRegion(base, region);
  }
};

template <typename T, typename U>
bool operator==(const HugeAllocator<T> &, const HugeAllocator<U> &) noexcept {
  return true;
}

template <typename T, typename U>
bool operator!=(const HugeAllocator<T> &, const HugeAllocator<U> &) noexcept {
  return false;
}

}  // namespace vmsdk

#endif  // VMSDK_SRC_HUGE_ALLOCATOR_H_
