/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_CHUNKED_ARRAY_H_
#define VMSDK_SRC_CHUNKED_ARRAY_H_

#include <atomic>
#include <cassert>
#include <cstddef>
#include <vector>

#include "vmsdk/src/numa_slab.h"

namespace vmsdk {

namespace config {
class Number;
}  // namespace config

// A fixed-element-size array stored in a vector of segments. Each segment is a
// slab of reserved address space (size taken from the
// "chunked-array-slab-size" config when the array is constructed) that is
// materialized in huge-page-sized units as the array grows and released as it
// shrinks. Elements never move and never straddle a segment. On Linux each
// slab is advised to use transparent huge pages.
//
// Materialized bytes are reported via ValkeyModule_IncrExternalMemory /
// DecrExternalMemory and the module's own memory counter. Memory contents are
// not guaranteed; the caller tracks element state.
//
// resize() and clear() must not run concurrently with any other access;
// getCapacity() may be read concurrently.
class ChunkedArray {
 public:
  static constexpr size_t kHugePageBytes = numa_slab::kHugePageBytes;

  // Throws std::length_error if an element does not fit in a slab, and
  // std::bad_alloc if memory cannot be reserved or materialized.
  explicit ChunkedArray(size_t element_byte_size, size_t element_count = 0);
  ~ChunkedArray();

  ChunkedArray(const ChunkedArray &) = delete;
  ChunkedArray &operator=(const ChunkedArray &) = delete;

  size_t getCapacity() const {
    return element_count_.load(std::memory_order_relaxed);
  }

  size_t getSizePerElement() const { return element_byte_size_; }

  char *operator[](size_t i) const {
    assert(i < getCapacity());
    if (i >= getCapacity()) {
      return nullptr;
    }
    return segments_[i / elements_per_segment_].base +
           element_byte_size_ * (i % elements_per_segment_);
  }

  // Grows or shrinks the array. Throws std::bad_alloc on failure to grow, in
  // which case the capacity is unchanged.
  void resize(size_t new_element_count);

  void clear() { resize(0); }

 private:
  struct Segment {
    char *base;
    size_t committed_bytes;
  };

  void AddSegment();
  void SetCommitted(Segment &segment, size_t bytes);

  size_t element_byte_size_;
  size_t slab_bytes_;
  size_t elements_per_segment_;
  std::atomic<size_t> element_count_{0};
  std::vector<Segment> segments_;
};

config::Number &GetChunkedArraySlabSize();

}  // namespace vmsdk

#endif  // VMSDK_SRC_CHUNKED_ARRAY_H_
