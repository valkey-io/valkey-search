/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/chunked_array.h"

#include <algorithm>
#include <stdexcept>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/numa_slab.h"

namespace vmsdk {

namespace {

using numa_slab::RoundUpToHugePage;

constexpr size_t kHugePage = ChunkedArray::kHugePageBytes;
constexpr long long kDefaultSlabBytes = 256LL * 1024 * 1024;
constexpr long long kMaxSlabBytes = 64LL * 1024 * 1024 * 1024;

constexpr absl::string_view kSlabSizeConfig{"chunked-array-slab-size"};

absl::Status ValidateSlabSize(long long new_value) {
  if (new_value < static_cast<long long>(kHugePage) ||
      new_value > kMaxSlabBytes || new_value % kHugePage != 0) {
    return absl::InvalidArgumentError(
        absl::StrCat(kSlabSizeConfig, " must be a multiple of ", kHugePage,
                     " between ", kHugePage, " and ", kMaxSlabBytes));
  }
  return absl::OkStatus();
}

/// Register the "chunked-array-slab-size" config: the address space reserved
/// for each segment of a ChunkedArray. Applies to arrays created afterwards.
auto slab_size = config::NumberBuilder(kSlabSizeConfig,    // name
                                       kDefaultSlabBytes,  // default
                                       kHugePage,          // min
                                       kMaxSlabBytes)      // max
                     .WithFlags(config::kMemory)
                     .WithValidationCallback(ValidateSlabSize)
                     .Build();

}  // namespace

config::Number &GetChunkedArraySlabSize() {
  return dynamic_cast<config::Number &>(*slab_size);
}

ChunkedArray::ChunkedArray(size_t element_byte_size, size_t element_count)
    : element_byte_size_(element_byte_size),
      slab_bytes_(GetChunkedArraySlabSize().GetValue()) {
  if (element_byte_size_ == 0 || element_byte_size_ > slab_bytes_) {
    throw std::length_error(
        absl::StrCat("ChunkedArray: element size ", element_byte_size_,
                     " does not fit in a slab of ", slab_bytes_, " bytes"));
  }
  elements_per_segment_ = slab_bytes_ / element_byte_size_;
  try {
    resize(element_count);
  } catch (...) {
    resize(0);
    throw;
  }
}

ChunkedArray::~ChunkedArray() { resize(0); }

void ChunkedArray::AddSegment() {
  segments_.push_back({numa_slab::ReserveRegion(slab_bytes_), 0});
}

void ChunkedArray::SetCommitted(Segment &segment, size_t bytes) {
  if (bytes > segment.committed_bytes) {
    numa_slab::Materialize(segment.base + segment.committed_bytes,
                           bytes - segment.committed_bytes);
  } else if (bytes < segment.committed_bytes) {
    numa_slab::Release(segment.base + bytes, segment.committed_bytes - bytes);
  }
  segment.committed_bytes = bytes;
}

void ChunkedArray::resize(size_t new_element_count) {
  size_t new_segment_count =
      (new_element_count + elements_per_segment_ - 1) / elements_per_segment_;
  // Bytes segment `s` must have materialized to hold `new_element_count`.
  auto needed_bytes = [&](size_t s) -> size_t {
    if (s >= new_segment_count) {
      return 0;
    }
    size_t elements = std::min(elements_per_segment_,
                               new_element_count - s * elements_per_segment_);
    return RoundUpToHugePage(elements * element_byte_size_);
  };

  if (new_element_count > getCapacity()) {
    while (segments_.size() < new_segment_count) {
      AddSegment();
    }
    for (size_t s = 0; s < new_segment_count; ++s) {
      if (needed_bytes(s) > segments_[s].committed_bytes) {
        SetCommitted(segments_[s], needed_bytes(s));
      }
    }
    element_count_.store(new_element_count, std::memory_order_relaxed);
    return;
  }

  // Also releases anything left materialized by a failed grow.
  element_count_.store(new_element_count, std::memory_order_relaxed);
  for (size_t s = 0; s < segments_.size(); ++s) {
    SetCommitted(segments_[s], needed_bytes(s));
  }
  while (segments_.size() > new_segment_count) {
    numa_slab::UnreserveRegion(segments_.back().base, slab_bytes_);
    segments_.pop_back();
  }
}

}  // namespace vmsdk
