/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/sparse_search.h"

#include <algorithm>
#include <bit>
#include <cstdint>
#include <limits>
#include <vector>

#include "absl/base/internal/endian.h"
#include "absl/status/status.h"
#include "src/indexes/fp16.h"

namespace valkey_search::query {
namespace {

// A fixed working set independent of the largest point ID. Empty ID ranges
// are skipped by selecting the next block from the posting-list cursors.
constexpr uint64_t kBlockSize = 8192;

// Avoid std::isfinite: an out-of-line inline definition from another
// translation unit compiled with fast-math can be selected at link time.
bool IsFiniteDouble(double value) {
  constexpr uint64_t kExponentMask = 0x7ff0000000000000ULL;
  return (std::bit_cast<uint64_t>(value) & kExponentMask) != kExponentMask;
}

bool Better(const SparseSearchHit &a, const SparseSearchHit &b) {
  return a.score > b.score || (a.score == b.score && a.id < b.id);
}

struct Cursor {
  absl::string_view remaining;
  double weight;
  uint32_t previous_id = 0;
  bool has_previous = false;
};

}  // namespace

absl::StatusOr<std::vector<SparseSearchHit>> SparseSearch(
    absl::Span<const SparsePostingList> lists, size_t k) {
  if (k == 0 || k > kSparseMaxTopK || lists.empty() ||
      lists.size() > kSparseMaxDimensions) {
    return absl::InvalidArgumentError(
        "Invalid sparse top-K or dimension count");
  }
  std::vector<Cursor> cursors;
  cursors.reserve(lists.size());
  for (const auto &list : lists) {
    if (!IsFiniteDouble(list.weight) || list.weight < 0) {
      return absl::InvalidArgumentError(
          "Query weights must be finite and nonnegative");
    }
    if (list.data.size() % kSparsePostingBytes != 0) {
      return absl::InvalidArgumentError("Invalid packed posting list length");
    }
    cursors.push_back({list.data, list.weight});
  }

  // Negative scores cannot occur, so -1 also marks untouched accumulator slots.
  std::vector<double> scores(kBlockSize, -1);
  std::vector<uint32_t> touched;
  touched.reserve(kBlockSize);
  std::vector<SparseSearchHit> heap;
  heap.reserve(k);

  while (true) {
    uint64_t next_id = std::numeric_limits<uint64_t>::max();
    for (const auto &cursor : cursors) {
      if (!cursor.remaining.empty()) {
        next_id = std::min(
            next_id, static_cast<uint64_t>(
                         absl::little_endian::Load32(cursor.remaining.data())));
      }
    }
    if (next_id == std::numeric_limits<uint64_t>::max()) {
      break;
    }
    const uint64_t base = next_id / kBlockSize * kBlockSize;
    for (auto &cursor : cursors) {
      while (!cursor.remaining.empty()) {
        const uint32_t id =
            absl::little_endian::Load32(cursor.remaining.data());
        if (cursor.has_previous && id <= cursor.previous_id) {
          return absl::InvalidArgumentError(
              "Posting IDs must be strictly increasing");
        }
        if (id >= base + kBlockSize) {
          break;
        }
        const uint16_t bits = absl::little_endian::Load16(
            cursor.remaining.data() + sizeof(uint32_t));
        // Reject negative encodings, NaN and infinity before FP16 conversion.
        if (bits >= 0x7c00) {
          return absl::InvalidArgumentError(
              "Posting weights must be finite and nonnegative");
        }
        const double value = float16_to_float(std::bit_cast<float16>(bits));
        const uint32_t local_id = id - base;
        if (scores[local_id] < 0) {
          scores[local_id] = 0;
          touched.push_back(local_id);
        }
        scores[local_id] += cursor.weight * value;
        if (!IsFiniteDouble(scores[local_id])) {
          return absl::InvalidArgumentError("Sparse score overflow");
        }
        cursor.previous_id = id;
        cursor.has_previous = true;
        cursor.remaining.remove_prefix(kSparsePostingBytes);
      }
    }
    for (uint32_t local_id : touched) {
      const SparseSearchHit hit{static_cast<uint32_t>(base + local_id),
                                scores[local_id]};
      if (heap.size() < k) {
        heap.push_back(hit);
        std::push_heap(heap.begin(), heap.end(), Better);
      } else if (Better(hit, heap.front())) {
        std::pop_heap(heap.begin(), heap.end(), Better);
        heap.back() = hit;
        std::push_heap(heap.begin(), heap.end(), Better);
      }
      scores[local_id] = -1;
    }
    touched.clear();
  }
  std::sort_heap(heap.begin(), heap.end(), Better);
  return heap;
}

}  // namespace valkey_search::query
