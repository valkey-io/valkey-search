/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_SPARSE_SEARCH_H_
#define VALKEYSEARCH_SRC_QUERY_SPARSE_SEARCH_H_

#include <cstddef>
#include <cstdint>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"

namespace valkey_search::query {

inline constexpr size_t kSparsePostingBytes = 6;
inline constexpr size_t kSparseMaxDimensions = 64;
inline constexpr size_t kSparseMaxTopK = 10000;

struct SparsePostingList {
  absl::string_view data;
  double weight;
};

struct SparseSearchHit {
  uint32_t id;
  double score;
};

// Exact dot-product top-K over the union of the supplied posting lists.
// Each list contains strictly increasing uint32 IDs followed by nonnegative,
// finite binary16 weights, all little-endian. Query weights must be finite and
// nonnegative. The caller owns the data for the duration of the call.
// Results are ordered by decreasing score, then increasing ID.
absl::StatusOr<std::vector<SparseSearchHit>> SparseSearch(
    absl::Span<const SparsePostingList> lists, size_t k);

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_SPARSE_SEARCH_H_
