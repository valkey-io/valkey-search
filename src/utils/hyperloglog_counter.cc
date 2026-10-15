
/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/hyperloglog_counter.h"

#include <algorithm>

namespace valkey_search {

void HyperLogLog::Add(const expr::Value& value) {
  if (value.IsNil()) {
    return;
  }
  // Arrays have no scalar string form -- AsStringView() returns the shared
  // kArrayAsString sentinel for every array, which would collapse all distinct
  // arrays into one bucket -- so serialize them to a deterministic
  // representation first. Scalars take the zero-copy AsStringView() fast path.
  if (value.IsArray()) {
    std::string serialized = value.Serialize();
    AddBuffer(serialized.data(), serialized.size());
    return;
  }
  // Doubles format to 12 significant digits, so hash their bits instead.
  if (value.IsDouble()) {
    double d = *value.AsDouble();
    if (d == 0.0) {
      d = 0.0;  // -0.0 and 0.0 hash alike
    }
    AddBuffer(&d, sizeof(d));
    return;
  }
  auto sv = value.AsStringView();
  if (sv.has_value()) {
    AddBuffer(sv->data(), sv->size());
  }
}

void HyperLogLog::AddBuffer(const void* buf, size_t len) {
  long index;
  int count = hll_pat_len(buf, len, &index);
  if (dense_) {
    hll_set(dense_.get(), index, count);
    return;
  }
  uint32_t entry = static_cast<uint32_t>(index) << 8 | count;
  auto it = std::lower_bound(sparse_.begin(), sparse_.end(), entry & ~0xffu);
  if (it != sparse_.end() && (*it >> 8) == (entry >> 8)) {
    *it = std::max(*it, entry);
  } else if (sparse_.size() < kMaxSparse) {
    sparse_.insert(it, entry);
  } else {
    dense_ = std::make_unique<struct HLL>();
    hll_init(dense_.get());
    for (uint32_t e : sparse_) {
      hll_set(dense_.get(), e >> 8, e & 0xff);
    }
    hll_set(dense_.get(), index, count);
    sparse_.clear();
    sparse_.shrink_to_fit();
  }
}

uint64_t HyperLogLog::Estimate() const {
  if (dense_) {
    return hll_count(dense_.get());
  }
  int reghisto[64] = {0};
  reghisto[0] = HLL_REGISTERS - static_cast<int>(sparse_.size());
  for (uint32_t e : sparse_) {
    ++reghisto[e & 0xff];
  }
  return hll_count_histogram(reghisto);
}

}  // namespace valkey_search
