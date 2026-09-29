
/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/hyperloglog_counter.h"

namespace valkey_search {

HyperLogLog::HyperLogLog() { hll_init(&hll_); }

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
    hll_add(&hll_, serialized.data(), serialized.size());
    return;
  }
  auto sv = value.AsStringView();
  if (sv.has_value()) {
    hll_add(&hll_, sv->data(), sv->size());
  }
}

uint64_t HyperLogLog::Estimate() const { return hll_count(&hll_); }

}  // namespace valkey_search
