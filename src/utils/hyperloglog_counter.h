
/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_UTILS_HYPERLOGLOG_COUNTER_H_
#define VALKEYSEARCH_SRC_UTILS_HYPERLOGLOG_COUNTER_H_

#include <cstdint>
#include <memory>

#include "absl/container/inlined_vector.h"
#include "src/expr/value.h"
#include "src/utils/hyperloglog.h"

namespace valkey_search {

// C++ wrapper around the Valkey HyperLogLog implementation for
// use with expr::Value types. Uses P=14 (16384 registers, ~0.81%
// standard error) matching the Valkey core HyperLogLog.
class HyperLogLog {
 public:
  HyperLogLog() = default;
  ~HyperLogLog() = default;

  HyperLogLog(const HyperLogLog&) = delete;
  HyperLogLog& operator=(const HyperLogLog&) = delete;
  HyperLogLog(HyperLogLog&&) = delete;
  HyperLogLog& operator=(HyperLogLog&&) = delete;

  // Add a value to the counter. Nil values are ignored.
  void Add(const expr::Value& value);

  // Return the estimated cardinality.
  uint64_t Estimate() const;

 private:
  void AddBuffer(const void* buf, size_t len);

  // The dense registers take 12 KB and most GROUPBY groups see few values,
  // so the non-zero registers are kept in sparse_, sorted as
  // (index << 8 | value), until there are more than kMaxSparse of them.
  // Both forms hold the same registers and give the same estimate.
  static constexpr size_t kMaxSparse = 1024;
  absl::InlinedVector<uint32_t, 4> sparse_;
  std::unique_ptr<struct HLL> dense_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_UTILS_HYPERLOGLOG_COUNTER_H_
