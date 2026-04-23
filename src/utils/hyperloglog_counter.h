
#ifndef VALKEYSEARCH_SRC_UTILS_HYPERLOGLOG_COUNTER_H_
#define VALKEYSEARCH_SRC_UTILS_HYPERLOGLOG_COUNTER_H_

#include "src/expr/value.h"
#include "src/utils/hyperloglog.h"

namespace valkey_search {

// C++ wrapper around the Valkey HyperLogLog dense implementation for
// use with expr::Value types. Uses P=14 (16384 registers, ~0.81%
// standard error) matching the Valkey core HyperLogLog.
class HyperLogLog {
 public:
  HyperLogLog();
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
  struct HLL hll_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_UTILS_HYPERLOGLOG_COUNTER_H_
