
#include "src/utils/hyperloglog_counter.h"

namespace valkey_search {

HyperLogLog::HyperLogLog() { hll_init(&hll_); }

void HyperLogLog::Add(const expr::Value& value) {
  if (value.IsNil()) {
    return;
  }
  auto sv = value.AsStringView();
  hll_add(&hll_, sv.data(), sv.size());
}

uint64_t HyperLogLog::Estimate() const { return hll_count(&hll_); }

}  // namespace valkey_search
