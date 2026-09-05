
#include "src/utils/hyperloglog_counter.h"

namespace valkey_search {

HyperLogLog::HyperLogLog() { hll_init(&hll_); }

void HyperLogLog::Add(const expr::Value& value) {
  if (value.IsNil()) {
    return;
  }
  // For scalar values, use AsStringView() directly (fast path, zero-copy).
  // For arrays, use Serialize() which produces a deterministic JSON-like
  // string representation.
  auto sv = value.AsStringView();
  if (sv.has_value()) {
    hll_add(&hll_, sv->data(), sv->size());
  } else if (value.IsArray()) {
    std::string serialized = value.Serialize();
    hll_add(&hll_, serialized.data(), serialized.size());
  }
}

uint64_t HyperLogLog::Estimate() const { return hll_count(&hll_); }

}  // namespace valkey_search
