
#include "src/utils/hyperloglog_counter.h"

#include <algorithm>

namespace valkey_search {

void HyperLogLog::Add(const expr::Value& value) {
  if (value.IsNil()) {
    return;
  }
  // For scalar values, use AsStringView() directly (fast path, zero-copy).
  // For arrays, use Serialize() which produces a deterministic JSON-like
  // string representation.
  auto sv = value.AsStringView();
  if (sv.has_value()) {
    AddBuffer(sv->data(), sv->size());
  } else if (value.IsArray()) {
    std::string serialized = value.Serialize();
    AddBuffer(serialized.data(), serialized.size());
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
