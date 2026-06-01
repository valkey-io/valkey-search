/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/key_attr_value.h"

#include <cstdint>
#include <cstring>
#include <new>

#include "absl/log/check.h"

namespace valkey_search::indexes {

namespace {

// Confirms that the bit-field `SlotBase{occupied:1, user_data_len:31}` places
// `occupied` at bit 0 of the underlying uint32_t. This is the Itanium ABI
// behavior on LE targets, but the standard leaves bit-field layout
// implementation-defined, so we verify it once at process startup.
[[maybe_unused]] const bool kBitfieldLayoutChecked = [] {
  SlotBase s{1u, 0u};
  uint32_t raw = 0;
  std::memcpy(&raw, &s, sizeof(s));
  CHECK_EQ(raw, 1u)
      << "SlotBase::occupied must be bit 0 of the uint32_t bit-field";
  return true;
}();

}  // namespace

KeyAttrValue* KeyAttrValue::Allocate(uint16_t num_slots) {
  void* buf = ::operator new(AllocSize(num_slots),
                             std::align_val_t{kAlignment});
  return reinterpret_cast<KeyAttrValue*>(buf);
}

void KeyAttrValue::Free(KeyAttrValue* p) noexcept {
  if (p == nullptr) {
    return;
  }
  ::operator delete(p, std::align_val_t{kAlignment});
}

}  // namespace valkey_search::indexes
