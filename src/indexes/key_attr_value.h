/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_KEY_ATTR_VALUE_H_
#define VALKEYSEARCH_SRC_INDEXES_KEY_ATTR_VALUE_H_

#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>

#include "src/utils/string_interning.h"

namespace valkey_search {

using MutationSequenceNumber = uint64_t;

namespace indexes {

// Per-slot header used by every occupied SlotT. The `occupied` bit lives at
// bit 0 of the uint32_t, sharing space with the user-data length. When the
// slot is empty, the first 8 bytes are the head of an InternedStringPtr; its
// LSB is always 0 (alignment), so a masked load of the slot's first 4 bytes
// reads 0 ⇒ slot is empty without needing a separate bitmask.
struct SlotBase {
  uint32_t occupied : 1;
  uint32_t user_data_len : 31;
};

static_assert(sizeof(SlotBase) == 4, "SlotBase must be exactly 4 bytes");

static_assert(std::endian::native == std::endian::little,
              "KeyAttrValue overlay requires a little-endian target");
// The bit-cast layout check (occupied at bit 0) is enforced at static-init
// time in key_attr_value.cc; std::bit_cast on a bit-field aggregate is not
// constexpr in clang/libc++ as of C++20.

static_assert(sizeof(InternedStringPtr) == 8,
              "InternedStringPtr must be a single 8-byte pointer");
static_assert(alignof(InternedStringPtr) >= 8,
              "InternedStringPtr must be 8-byte aligned so its LSB is 0");

// Empty-slot payload: two interned-string pointers threading the owning key
// into a per-attribute "missing keys" doubly-linked list. The caller
// placement-news a `Missing{}` into a Slot's storage when the slot
// transitions to (or is initialized as) empty, and explicitly destroys it
// before reuse.
struct alignas(8) Missing {
  InternedStringPtr prev;
  InternedStringPtr next;
};

static_assert(sizeof(Missing) == 16, "Missing must be exactly 16 bytes");
static_assert(alignof(Missing) == 8, "Missing must be 8-byte aligned");

// A 16-byte, 8-byte-aligned storage cell. Trivial: no implicit construction
// or destruction. The caller is responsible for placement-new'ing the
// appropriate type into `storage` (either Missing for an empty slot or a
// derived index's SlotT for an occupied slot) and for explicitly destroying
// that object before the slot is reused, freed, or copied.
struct alignas(8) Slot {
  std::byte storage[16];
};

static_assert(sizeof(Slot) == 16, "Slot must be exactly 16 bytes");
static_assert(alignof(Slot) == 8, "Slot must be 8-byte aligned");
static_assert(std::is_trivial_v<Slot>, "Slot must be trivial");

// Reads bit 0 of the slot's first 4 bytes:
//   - occupied SlotT: SlotBase::occupied (1 by construction)
//   - empty slot:     bit 0 of the prev InternedStringPtr (always 0)
inline bool IsOccupied(const Slot& slot) {
  return (*reinterpret_cast<const uint32_t*>(slot.storage) & 1u) != 0;
}

// Per-key trivial struct: 16-byte header + a flexible-array-style run of N
// Slots, one per declared attribute of the schema. ALL construction and
// destruction is the caller's responsibility (typically IndexSchema):
//
//   1. Memory is obtained via KeyAttrValue::Allocate(N) and released via
//      KeyAttrValue::Free(p). Allocate returns uninitialized memory.
//   2. The caller writes `seq`, `document_score`, and `reserved` directly.
//   3. The caller placement-news a `Missing{}` into each `slots[i].storage`
//      before first use (gives a valid empty slot: prev=next=null).
//   4. The caller explicitly destroys the contents of each slot
//      (`Missing` or the index's SlotT) before reusing the slot or freeing
//      the KeyAttrValue.
//
// `N` is NOT stored in the struct — the schema knows the attribute count.
// SlotAt-style bounds checking is not provided; the caller is trusted.
struct alignas(8) KeyAttrValue {
  static constexpr float kDefaultDocumentScore = 1.0f;
  static constexpr size_t kAlignment = 8;

  MutationSequenceNumber seq;
  float document_score;
  uint32_t reserved;  // 4B free space; use before adding to the slot block.
  Slot slots[0];      // FAM; over-allocated by Allocate(num_slots).

  static constexpr size_t AllocSize(uint16_t num_slots) {
    return sizeof(KeyAttrValue) +
           static_cast<size_t>(num_slots) * sizeof(Slot);
  }

  // Memory management — does NOT construct or destruct any members. Allocate
  // returns raw, uninitialized memory; the caller must initialize the header
  // and placement-new every slot's `Missing{}` before use. Free deallocates;
  // the caller must have explicitly destroyed every slot's contents first.
  static KeyAttrValue* Allocate(uint16_t num_slots);
  static void Free(KeyAttrValue* p) noexcept;
};

static_assert(sizeof(KeyAttrValue) == 16,
              "KeyAttrValue header must be exactly 16 bytes");
static_assert(alignof(KeyAttrValue) == KeyAttrValue::kAlignment,
              "KeyAttrValue alignment mismatch");
static_assert(std::is_trivially_default_constructible_v<KeyAttrValue>,
              "KeyAttrValue must be trivially default-constructible");
static_assert(std::is_trivially_destructible_v<KeyAttrValue>,
              "KeyAttrValue must be trivially destructible");
static_assert(std::is_trivially_copyable_v<KeyAttrValue>,
              "KeyAttrValue must be trivially copyable");

// Custom deleter that only frees the underlying buffer via KeyAttrValue::Free.
// Performs NO slot teardown — the caller (IndexSchema::DestroyKeyAttrValue)
// must have run that explicitly before the unique_ptr is reset / destroyed.
struct KeyAttrValueDeleter {
  void operator()(KeyAttrValue* p) const noexcept { KeyAttrValue::Free(p); }
};
using KeyAttrValuePtr = std::unique_ptr<KeyAttrValue, KeyAttrValueDeleter>;

}  // namespace indexes
}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_INDEXES_KEY_ATTR_VALUE_H_
