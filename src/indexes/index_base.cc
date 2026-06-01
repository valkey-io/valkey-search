/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

// Bodies for the non-trivial declarations in index_base.h that touch
// IndexSchema: MissingListIterator, MissingListBegin, and the
// TypedIndex<Derived, SlotT> slot-transition helpers. Lives in a .cc
// (not the header) because IndexSchema's header already #includes
// index_base.h — defining these inline would create an include cycle.
// Each concrete (Derived, SlotT) pair is explicitly instantiated at the
// bottom of this file so derived index .cc files don't have to.

#include <cstddef>
#include <cstdint>

#include "absl/log/check.h"
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/indexes/key_attr_value.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/indexes/text.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes {

// ---- MissingListIterator ----

MissingListIterator::MissingListIterator(const IndexSchema* schema,
                                         uint16_t pos,
                                         InternedStringPtr start)
    : schema_(schema), pos_(pos), current_(std::move(start)) {}

bool MissingListIterator::Done() const { return !current_; }

const InternedStringPtr& MissingListIterator::Key() const { return current_; }

void MissingListIterator::Next() {
  // Negation queries that walk the missing list run in the time-sliced
  // mutex's read phase, so writers are excluded and the bytes are stable.
  // WithKAV's brief reader lock keeps the absl annotations honest.
  schema_->WithKAV(current_, [&](const KeyAttrValue* kav) {
    CHECK(kav != nullptr);
    const auto& slot = kav->slots[pos_];
    CHECK(!IsOccupied(slot));
    current_ = reinterpret_cast<const Missing*>(slot.storage)->next;
  });
}

MissingListIterator MissingListBegin(const IndexSchema* schema, uint16_t pos) {
  return MissingListIterator(schema, pos, schema->MissingListAt(pos).head);
}

// ---- TypedIndex slot transition helpers ----
//
// Each derived index calls these inside its AddRecord / ModifyRecord /
// RemoveRecord (and equivalents), holding its own index_mutex_. The helpers
// touch IndexSchema's missing list / occupied_count_ / index_key_info_, all
// of which are serialized by `schema_mutex_` (acquired inside the schema
// helpers we call here).

template <typename Derived, typename SlotT>
std::byte* TypedIndex<Derived, SlotT>::OccupySlot(
    const InternedStringPtr& key, size_t data_len) {
  CHECK(schema_ != nullptr) << "BindSlot not called";
  KeyAttrValue& kav = schema_->EnsureKeyAttrValue(key);
  Slot& slot = kav.slots[pos_];
  CHECK(!IsOccupied(slot))
      << "OccupySlot called on slot that is already occupied";
  if (schema_->IsLinked(pos_, key)) {
    schema_->UnlinkMissing(pos_, key);
  }
  // Slot is empty + unlinked. Destroy the Missing payload; caller will
  // placement-new SlotT into storage. SlotT's first member is SlotBase, so
  // {occupied=1, user_data_len=data_len} is written by the caller via
  // placement-new of SlotT{base{1, data_len}, ...payload...}.
  reinterpret_cast<Missing*>(slot.storage)->~Missing();
  schema_->IncrementOccupiedCount(pos_);
  // user_data_len is set when the caller placement-news the SlotT; no need
  // to write SlotBase here. Just hand back the storage pointer.
  (void)data_len;  // size accounting moves to slot helpers in a later chunk.
  return slot.storage;
}

template <typename Derived, typename SlotT>
void TypedIndex<Derived, SlotT>::ResizeSlot(const InternedStringPtr& key,
                                            size_t new_len) {
  CHECK(schema_ != nullptr) << "BindSlot not called";
  KeyAttrValue* kav = schema_->FindKAV(key);
  CHECK(kav != nullptr);
  Slot& slot = kav->slots[pos_];
  CHECK(IsOccupied(slot)) << "ResizeSlot called on empty slot";
  auto* base = reinterpret_cast<SlotBase*>(slot.storage);
  base->user_data_len = static_cast<uint32_t>(new_len);
}

template <typename Derived, typename SlotT>
void TypedIndex<Derived, SlotT>::VacateSlot(const InternedStringPtr& key,
                                            bool relink) {
  CHECK(schema_ != nullptr) << "BindSlot not called";
  KeyAttrValue* kav = schema_->FindKAV(key);
  CHECK(kav != nullptr);
  Slot& slot = kav->slots[pos_];
  CHECK(IsOccupied(slot)) << "VacateSlot called on empty slot";
  // SlotBase header is still readable at offset 0. The caller has invoked
  // DestructTyped (or otherwise released the SlotT's heap state); we now
  // overwrite the bytes with a fresh empty Missing and optionally link.
  schema_->DecrementOccupiedCount(pos_);
  new (slot.storage) Missing{};
  if (relink) {
    schema_->LinkMissing(pos_, key);
  }
}

// ---- Explicit instantiations ----
//
// Add a line here when a new derived index adopts slot-based storage. The
// linker resolves derived .cc references to OccupySlot/ResizeSlot/VacateSlot
// against these instantiations.
template class TypedIndex<Numeric, NumericSlot>;
template class TypedIndex<Tag, TagSlot>;
template class TypedIndex<Text, TextSlot>;

}  // namespace valkey_search::indexes
