/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_INDEX_BASE_H
#define VALKEYSEARCH_SRC_INDEXES_INDEX_BASE_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/key_attr_value.h"
#include "src/rdb_serialization.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
class IndexSchema;  // Forward declaration for the back-pointer on IndexBase.
}  // namespace valkey_search

namespace valkey_search::indexes {
enum class IndexerType { kHNSW, kFlat, kNumeric, kTag, kVector, kNone, kText };

enum class DeletionType {
  kRecord,      // The record was deleted from the index.
  kIdentifier,  // One or more fields of the record were deleted.
  kNone         // No deletion occurred.
};

const absl::NoDestructor<absl::flat_hash_map<absl::string_view, IndexerType>>
    kIndexerTypeByStr({{"VECTOR", IndexerType::kVector},
                       {"TAG", IndexerType::kTag},
                       {"NUMERIC", IndexerType::kNumeric},
                       {"TEXT", IndexerType::kText}});

class IndexBase {
 public:
  explicit IndexBase(IndexerType indexer_type) : indexer_type_(indexer_type) {}
  virtual ~IndexBase() = default;

  // Add/Remove/Modify will return true if the operation was successful, false
  // if it was skipped.  Returns an error status if there is an unexpected
  // failure.
  virtual absl::StatusOr<bool> AddRecord(const InternedStringPtr& key,
                                         absl::string_view data) = 0;
  virtual absl::StatusOr<bool> RemoveRecord(const InternedStringPtr& key,
                                            DeletionType deletion_type) = 0;
  virtual absl::StatusOr<bool> ModifyRecord(const InternedStringPtr& key,
                                            absl::string_view data) = 0;
  virtual int RespondWithInfo(ValkeyModuleCtx* ctx) const = 0;
  IndexerType GetIndexerType() const { return indexer_type_; }
  virtual absl::Status SaveIndex(RDBChunkOutputStream chunked_out) const = 0;

  virtual std::unique_ptr<data_model::Index> ToProto() const = 0;

  virtual size_t GetTrackedKeyCount() const = 0;
  virtual size_t GetUnTrackedKeyCount() const = 0;
  virtual bool IsTracked(const InternedStringPtr& key) const = 0;
  virtual bool IsUnTracked(const InternedStringPtr& key) const = 0;
  virtual void UnTrack(const InternedStringPtr& key) = 0;
  virtual absl::Status ForEachTrackedKey(
      absl::AnyInvocable<absl::Status(const InternedStringPtr&)> fn) const = 0;
  virtual absl::Status ForEachUnTrackedKey(
      absl::AnyInvocable<absl::Status(const InternedStringPtr&)> fn) const = 0;

  virtual vmsdk::UniqueValkeyString NormalizeStringRecord(
      vmsdk::UniqueValkeyString input) const {
    return input;
  }

  /// Returns the mutation weight for this index type
  virtual uint32_t GetMutationWeight() const = 0;

  // ---- Slot-based storage hooks (KeyAttrValue refactor) ----
  //
  // Bind this index to its owning schema and its assigned attribute position.
  // Called once by IndexSchema::AddIndex after the position is allocated.
  void BindSlot(IndexSchema* schema, uint16_t pos) {
    schema_ = schema;
    pos_ = pos;
  }

  uint16_t SlotPos() const { return pos_; }
  bool IsBound() const { return schema_ != nullptr; }
  IndexSchema* GetSchema() const { return schema_; }

  // Whole-key-delete safety net: invoked by IndexSchema::DestroyKeyAttrValue
  // when it finds a still-occupied slot for this index. Derived classes that
  // have been migrated to slot-based storage override this to forward to
  // their typed DestructTyped (which removes from aux structures, frees any
  // heap state, and destroys the SlotT in place). Unmigrated derived classes
  // inherit the CHECK-fail default — they shouldn't ever reach this path
  // because they keep per-key state in their own containers.
  virtual void DestructSlot(const InternedStringPtr& /*key*/,
                            std::byte* /*storage*/) {
    CHECK(false)
        << "IndexBase::DestructSlot called on an index that has not been "
           "migrated to slot-based storage";
  }

  // Bytes of indexed user-data this index holds for the given KeyAttrValue.
  // Reads SlotBase at the fixed offset 0 of the slot; no SlotT awareness.
  // Returns 0 for an empty slot.
  size_t GetKeyFieldDataSize(const KeyAttrValue& kav) const {
    const Slot& slot = kav.slots[pos_];
    if (!IsOccupied(slot)) {
      return 0;
    }
    return reinterpret_cast<const SlotBase*>(slot.storage)->user_data_len;
  }

 protected:
  // Set by BindSlot; nullptr / 0 until then. Only the slot-based code paths
  // dereference these — the legacy per-index containers don't need them.
  IndexSchema* schema_{nullptr};
  uint16_t pos_{0};

 private:
  IndexerType indexer_type_{IndexerType::kNone};
};

// Iterator over a per-attribute missing-keys doubly-linked list. Construct
// via `MissingListBegin(schema, pos)`. Caller arranges the read-phase
// invariant — negation queries that walk the list run in the time-sliced
// mutex's read phase so no writer mutates Missing.next while we step.
// (IndexSchema is forward-declared in the outer `valkey_search` namespace
// above; unqualified name lookup finds it from inside this namespace.)
class MissingListIterator {
 public:
  MissingListIterator(const IndexSchema* schema, uint16_t pos,
                      InternedStringPtr start);
  bool Done() const;
  const InternedStringPtr& Key() const;
  void Next();

 private:
  const IndexSchema* schema_;
  uint16_t pos_;
  InternedStringPtr current_;
};

MissingListIterator MissingListBegin(const IndexSchema* schema, uint16_t pos);

// CRTP intermediate that gives migrated indexes typed slot access plus
// compile-time fit checks. Derived classes inherit `TypedIndex<Self, SlotT>`,
// define their local SlotT (which must derive SlotBase and fit in 16 bytes),
// and implement `DestructTyped(key, slot)`. The body of `DestructSlot`
// (declared on IndexBase) is provided here so derived classes get the
// type-erased entry point for free.
//
// The slot transition helpers (OccupySlot / ResizeSlot / VacateSlot) are
// declared on this template; their bodies live in `typed_index.cc` and the
// template is explicitly instantiated there for each concrete (Derived,
// SlotT) combination. Derived index .cc files don't need to include any
// extra header — they just call these helpers and the linker resolves them
// against the explicit instantiations.
template <typename Derived, typename SlotT>
class TypedIndex : public IndexBase {
  static_assert(std::is_base_of_v<SlotBase, SlotT>,
                "TypedIndex SlotT must derive SlotBase");
  static_assert(sizeof(SlotT) <= sizeof(Slot),
                "TypedIndex SlotT must fit in 16 bytes");
  static_assert(alignof(SlotT) <= alignof(Slot),
                "TypedIndex SlotT must not be over-aligned");

 public:
  using IndexBase::IndexBase;

 protected:
  // Locally-available typed accessor — the only sanctioned way to view slot
  // bytes as the derived index's SlotT.
  static SlotT& SlotAs(std::byte* storage) {
    return *std::launder(reinterpret_cast<SlotT*>(storage));
  }
  static const SlotT& SlotAs(const std::byte* storage) {
    return *std::launder(reinterpret_cast<const SlotT*>(storage));
  }

  // Derived classes implement to remove this slot's key from the aux
  // structure and free any per-slot heap state (e.g. Tag's TagInfo*). It
  // does NOT touch the slot bytes — those are overwritten when VacateSlot
  // placement-news a fresh Missing on top.
  virtual void DestructTyped(const InternedStringPtr& key, SlotT& slot) = 0;

  // Slot-transition helpers. Bodies live in typed_index_inl.h so derived
  // index .cc files (which include that header) get the IndexSchema-aware
  // template instantiation. All three require the caller to hold this
  // index's `index_mutex_`.
  //
  // OccupySlot — slot must be empty. Unlinks from the missing list if
  // currently linked, destroys the empty Missing payload, bumps the
  // occupied count. Returns the storage pointer so the caller can
  // placement-new the SlotT (which writes SlotBase{occupied=1,
  // user_data_len=data_len} at offset 0).
  std::byte* OccupySlot(const InternedStringPtr& key, size_t data_len);

  // ResizeSlot — slot must be occupied. Rewrites SlotBase::user_data_len
  // in place; the `occupied` bit stays 1. Used by ModifyRecord when the
  // payload size changes but the type still parses.
  void ResizeSlot(const InternedStringPtr& key, size_t new_len);

  // VacateSlot — slot must be occupied. Caller has already invoked
  // DestructTyped (or otherwise released the SlotT's heap state).
  // Decrements the occupied count, placement-news a fresh empty Missing
  // into storage, and if `relink` is true splices the key into the
  // missing list.
  void VacateSlot(const InternedStringPtr& key, bool relink);

 public:
  void DestructSlot(const InternedStringPtr& key, std::byte* storage) override {
    DestructTyped(key, SlotAs(storage));
  }
};

class EntriesFetcherIteratorBase {
 public:
  virtual bool Done() const = 0;
  virtual void Next() = 0;
  virtual const InternedStringPtr& operator*() const = 0;
  virtual ~EntriesFetcherIteratorBase() = default;
};

class EntriesFetcherBase {
 public:
  virtual size_t Size() const = 0;
  virtual ~EntriesFetcherBase() = default;
  virtual std::unique_ptr<EntriesFetcherIteratorBase> Begin() = 0;
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_INDEX_BASE_H
