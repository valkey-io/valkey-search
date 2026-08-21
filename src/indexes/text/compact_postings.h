/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_COMPACT_POSTINGS_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_COMPACT_POSTINGS_H_

#include <cstddef>
#include <cstdint>

#include "absl/container/btree_map.h"
#include "src/indexes/text/flat_position_map.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes::text {

// CompactPostings is a space-optimized replacement for the Postings class's
// internal btree_map<Key, FlatPositionMap*>. Following the BagOfInternedStringPtrs
// pattern from PR #1026, it uses a tagged pointer to select between
// representations optimized for the common case (1-4 postings per term):
//
//   storage_ == 0                  -> empty
//   storage_ low 2 bits == 00      -> Single: one (Key, FlatPositionMap*) pair
//                                     stored in a heap-allocated SingleEntry
//   storage_ low 2 bits == 01      -> SmallVec: heap array of up to 4 entries,
//                                     sorted by Key for binary search / iteration
//   storage_ low 2 bits == 10      -> (reserved)
//   storage_ low 2 bits == 11      -> BTreeMap: heap btree_map<Key, FlatPositionMap*>
//                                     for 5+ entries
//
// Space savings for a term with 1 posting:
//   Old: btree_map (56B metadata + node allocation) ~ 100+ bytes
//   New: SingleEntry (16 bytes) + 8 bytes storage_ overhead = 24 bytes effective
//
// For terms with 2-4 postings:
//   Old: btree_map ~ 100-200 bytes
//   New: SmallVec (4 * 16 bytes = 64 bytes) + pointer = ~72 bytes
//
// Invariants:
//   - Entries in SmallVec mode are sorted by Key (for KeyIterator compatibility)
//   - FlatPositionMap ownership: CompactPostings OWNS the FlatPositionMap pointers
//     and is responsible for calling FlatPositionMap::Destroy on removal/destruction
//
class CompactPostings {
 public:
  struct Entry {
    InternedStringPtr key;
    FlatPositionMap* positions;
  };

  using MapType = absl::btree_map<InternedStringPtr, FlatPositionMap*>;

  CompactPostings() = default;
  ~CompactPostings() { Destroy(); }

  CompactPostings(const CompactPostings&) = delete;
  CompactPostings& operator=(const CompactPostings&) = delete;
  CompactPostings(CompactPostings&& other) noexcept : storage_(other.storage_) {
    other.storage_ = 0;
  }
  CompactPostings& operator=(CompactPostings&& other) noexcept {
    if (this != &other) {
      Destroy();
      storage_ = other.storage_;
      other.storage_ = 0;
    }
    return *this;
  }

  bool IsEmpty() const { return storage_ == 0; }
  size_t GetKeyCount() const;

  // Insert a key with its FlatPositionMap. Takes ownership of flat_map.
  // Returns true if newly inserted, false if key already exists (does NOT
  // update the map in that case — caller should RemoveKey first).
  bool InsertKey(const InternedStringPtr& key, FlatPositionMap* flat_map);

  // Remove a key. Returns the FlatPositionMap* that was associated (caller
  // must Destroy it), or nullptr if key was not found.
  FlatPositionMap* RemoveKey(const InternedStringPtr& key);

  // Find the FlatPositionMap for a key, or nullptr if not found.
  FlatPositionMap* Find(const InternedStringPtr& key) const;

  // Iterator for sorted key traversal (needed by TermIterator).
  class KeyIterator {
   public:
    KeyIterator() = default;
    bool IsValid() const;
    void Next();
    bool SkipForward(const InternedStringPtr& key);
    const InternedStringPtr& GetKey() const;
    FlatPositionMap* GetPositions() const;

   private:
    friend class CompactPostings;
    enum class Mode { kEmpty, kSingle, kSmallVec, kMap };

    Mode mode_{Mode::kEmpty};
    // Single mode state
    const Entry* single_entry_{nullptr};
    bool single_consumed_{false};
    // SmallVec mode state
    const Entry* vec_data_{nullptr};
    size_t vec_count_{0};
    size_t vec_idx_{0};
    // Map mode state
    const MapType* map_{nullptr};
    MapType::const_iterator map_iter_;
    MapType::const_iterator map_end_;
  };

  KeyIterator GetKeyIterator() const;

 private:
  static constexpr size_t kSmallVecCap = 4;
  static constexpr uintptr_t kTagMask = 0x3;
  static constexpr uintptr_t kSingleTag = 0;
  static constexpr uintptr_t kSmallVecTag = 1;
  static constexpr uintptr_t kMapTag = 3;

  struct SingleEntry {
    Entry entry;
  };

  struct SmallVec {
    size_t count{0};
    Entry entries[kSmallVecCap];
    SmallVec() = default;
  };

  uintptr_t storage_ = 0;

  bool IsSingle() const {
    return storage_ != 0 && (storage_ & kTagMask) == kSingleTag;
  }
  bool IsSmallVec() const { return (storage_ & kTagMask) == kSmallVecTag; }
  bool IsMap() const { return (storage_ & kTagMask) == kMapTag; }

  SingleEntry* GetSingle() const {
    return reinterpret_cast<SingleEntry*>(storage_ & ~kTagMask);
  }
  SmallVec* GetSmallVec() const {
    return reinterpret_cast<SmallVec*>(storage_ & ~kTagMask);
  }
  MapType* GetMap() const {
    return reinterpret_cast<MapType*>(storage_ & ~kTagMask);
  }

  void SetSingle(SingleEntry* p) {
    storage_ = reinterpret_cast<uintptr_t>(p) | kSingleTag;
  }
  void SetSmallVec(SmallVec* p) {
    storage_ = reinterpret_cast<uintptr_t>(p) | kSmallVecTag;
  }
  void SetMap(MapType* p) {
    storage_ = reinterpret_cast<uintptr_t>(p) | kMapTag;
  }

  // Find position in sorted SmallVec where key should be (lower_bound).
  static size_t SmallVecLowerBound(const SmallVec* vec,
                                   const InternedStringPtr& key);

  // Promote SmallVec -> Map when SmallVec is full and a new key arrives.
  void PromoteToMap(const InternedStringPtr& key, FlatPositionMap* flat_map);

  // Demote Map -> SmallVec when map size drops to kSmallVecCap.
  void DemoteToSmallVec();

  // Destroy all owned FlatPositionMaps and free heap memory.
  void Destroy();
};

static_assert(sizeof(CompactPostings) == 8,
              "CompactPostings must be exactly 8 bytes");

}  // namespace valkey_search::indexes::text

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_COMPACT_POSTINGS_H_
