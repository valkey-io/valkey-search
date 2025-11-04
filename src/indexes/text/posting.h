/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_POSTING_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_POSTING_H_

/*

For each entry in the inverted term index, there is an instance of
this structure which is used to contain the key/field/position information for
each word. It is expected that there will be a very large number of these
objects most of which will have only a small number of key/field/position
entries. However, there will be a small number of instances where the number of
key/field/position entries is quite large. Thus it's likely that the fully
optimized version of this object will have two or more encodings for its
contents. This optimization is hidden from external view.

This object is NOT multi-thread safe, it's expected that the caller performs
locking for mutation operations.

Conceptually, this object holds an ordered list of Keys and for each Key there
is an ordered list of Positions. Each position is tagged with a bitmask of
fields.

A KeyIterator is provided to iterate over the keys within this object.
A PositionIterator is provided to iterate over the positions of an individual
Key.

*/

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "src/utils/string_interning.h"

namespace valkey_search::indexes::text {

using Key = InternedStringPtr;
using Position = uint32_t;
using FieldMaskPredicate = uint64_t;

// Field mask interface optimized for different field counts
class FieldMask {
 public:
  static std::unique_ptr<FieldMask> Create(size_t num_fields);
  virtual ~FieldMask() = default;
  virtual void SetField(size_t field_index) = 0;
  virtual void ClearField(size_t field_index) = 0;
  virtual bool HasField(size_t field_index) const = 0;
  virtual void SetAllFields() = 0;
  virtual void ClearAllFields() = 0;
  virtual size_t CountSetFields() const = 0;
  virtual uint64_t AsUint64() const = 0;
  virtual size_t MaxFields() const = 0;
};

using PositionMap = std::map<Position, std::unique_ptr<FieldMask>>;

//
// this is the logical view of a posting.
//
struct Posting {
  const Key& GetKey() const;
  uint64_t GetFieldMask() const;
  uint32_t GetPosition() const;
};

struct Postings {
  struct KeyIterator;
  struct PositionIterator;

  // Are there any postings in this object?
  bool IsEmpty() const;

  // Insert the key with its position map
  void InsertKey(const Key& key, PositionMap&& pos_map);

  // Remove a key and all positions for it
  void RemoveKey(const Key& key);

  // Total number of keys
  size_t GetKeyCount() const;

  // Total number of postings for all keys
  size_t GetPostingCount() const;

  // Total frequency of the term across all keys and positions
  size_t GetTotalTermFrequency() const;

  // Defrag this contents of this object. Returns the updated "this" pointer.
  Postings* Defrag();

  // Get a Key iterator.
  KeyIterator GetKeyIterator() const;

  // The Key Iterator
  struct KeyIterator {
    // Is valid?
    bool IsValid() const;

    // Advance to next key
    void NextKey();

    // Skip forward to next key that is equal to or greater than.
    // return true if it lands on an equal key, false otherwise.
    bool SkipForwardKey(const Key& key);

    // Get Current key
    const Key& GetKey() const;

    // Check if word is present in any of the fields specified by field_mask for
    // current key
    bool ContainsFields(uint64_t field_mask) const;

    // Get Position Iterator
    PositionIterator GetPositionIterator() const;

   private:
    friend struct Postings;

    // Iterator state - pointer to key_to_positions map
    using PositionMap = std::map<Position, std::unique_ptr<class FieldMask>>;
    const std::map<Key, PositionMap>* key_map_;
    std::map<Key, PositionMap>::const_iterator current_;
    std::map<Key, PositionMap>::const_iterator end_;
  };

  // The Position Iterator
  struct PositionIterator {
    // Is valid?
    bool IsValid() const;

    // Advance to next position
    void NextPosition();

    // Skip forward to next position that is equal to or greater than.
    // return true if it lands on an equal position, false otherwise.
    bool SkipForwardPosition(const Position& position);

    // Get Current Position
    const Position& GetPosition() const;

    // Get field mask for current position
    uint64_t GetFieldMask() const;

   private:
    friend struct KeyIterator;

    // Iterator state - pointer to positions map
    using PositionMap = std::map<Position, std::unique_ptr<class FieldMask>>;
    const PositionMap* position_map_;
    PositionMap::const_iterator current_;
    PositionMap::const_iterator end_;
  };

 private:
  std::map<Key, PositionMap> key_to_positions_;
};

}  // namespace valkey_search::indexes::text

#endif
