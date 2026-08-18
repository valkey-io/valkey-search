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

#include "absl/container/btree_map.h"
#include "src/indexes/text/flat_position_map.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes::text {

// Forward declaration
struct TextIndexMetadata;

using Key = InternedStringPtr;
using Position = uint32_t;
using FieldMaskPredicate = uint64_t;

struct FieldMask {
  // Constructors
  FieldMask() = default;
  explicit FieldMask(size_t num_fields);

  // FieldMask functions
  void SetField(size_t field_index);
  size_t CountSetFields() const;
  uint64_t GetMask() const;

 private:
  uint64_t mask_{0};
  uint8_t num_fields_{0};
};

static_assert(sizeof(FieldMask) == 16, "FieldMask should exactly be 16 bytes");

using PositionMap = absl::btree_map<Position, FieldMask>;

struct Postings {
  struct KeyIterator;

  static void *operator new(size_t size) { return ::operator new(size); }
  static void operator delete(void *ptr, size_t size) {
    ::operator delete(ptr);
  }

  // Destructor: clean up all FlatPositionMaps
  ~Postings();

  // Are there any postings in this object?
  bool IsEmpty() const;

  // Insert the key with FlatPositionMap
  void InsertKey(const Key &key, FlatPositionMap *flat_map);

  // Remove a key and all positions for it
  void RemoveKey(const Key &key, TextIndexMetadata *metadata);

  // Total number of keys
  size_t GetKeyCount() const;

  // Total number of positions for all keys
  size_t GetPositionCount() const;

  // Total frequency of the term across all keys and positions
  size_t GetTotalTermFrequency() const;

  // Defrag this contents of this object. Returns the updated "this" pointer.
  Postings *Defrag();

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
    bool SkipForwardKey(const Key &key);

    // Get Current key
    const Key &GetKey() const;

    // Check if word is present in any of the fields specified by field_mask for
    // current key
    bool ContainsFields(uint64_t field_mask) const;

    // Get Position Iterator
    PositionIterator GetPositionIterator() const;

   private:
    friend struct Postings;

    const Postings *postings_{nullptr};
    size_t byte_offset_{0};
    uint32_t current_doc_id_{0};
    uint64_t current_pos_count_{0};
    size_t next_doc_offset_{0};
    mutable Key current_key_cache_{};
    mutable FlatPositionMap *current_map_cache_{nullptr};
  };

 private:
  std::vector<uint8_t> stream_;
  size_t key_count_{0};
  size_t total_positions_{0};
  size_t total_term_frequency_{0};
};

}  // namespace valkey_search::indexes::text

#endif
