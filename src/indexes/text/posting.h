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
this structure which is used to contain the key/field/position information for each
word. It is expected that there will be a very large number of these objects
most of which will have only a small number of key/field/position entries. However,
there will be a small number of instances where the number of key/field/position
entries is quite large. Thus it's likely that the fully optimized version of
this object will have two or more encodings for its contents. This optimization
is hidden from external view.

This object is NOT multi-thread safe, it's expected that the caller performs
locking for mutation operations.

Conceptually, this object holds an ordered list of Keys and for each Key there is
an ordered list of Positions. Each position is tagged with a bitmask of fields.

A KeyIterator is provided to iterate over the keys within this object.
A PositionIterator is provided to iterate over the positions of an individual Key.

*/

// Will add later when lexer and text are implemented so that posting_test.cc works 
// #include "src/indexes/text/lexer.h"
// #include "src/indexes/text/text.h"
#include <span>
#include <memory>
#include <cstdint>
#include <string>

namespace valkey_search::text {

// Will remove later when lexer and text are implemented so that posting_test.cc works 
// Basic type definitions needed for posting system
using Key = std::string;
using Position = uint32_t;

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
  virtual std::unique_ptr<FieldMask> Clone() const = 0;
  virtual size_t MaxFields() const = 0;
};

// Template implementation for field mask with optimized storage
template<typename MaskType, size_t MAX_FIELDS>
class FieldMaskImpl : public FieldMask {
public:
  explicit FieldMaskImpl(size_t num_fields = MAX_FIELDS);
  void SetField(size_t field_index) override;
  void ClearField(size_t field_index) override;
  bool HasField(size_t field_index) const override;
  void SetAllFields() override;
  void ClearAllFields() override;
  size_t CountSetFields() const override;
  uint64_t AsUint64() const override;
  std::unique_ptr<FieldMask> Clone() const override;
  size_t MaxFields() const override { return MAX_FIELDS; }
private:
  MaskType mask_;
  size_t num_fields_;
};

// Optimized implementations for different field counts
using SingleFieldMask = FieldMaskImpl<bool, 1>;
using ByteFieldMask = FieldMaskImpl<uint8_t, 8>;
using Uint64FieldMask = FieldMaskImpl<uint64_t, 64>;

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
  // Construct a posting. If save_positions is off, then any keys that
  // are inserted have an assumed single position of 0.
  // The "num_text_fields" entry identifies how many bits of the field-mask are required
  // and is used to select the representation.
  Postings(bool save_positions, size_t num_text_fields);
  
  // Destructor
  ~Postings();
  
  // Copy constructor and assignment operator
  Postings(const Postings& other);
  Postings& operator=(const Postings& other);

  // Are there any postings in this object?
  bool IsEmpty() const;

  // Add a key for boolean search (save_positions=false mode only)
  // Sets document presence with assumed position 0 and empty field mask
  void SetKey(const Key& key);
  
  // Add a posting entry for a specific position and field
  void AddPositionForField(const Key& key, Position position, size_t field_index);
  
  // Add multiple posting entries for specific positions and fields (replaces existing positions)
  void SetKeyWithFieldPositions(const Key& key, std::span<std::pair<Position, size_t>> position_field_pairs);
  
  // Update key by adding/merging positions with existing ones (preserves existing positions)
  void UpdateKeyWithFieldPositions(const Key& key, std::span<std::pair<Position, size_t>> position_field_pairs);

  // Remove a key and all positions for it
  void RemoveKey(const Key& key);

  // Total number of keys
  size_t GetKeyCount() const;

  // Total number of postings for all keys
  size_t GetPostingCount() const;
  
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
    bool SkipForwardKey(const Key& key);

    // Get Current key
    const Key& GetKey() const;

    // Get Position Iterator
    PositionIterator GetPositionIterator() const;
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
  };

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace valkey_search::text

#endif
