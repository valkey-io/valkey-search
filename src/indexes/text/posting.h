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
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "src/index_schema.h"

namespace valkey_search::text {

// Will remove later when lexer and text are implemented so that posting_test.cc works 
// Basic type definitions needed for posting system
using Key = std::string;
using Position = uint32_t;


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
  explicit Postings(const valkey_search::IndexSchema& index_schema);
  
  // Destructor
  ~Postings();

  // Are there any postings in this object?
  bool IsEmpty() const;

  // Insert a posting entry for a key and field
  // If save_positions=false: Only key and field are stored (position ignored if provided)
  // If save_positions=true: Key, position, and field are stored (position must be provided)
  void InsertPosting(const Key& key, size_t field_index, Position position = UINT32_MAX);

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

  private:
    friend struct Postings;
    
    // Internal implementation details
    void* impl_data_;
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
    
    // Internal implementation details
    void* impl_data_;
  };

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace valkey_search::text

#endif
