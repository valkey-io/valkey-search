/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_ITERATOR_H_
#define VALKEY_SEARCH_INDEXES_TEXT_ITERATOR_H_

#include "src/indexes/text/posting.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes::text {

struct PositionRange {
  Position start;
  Position end;
  PositionRange() = default;
  PositionRange(Position s, Position e) : start(s), end(e) {
    CHECK(start <= end);
  }
};

/* Base Class for all Text Search Iterators.
 * This contract holds for both keys and positions.
 * // Initializes the TextIterator and primes to the first match of
 * keys/positions. TextIterator();
 * // Tells the caller site if there was no initial match upon init.
 * // Post init, it tells us whether there are keys remaining that can be
 * searched for.
 *. If (!DoneKeys()) {
 *   // Access the current key match.
 *   auto key = CurrentKey();
 *   // Move to the next key which matches the criteria/s. This can result in
 * moving
 *   // till the end if there are no matches.
 *   NextKey();
 *  }
 */
class TextIterator {
 public:
  virtual ~TextIterator() = default;

  // The field which the iterator was initialized to search for.
  virtual FieldMaskPredicate QueryFieldMask() const = 0;

  // Key-level iteration
  // Returns true if there is a match (i.e. `CurrentKey()` is valid) provided
  // the TextIterator is used as described above. Use `CurrentKey()` to access
  // the matching document. Otherwise, returns false. Returns false if we have
  // exhausted all keys, and there are no more search results. In this case no
  // more calls should be made to `NextKey()`.
  virtual bool DoneKeys() const = 0;
  // Returns the current key.
  // ASSERT: !DoneKeys()
  virtual const Key& CurrentKey() const = 0;
  // Advances the key iteration until there is a match OR until we have
  // exhausted all keys. Returns true when there is a match wrt constraints
  // (e.g. field, position, inorder, slop, etc). Returns false otherwise. When
  // false is returned, `CurrentKey()` should no longer be accessed.
  // Returns false if no key is found. In this case, the DoneKeys and
  // DonePositions APIs will return true.
  // This API  resets the Positions.
  // ASSERT: !DoneKeys()
  virtual bool NextKey() = 0;
  // Seeks forward to the first key >= target_key that matches all constraints.
  // Returns true if such a key is found, false if no more matching keys exist.
  // If current key is already >= target_key, returns true without changing
  // state. This is intended to be used after a previous call to NextKey().
  // Returns false if no key is found. In this case, the DoneKeys and
  // DonePositions APIs will return true.
  // This API resets the Positions.
  // ASSERT: !DoneKeys().
  virtual bool SeekForwardKey(const Key& target_key) = 0;

  // Position-level iteration
  // Returns true if there is a match (i.e. `CurrentPosition()` is valid)
  // provided the TextIterator is used as described above. Use
  // `CurrentPosition()` to access the matching document. Otherwise, returns
  // false. Returns false if we have exhausted all keys, and there are no more
  // search results. In this case no more calls should be made to
  // `NextPosition()`.
  virtual bool DonePositions() const = 0;
  // Returns the current position as a closed interval within a document.
  // Represents start and end. start == end in all TextIterators except for the
  // OR Proximity since it can contain a nested proximity block.
  // ASSERT: !DonePositions()
  virtual const PositionRange& CurrentPosition() const = 0;
  // Moves to the next position match. Returns true if there is one.
  // Otherwise, returns false if we have exhausted all positions.
  // ASSERT: !DonePositions()
  virtual bool NextPosition() = 0;
  // Seeks forward to the first position >= target_position that matches all
  // constraints. Returns true if such a position is found, false if no more
  // matching positions exist. If current position is already >=
  // target_position, returns true without changing state. ASSERT:
  // !DonePositions()
  virtual bool SeekForwardPosition(Position target_position) = 0;
  // Returns the field mask for the current position.
  // ASSERT: !DonePositions()
  virtual FieldMaskPredicate CurrentFieldMask() const = 0;
  // Returns true if iterator is at a valid state (has current key, position,
  // and field)
  virtual bool IsIteratorValid() const = 0;
};

}  // namespace valkey_search::indexes::text
#endif
