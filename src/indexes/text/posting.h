#ifndef _VALKEY_SEARCH_INDEXES_TEXT_POSTING_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_POSTING_H_

/*

For each entry in the inverted term index, there is an instance of
this structure which is used to contain the key/position information for each
word. It is expected that there will be a very large number of these objects
most of which will have only a small nmber of key/position entries. However,
there will be a small number of instances where the number of key/position
entries is quite large. Thus it's likely that the fully optimized version of
this object will have two or more encodings for its contents. This optimization
is hidden from view.

This object is NOT multi-thread safe, it's expected that the caller performs
locking for mutation operations.

Conceptually, this object holds an ordered list of Keys and for each Key there is
an ordered list of Positions.

A KeyIterator is provided to iterate over the keys within this object.
A PositionIterator is provided to iterate over the positions of an individual Key.

*/

#include "src/indexes/text/lexer.h"
#include "src/text/text.h"

namespace valkey_search::text {

struct Postings : public std::enable_shared_from_this<Postings> {
  struct KeyIterator;
  struct PositionIterator;
  // Construct a posting. If save_positions is off, then any keys that
  // are inserted have an assumed single position of 0.
  Postings(bool save_positions);

  // Are there any postings in this object?
  bool IsEmpty() const;

  // Add a posting
  void SetKey(const Key& key, std::span<Position> positions);

  // Remove a key and all positions for it
  void RemoveKey(const Key& key);

  // Total number of keys
  size_t GetKeyCount() const;

  // Total number of postings for all keys
  size_t GetPostingCount() const;

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
  struct KeyIterator {
    // Is valid?
    bool IsValid() const;

    // Advance to next key
    void NextPosition();

    // Skip forward to next position that is equal to or greater than.
    // return true if it lands on an equal position, false otherwise.
    bool SkipForwardPosition(const Position& position);

    // Get Current Position
    const Position& GetPosition() const;
  };

};

}  // namespace valkey_search::text

#endif