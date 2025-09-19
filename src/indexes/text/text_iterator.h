/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_WORD_ITERATOR_H_
#define VALKEY_SEARCH_INDEXES_TEXT_WORD_ITERATOR_H_

#include <cstddef>
#include "src/utils/string_interning.h"

namespace valkey_search::indexes::text {

/* Base Class for all Text Search Iterators. 
 * If (!DoneKeys) {
 *   auto key = CurrentKey();
 *   NextKey();
 * }
 */
class TextIterator {
public:
    virtual ~TextIterator() = default;

    // Key-level iteration
    // Returns true if there are more keys left to search.
    // It does not guarantee whether there will be a match wrt constraints
    // (e.g. field, position, inorder, slop, etc). This is just an indication that it is
    // safe to call `NextKey()`.
    // Returns false if we have exhausted all keys, and there are no more search. In this case
    // no more calls should be made to `NextKey()`.

    // Returns true if there is a match. Use `CurrentKey()` to access the matching document.
    // Returns false otherwise. When false is returned, `CurrentKey()` should no longer be accessed.
    virtual bool DoneKeys() const = 0;
    // Returns the current key.
    // PRECONDITION: !DoneKeys
    virtual const InternedStringPtr& CurrentKey() = 0;
    // Advances the key iteration until there is a match OR until we have exhausted all keys.
    // Returns true when there is a match wrt constraints (e.g. field, position, inorder, slop, etc).
    // Returns false otherwise. When false is returned, `CurrentKey()` should no longer be accessed.
    virtual bool NextKey() = 0;


    // Position-level iteration
    virtual bool DonePositions() const = 0;
    virtual bool NextPosition() = 0;
    // Represents start and end. start == end in all TextIterators except for the OR Proximity
    // since it can contain a nested proximity block.
    virtual std::pair<uint32_t, uint32_t> CurrentPosition() = 0;
    virtual uint64_t CurrentFieldMask() const = 0;
};

}  // namespace valkey_search::indexes::text
#endif
