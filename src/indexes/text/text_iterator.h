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

/* Base Class for all Text Search Iterators. */
class TextIterator {
public:
    virtual ~TextIterator() = default;

    // Key-level iteration
    virtual bool DoneKeys() const = 0;
    virtual bool NextKey() = 0;
    virtual const InternedStringPtr& CurrentKey() = 0;

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
