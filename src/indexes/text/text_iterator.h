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

/* Base Class for all Word Iterators. currently this includes WildCard and Fuzzy, more may come in the future.
*/
class TextIterator {
public:
    virtual ~TextIterator() = default;

    // // Word-level iteration
    // virtual bool NextWord() = 0;
    // virtual absl::string_view CurrentWord() = 0;

    // Key-level iteration
    virtual bool DoneKeys() const = 0;
    virtual bool NextKey() = 0;
    virtual const InternedStringPtr& CurrentKey() = 0;

    // Position-level iteration
    virtual bool DonePositions() const = 0;
    virtual bool NextPosition() = 0;
    // TODO: This needs to be updated to return a start and end. std::pair<uint32_t, uint32_t>
    virtual uint32_t CurrentPosition() = 0;
    virtual uint64_t GetFieldMask() const = 0;
};

}  // namespace valkey_search::indexes::text
#endif
