/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_TERM_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_TERM_H_

#include <vector>

#include "src/indexes/text/text_iterator.h"

namespace valkey_search::indexes::text {

/*

Top level iterator for a Term.
This is a union of the postings key iterators and derived position iterators
allowing a single lexically ordered iteration of keys and positions (where the
word/s, based on postings, exist in the key).

TermIterator Responsibilities:
- Manages a vector of posting (key) iterator/s, which operates in lexical order.
- Key iteration (of documents) takes place by advancing the posting iterator who
is on the smallest key until it is on a key whose field matches the field mask
of the search operation. Since multiple posting iterators can have the same key
amd same field, we create a vector of position iterators, one from each posting
iterator who are on the same key & field. Once no more keys are found, DoneKeys
returns true. Through this process, it "merges" multiple posting iterators.
- Position iteration happens across all the position iterators, allowing us to
search for positions in asc order across all the required words within the same
key and same field. Once no more positions are found, DonePositions returns
true. Thus, position iteration is a union of all position iterators obtained
from all the posting iterators that are on the current key and field mask.

*/
class TermIterator : public TextIterator {
 public:
  TermIterator(std::vector<Postings::KeyIterator>&& key_iterators,
               const FieldMaskPredicate field_mask,
               const InternedStringSet* untracked_keys,
               const bool require_positions);
  /* Implementation of TextIterator APIs */
  FieldMaskPredicate QueryFieldMask() const override;
  // Key-level iteration
  bool DoneKeys() const override;
  const Key& CurrentKey() const override;
  bool NextKey() override;
  bool SeekForwardKey(const Key& target_key) override;
  // Position-level iteration
  bool DonePositions() const override;
  const PositionRange& CurrentPosition() const override;
  bool NextPosition() override;
  FieldMaskPredicate CurrentFieldMask() const override;
  /* Implementation of APIs unique to TermIterator */
  // It is possible to implement a `CurrentKeyIterVecIdx` API that returns the
  // index of the vector of the posting iterator (provided on init) that matches
  // the current position

 private:
  const FieldMaskPredicate query_field_mask_;
  std::vector<Postings::KeyIterator> key_iterators_;
  std::vector<Postings::PositionIterator> pos_iterators_;
  Key current_key_;
  std::optional<PositionRange> current_position_;
  FieldMaskPredicate current_field_mask_;
  const InternedStringSet* untracked_keys_;
  const bool require_positions_;
  bool FindMinimumValidKey();
};

}  // namespace valkey_search::indexes::text

#endif
