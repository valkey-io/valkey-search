/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_TERM_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_TERM_H_

#include <queue>
#include <set>

#include "absl/container/inlined_vector.h"
#include "src/indexes/text.h"
#include "src/indexes/text/flat_position_map.h"
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
  TermIterator(
      absl::InlinedVector<Postings::KeyIterator, kWordExpansionInlineCapacity>&&
          key_iterators,
      const FieldMaskPredicate query_field_mask, const bool require_positions,
      const FieldMaskPredicate stem_field_mask = 0, bool has_original = false);
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
  bool SeekForwardPosition(Position target_position) override;
  FieldMaskPredicate CurrentFieldMask() const override;
  // Returns true if iterator is at a valid state with current key, position,
  // and field.
  bool IsIteratorValid() const override {
    if (require_positions_) {
      return current_key_ && current_position_.has_value() &&
             current_field_mask_ != 0ULL;
    }
    return current_key_ ? true : false;
  }
  /* Implementation of APIs unique to TermIterator */
  // It is possible to implement a `CurrentKeyIterVecIdx` API that returns the
  // index of the vector of the posting iterator (provided on init) that matches
  // the current position

 private:
  const FieldMaskPredicate query_field_mask_;
  const FieldMaskPredicate stem_field_mask_;
  absl::InlinedVector<Postings::KeyIterator, kWordExpansionInlineCapacity>
      key_iterators_;
  absl::InlinedVector<PositionIterator, kWordExpansionInlineCapacity>
      pos_iterators_;
  Key current_key_;
  std::optional<PositionRange> current_position_;
  FieldMaskPredicate current_field_mask_;
  const bool require_positions_;
  const bool has_original_;

  // Pending queue: valid iterators not currently being processed, sorted by key
  std::multiset<std::pair<Key, size_t>> key_set_;
  // Min-heap for position iteration within current key
  std::priority_queue<std::pair<uint32_t, size_t>,
                      std::vector<std::pair<uint32_t, size_t>>, std::greater<>>
      pos_heap_;
  // Indices of iterators at current_key_ (active, not in key_set_)
  std::vector<size_t> current_key_indices_;
  // Indices of iterators at current_position_ (active, not in pos_heap_)
  std::vector<size_t> current_pos_indices_;

  bool FindMinimumValidKey();
  void InsertValidKeyIterator(
      size_t idx, std::optional<InternedStringPtr> min_key = std::nullopt);
  void InsertValidPositionIterator(size_t idx);
};

}  // namespace valkey_search::indexes::text

#endif
