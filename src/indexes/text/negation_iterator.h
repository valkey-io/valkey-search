/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_NEGATION_ITERATOR_H_
#define VALKEY_SEARCH_INDEXES_TEXT_NEGATION_ITERATOR_H_

#include <memory>

#include "src/indexes/text/text_iterator.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes::text {

/*
 * Iterator for negated text queries.
 * Returns: (schema_tracked - matched) ∪ schema_untracked
 *
 * If positive_iterator is null, matched set is empty (returns all keys).
 */
class NegationTextIterator : public TextIterator {
 public:
  NegationTextIterator(std::unique_ptr<TextIterator> positive_iterator,
                       const InternedStringSet& schema_tracked_keys,
                       const InternedStringSet& schema_untracked_keys,
                       FieldMaskPredicate query_field_mask);

  FieldMaskPredicate QueryFieldMask() const override;
  bool DoneKeys() const override;
  const Key& CurrentKey() const override;
  bool NextKey() override;
  bool SeekForwardKey(const Key& target_key) override;
  bool DonePositions() const override;
  const PositionRange& CurrentPosition() const override;
  bool NextPosition() override;
  FieldMaskPredicate CurrentFieldMask() const override;
  bool IsIteratorValid() const override;

 private:
  enum Phase { TRACKED, UNTRACKED, DONE };

  const InternedStringSet& schema_tracked_keys_;
  const InternedStringSet& schema_untracked_keys_;
  InternedStringSet matched_keys_;
  Phase phase_;
  InternedStringSet::const_iterator tracked_iter_;
  InternedStringSet::const_iterator untracked_iter_;
  FieldMaskPredicate query_field_mask_;
  PositionRange dummy_position_{0, 0};
  bool positions_exhausted_ = false;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_NEGATION_ITERATOR_H_
