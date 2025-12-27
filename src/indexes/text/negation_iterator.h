/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_NEGATION_ITERATOR_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_NEGATION_ITERATOR_H_

#include <memory>
#include <optional>

#include "src/indexes/text/text_iterator.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes::text {

// NegationTextIterator returns all keys from tracked_keys that are NOT in the
// positive query results, plus all untracked_keys.
// Formula: (tracked_keys - matched_keys) ∪ untracked_keys
class NegationTextIterator : public TextIterator {
 public:
  NegationTextIterator(std::unique_ptr<TextIterator> positive_iterator,
                       const InternedStringSet* tracked_keys,
                       const InternedStringSet* untracked_keys,
                       FieldMaskPredicate query_field_mask);

  // TextIterator implementation
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
  std::unique_ptr<TextIterator> positive_iterator_;
  const InternedStringSet* tracked_keys_;
  const InternedStringSet* untracked_keys_;
  FieldMaskPredicate query_field_mask_;

  // Build set of matched keys for fast lookup
  InternedStringSet matched_keys_;

  // Current iteration state
  std::optional<InternedStringSet::const_iterator> tracked_iter_;
  std::optional<InternedStringSet::const_iterator> untracked_iter_;
  Key current_key_;
};

}  // namespace valkey_search::indexes::text

#endif
