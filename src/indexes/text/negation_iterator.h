/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_NEGATION_ITERATOR_H_
#define VALKEY_SEARCH_INDEXES_TEXT_NEGATION_ITERATOR_H_

#include <memory>
#include <optional>

#include "src/indexes/text/text_iterator.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes::text {

/*
 * NegationIterator wraps a TextIterator and returns all keys that do NOT
 * match the wrapped iterator's criteria.
 *
 * It iterates through schema_tracked_keys_, excluding keys present in the
 * wrapped iterator, then iterates through schema_untracked_keys_.
 *
 * Position iteration returns dummy positions since negation doesn't require
 * positional matching.
 */
class NegationIterator : public TextIterator {
 public:
  NegationIterator(std::unique_ptr<TextIterator> wrapped_iter,
                   const InternedStringSet* schema_tracked_keys,
                   const InternedStringSet* schema_untracked_keys,
                   FieldMaskPredicate query_field_mask);

  // TextIterator interface
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

  bool IsIteratorValid() const override;

 private:
  // OLD: Advance to next valid key - no longer needed with sorted vector
  // void AdvanceToNextValidKey();

  // Build set of keys that match the wrapped iterator
  void BuildMatchedKeysSet();

  std::unique_ptr<TextIterator> wrapped_iter_;
  const InternedStringSet* schema_tracked_keys_;
  const InternedStringSet* schema_untracked_keys_;
  FieldMaskPredicate query_field_mask_;

  // Set of keys that match the wrapped iterator (to exclude)
  // Store as string values for correct comparison across different InternedStringPtr instances
  absl::flat_hash_set<std::string> matched_keys_;

  // Sorted vector of negated keys for deterministic, lexically-ordered iteration
  // Required by SeekForwardKey which assumes sorted order
  std::vector<Key> sorted_negated_keys_;
  size_t current_index_;

  // Current key being returned
  Key current_key_;

  // Dummy position for negation (negation doesn't need real positions)
  PositionRange dummy_position_{0, 0};
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_NEGATION_ITERATOR_H_
