/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_NEGATION_ITERATOR_H_
#define VALKEY_SEARCH_INDEXES_TEXT_NEGATION_ITERATOR_H_

#include <memory>
#include <vector>

#include "src/indexes/text/text_index.h"
#include "src/indexes/text/text_iterator.h"

namespace valkey_search::indexes::text {

// NegationIterator implements document-level negation for text queries.
//
// Formula: negated_results = (tracked_keys - matched_keys) + untracked_keys
// Equivalent to: (total_keys - matched_keys) since matched ⊆ tracked
//
// Returns all documents that DON'T match the wrapped query, in sorted order.
// This is a document-level iterator only - position methods are stubs.
class NegationIterator : public TextIterator {
 public:
  // Constructor: wraps a positive iterator and computes negation
  // Parameters:
  //   wrapped_iter: The positive query iterator to negate
  //   text_index_schema: Schema for accessing tracked/untracked keys
  //   query_field_mask: Field mask from query predicate (for field-level
  //   negation)
  NegationIterator(std::unique_ptr<TextIterator> wrapped_iter,
                   std::shared_ptr<TextIndexSchema> text_index_schema,
                   uint64_t query_field_mask);


  FieldMaskPredicate QueryFieldMask() const override;
  // Key-level functions 
  bool DoneKeys() const override;
  const Key& CurrentKey() const override;
  bool NextKey() override;
  bool SeekForwardKey(const Key& target_key) override;

  // Negation doesn't need positional apis - these return dummy values
  bool DonePositions() const override;
  const PositionRange& CurrentPosition() const override;
  bool NextPosition() override;
  bool SeekForwardPosition(Position target_position) override;

  FieldMaskPredicate CurrentFieldMask() const override;
  bool IsIteratorValid() const override;

 private:
  // Build sorted result vectors in constructor
  // Computes: (tracked_keys - matched_keys) + untracked_keys
  // TODO: Verify thread safety during concurrent query execution
  void BuildResults();

  // The wrapped positive query iterator
  std::unique_ptr<TextIterator> wrapped_iter_;

  // Schema for accessing tracked/untracked keys
  std::shared_ptr<TextIndexSchema> text_index_schema_;

  // Field mask from query predicate (for field-level negation like
  // -@title:term)
  uint64_t query_field_mask_;

  // Sorted result keys: (tracked - matched) + untracked
  std::vector<Key> result_keys_;

  // Current position in result_keys_
  size_t current_offset_ = 0;

  // Dummy position/range for position stubs
  static constexpr Position kDummyPosition = UINT32_MAX;
  PositionRange dummy_range_{kDummyPosition, kDummyPosition};
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_NEGATION_ITERATOR_H_
