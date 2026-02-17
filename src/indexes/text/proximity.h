/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_PHRASE_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_PHRASE_H_

#include <vector>

#include "absl/container/inlined_vector.h"
#include "src/indexes/text.h"
#include "src/indexes/text/text_iterator.h"

namespace valkey_search::indexes::text {

/*

Top level iterator for proximity queries (exact phrase / proximity).

ProximityIterator coordinates multiple TextIterators to find documents where
terms appear within a specified distance (slop) and/or inorder.

ProximityIterator Responsibilities:
- Manages a vector of TextIterators (TermIterator, ProximityIterator,
potentially others in the future).
- Performs key-level iteration across all text iterators to find common key.
Relies on the TextIterator contract of NextKey API being in lexical order.
- Performs position-level proximity validation (slop and ordering/overlap
constraints). Relies on the TextIterator contract of NextPosition API being in
asc order.

Note: The construction of ProximityIterator
happens only when all terms in the query are on the same field and also only
when the query involves some positional constraint (inorder or slop). Also, the
ProximityIterator can contain nested ProximityIterators and this happens when
there is a ProximityOR term inside a ProximityAND term.

Example of a simple proximity search: For query `hello worl*` with constraints
of (slop=2, in_order=true):
- TextIterator[0] - TermIterator managing postings iteration of "hello"
- TextIterator[1] - TermIterator managing postings iteration of "worl*"
- ProximityIterator finds positions where wildcard matches appear within
  2 words of allowed slop after "hello" in the same key / document and field.

*/
class ProximityIterator : public TextIterator {
 public:
  ProximityIterator(absl::InlinedVector<std::unique_ptr<TextIterator>,
                                        kProximityTermsInlineCapacity>&& iters,
                    const std::optional<uint32_t> slop, const bool in_order,
                    bool skip_positional_checks);
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
    if (skip_positional_checks_) {
      return current_key_;
    }
    return current_key_ && current_position_.has_value() &&
           current_field_mask_ != 0ULL && query_field_mask_ != 0ULL;
  }

 private:
  // List of all the Text Predicates contained in the Proximity AND.
  absl::InlinedVector<std::unique_ptr<TextIterator>,
                      kProximityTermsInlineCapacity>
      iters_;
  std::optional<uint32_t> slop_;
  bool in_order_;
  FieldMaskPredicate query_field_mask_;
  // Current key/position/field
  Key current_key_;
  std::optional<PositionRange> current_position_;
  FieldMaskPredicate current_field_mask_;
  // Vectors used for positional checks
  absl::InlinedVector<PositionRange, kProximityTermsInlineCapacity> positions_;
  absl::InlinedVector<std::pair<Position, size_t>,
                      kProximityTermsInlineCapacity>
      pos_with_idx_;
  // Flag used to skip positional checks. This is used when performing
  // an AND on text predicates without any positional constraints.
  bool skip_positional_checks_;

  struct ViolationInfo {
    // Iterator index to advance
    size_t iter_idx;
    // Optional target position if seeking is needed
    std::optional<Position> seek_target;
  };

  bool FindCommonKey();
  bool HasOrderingViolation(size_t first_idx, size_t second_idx) const;
  bool IsCompatModeInorder() const;
  std::optional<ViolationInfo> FindViolatingIterator();
};
}  // namespace valkey_search::indexes::text

#endif
