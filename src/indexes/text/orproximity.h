/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_ORPROXIMITY_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_ORPROXIMITY_H_

#include <set>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "src/indexes/text.h"
#include "src/indexes/text/text_iterator.h"

namespace valkey_search::indexes::text {

/*
OrProximityIterator implements OR semantics for proximity queries.

Unlike ProximityIterator which requires all terms to be present (AND semantics),
OrProximityIterator returns results when any term is present (OR semantics).

Key differences from ProximityIterator:
1. NextKey finds the smallest key amongst all iterators (not common key)
2. NextPosition returns next smallest position for all iterators on same key
3. No proximity validation - just returns positions in order
*/
class OrProximityIterator : public TextIterator {
 public:
  OrProximityIterator(
      absl::InlinedVector<std::unique_ptr<TextIterator>,
                          kProximityTermsInlineCapacity>&& iters,
      const InternedStringSet* untracked_keys = nullptr);

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
  bool IsIteratorValid() const override;

 private:
  absl::InlinedVector<std::unique_ptr<TextIterator>,
                      kProximityTermsInlineCapacity>
      iters_;
  Key current_key_;
  std::optional<PositionRange> current_position_;
  FieldMaskPredicate current_field_mask_;
  FieldMaskPredicate query_field_mask_;
  const InternedStringSet* untracked_keys_;

  // Multiset for efficient key management
  std::multiset<std::pair<Key, size_t>> key_set_;
  // Current iterators on same key
  absl::InlinedVector<size_t, kProximityTermsInlineCapacity>
      current_key_indices_;
  // Multiset for position optimization (supports future SeekForwardPosition)
  std::multiset<std::pair<Position, size_t>> pos_set_;
  absl::InlinedVector<size_t, kProximityTermsInlineCapacity>
      current_pos_indices_;

  void InsertValidKeyIterator(size_t idx);
  bool FindMinimumKey();
  void InsertValidPositionIterator(size_t idx);
};

}  // namespace valkey_search::indexes::text

#endif
