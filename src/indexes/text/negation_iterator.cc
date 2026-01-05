/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/negation_iterator.h"

#include "absl/log/check.h"

namespace valkey_search::indexes::text {

NegationTextIterator::NegationTextIterator(
    std::unique_ptr<TextIterator> positive_iterator,
    const InternedStringSet& schema_tracked_keys,
    const InternedStringSet& schema_untracked_keys,
    FieldMaskPredicate query_field_mask)
    : schema_tracked_keys_(schema_tracked_keys),
      schema_untracked_keys_(schema_untracked_keys),
      query_field_mask_(query_field_mask),
      tracked_iter_(schema_tracked_keys.begin()),
      untracked_iter_(schema_untracked_keys.begin()) {
  if (positive_iterator) {
    matched_keys_.reserve(schema_tracked_keys.size());
    while (!positive_iterator->DoneKeys()) {
      matched_keys_.insert(positive_iterator->CurrentKey());
      positive_iterator->NextKey();
    }
  }
}

FieldMaskPredicate NegationTextIterator::QueryFieldMask() const {
  return query_field_mask_;
}

bool NegationTextIterator::DoneKeys() const {
  return !IsTrackedPhase() && !IsUntrackedPhase();
}

const Key& NegationTextIterator::CurrentKey() const {
  CHECK(!DoneKeys());
  return IsTrackedPhase() ? *tracked_iter_ : *untracked_iter_;
}

bool NegationTextIterator::NextKey() {
  positions_exhausted_ = false;

  if (!initialized_) {
    initialized_ = true;
    // First call: skip matched without advancing
    while (IsTrackedPhase() && matched_keys_.contains(*tracked_iter_)) {
      ++tracked_iter_;
    }
    return !DoneKeys();
  }

  // Subsequent calls: advance then skip
  if (IsTrackedPhase()) {
    ++tracked_iter_;
  } else if (IsUntrackedPhase()) {
    ++untracked_iter_;
  } else {
    return false;
  }

  // Skip matched keys in tracked phase
  while (IsTrackedPhase() && matched_keys_.contains(*tracked_iter_)) {
    ++tracked_iter_;
  }

  return !DoneKeys();
}

bool NegationTextIterator::SeekForwardKey(const Key& target_key) {
  positions_exhausted_ = false;

  while (!DoneKeys() && CurrentKey() < target_key) {
    NextKey();
  }

  return !DoneKeys();
}

bool NegationTextIterator::DonePositions() const {
  return positions_exhausted_;
}

const PositionRange& NegationTextIterator::CurrentPosition() const {
  CHECK(!positions_exhausted_);
  return dummy_position_;
}

bool NegationTextIterator::NextPosition() {
  if (positions_exhausted_) return false;
  positions_exhausted_ = true;
  return false;
}

FieldMaskPredicate NegationTextIterator::CurrentFieldMask() const {
  return query_field_mask_;
}

bool NegationTextIterator::IsIteratorValid() const { return !DoneKeys(); }

}  // namespace valkey_search::indexes::text
