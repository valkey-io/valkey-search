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
      phase_(TRACKED),
      tracked_iter_(schema_tracked_keys.begin()),
      untracked_iter_(schema_untracked_keys.begin()) {
  if (positive_iterator) {
    matched_keys_.reserve(schema_tracked_keys.size());
    while (!positive_iterator->DoneKeys()) {
      matched_keys_.insert(positive_iterator->CurrentKey());
      positive_iterator->NextKey();
    }
  }

  while (tracked_iter_ != schema_tracked_keys_.end() &&
         matched_keys_.contains(*tracked_iter_)) {
    ++tracked_iter_;
  }

  if (tracked_iter_ == schema_tracked_keys_.end()) {
    phase_ =
        (untracked_iter_ != schema_untracked_keys_.end()) ? UNTRACKED : DONE;
  }
}

FieldMaskPredicate NegationTextIterator::QueryFieldMask() const {
  return query_field_mask_;
}

bool NegationTextIterator::DoneKeys() const { return phase_ == DONE; }

const Key& NegationTextIterator::CurrentKey() const {
  CHECK(phase_ != DONE);
  return (phase_ == TRACKED) ? *tracked_iter_ : *untracked_iter_;
}

bool NegationTextIterator::NextKey() {
  positions_exhausted_ = false;

  if (phase_ == TRACKED) {
    do {
      ++tracked_iter_;
      if (tracked_iter_ == schema_tracked_keys_.end()) {
        phase_ = (untracked_iter_ != schema_untracked_keys_.end()) ? UNTRACKED
                                                                   : DONE;
        return (phase_ == UNTRACKED);
      }
    } while (matched_keys_.contains(*tracked_iter_));
    return true;
  }

  if (phase_ == UNTRACKED) {
    ++untracked_iter_;
    if (untracked_iter_ == schema_untracked_keys_.end()) {
      phase_ = DONE;
      return false;
    }
    return true;
  }

  return false;
}

bool NegationTextIterator::SeekForwardKey(const Key& target_key) {
  positions_exhausted_ = false;

  while (phase_ != DONE && CurrentKey() < target_key) {
    if (phase_ == TRACKED) {
      do {
        ++tracked_iter_;
        if (tracked_iter_ == schema_tracked_keys_.end()) {
          phase_ = (untracked_iter_ != schema_untracked_keys_.end()) ? UNTRACKED
                                                                     : DONE;
          break;
        }
      } while (matched_keys_.contains(*tracked_iter_));
    } else {
      ++untracked_iter_;
      if (untracked_iter_ == schema_untracked_keys_.end()) {
        phase_ = DONE;
      }
    }
  }

  return (phase_ != DONE);
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

bool NegationTextIterator::IsIteratorValid() const { return (phase_ != DONE); }

}  // namespace valkey_search::indexes::text
