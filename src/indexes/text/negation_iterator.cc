/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/negation_iterator.h"

#include "absl/log/check.h"
#include "vmsdk/src/log.h"

namespace valkey_search::indexes::text {

NegationTextIterator::NegationTextIterator(
    std::unique_ptr<TextIterator> positive_iterator,
    const InternedStringSet* tracked_keys,
    const InternedStringSet* untracked_keys,
    FieldMaskPredicate query_field_mask)
    : positive_iterator_(std::move(positive_iterator)),
      tracked_keys_(tracked_keys),
      untracked_keys_(untracked_keys),
      query_field_mask_(query_field_mask) {
  // Collect all keys that matched the positive query
  if (positive_iterator_) {
    while (!positive_iterator_->DoneKeys()) {
      matched_keys_.insert(positive_iterator_->CurrentKey());
      positive_iterator_->NextKey();
    }
  }
  // Prime to first negated key
  NextKey();
}

FieldMaskPredicate NegationTextIterator::QueryFieldMask() const {
  return query_field_mask_;
}

bool NegationTextIterator::DoneKeys() const { return !current_key_; }

const Key& NegationTextIterator::CurrentKey() const {
  CHECK(current_key_) << "CurrentKey called when iterator is done";
  return current_key_;
}

bool NegationTextIterator::NextKey() {
  // Phase 1: Iterate tracked_keys
  if (tracked_keys_ && !tracked_iter_.has_value()) {
    tracked_iter_ = tracked_keys_->begin();
  }

  if (tracked_keys_ && tracked_iter_.has_value()) {
    while (tracked_iter_.value() != tracked_keys_->end()) {
      const auto& key = *tracked_iter_.value();
      ++tracked_iter_.value();  // Increment BEFORE checking
      // Skip if this key matched the positive query
      if (!matched_keys_.contains(key)) {
        current_key_ = key;
        return true;
      }
    }
  }

  // Phase 2: Iterate untracked_keys
  if (untracked_keys_ && !untracked_iter_.has_value()) {
    untracked_iter_ = untracked_keys_->begin();
  }

  if (untracked_keys_ && untracked_iter_.has_value()) {
    if (untracked_iter_.value() != untracked_keys_->end()) {
      current_key_ = *untracked_iter_.value();
      ++untracked_iter_.value();
      return true;
    }
  }

  current_key_ = InternedStringPtr();
  return false;
}

bool NegationTextIterator::SeekForwardKey(const Key& target_key) {
  CHECK(false) << "SeekForwardKey not supported for negation queries";
  return false;
}

// Position-related methods - not supported for negation queries
bool NegationTextIterator::DonePositions() const {
  return true;  // Negation doesn't track positions
}

const PositionRange& NegationTextIterator::CurrentPosition() const {
  CHECK(false) << "Positions not supported for negation queries";
  static PositionRange dummy{0, 0};
  return dummy;
}

bool NegationTextIterator::NextPosition() {
  return false;  // No positions for negation
}

FieldMaskPredicate NegationTextIterator::CurrentFieldMask() const {
  return query_field_mask_;  // All matched fields
}

bool NegationTextIterator::IsIteratorValid() const {
  return static_cast<bool>(current_key_);
}

}  // namespace valkey_search::indexes::text
