/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/term.h"

namespace valkey_search::indexes::text {

TermIterator::TermIterator(
    absl::InlinedVector<Postings::KeyIterator, kWordExpansionInlineCapacity>&&
        key_iterators,
    const FieldMaskPredicate query_field_mask,
    const InternedStringSet* untracked_keys, const bool require_positions)
    : query_field_mask_(query_field_mask),
      key_iterators_(std::move(key_iterators)),
      current_position_(std::nullopt),
      current_field_mask_(0ULL),
      untracked_keys_(untracked_keys),
      require_positions_(require_positions) {
  // Prime the first key and position if they exist.
  if (!key_iterators_.empty()) {
    TermIterator::NextKey();
  }
}

FieldMaskPredicate TermIterator::QueryFieldMask() const {
  return query_field_mask_;
}

bool TermIterator::DoneKeys() const {
  for (const auto& key_iter : key_iterators_) {
    if (key_iter.IsValid()) return false;
  }
  return true;
}

const InternedStringPtr& TermIterator::CurrentKey() const {
  CHECK(current_key_);
  return current_key_;
}

bool TermIterator::FindMinimumValidKey() {
  current_key_ = nullptr;
  current_position_ = std::nullopt;
  current_field_mask_ = 0ULL;
  for (auto& key_iter : key_iterators_) {
    while (key_iter.IsValid() && !key_iter.ContainsFields(query_field_mask_)) {
      key_iter.NextKey();
    }
    if (key_iter.IsValid()) {
      const auto& key = key_iter.GetKey();
      // Only populate position iterators if the query requires positions.
      // For example, an index with NOOFFSETS can still support AND
      // intersections of term predicates.
      if (require_positions_) {
        if (!current_key_ || key < current_key_) {
          pos_iterators_.clear();
          pos_iterators_.emplace_back(key_iter.GetPositionIterator());
          current_key_ = key;
        } else if (key == current_key_) {
          pos_iterators_.emplace_back(key_iter.GetPositionIterator());
        }
      } else if (!current_key_ || key < current_key_) {
        current_key_ = key;
      }
    }
  }
  if (!current_key_) {
    return false;
  }
  if (require_positions_) {
    // No need to check the result since we know that at least one position
    // exists based on ContainsFields.
    TermIterator::NextPosition();
  }
  return true;
}

bool TermIterator::NextKey() {
  if (current_key_) {
    for (auto& key_iter : key_iterators_) {
      if (key_iter.IsValid() && key_iter.GetKey() == current_key_) {
        key_iter.NextKey();
      }
    }
  }
  return FindMinimumValidKey();
}

bool TermIterator::SeekForwardKey(const InternedStringPtr& target_key) {
  if (current_key_ && current_key_ >= target_key) {
    return true;
  }
  // Use SkipForwardKey to efficiently seek all key iterators to target_key or
  // beyond
  for (auto& key_iter : key_iterators_) {
    key_iter.SkipForwardKey(target_key);
  }
  return FindMinimumValidKey();
}

bool TermIterator::DonePositions() const {
  for (const auto& pos_iter : pos_iterators_) {
    if (pos_iter.IsValid()) return false;
  }
  return true;
}

const PositionRange& TermIterator::CurrentPosition() const {
  CHECK(current_position_.has_value());
  return current_position_.value();
}

bool TermIterator::NextPosition() {
  if (current_position_.has_value()) {
    for (auto& pos_iter : pos_iterators_) {
      if (pos_iter.IsValid() &&
          pos_iter.GetPosition() == current_position_.value().start) {
        pos_iter.NextPosition();
      }
    }
  }
  uint32_t min_position = UINT32_MAX;
  bool found = false;
  FieldMaskPredicate field;
  for (auto& pos_iter : pos_iterators_) {
    while (pos_iter.IsValid() &&
           !(pos_iter.GetFieldMask() & query_field_mask_)) {
      pos_iter.NextPosition();
    }
    if (pos_iter.IsValid()) {
      uint32_t position = pos_iter.GetPosition();
      if (position < min_position) {
        min_position = position;
        field = pos_iter.GetFieldMask();
        found = true;
      }
    }
  }
  if (!found) {
    current_position_ = std::nullopt;
    current_field_mask_ = 0ULL;
    return false;
  }
  current_position_ = PositionRange{min_position, min_position};
  current_field_mask_ = field;
  return true;
}

bool TermIterator::SeekForwardPosition(Position target_position) {
  if (current_position_.has_value() &&
      current_position_.value().start >= target_position) {
    return true;
  }
  // for (auto& pos_iter : pos_iterators_) {
  //   if (pos_iter.IsValid()) {
  //     pos_iter.SkipForwardPosition(target_position);
  //   }
  // }
  // 2. Individual child guard
  for (auto& pos_iter : pos_iterators_) {
    if (pos_iter.IsValid()) {
      // TRACKING CHECK: Only skip if the target is actually ahead of 
      // this specific child's current internal cumulative position.
      if (target_position > pos_iter.GetPosition()) {
        pos_iter.SkipForwardPosition(target_position);
      }
      // If target_position <= GetPosition(), we do nothing. This is safe 
      // because that child is already at or past the target.
    }
  }
  current_position_ = std::nullopt;
  return NextPosition();
}

FieldMaskPredicate TermIterator::CurrentFieldMask() const {
  CHECK(current_field_mask_ != 0ULL);
  return current_field_mask_;
}

}  // namespace valkey_search::indexes::text
