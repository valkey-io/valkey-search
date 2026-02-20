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
    const FieldMaskPredicate query_field_mask, const bool require_positions,
    const FieldMaskPredicate stem_field_mask, bool has_original)
    : query_field_mask_(query_field_mask),
      stem_field_mask_(stem_field_mask),
      key_iterators_(std::move(key_iterators)),
      current_position_(std::nullopt),
      current_field_mask_(0ULL),
      require_positions_(require_positions),
      has_original_(has_original) {
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

// Helper function to insert a key iterator into the heap if it is valid
void TermIterator::InsertValidKeyIterator(size_t idx) {
  auto& key_iter = key_iterators_[idx];
  const auto field_mask = ((idx == 0 && has_original_) || stem_field_mask_ == 0)
                              ? query_field_mask_
                              : stem_field_mask_;
  while (key_iter.IsValid() && !key_iter.ContainsFields(field_mask)) {
    key_iter.NextKey();
  }
  if (key_iter.IsValid()) {
    key_set_.emplace(key_iter.GetKey(), idx);
  }
}

bool TermIterator::FindMinimumValidKey() {
  // Build heap only if empty
  if (key_set_.empty()) {
    for (size_t i = 0; i < key_iterators_.size(); ++i) {
      InsertValidKeyIterator(i);
    }
  }
  // If still empty, we are done.
  if (key_set_.empty()) {
    current_key_ = nullptr;
    current_position_ = std::nullopt;
    current_field_mask_ = 0ULL;
    return false;
  }
  // Get minimum key
  current_key_ = key_set_.begin()->first;
  pos_iterators_.clear();
  current_key_indices_.clear();
  // Collect all iterators with minimum key
  for (auto it = key_set_.begin();
       it != key_set_.end() && it->first == current_key_;) {
    size_t idx = it->second;
    current_key_indices_.push_back(idx);
    if (require_positions_) {
      pos_iterators_.emplace_back(key_iterators_[idx].GetPositionIterator());
    }
    it = key_set_.erase(it);
  }
  // Clear position state for new key
  if (require_positions_) {
    pos_heap_ = {};
    current_position_ = std::nullopt;
    current_field_mask_ = 0ULL;
    TermIterator::NextPosition();
  }
  return true;
}

bool TermIterator::NextKey() {
  if (current_key_) {
    // First advance all iterators at current key
    for (size_t idx : current_key_indices_) {
      key_iterators_[idx].NextKey();
    }
    // Then insert them back if still valid
    for (size_t idx : current_key_indices_) {
      InsertValidKeyIterator(idx);
    }
  }
  return FindMinimumValidKey();
}

// This code is questionable.
bool TermIterator::SeekForwardKey(const InternedStringPtr& target_key) {
  if (current_key_ && current_key_ >= target_key) {
    return true;
  }

  auto it = key_set_.lower_bound(std::make_pair(target_key, 0));
  for (auto iter = key_set_.begin(); iter != it;) {
    size_t idx = iter->second;
    iter = key_set_.erase(iter);
    key_iterators_[idx].SkipForwardKey(target_key);
    InsertValidKeyIterator(idx);
  }

  for (size_t i = 0; i < key_iterators_.size(); ++i) {
    if (key_iterators_[i].IsValid() &&
        key_iterators_[i].GetKey() < target_key) {
      key_iterators_[i].SkipForwardKey(target_key);
      InsertValidKeyIterator(i);
    }
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

// Helper function to insert a position iterator into the heap if it is valid
void TermIterator::InsertValidPositionIterator(size_t idx) {
  auto& pos_iter = pos_iterators_[idx];
  while (pos_iter.IsValid() && !(pos_iter.GetFieldMask() & query_field_mask_)) {
    pos_iter.NextPosition();
  }
  if (pos_iter.IsValid()) {
    pos_heap_.emplace(pos_iter.GetPosition(), idx);
  }
}

bool TermIterator::NextPosition() {
  if (current_position_.has_value()) {
    // Advance all iterators at current position
    for (size_t idx : current_pos_indices_) {
      pos_iterators_[idx].NextPosition();
    }
    // Then insert them back if still valid
    for (size_t idx : current_pos_indices_) {
      InsertValidPositionIterator(idx);
    }
  } else {
    // Initialize heap (new key)
    for (size_t i = 0; i < pos_iterators_.size(); ++i) {
      InsertValidPositionIterator(i);
    }
  }

  if (pos_heap_.empty()) {
    current_position_ = std::nullopt;
    current_field_mask_ = 0ULL;
    return false;
  }

  uint32_t min_position = pos_heap_.top().first;
  current_pos_indices_.clear();
  // Collect all iterators at minimum position
  while (!pos_heap_.empty() && pos_heap_.top().first == min_position) {
    current_pos_indices_.push_back(pos_heap_.top().second);
    pos_heap_.pop();
  }
  current_position_ = PositionRange{min_position, min_position};
  current_field_mask_ = pos_iterators_[current_pos_indices_[0]].GetFieldMask();
  return true;
}

bool TermIterator::SeekForwardPosition(Position target_position) {
  if (current_position_.has_value() &&
      current_position_.value().start >= target_position) {
    return true;
  }
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
  current_field_mask_ = 0ULL;
  return NextPosition();
}

FieldMaskPredicate TermIterator::CurrentFieldMask() const {
  CHECK(current_field_mask_ != 0ULL);
  return current_field_mask_;
}

}  // namespace valkey_search::indexes::text
