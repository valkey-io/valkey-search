/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/term.h"

namespace valkey_search::indexes::text {

TermIterator::TermIterator(std::vector<Postings::KeyIterator>&& key_iterators,
                           const FieldMaskPredicate query_field_mask,
                           const InternedStringSet* untracked_keys)
    : query_field_mask_(query_field_mask),
      key_iterators_(std::move(key_iterators)),
      pos_iterators_(),
      current_key_(),
      current_position_(std::nullopt),
      current_field_mask_(0ULL),
      untracked_keys_(untracked_keys),
      key_heap_(),
      pos_heap_(),
      current_key_indices_(),
      current_pos_indices_() {
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
  while (key_iter.IsValid() && !key_iter.ContainsFields(query_field_mask_)) {
    key_iter.NextKey();
  }
  if (key_iter.IsValid()) {
    key_heap_.emplace(key_iter.GetKey(), idx);
  }
}

bool TermIterator::FindMinimumValidKey() {
  // Build heap only if empty
  if (key_heap_.empty()) {
    for (size_t i = 0; i < key_iterators_.size(); ++i) {
      InsertValidKeyIterator(i);
    }
  }
  if (key_heap_.empty()) {
    current_key_ = nullptr;
    current_position_ = std::nullopt;
    current_field_mask_ = 0ULL;
    return false;
  }
  // Get minimum key
  current_key_ = key_heap_.top().first;
  pos_iterators_.clear();
  current_key_indices_.clear();
  // Collect all iterators with minimum key
  while (!key_heap_.empty() && key_heap_.top().first == current_key_) {
    size_t idx = key_heap_.top().second;
    key_heap_.pop();
    current_key_indices_.push_back(idx);
    pos_iterators_.emplace_back(key_iterators_[idx].GetPositionIterator());
  }
  // Clear position state for new key
  pos_heap_ = {};
  current_position_ = std::nullopt;
  TermIterator::NextPosition();
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

FieldMaskPredicate TermIterator::CurrentFieldMask() const {
  CHECK(current_field_mask_ != 0ULL);
  return current_field_mask_;
}

}  // namespace valkey_search::indexes::text
