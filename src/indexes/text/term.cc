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
                           const InternedStringSet* untracked_keys,
                           const bool require_positions)
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
  // Build heap only if empty or invalid
  if (key_heap_.empty()) {
    for (size_t i = 0; i < key_iterators_.size(); ++i) {
      auto& key_iter = key_iterators_[i];
      while (key_iter.IsValid() && !key_iter.ContainsFields(query_field_mask_)) {
        key_iter.NextKey();
      }
      if (key_iter.IsValid()) {
        key_heap_.emplace(key_iter.GetKey(), i);
      }
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
  // Collect all iterators with minimum key
  current_key_indices_.clear();
  while (!key_heap_.empty() && key_heap_.top().first == current_key_) {
    size_t idx = key_heap_.top().second;
    current_key_indices_.push_back(idx);
    pos_iterators_.emplace_back(key_iterators_[idx].GetPositionIterator());
    key_heap_.pop();
  }
  if (!current_key_) {
    return false;
  }
  // No need to check since we know that at least one position exists based on
  // ContainsFields.
  TermIterator::NextPosition();
  return true;
}

bool TermIterator::NextKey() {
  if (current_key_) {
    for (size_t idx : current_key_indices_) {
      key_iterators_[idx].NextKey();
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

FieldMaskPredicate TermIterator::CurrentFieldMask() const {
  CHECK(current_field_mask_ != 0ULL);
  return current_field_mask_;
}

}  // namespace valkey_search::indexes::text
