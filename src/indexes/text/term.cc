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

bool TermIterator::DoneKeys() const { return !current_key_; }

const InternedStringPtr& TermIterator::CurrentKey() const {
  CHECK(current_key_);
  return current_key_;
}

// Helper function to advance key iterator and collect valid entries
void TermIterator::InsertValidKeyIterator(
    size_t idx, std::optional<InternedStringPtr> min_key) {
  auto& key_iter = key_iterators_[idx];
  const auto field_mask = ((idx == 0 && has_original_) || stem_field_mask_ == 0)
                              ? query_field_mask_
                              : stem_field_mask_;
  while (key_iter.IsValid() && (!key_iter.ContainsFields(field_mask) ||
                                (min_key && key_iter.GetKey() < *min_key))) {
    key_iter.NextKey();
  }
  if (key_iter.IsValid()) {
    key_set_.push_back_unsorted(key_iter.GetKey(), idx);
  }
}

bool TermIterator::FindMinimumValidKey() {
  // Build heap only if empty
  if (key_set_.empty()) {
    for (size_t i = 0; i < key_iterators_.size(); ++i) {
      InsertValidKeyIterator(i);
    }
  }
  if (key_set_.empty()) {
    current_key_ = nullptr;
    current_position_ = std::nullopt;
    current_field_mask_ = 0ULL;
    return false;
  }
  key_set_.heapify();  // O(K) building of the heap
  // 1. Get the minimum key from the heap root
  current_key_ = key_set_.min().first;
  current_key_indices_.clear();
  // 2. Extract ALL iterators sharing this minimum key
  // In a heap, we must pop to see the next minimum
  while (!key_set_.empty() && key_set_.min().first == current_key_) {
    current_key_indices_.push_back(key_set_.min().second);
    key_set_.pop_min();  // O(log K)
  }
  // 3. FULL RESET for the new Key's positions
  if (require_positions_) {
    pos_iterators_.clear();
    for (size_t idx : current_key_indices_) {
      pos_iterators_.emplace_back(key_iterators_[idx].GetPositionIterator());
    }
    pos_set_.clear();
    current_pos_indices_.clear();
    current_position_ = std::nullopt;
    current_field_mask_ = 0ULL;
    TermIterator::NextPosition();
  }
  return true;
}

bool TermIterator::NextKey() {
  if (current_key_) {
    for (size_t idx : current_key_indices_) {
      key_iterators_[idx].NextKey();
      InsertValidKeyIterator(idx);
    }
  }
  return FindMinimumValidKey();
}

bool TermIterator::SeekForwardKey(const InternedStringPtr& target_key) {
  if (current_key_ && current_key_ >= target_key) return true;
  // Remove and seek laggards from heap
  while (!key_set_.empty() && key_set_.min().first < target_key) {
    size_t idx = key_set_.min().second;
    key_set_.pop_min();
    key_iterators_[idx].SkipForwardKey(target_key);
    InsertValidKeyIterator(idx, target_key);
  }
  // Handle currently active indices
  if (current_key_) {
    for (size_t idx : current_key_indices_) {
      key_iterators_[idx].SkipForwardKey(target_key);
      InsertValidKeyIterator(idx, target_key);
    }
    current_key_indices_.clear();
  }
  return FindMinimumValidKey();
}

bool TermIterator::DonePositions() const {
  return !current_position_.has_value();
}

const PositionRange& TermIterator::CurrentPosition() const {
  CHECK(current_position_.has_value());
  return current_position_.value();
}

void TermIterator::InsertValidPositionIterator(
    size_t idx, std::optional<uint32_t> min_position) {
  auto& pos_iter = pos_iterators_[idx];
  while (pos_iter.IsValid() &&
         (!(pos_iter.GetFieldMask() & query_field_mask_) ||
          (min_position && pos_iter.GetPosition() < *min_position))) {
    pos_iter.NextPosition();
  }
  if (pos_iter.IsValid()) {
    pos_set_.push_back_unsorted(pos_iter.GetPosition(), idx);
  }
}

// Position Logic follows the same Heap pattern
bool TermIterator::FindMinimumValidPosition() {
  if (pos_set_.empty()) {
    for (size_t i = 0; i < pos_iterators_.size(); ++i) {
      InsertValidPositionIterator(i);
    }
  }
  if (pos_set_.empty()) {
    current_position_ = std::nullopt;
    current_field_mask_ = 0ULL;
    return false;
  }
  pos_set_.heapify();
  uint32_t min_position = pos_set_.min().first;
  current_pos_indices_.clear();
  current_field_mask_ = 0ULL;  // Reset before the loop
  while (!pos_set_.empty() && pos_set_.min().first == min_position) {
    size_t idx = pos_set_.min().second;
    current_pos_indices_.push_back(idx);
    current_field_mask_ |=
        pos_iterators_[idx]
            .GetFieldMask();  // Bitwise OR - Check if this is needed.
    pos_set_.pop_min();
  }
  current_position_ = PositionRange{min_position, min_position};
  // current_field_mask_ =
  // pos_iterators_[current_pos_indices_[0]].GetFieldMask();
  return true;
}

bool TermIterator::NextPosition() {
  if (current_position_.has_value()) {
    for (size_t idx : current_pos_indices_) {
      pos_iterators_[idx].NextPosition();
      InsertValidPositionIterator(idx);
    }
  }
  return FindMinimumValidPosition();
}

bool TermIterator::SeekForwardPosition(Position target_position) {
  if (current_position_.has_value() &&
      current_position_.value().start >= target_position) {
    return true;
  }
  // Remove and seek laggards from heap
  while (!pos_set_.empty() && pos_set_.min().first < target_position) {
    size_t idx = pos_set_.min().second;
    pos_set_.pop_min();
    pos_iterators_[idx].SkipForwardPosition(target_position);
    InsertValidPositionIterator(idx, target_position);
  }
  // Handle currently active indices
  if (current_position_.has_value()) {
    for (size_t idx : current_pos_indices_) {
      pos_iterators_[idx].SkipForwardPosition(target_position);
      InsertValidPositionIterator(idx, target_position);
    }
    current_pos_indices_.clear();
  }
  return FindMinimumValidPosition();
}

FieldMaskPredicate TermIterator::CurrentFieldMask() const {
  CHECK(current_field_mask_ != 0ULL);
  return current_field_mask_;
}

}  // namespace valkey_search::indexes::text