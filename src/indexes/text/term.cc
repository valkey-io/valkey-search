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
  // O(1) check: current_key_ is nullified when FindMinimumValidKey exhausts all
  // iterators.
  return !current_key_;
}

const InternedStringPtr& TermIterator::CurrentKey() const {
  CHECK(current_key_);
  return current_key_;
}

// Helper function to advance key iterators and populate the heap with valid
// iterators for the new key.
void TermIterator::InsertValidKeyIterator(size_t idx) {
  auto& key_iter = key_iterators_[idx];
  // Use query_field_mask for the original word (idx 0) or if no stem mask is
  // provided.
  const auto field_mask = ((idx == 0 && has_original_) || stem_field_mask_ == 0)
                              ? query_field_mask_
                              : stem_field_mask_;
  // Advance the iterator until it matches the required fields.
  while (key_iter.IsValid() && (!key_iter.ContainsFields(field_mask))) {
    key_iter.NextKey();
  }
  if (key_iter.IsValid()) {
    key_set_.push_back_unsorted(key_iter.GetKey(), idx);
  }
}

bool TermIterator::FindMinimumValidKey() {
  // 1. If the heap is empty, it means all iterators from the previous match
  // were extracted. Attempt to re-populate the heap with any iterators
  // that still have valid data.
  if (key_set_.empty()) {
    for (size_t i = 0; i < key_iterators_.size(); ++i) {
      InsertValidKeyIterator(i);
    }
  }
  // 2. If the heap is still empty after the re-population attempt, it means
  // all underlying expansion iterators are exhausted.
  if (key_set_.empty()) {
    ClearKeyState();
    return false;
  }
  // 3. Restore the min-heap property. O(K) is efficient here because
  // multiple expansions often advance simultaneously.
  key_set_.heapify();
  current_key_ = key_set_.min().first;
  current_key_indices_.clear();
  // 4. Extract all iterators that share this minimum key.
  // This physically removes them from the heap (making it "empty" if all
  // match).
  while (!key_set_.empty() && key_set_.min().first == current_key_) {
    current_key_indices_.push_back(key_set_.min().second);
    key_set_.pop_min();  // O(log K)
  }
  // 5. Initialize position iteration for the specific new key if required.
  if (require_positions_) {
    ClearPositionState();
    for (size_t idx : current_key_indices_) {
      pos_iterators_.emplace_back(key_iterators_[idx].GetPositionIterator());
    }
    TermIterator::NextPosition();
  }
  return true;
}

bool TermIterator::NextKey() {
  if (current_key_) {
    // Advance all iterators that contributed to the current key.
    for (size_t idx : current_key_indices_) {
      key_iterators_[idx].NextKey();
      InsertValidKeyIterator(idx);
    }
    current_key_indices_.clear();
  }
  return FindMinimumValidKey();
}

bool TermIterator::SeekForwardKey(const InternedStringPtr& target_key) {
  if (current_key_ && current_key_ >= target_key) return true;
  // Drain laggards from the heap that are behind the target.
  while (!key_set_.empty() && key_set_.min().first < target_key) {
    size_t idx = key_set_.min().second;
    key_set_.pop_min();
    key_iterators_[idx].SkipForwardKey(target_key);
    InsertValidKeyIterator(idx);
  }
  // Update active indices that were already extracted from the heap.
  if (current_key_) {
    for (size_t idx : current_key_indices_) {
      key_iterators_[idx].SkipForwardKey(target_key);
      InsertValidKeyIterator(idx);
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

// Helper function to advance position iterators and populate the heap with
// valid iterators for the new position.
void TermIterator::InsertValidPositionIterator(size_t idx) {
  auto& pos_iter = pos_iterators_[idx];
  // Skip positions that don't match the query field mask.
  while (pos_iter.IsValid() &&
         (!(pos_iter.GetFieldMask() & query_field_mask_))) {
    pos_iter.NextPosition();
  }
  if (pos_iter.IsValid()) {
    pos_set_.push_back_unsorted(pos_iter.GetPosition(), idx);
  }
}

// Position Logic follows the same Heap pattern as Key logic.
bool TermIterator::FindMinimumValidPosition() {
  // 1. If the heap is empty, it means all iterators from the previous position
  // match were extracted. Attempt to re-populate the heap from the
  // current key's position iterators.
  if (pos_set_.empty()) {
    for (size_t i = 0; i < pos_iterators_.size(); ++i) {
      InsertValidPositionIterator(i);
    }
  }
  // 2. If the heap remains empty after the re-population attempt, it means
  // we have exhausted all positions for the current key.
  if (pos_set_.empty()) {
    ClearPositionState();
    return false;
  }
  // 3. Restore the min-heap property.
  pos_set_.heapify();
  uint32_t min_position = pos_set_.min().first;
  current_pos_indices_.clear();
  current_field_mask_ = 0ULL;
  // 4. Collect all iterators at the same position and aggregate their field
  // masks. This physically extracts them from the heap (making it "empty" if
  // all match).
  while (!pos_set_.empty() && pos_set_.min().first == min_position) {
    size_t idx = pos_set_.min().second;
    current_pos_indices_.push_back(idx);
    current_field_mask_ |= pos_iterators_[idx].GetFieldMask();
    pos_set_.pop_min();  // O(log K)
  }
  current_position_ = PositionRange{min_position, min_position};
  return true;
}

bool TermIterator::NextPosition() {
  if (current_position_.has_value()) {
    for (size_t idx : current_pos_indices_) {
      pos_iterators_[idx].NextPosition();
      InsertValidPositionIterator(idx);
    }
    current_pos_indices_.clear();
  }
  return FindMinimumValidPosition();
}

bool TermIterator::SeekForwardPosition(Position target_position) {
  if (current_position_.has_value() &&
      current_position_.value().start >= target_position) {
    return true;
  }
  // Drain laggard positions from the heap.
  while (!pos_set_.empty() && pos_set_.min().first < target_position) {
    size_t idx = pos_set_.min().second;
    pos_set_.pop_min();
    pos_iterators_[idx].SkipForwardPosition(target_position);
    InsertValidPositionIterator(idx);
  }
  // Update active position indices.
  if (current_position_.has_value()) {
    for (size_t idx : current_pos_indices_) {
      pos_iterators_[idx].SkipForwardPosition(target_position);
      InsertValidPositionIterator(idx);
    }
    current_pos_indices_.clear();
  }
  return FindMinimumValidPosition();
}

FieldMaskPredicate TermIterator::CurrentFieldMask() const {
  CHECK(current_field_mask_ != 0ULL);
  return current_field_mask_;
}

void TermIterator::ClearKeyState() {
  current_key_ = {};
  key_set_.clear();
  current_key_indices_.clear();
  ClearPositionState();
}

void TermIterator::ClearPositionState() {
  pos_iterators_.clear();
  pos_set_.clear();
  current_pos_indices_.clear();
  current_position_ = std::nullopt;
  current_field_mask_ = 0ULL;
}

}  // namespace valkey_search::indexes::text
