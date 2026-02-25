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
    key_set_.emplace(key_iter.GetKey(), idx);
  }
}

bool TermIterator::FindMinimumValidKey() {
  // Build set only if empty
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
  // 1. Peek minimum
  auto it_begin = key_set_.begin();
  current_key_ = it_begin->first;
  current_key_indices_.clear();
  // 2. Identify range
  auto it_end = it_begin;
  while (it_end != key_set_.end() && it_end->first == current_key_) {
    current_key_indices_.push_back(it_end->second);
    ++it_end;
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
  // 4. Batch Erase
  key_set_.erase(it_begin, it_end);
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
  // 1. Early exit: already at or past the target key.
  if (current_key_ && current_key_ >= target_key) {
    return true;
  }
  // 2. Identify laggard keys in the set (keys < target_key)
  // We use the same class-member scratch_indices_ for Key seeks.
  scratch_indices_.clear();
  auto it_end = key_set_.begin();
  while (it_end != key_set_.end() && it_end->first < target_key) {
    scratch_indices_.push_back(it_end->second);
    ++it_end;
  }
  // 3. Batch process laggard keys from the set.
  if (!scratch_indices_.empty()) {
    key_set_.erase(key_set_.begin(), it_end);
    for (size_t idx : scratch_indices_) {
      key_iterators_[idx].SkipForwardKey(target_key);
      InsertValidKeyIterator(idx, target_key);
    }
    // Optimization: Clear immediately to keep memory ready.
    scratch_indices_.clear();
  }
  // 4. Handle the "currently active" key indices.
  // Redundant check removed: if we are here, current_key_ is either
  // null or < target_key.
  if (current_key_) {
    for (size_t idx : current_key_indices_) {
      key_iterators_[idx].SkipForwardKey(target_key);
      InsertValidKeyIterator(idx, target_key);
    }
    current_key_indices_.clear();
  }
  // 5. Finalize state by finding the new minimum valid key.
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

// Helper function to insert a position iterator into the set if it is valid
void TermIterator::InsertValidPositionIterator(
    size_t idx, std::optional<uint32_t> min_position) {
  auto& pos_iter = pos_iterators_[idx];
  while (pos_iter.IsValid() &&
         (!(pos_iter.GetFieldMask() & query_field_mask_) ||
          (min_position && pos_iter.GetPosition() < *min_position))) {
    pos_iter.NextPosition();
  }
  if (pos_iter.IsValid()) {
    pos_set_.emplace(pos_iter.GetPosition(), idx);
  }
}

bool TermIterator::FindMinimumValidPosition() {
  // Build set only if empty
  if (pos_set_.empty()) {
    for (size_t i = 0; i < pos_iterators_.size(); ++i) {
      InsertValidPositionIterator(i);
    }
  }
  // If still empty, we are done.
  if (pos_set_.empty()) {
    current_position_ = std::nullopt;
    current_field_mask_ = 0ULL;
    return false;
  }
  // 1. Get the minimum value for the position
  uint32_t min_position = pos_set_.begin()->first;
  current_pos_indices_.clear();
  // 2. Collect ALL matching indices into our inlined vector
  auto it_end = pos_set_.begin();
  while (it_end != pos_set_.end() && it_end->first == min_position) {
    current_pos_indices_.push_back(it_end->second);
    ++it_end;
  }
  // 3. Batch erase from the set (Now we only shift the vector once)
  pos_set_.erase(pos_set_.begin(), it_end);
  // 4. Set current state using the collected indices
  current_position_ = PositionRange{min_position, min_position};
  current_field_mask_ = pos_iterators_[current_pos_indices_[0]].GetFieldMask();
  return true;
}

bool TermIterator::NextPosition() {
  if (current_position_.has_value()) {
    // First advance all iterators at current position
    for (size_t idx : current_pos_indices_) {
      pos_iterators_[idx].NextPosition();
    }
    // Then insert them back if still valid
    for (size_t idx : current_pos_indices_) {
      InsertValidPositionIterator(idx);
    }
  }
  return FindMinimumValidPosition();
}

bool TermIterator::SeekForwardPosition(Position target_position) {
  // 1. Early exit: already at or past the target.
  if (current_position_.has_value() &&
      current_position_.value().start >= target_position) {
    return true;
  }
  // 2. Identify laggards in the set (positions < target).
  // We use the class-member scratch_indices_ to avoid stack/heap churn.
  scratch_indices_.clear();
  auto it_end = pos_set_.begin();
  while (it_end != pos_set_.end() && it_end->first < target_position) {
    scratch_indices_.push_back(it_end->second);
    ++it_end;
  }
  // 3. Batch process laggards from the set.
  if (!scratch_indices_.empty()) {
    pos_set_.erase(pos_set_.begin(), it_end);
    for (size_t idx : scratch_indices_) {
      pos_iterators_[idx].SkipForwardPosition(target_position);
      InsertValidPositionIterator(idx, target_position);
    }
    // Optimization: Clear immediately after use to keep the
    // memory ready for the next phase or next call.
    scratch_indices_.clear();
  }
  // 4. Handle the "currently active" indices.
  // Redundant check removed: if we are here, current_position_ is either
  // nullopt or < target_position.
  if (current_position_.has_value()) {
    for (size_t idx : current_pos_indices_) {
      pos_iterators_[idx].SkipForwardPosition(target_position);
      InsertValidPositionIterator(idx, target_position);
    }
    current_pos_indices_.clear();
  }
  // 5. Finalize state by finding the new minimum.
  return FindMinimumValidPosition();
}

FieldMaskPredicate TermIterator::CurrentFieldMask() const {
  CHECK(current_field_mask_ != 0ULL);
  return current_field_mask_;
}

}  // namespace valkey_search::indexes::text
