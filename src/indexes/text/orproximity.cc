#include "orproximity.h"

namespace valkey_search::indexes::text {

OrProximityIterator::OrProximityIterator(
    absl::InlinedVector<std::unique_ptr<TextIterator>,
                        kProximityTermsInlineCapacity>&& iters,
    const InternedStringSet* untracked_keys)
    : iters_(std::move(iters)),
      current_key_(),
      current_position_(std::nullopt),
      current_field_mask_(0ULL),
      untracked_keys_(untracked_keys),
      key_set_(),
      current_key_indices_(),
      pos_set_(),
      current_pos_indices_() {
  CHECK(!iters_.empty()) << "must have at least one text iterator";
  NextKey();
}

// Returns the field mask based on the current active text iterators.
// When we are at a certain position, multiple iterators may be on that
// position. We combine their field masks using bitwise OR to get the overall
// field mask.
FieldMaskPredicate OrProximityIterator::QueryFieldMask() const {
  CHECK(!current_pos_indices_.empty());
  FieldMaskPredicate mask = 0ULL;
  for (size_t idx : current_pos_indices_) {
    mask |= iters_[idx]->QueryFieldMask();
  }
  return mask;
}

bool OrProximityIterator::DoneKeys() const {
  for (const auto& iter : iters_) {
    if (!iter->DoneKeys()) return false;
  }
  return true;
}

const Key& OrProximityIterator::CurrentKey() const {
  CHECK(current_key_);
  return current_key_;
}

void OrProximityIterator::InsertValidKeyIterator(size_t idx) {
  auto& iter = iters_[idx];
  if (!iter->DoneKeys()) {
    key_set_.emplace(iter->CurrentKey(), idx);
  }
}

bool OrProximityIterator::FindMinimumKey() {
  if (key_set_.empty()) {
    for (size_t i = 0; i < iters_.size(); ++i) {
      InsertValidKeyIterator(i);
    }
  }
  if (key_set_.empty()) {
    current_key_ = nullptr;
    current_position_ = std::nullopt;
    current_field_mask_ = 0ULL;
    return false;
  }
  current_key_ = key_set_.begin()->first;
  current_key_indices_.clear();
  // Collect all iterators with minimum key.
  // key_set_ is sorted, so all matching keys are consecutive
  // and we stop when we find a different key.
  for (auto it = key_set_.begin();
       it != key_set_.end() && it->first == current_key_;) {
    current_key_indices_.push_back(it->second);
    it = key_set_.erase(it);
  }
  pos_set_ = {};
  current_position_ = std::nullopt;
  NextPosition();
  return true;
}

bool OrProximityIterator::NextKey() {
  if (current_key_) {
    // Advance all iterators at current key
    for (size_t idx : current_key_indices_) {
      iters_[idx]->NextKey();
    }
    // Insert them back if still valid
    for (size_t idx : current_key_indices_) {
      InsertValidKeyIterator(idx);
    }
  }
  return FindMinimumKey();
}

bool OrProximityIterator::SeekForwardKey(const Key& target_key) {
  if (current_key_ && current_key_ >= target_key) {
    return true;
  }
  // Clear set and seek all iterators to target_key or beyond.
  key_set_.clear();
  for (size_t i = 0; i < iters_.size(); ++i) {
    if (!iters_[i]->DoneKeys() && iters_[i]->CurrentKey() < target_key) {
      iters_[i]->SeekForwardKey(target_key);
    }
  }
  // Rebuild key set if needed or returns false if all are exhausted.
  return FindMinimumKey();
}

bool OrProximityIterator::DonePositions() const {
  for (size_t idx : current_key_indices_) {
    if (!iters_[idx]->DonePositions()) return false;
  }
  return true;
}

const PositionRange& OrProximityIterator::CurrentPosition() const {
  CHECK(current_position_.has_value());
  return current_position_.value();
}

void OrProximityIterator::InsertValidPositionIterator(size_t idx) {
  if (!iters_[idx]->DonePositions()) {
    pos_set_.emplace(iters_[idx]->CurrentPosition().start, idx);
  }
}

bool OrProximityIterator::NextPosition() {
  if (current_position_.has_value()) {
    // Advance all iterators at current position
    for (size_t idx : current_pos_indices_) {
      iters_[idx]->NextPosition();
    }
    // Insert them back if still valid
    for (size_t idx : current_pos_indices_) {
      InsertValidPositionIterator(idx);
    }
  } else {
    // Initialize heap (new key)
    for (size_t idx : current_key_indices_) {
      InsertValidPositionIterator(idx);
    }
  }
  if (pos_set_.empty()) {
    current_position_ = std::nullopt;
    current_field_mask_ = 0ULL;
    return false;
  }
  Position min_pos = pos_set_.begin()->first;
  current_pos_indices_.clear();
  // Collect all iterators at minimum position.
  // pos_set_ is sorted, so all iterators on the position are consecutive
  // and we stop when we find a different position.
  for (auto it = pos_set_.begin();
       it != pos_set_.end() && it->first == min_pos;) {
    current_pos_indices_.push_back(it->second);
    it = pos_set_.erase(it);
  }
  current_position_ = iters_[current_pos_indices_[0]]->CurrentPosition();
  current_field_mask_ = 0ULL;
  for (size_t idx : current_pos_indices_) {
    current_field_mask_ |= iters_[idx]->CurrentFieldMask();
  }
  return true;
}

FieldMaskPredicate OrProximityIterator::CurrentFieldMask() const {
  CHECK(current_field_mask_ != 0ULL);
  return current_field_mask_;
}

bool OrProximityIterator::IsIteratorValid() const {
  return current_key_ && current_position_.has_value() &&
         current_field_mask_ != 0ULL;
}

}  // namespace valkey_search::indexes::text
