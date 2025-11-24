#include "proximity.h"

namespace valkey_search::indexes::text {

ProximityIterator::ProximityIterator(
    std::vector<std::unique_ptr<TextIterator>>&& iters,
    std::optional<uint32_t> slop, bool in_order,
    FieldMaskPredicate query_field_mask,
    const InternedStringSet* untracked_keys)
    : iters_(std::move(iters)),
      slop_(slop),
      in_order_(in_order),
      untracked_keys_(untracked_keys),
      current_key_(nullptr),
      current_position_(std::nullopt),
      current_field_mask_(0ULL),
      query_field_mask_(query_field_mask) {
  CHECK(!iters_.empty()) << "must have at least one text iterator";
  CHECK(slop_.has_value() || in_order_)
      << "ProximityIterator requires either slop or inorder=true";
  // Pre-allocate vectors used for positional checks to avoid reallocation
  positions_.resize(iters_.size());
  pos_with_idx_.resize(iters_.size());
  // Prime iterators to the first common key and valid position combo
  NextKey();
}

FieldMaskPredicate ProximityIterator::QueryFieldMask() const {
  return query_field_mask_;
}

bool ProximityIterator::DoneKeys() const {
  for (auto& iter : iters_) {
    if (iter->DoneKeys()) {
      return true;
    }
  }
  return false;
}

const Key& ProximityIterator::CurrentKey() const {
  CHECK(current_key_ != nullptr);
  return current_key_;
}

bool ProximityIterator::NextKey() {
  // On the second call onwards, advance any text iterators that are still
  // sitting on the old key.
  auto advance = [&]() -> void {
    for (auto& iter : iters_) {
      if (!iter->DoneKeys() && iter->CurrentKey() == current_key_) {
        iter->NextKey();
      }
    }
  };
  if (current_key_) {
    advance();
  }
  while (!DoneKeys()) {
    // 1) Move to the next common key amongst all text iterators.
    if (FindCommonKey()) {
      current_position_ = std::nullopt;
      current_field_mask_ = 0ULL;
      // 2) Move to the next valid position combination across all text
      // iterators.
      // Exit, if no key with a valid position combination is found.
      if (NextPosition()) {
        return true;
      }
    }
    // Otherwise, loop and try again.
    advance();
  }
  current_key_ = nullptr;
  return false;
}

bool ProximityIterator::FindCommonKey() {
  // 1) Validate children and compute min/max among current keys
  Key min_key = nullptr;
  Key max_key = nullptr;
  for (auto& iter : iters_) {
    auto k = iter->CurrentKey();
    if (!min_key || k < min_key) min_key = k;
    if (!max_key || k > max_key) max_key = k;
  }
  // 2) If min == max, we found a common key
  if (min_key == max_key) {
    current_key_ = max_key;
    return true;
  }
  // 3) Advance all iterators that are strictly behind the current max_key
  for (auto& iter : iters_) {
    iter->SeekForwardKey(max_key);
  }
  return false;
}

bool ProximityIterator::SeekForwardKey(const Key& target_key) {
  // If current key is already >= target_key, no need to seek
  if (current_key_ && current_key_ >= target_key) {
    return true;
  }
  // Skip all keys less than target_key for all iterators
  for (auto& iter : iters_) {
    if (!iter->DoneKeys() && iter->CurrentKey() < target_key) {
      iter->SeekForwardKey(target_key);
    }
  }
  // Find next valid key/position combination
  while (!DoneKeys()) {
    if (FindCommonKey()) {
      current_position_ = std::nullopt;
      current_field_mask_ = 0ULL;
      if (NextPosition()) {
        return true;
      }
    }
    // Advance past current key and try again
    for (auto& iter : iters_) {
      if (!iter->DoneKeys() && iter->CurrentKey() == current_key_) {
        iter->NextKey();
      }
    }
  }
  current_key_ = nullptr;
  return false;
}

bool ProximityIterator::DonePositions() const {
  for (auto& iter : iters_) {
    if (iter->DonePositions()) {
      return true;
    }
  }
  return false;
}

const PositionRange& ProximityIterator::CurrentPosition() const {
  CHECK(current_position_.has_value());
  return current_position_.value();
}

std::optional<size_t> ProximityIterator::FindViolatingIterator() {
  const size_t n = positions_.size();
  if (in_order_) {
    for (size_t i = 0; i < n - 1; ++i) {
      // Check overlap / ordering violations.
      if (positions_[i].end >= positions_[i + 1].start) {
        return i + 1;
      }
      // Check slop violations.
      if (slop_.has_value() &&
          positions_[i + 1].start - positions_[i].end - 1 > *slop_) {
        return i;
      }
    }
    // Check for field mask intersection (terms exist in the same field)
    FieldMaskPredicate field_mask = query_field_mask_;
    for (size_t i = 0; i < n; ++i) {
      field_mask &= iters_[i]->CurrentFieldMask();
      // No common fields, advance this iterator which is lagging behind the
      // previous one.
      if (field_mask == 0) {
        return i;
      }
    }
    return std::nullopt;
  }
  // For unordered, use an index mapping to help validate constraints.
  for (size_t i = 0; i < n; ++i) {
    pos_with_idx_[i] = {positions_[i].start, i};
  }
  std::sort(pos_with_idx_.begin(), pos_with_idx_.end());
  FieldMaskPredicate field_mask = query_field_mask_;
  for (size_t i = 0; i < n - 1; ++i) {
    size_t curr_idx = pos_with_idx_[i].second;
    size_t next_idx = pos_with_idx_[i + 1].second;
    if (positions_[curr_idx].end >= positions_[next_idx].start) {
      return next_idx;
    }
    if (slop_.has_value() &&
        positions_[next_idx].start - positions_[curr_idx].end - 1 > *slop_) {
      return curr_idx;
    }
    // Check for field mask intersection (terms exist in the same field)
    field_mask &= iters_[i]->CurrentFieldMask();
    // No common fields, advance this iterator which is lagging behind the
    // previous one.
    if (field_mask == 0) {
      return i;
    }
  }
  return std::nullopt;
}

bool ProximityIterator::NextPosition() {
  const size_t n = iters_.size();
  bool should_advance = current_position_.has_value();
  while (!DonePositions()) {
    for (size_t i = 0; i < n; ++i) {
      positions_[i] = iters_[i]->CurrentPosition();
    }
    auto violating_iter = FindViolatingIterator();
    if (should_advance) {
      should_advance = false;
      if (violating_iter) {
        iters_[*violating_iter]->NextPosition();
      } else {
        // No violations, advance first non-done iterator
        for (size_t i = 0; i < n; ++i) {
          if (!iters_[i]->DonePositions()) {
            iters_[i]->NextPosition();
            break;
          }
        }
      }
      continue;
    }
    // No violations - so this positional combination is valid.
    if (!violating_iter.has_value()) {
      // Set the current field based on field mask intersection.
      current_field_mask_ = iters_[0]->CurrentFieldMask();
      for (size_t i = 1; i < n; ++i) {
        current_field_mask_ &= iters_[i]->CurrentFieldMask();
      }
      // Set the current position range.
      if (in_order_) {
        current_position_ =
            PositionRange(positions_[0].start, positions_[n - 1].end);
      } else {
        // Use sorted positions for unordered queries
        Position min_start = pos_with_idx_[0].first;
        Position max_end = positions_[pos_with_idx_[n - 1].second].end;
        current_position_ = PositionRange(min_start, max_end);
      }
      return true;
    }
    should_advance = true;
  }
  current_position_ = std::nullopt;
  current_field_mask_ = 0ULL;
  return false;
}

FieldMaskPredicate ProximityIterator::CurrentFieldMask() const {
  CHECK(current_field_mask_ != 0ULL);
  return current_field_mask_;
}

}  // namespace valkey_search::indexes::text
