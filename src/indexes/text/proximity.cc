#include "proximity.h"

namespace valkey_search::indexes::text {

ProximityIterator::ProximityIterator(
    std::vector<std::unique_ptr<TextIterator>>&& iters, size_t slop,
    bool in_order, uint64_t field_mask, const InternedStringSet* untracked_keys)
    : iters_(std::move(iters)),
      slop_(slop),
      in_order_(in_order),
      untracked_keys_(untracked_keys),
      current_key_(nullptr),
      current_start_pos_(std::nullopt),
      current_end_pos_(std::nullopt),
      field_mask_(field_mask) {
  CHECK(!iters_.empty()) << "must have at least one text iterator";
  CHECK(slop_ >= 0) << "slop must be non-negative";
  // Pre-allocate vectors used for positional checks to avoid reallocation
  positions_.resize(iters_.size());
  pos_with_idx_.resize(iters_.size());
  // Prime iterators to the first common key and valid position combo
  NextKey();
}

uint64_t ProximityIterator::FieldMask() const { return field_mask_; }

bool ProximityIterator::DoneKeys() const {
  for (auto& iter : iters_) {
    if (iter->DoneKeys()) {
      return true;
    }
  }
  return false;
}

const InternedStringPtr& ProximityIterator::CurrentKey() const {
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
      current_start_pos_ = std::nullopt;
      current_end_pos_ = std::nullopt;
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
  InternedStringPtr min_key = nullptr;
  InternedStringPtr max_key = nullptr;
  for (auto& iter : iters_) {
    auto k = iter->CurrentKey();
    if (!min_key || k->Str() < min_key->Str()) min_key = k;
    if (!max_key || k->Str() > max_key->Str()) max_key = k;
  }
  // 2) If min == max, we found a common key
  if (min_key->Str() == max_key->Str()) {
    current_key_ = max_key;
    return true;
  }
  // 3) Advance all iterators that are strictly behind the current max_key
  for (auto& iter : iters_) {
    iter->SeekForwardKey(max_key);
  }
  return false;
}

bool ProximityIterator::SeekForwardKey(const InternedStringPtr& target_key) {
  // If current key is already >= target_key, no need to seek
  if (current_key_ && current_key_->Str() >= target_key->Str()) {
    return true;
  }
  // Skip all keys less than target_key for all iterators
  for (auto& iter : iters_) {
    if (!iter->DoneKeys() && iter->CurrentKey()->Str() < target_key->Str()) {
      iter->SeekForwardKey(target_key);
    }
  }
  // Find next valid key/position combination
  while (!DoneKeys()) {
    if (FindCommonKey()) {
      current_start_pos_ = std::nullopt;
      current_end_pos_ = std::nullopt;
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

std::pair<uint32_t, uint32_t> ProximityIterator::CurrentPosition() const {
  CHECK(current_start_pos_.has_value() && current_end_pos_.has_value());
  return std::make_pair(current_start_pos_.value(), current_end_pos_.value());
}

std::optional<size_t> ProximityIterator::FindViolatingIterator() const {
  const size_t n = positions_.size();
  if (in_order_) {
    for (size_t i = 0; i < n - 1; ++i) {
      // Check overlap / ordering violations.
      if (positions_[i].second >= positions_[i + 1].first) {
        return i + 1;
      }
      // Check slop violations.
      if (slop_ >= 0 &&
          positions_[i + 1].first - positions_[i].second - 1 > slop_) {
        return i;
      }
    }
    return std::nullopt;
  }
  // For unordered, use an index mapping to help validate constraints.
  for (size_t i = 0; i < n; ++i) {
    pos_with_idx_[i] = {positions_[i].first, i};
  }
  std::sort(pos_with_idx_.begin(), pos_with_idx_.end());
  for (size_t i = 0; i < n - 1; ++i) {
    size_t curr_idx = pos_with_idx_[i].second;
    size_t next_idx = pos_with_idx_[i + 1].second;
    if (positions_[curr_idx].second >= positions_[next_idx].first) {
      return next_idx;
    }
    if (slop_ >= 0 &&
        positions_[next_idx].first - positions_[curr_idx].second - 1 > slop_) {
      return curr_idx;
    }
  }
  return std::nullopt;
}

bool ProximityIterator::NextPosition() {
  const size_t n = iters_.size();
  bool should_advance = current_start_pos_.has_value();
  while (!DonePositions()) {
    for (size_t i = 0; i < n; ++i) {
      positions_[i] = iters_[i]->CurrentPosition();
    }
    if (should_advance) {
      should_advance = false;
      if (auto iter_to_advance = FindViolatingIterator()) {
        iters_[*iter_to_advance]->NextPosition();
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
    // No violations mean that this positional combination is valid.
    if (!FindViolatingIterator().has_value()) {
      current_start_pos_ = positions_[0].first;
      current_end_pos_ = positions_[n - 1].second;
      return true;
    }
    should_advance = true;
  }
  current_start_pos_ = std::nullopt;
  current_end_pos_ = std::nullopt;
  return false;
}

}  // namespace valkey_search::indexes::text
