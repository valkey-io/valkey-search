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
  CHECK(iters_.size() > 0) << "must have at least one text iterator";
  CHECK(slop_ >= 0) << "slop must be non-negative";
  // Prime iterators to the first common key and valid position combo
  NextKey();
}

uint64_t ProximityIterator::FieldMask() const { return field_mask_; }

bool ProximityIterator::DoneKeys() const {
  for (auto& c : iters_) {
    if (c->DoneKeys()) {
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
    for (auto& c : iters_) {
      if (!c->DoneKeys() && c->CurrentKey() == current_key_) {
        c->NextKey();
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
    for (auto& c : iters_) {
      if (!c->DoneKeys() && c->CurrentKey() == current_key_) {
        c->NextKey();
      }
    }
  }
  current_key_ = nullptr;
  return false;
}

bool ProximityIterator::DonePositions() const {
  for (auto& c : iters_) {
    if (c->DonePositions()) {
      return true;
    }
  }
  return false;
}

std::pair<uint32_t, uint32_t> ProximityIterator::CurrentPosition() const {
  CHECK(current_start_pos_.has_value() && current_end_pos_.has_value());
  return std::make_pair(current_start_pos_.value(), current_end_pos_.value());
}

std::optional<size_t> ProximityIterator::FindViolatingIterator(
    const std::vector<std::pair<uint32_t, uint32_t>>& positions) const {
  const size_t n = positions.size();
  if (in_order_) {
    for (size_t i = 0; i < n - 1; ++i) {
      // Check overlap / ordering violations.
      if (positions[i].second >= positions[i + 1].first) {
        return i + 1;
      }
      // Check slop violations.
      if (slop_ >= 0 &&
          positions[i + 1].first - positions[i].second - 1 > slop_) {
        return i;
      }
    }
    return std::nullopt;
  }
  // For unordered, use an index mapping to help validate constraints.
  std::vector<std::pair<uint32_t, size_t>> pos_with_idx;
  for (size_t i = 0; i < n; ++i) {
    pos_with_idx.emplace_back(positions[i].first, i);
  }
  std::sort(pos_with_idx.begin(), pos_with_idx.end());
  for (size_t i = 0; i < n - 1; ++i) {
    size_t curr_idx = pos_with_idx[i].second;
    size_t next_idx = pos_with_idx[i + 1].second;
    if (positions[curr_idx].second >= positions[next_idx].first) {
      return next_idx;
    }
    if (slop_ >= 0 &&
        positions[next_idx].first - positions[curr_idx].second - 1 > slop_) {
      return curr_idx;
    }
  }
  return std::nullopt;
}

bool ProximityIterator::NextPosition() {
  const size_t n = iters_.size();
  std::vector<std::pair<uint32_t, uint32_t>> positions(n);
  bool should_advance = current_start_pos_.has_value();
  while (!DonePositions()) {
    for (size_t i = 0; i < n; ++i) {
      positions[i] = iters_[i]->CurrentPosition();
    }
    if (should_advance) {
      should_advance = false;
      if (auto iter_to_advance = FindViolatingIterator(positions)) {
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
    if (!FindViolatingIterator(positions).has_value()) {
      current_start_pos_ = positions[0].first;
      current_end_pos_ = positions[n - 1].second;
      return true;
    }
    should_advance = true;
  }
  current_start_pos_ = std::nullopt;
  current_end_pos_ = std::nullopt;
  return false;
}

}  // namespace valkey_search::indexes::text
