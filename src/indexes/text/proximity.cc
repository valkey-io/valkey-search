#include "proximity.h"

namespace valkey_search::indexes::text {

ProximityIterator::ProximityIterator(
    std::vector<std::unique_ptr<TextIterator>>&& iters, size_t slop,
    bool in_order, FieldMaskPredicate field_mask,
    const InternedStringSet* untracked_keys)
    : iters_(std::move(iters)),
      done_(false),
      slop_(slop),
      in_order_(in_order),
      untracked_keys_(untracked_keys),
      current_key_(nullptr),
      current_start_pos_(std::nullopt),
      current_end_pos_(std::nullopt),
      field_mask_(field_mask) {
  if (iters_.empty()) {
    done_ = true;
    return;
  }
  // Prime iterators to the first common key and valid position combo
  NextKey();
}

uint64_t ProximityIterator::FieldMask() const { return field_mask_; }

bool ProximityIterator::DoneKeys() const {
  if (done_) {
    return true;
  }
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
      //    Exit, if no valid position combination key is found.
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
    while (!iter->DoneKeys() && iter->CurrentKey()->Str() < target_key->Str()) {
      iter->NextKey();
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
  if (done_) {
    return true;
  }
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

bool ProximityIterator::NextPosition() {
  const size_t n = iters_.size();
  std::vector<std::pair<uint32_t, uint32_t>> positions(n);
  auto advance_smallest = [&]() -> void {
    size_t advance_idx = 0;
    for (size_t i = 1; i < n; ++i) {
      if (positions[i].first < positions[advance_idx].first) {
        advance_idx = i;
      }
    }
    iters_[advance_idx]->NextPosition();
  };
  bool should_advance = false;
  // On a second call, we advance.
  if (current_start_pos_.has_value() && current_end_pos_.has_value()) {
    should_advance = true;
  }
  while (!DonePositions()) {
    // Collect current positions (start, end) of the text iterators.
    for (size_t i = 0; i < n; ++i) {
      positions[i] = iters_[i]->CurrentPosition();
    }
    if (should_advance) {
      should_advance = false;
      advance_smallest();
    }
    bool valid = true;
    // Check if the current combination of positions satisfies the proximity
    // constraints (order/slop) using start and end positions.
    if (valid) {
      for (size_t i = 0; i + 1 < n && valid; ++i) {
        if (in_order_ && positions[i].first >= positions[i + 1].first) {
          valid = false;
        } else if (positions[i + 1].first - positions[i].second - 1 > slop_) {
          valid = false;
        }
      }
    }
    if (valid) {
      current_start_pos_ = positions[0].first;
      current_end_pos_ = positions[n - 1].second;
      return true;
    }
    advance_smallest();
  }
  current_start_pos_ = std::nullopt;
  current_end_pos_ = std::nullopt;
  return false;
}

}  // namespace valkey_search::indexes::text
