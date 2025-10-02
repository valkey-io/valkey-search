#include "proximity.h"

namespace valkey_search::indexes::text {

ProximityIterator::ProximityIterator(
    std::vector<std::unique_ptr<TextIterator>>&& iters, size_t slop,
    bool in_order, FieldMaskPredicate field_mask,
    const InternedStringSet* untracked_keys)
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

// bool ProximityIterator::IsValidPositionCombination(
//     const std::vector<std::pair<uint32_t, uint32_t>>& positions) const {
//   const size_t n = positions.size();
//   // Check for overlapping positions (span), which includes the same position.
//   for (size_t i = 0; i < n; ++i) {
//     for (size_t j = i + 1; j < n; ++j) {
//       if (positions[i].first <= positions[j].second && 
//           positions[j].first <= positions[i].second) {
//         return false;
//       }
//     }
//   }
//   const auto& pos = in_order_ ? positions : [&]() {
//     auto sorted = positions;
//     std::sort(sorted.begin(), sorted.end());
//     return sorted;
//   }();
//   for (size_t i = 0; i < n - 1; ++i) {
//     if (in_order_ && pos[i].second >= pos[i + 1].first) return false;
//     if (pos[i + 1].first - pos[i].second - 1 > slop_) return false;
//   }
//   return true;
// }

// bool ProximityIterator::NextPosition() {
//   const size_t n = iters_.size();
//   std::vector<std::pair<uint32_t, uint32_t>> positions(n);
//   bool should_advance = current_start_pos_.has_value();
//   while (!DonePositions()) {
//     // Collect current positions
//     for (size_t i = 0; i < n; ++i) {
//       positions[i] = iters_[i]->CurrentPosition();
//     }
//     if (should_advance) {
//       should_advance = false;
//       // Inline advance logic
//       std::optional<size_t> iter_to_advance;
//       // Check for overlapping positions and the same position case.
//       for (size_t i = 0; i < n && !iter_to_advance; ++i) {
//         for (size_t j = i + 1; j < n; ++j) {
//           if (positions[i].first <= positions[j].second && 
//               positions[j].first <= positions[i].second) {
//             iter_to_advance = j;
//             break;
//           }
//         }
//       }
//       // Check order violations
//       if (!iter_to_advance && in_order_) {
//         for (size_t i = 0; i < n - 1; ++i) {
//           if (positions[i].second >= positions[i + 1].first) {
//             iter_to_advance = i + 1;
//             break;
//           }
//         }
//       }
//       // Check slop violations
//       if (!iter_to_advance && slop_ >= 0) {
//         // Note: We should avoid ordering if inorder_
//         std::vector<std::pair<uint32_t, size_t>> sorted_pos;
//         for (size_t i = 0; i < n; ++i) {
//           sorted_pos.emplace_back(positions[i].first, i);
//         }
//         std::sort(sorted_pos.begin(), sorted_pos.end());
//         for (size_t i = 0; i < n - 1; ++i) {
//           size_t curr_idx = sorted_pos[i].second;
//           size_t next_idx = sorted_pos[i + 1].second;
//           if (positions[next_idx].first - positions[curr_idx].second - 1 > slop_) {
//             iter_to_advance = curr_idx;
//             break;
//           }
//         }
//       }
//       if (iter_to_advance) {
//         iters_[*iter_to_advance]->NextPosition();
//       } else {
//         // No violations, advance first non-done iterator
//         for (size_t i = 0; i < n; ++i) {
//           if (!iters_[i]->DonePositions()) {
//             iters_[i]->NextPosition();
//             break;
//           }
//         }
//       }
//       continue;
//     }
//     if (IsValidPositionCombination(positions)) {
//       current_start_pos_ = positions[0].first;
//       current_end_pos_ = positions[n - 1].second;
//       return true;
//     }
//     should_advance = true;
//   }
//   current_start_pos_ = std::nullopt;
//   current_end_pos_ = std::nullopt;
//   return false;
// }






// Working code

// bool ProximityIterator::IsValidPositionCombination(
//     const std::vector<std::pair<uint32_t, uint32_t>>& positions) const {
//   const size_t n = positions.size();
//   // Get the appropriate ordering: original for in_order, sorted for unordered
//   const auto& pos = in_order_ ? positions : [&]() {
//     auto sorted = positions;
//     std::sort(sorted.begin(), sorted.end());
//     return sorted;
//   }();
//   // Single loop to check both overlap/order and slop constraints
//   for (size_t i = 0; i < n - 1; ++i) {
//     if (pos[i].second >= pos[i + 1].first) return false; // Overlap/order violation
//     if (slop_ >= 0 && pos[i + 1].first - pos[i].second - 1 > slop_) return false; // Slop violation
//   }
//   return true;
// }

// bool ProximityIterator::NextPosition() {
//   const size_t n = iters_.size();
//   std::vector<std::pair<uint32_t, uint32_t>> positions(n);
//   bool should_advance = current_start_pos_.has_value();
//   while (!DonePositions()) {
//     // Collect current positions
//     for (size_t i = 0; i < n; ++i) {
//       positions[i] = iters_[i]->CurrentPosition();
//     }
//     if (should_advance) {
//       should_advance = false;
//       std::optional<size_t> iter_to_advance;
//       // Get appropriate ordering and find violations
//       std::vector<std::pair<uint32_t, size_t>> pos_with_idx;
//       for (size_t i = 0; i < n; ++i) {
//         pos_with_idx.emplace_back(positions[i].first, i);
//       }
//       if (!in_order_) {
//         std::sort(pos_with_idx.begin(), pos_with_idx.end());
//       }
//       // Check violations in single pass
//       for (size_t i = 0; i < n - 1 && !iter_to_advance; ++i) {
//         size_t curr_idx = pos_with_idx[i].second;
//         size_t next_idx = pos_with_idx[i + 1].second;
//         // Check overlap/order violation
//         if (positions[curr_idx].second >= positions[next_idx].first) {
//           iter_to_advance = next_idx;
//           break;
//         }
//         // Check slop violation
//         if (slop_ >= 0 && positions[next_idx].first - positions[curr_idx].second - 1 > slop_) {
//           iter_to_advance = curr_idx;
//           break;
//         }
//       }
//       if (iter_to_advance) {
//         iters_[*iter_to_advance]->NextPosition();
//       } else {
//         // No violations, advance first non-done iterator
//         for (size_t i = 0; i < n; ++i) {
//           if (!iters_[i]->DonePositions()) {
//             iters_[i]->NextPosition();
//             break;
//           }
//         }
//       }
//       continue;
//     }
//     if (IsValidPositionCombination(positions)) {
//       current_start_pos_ = positions[0].first;
//       current_end_pos_ = positions[n - 1].second;
//       return true;
//     }
//     should_advance = true;
//   }
//   current_start_pos_ = std::nullopt;
//   current_end_pos_ = std::nullopt;
//   return false;
// }


// Helper that returns violation info
// std::optional<size_t> ProximityIterator::FindViolatingIterator(
//     const std::vector<std::pair<uint32_t, uint32_t>>& positions) const {
//   const size_t n = positions.size();
//   // Get appropriate ordering and find violations
//   std::vector<std::pair<uint32_t, size_t>> pos_with_idx;
//   for (size_t i = 0; i < n; ++i) {
//     pos_with_idx.emplace_back(positions[i].first, i);
//   }
//   if (!in_order_) {
//     std::sort(pos_with_idx.begin(), pos_with_idx.end());
//   }
//   // Check violations in single pass
//   for (size_t i = 0; i < n - 1; ++i) {
//     size_t curr_idx = pos_with_idx[i].second;
//     size_t next_idx = pos_with_idx[i + 1].second;
//     // Check overlap/order violation
//     if (positions[curr_idx].second >= positions[next_idx].first) {
//       return next_idx;
//     }
//     // Check slop violation
//     if (slop_ >= 0 && positions[next_idx].first - positions[curr_idx].second - 1 > slop_) {
//       return curr_idx;
//     }
//   }
//   return std::nullopt; // No violations
// }

std::optional<size_t> ProximityIterator::FindViolatingIterator(
    const std::vector<std::pair<uint32_t, uint32_t>>& positions) const {
  const size_t n = positions.size();
  if (in_order_) {
    // Direct check without any additional vectors - most efficient
    for (size_t i = 0; i < n - 1; ++i) {
      if (positions[i].second >= positions[i + 1].first) {
        return i + 1;
      }
      if (slop_ >= 0 && positions[i + 1].first - positions[i].second - 1 > slop_) {
        return i;
      }
    }
    return std::nullopt;
  }
  // For unordered, we need the index mapping (unavoidable)
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
    if (slop_ >= 0 && positions[next_idx].first - positions[curr_idx].second - 1 > slop_) {
      return curr_idx;
    }
  }
  return std::nullopt;
}

bool ProximityIterator::IsValidPositionCombination(
    const std::vector<std::pair<uint32_t, uint32_t>>& positions) const {
  return !FindViolatingIterator(positions).has_value();
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
    if (IsValidPositionCombination(positions)) {
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

// Questions for proximity:
// 0) What is the right order to test positional constraints?
// 1) Use first and second correctly for all positional constraint checks.
// 2) Validate the slop detection logic is correct
// 3) are we really correctly able to iterator through all positional combinations?
// 4) Is sorting the positions the right approach in case of inorder_=false?
// 5) Try to avoid any double loop
// 6) Try to reduce duplicate code.

// Question for wildcard.cc
// 1) It is the same as term.cc except for init. Can we reuse?
// 2) Is infix and suffix going to be the same and work with no changes as wildcard.cc?